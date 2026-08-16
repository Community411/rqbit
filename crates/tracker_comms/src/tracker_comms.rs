use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use futures::StreamExt;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::tracker_comms_http;
use crate::tracker_comms_http::TrackerRequestEvent;
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

/// Floor under the sleep between two announces. It only guards against a
/// tracker answering an interval of zero, which would turn the announce task
/// into a busy loop.
const MIN_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(1);
/// Backoff after a failed announce: doubling from the minimum, capped, with
/// jitter, and never giving up. A private tracker answers a failure while it
/// does not know a torrent yet, and the torrent must survive that.
const ERROR_BACKOFF_MIN: Duration = Duration::from_secs(10);
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(600);
/// Upper bound applied to a `Retry-After` a tracker sends, so a hostile or
/// mistaken value cannot park a torrent for a day.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    // This MUST be set as trackers don't work with 0 port.
    announce_port: u16,
    reqwest_client: reqwest::Client,
    udp_client: UdpTrackerClient,
    key: u32,
    trackers: Vec<SupportedTracker>,
    /// BEP 12 tiers, as indices into `trackers`, each tier already shuffled.
    /// One announce task per tier; inside a tier the trackers are fallbacks
    /// of each other, tried in order until one answers.
    tiers: Vec<Vec<usize>>,
    /// Which trackers have answered an announce at least once this session.
    /// A `stopped` only goes to those: a tracker that never registered this
    /// peer has nothing to forget.
    contacted: Vec<AtomicBool>,
    statuses: RwLock<Vec<TrackerStatus>>,
    reannounce_tx: watch::Sender<u64>,
    stopped_sent: AtomicBool,
}

#[derive(Default)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

#[derive(Default)]
pub struct TrackerCommsStats {
    pub uploaded_bytes: u64,
    /// Bytes actually fetched from peers this session: the announce
    /// `downloaded` value. Bytes verified on disk do not belong here: a hash
    /// check of preexisting data downloads nothing, and announcing it as
    /// `downloaded` charges the account for a transfer that never happened
    /// on a ratio-enforcing tracker.
    pub downloaded_bytes: u64,
    /// Bytes verified on disk. Drives `left` (BEP-3 wants remaining bytes,
    /// which fetched bytes are wrong for whenever data preexists) and the
    /// completion transition, never the `downloaded` value.
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        let total = self.total_bytes;
        let progress = self.progress_bytes;
        if total >= progress {
            return total - progress;
        }
        0
    }

    pub fn is_completed(&self) -> bool {
        self.progress_bytes >= self.total_bytes
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

/// What the announce task last managed to do with one tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackerState {
    /// No announce was attempted yet.
    NotContacted,
    /// An announce is in flight.
    Updating,
    /// The last announce was answered.
    Working,
    /// The last announce failed; the task keeps retrying.
    Error,
}

/// The state of one tracker, as plain data an embedder can display.
#[derive(Clone, Debug)]
pub struct TrackerStatus {
    /// Scheme, host and port of the tracker. The path and the query are
    /// elided on purpose: a private tracker carries the account's passkey in
    /// the announce path, and this value is meant to be displayed and logged.
    pub url: String,
    /// The BEP 12 tier this tracker belongs to, zero-based. Trackers of one
    /// tier are fallbacks of each other: one of them is announced to per
    /// cycle, the others stay `NotContacted` until it fails.
    pub tier: usize,
    pub state: TrackerState,
    pub last_announce: Option<SystemTime>,
    pub next_announce: Option<SystemTime>,
    /// The tracker's own words when there are any: a bencoded failure reason,
    /// a warning message, or the transport error of the last attempt.
    pub message: Option<String>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub downloaded: Option<u64>,
}

impl TrackerStatus {
    fn new(url: String, tier: usize) -> Self {
        Self {
            url,
            tier,
            state: TrackerState::NotContacted,
            last_announce: None,
            next_announce: None,
            message: None,
            seeders: None,
            leechers: None,
            downloaded: None,
        }
    }
}

/// A tracker URL stripped of its path and query.
///
/// The passkey of a private tracker lives in the announce path, so the full
/// URL must never reach a log line, a span field or a status snapshot.
pub fn redacted_tracker_url(url: &Url) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    s.push_str(url.scheme());
    s.push_str("://");
    if let Some(host) = url.host_str() {
        s.push_str(host);
    }
    if let Some(port) = url.port() {
        let _ = write!(s, ":{port}");
    }
    s
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl SupportedTracker {
    fn url(&self) -> &Url {
        match self {
            SupportedTracker::Udp(u) => u,
            SupportedTracker::Http(u) => u,
        }
    }
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&redacted_tracker_url(self.url()))
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

/// Which event the next announce to one tracker carries.
///
/// BEP-3 gives `started` to the first announce of a session and `completed`
/// to the announce that reports the download finished, and says a client that
/// was already complete when it was added must not send `completed` at all.
struct AnnounceEvents {
    /// Sticky: an event survives a failed announce, so a `completed` the
    /// tracker refused is retried instead of being dropped, and no later
    /// announce replaces it in the meantime.
    pending: Option<TrackerRequestEvent>,
    seen_incomplete: bool,
    completed_sent: bool,
}

impl AnnounceEvents {
    fn new() -> Self {
        Self {
            pending: Some(TrackerRequestEvent::Started),
            seen_incomplete: false,
            completed_sent: false,
        }
    }

    fn next_event(&mut self, stats: &TrackerCommsStats) -> Option<TrackerRequestEvent> {
        // "Complete" is read as "no bytes left" rather than through
        // is_completed(), whose zeroed default reads as complete: a torrent
        // whose size is not known yet must not be announced as a seed.
        let left = stats.get_left_to_download_bytes();
        if left > 0 {
            self.seen_incomplete = true;
        }
        if self.pending.is_none() && !self.completed_sent && self.seen_incomplete && left == 0 {
            self.pending = Some(TrackerRequestEvent::Completed);
        }
        self.pending
    }

    fn on_success(&mut self) {
        if matches!(self.pending, Some(TrackerRequestEvent::Completed)) {
            self.completed_sent = true;
        }
        self.pending = None;
    }
}

/// What an announce that the tracker answered told us.
struct AnnounceOk {
    interval: Duration,
    peers: Vec<SocketAddr>,
    seeders: Option<u64>,
    leechers: Option<u64>,
    downloaded: Option<u64>,
    message: Option<String>,
}

/// An announce that did not land, and how long to wait before the next try.
struct AnnounceFailure {
    message: String,
    retry_after: Option<Duration>,
}

impl AnnounceFailure {
    fn new(message: String) -> Self {
        Self {
            message,
            retry_after: None,
        }
    }
}

/// Bounded exponential backoff with jitter. The jitter keeps a fleet of
/// clients that lost the tracker at the same second from retrying in lockstep.
fn error_backoff(consecutive_errors: u32) -> Duration {
    let shift = consecutive_errors.saturating_sub(1).min(8);
    let delay = ERROR_BACKOFF_MIN
        .saturating_mul(1u32 << shift)
        .min(ERROR_BACKOFF_MAX);
    delay.mul_f64(0.5 + rand::random::<f64>() * 0.5)
}

/// `Retry-After` in its delta-seconds form. The HTTP-date form is ignored
/// deliberately: no tracker is known to send it, and parsing a date here
/// would pull a date library into this crate.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs).clamp(MIN_ANNOUNCE_INTERVAL, MAX_RETRY_AFTER))
}

async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let res = match host {
        url::Host::Domain(name) => {
            // Use the first IPv4 and the first IPv6 addresses only.

            let mut v4: Option<SocketAddrV4> = None;
            let mut v6: Option<SocketAddrV6> = None;
            for addr in tokio::net::lookup_host((name, port))
                .await
                .with_context(|| format!("error looking up hostname {name}"))?
            {
                match (v4, v6, addr) {
                    (None, _, SocketAddr::V4(addr)) => v4 = Some(addr),
                    (_, None, SocketAddr::V6(addr)) => v6 = Some(addr),
                    _ => continue,
                }
            }
            let res = match (v4, v6) {
                (Some(v4), Some(v6)) => UdpTrackerResolveResult::Two(v4, v6),
                (Some(v4), None) => UdpTrackerResolveResult::One(v4.into()),
                (None, Some(v6)) => UdpTrackerResolveResult::One(v6.into()),
                _ => anyhow::bail!("zero addresses returned looking up {name}"),
            };
            trace!(?res, "resolved");
            res
        }
        url::Host::Ipv4(addr) => UdpTrackerResolveResult::One((addr, port).into()),
        url::Host::Ipv6(addr) => UdpTrackerResolveResult::One((addr, port).into()),
    };
    Ok(res)
}

impl TrackerComms {
    /// `tiers` is the BEP 12 announce list: the trackers of one tier are
    /// fallbacks of each other and get one announce per cycle between them,
    /// every tier is announced to on its own schedule. A single tracker is a
    /// tier of one; a plain list of independent trackers is one tier each.
    // TODO: fix too many args
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        tiers: Vec<Vec<Url>>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
    ) -> Option<(BoxStream<'static, SocketAddr>, Arc<TrackerComms>)> {
        let mut trackers: Vec<SupportedTracker> = Vec::new();
        let mut tier_indices: Vec<usize> = Vec::new();
        let mut tier_of: Vec<Vec<usize>> = Vec::new();
        for tier in tiers {
            let mut members = Vec::new();
            for t in tier {
                let supported = match t.scheme() {
                    "http" | "https" => SupportedTracker::Http(t),
                    "udp" => SupportedTracker::Udp(t),
                    _ => {
                        debug!("unsupported tracker URL: {}", redacted_tracker_url(&t));
                        continue;
                    }
                };
                members.push(trackers.len());
                tier_indices.push(tier_of.len());
                trackers.push(supported);
            }
            if members.is_empty() {
                continue;
            }
            // BEP 12: a tier is shuffled once when first read, so the
            // trackers of one tier share the swarm instead of the first
            // listed one carrying every client.
            members.shuffle(&mut rand::rng());
            tier_of.push(members);
        }
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers, tiers = ?tier_of);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);
        let (reannounce_tx, _) = watch::channel(0u64);
        let statuses = trackers
            .iter()
            .zip(tier_indices)
            .map(|(t, tier)| TrackerStatus::new(redacted_tracker_url(t.url()), tier))
            .collect::<Vec<_>>();
        let contacted = trackers
            .iter()
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>();

        let comms = Arc::new(Self {
            info_hash,
            peer_id,
            stats,
            force_tracker_interval: force_interval,
            tx,
            announce_port,
            reqwest_client,
            udp_client,
            key: rand::random(),
            trackers,
            tiers: tier_of,
            contacted,
            statuses: RwLock::new(statuses),
            reannounce_tx,
            stopped_sent: AtomicBool::new(false),
        });

        let stream_comms = comms.clone();
        let s = async_stream::stream! {
            use futures::StreamExt;
            let comms = stream_comms;
            let mut futures = FuturesUnordered::new();
            for tier in 0..comms.tiers.len() {
                futures.push(comms.task_tier_monitor(tier))
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        if let Some(Err(e)) = e {
                            debug!("error: {e}");
                        }
                    }
                }
            }
        };

        Some((s.boxed(), comms))
    }

    /// Announce now instead of waiting for the next scheduled announce.
    ///
    /// Cuts the current sleep of every announce task of this torrent,
    /// including a backoff sleep after a failure.
    pub fn reannounce(&self) {
        self.reannounce_tx.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// The state of every tracker of this torrent, as of the last announce.
    pub fn tracker_statuses(&self) -> Vec<TrackerStatus> {
        self.statuses.read().clone()
    }

    /// Announce `stopped` to every tracker, once, giving up after `deadline`.
    ///
    /// It does not go through the announce tasks on purpose: those are dropped
    /// along with the peer stream the moment a torrent is paused, so the stop
    /// has to be an announce of its own to be sent at all.
    pub async fn announce_stopped(&self, deadline: Duration) {
        if self.stopped_sent.swap(true, Ordering::SeqCst) {
            return;
        }
        // Only the trackers that answered this session: a `stopped` to one
        // that never registered the peer is a request for nothing, and the
        // standby trackers of a tier are exactly those.
        let announces = self
            .trackers
            .iter()
            .enumerate()
            .filter(|(index, _)| self.contacted[*index].load(Ordering::SeqCst))
            .map(|(index, tracker)| {
                let span = debug_span!(
                    parent: None,
                    "announce_stopped",
                    tracker = %redacted_tracker_url(tracker.url()),
                    info_hash = ?self.info_hash
                );
                async move {
                    self.set_status_updating(index);
                    let mut udp_addrs = None;
                    match self
                        .announce_once(index, Some(TrackerRequestEvent::Stopped), &mut udp_addrs)
                        .await
                    {
                        Ok(ok) => self.set_status_ok(index, &ok, None),
                        Err(err) => {
                            debug!("error announcing stopped: {}", err.message);
                            self.set_status_error(index, err.message, None);
                        }
                    }
                }
                .instrument(span)
            });
        if tokio::time::timeout(deadline, futures::future::join_all(announces))
            .await
            .is_err()
        {
            debug!(?deadline, "gave up announcing stopped");
        }
    }

    fn with_status(&self, index: usize, f: impl FnOnce(&mut TrackerStatus)) {
        if let Some(s) = self.statuses.write().get_mut(index) {
            f(s)
        }
    }

    fn set_status_updating(&self, index: usize) {
        self.with_status(index, |s| s.state = TrackerState::Updating);
    }

    fn set_status_ok(&self, index: usize, ok: &AnnounceOk, sleep_for: Option<Duration>) {
        let now = SystemTime::now();
        self.with_status(index, |s| {
            s.state = TrackerState::Working;
            s.last_announce = Some(now);
            s.next_announce = sleep_for.map(|d| now + d);
            s.message = ok.message.clone();
            s.seeders = ok.seeders;
            s.leechers = ok.leechers;
            s.downloaded = ok.downloaded;
        });
    }

    fn set_status_error(&self, index: usize, message: String, retry_in: Option<Duration>) {
        let now = SystemTime::now();
        self.with_status(index, |s| {
            s.state = TrackerState::Error;
            s.last_announce = Some(now);
            s.next_announce = retry_in.map(|d| now + d);
            s.message = Some(message);
        });
    }

    /// Wait for the next announce, or until someone asks for one now.
    async fn sleep_or_reannounce(&self, sleep_for: Duration, rx: &mut watch::Receiver<u64>) {
        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            changed = rx.changed() => {
                if changed.is_err() {
                    // The sender lives in self, so this cannot happen while
                    // the task runs; sleep rather than spin if it ever does.
                    tokio::time::sleep(sleep_for).await;
                }
            }
        }
    }

    /// The announce loop of one tier.
    ///
    /// Each cycle walks the tier in order and stops at the first tracker
    /// that answers; that tracker moves to the front (BEP 12) and its
    /// interval sets the sleep. A tracker that fails is left in `Error` and
    /// the next one is tried in the same cycle, so a fallback takes over
    /// without waiting for a backoff. When the whole tier fails, the sleep
    /// is a bounded exponential backoff, or the `Retry-After` the last
    /// tracker sent.
    ///
    /// The BEP 3 events are per tier: `started` goes to a tracker the first
    /// time it is contacted (a fallback taking over mid-session included),
    /// `completed` exactly once per tier, on the transition.
    async fn task_tier_monitor(&self, tier: usize) -> anyhow::Result<()> {
        let mut order = self.tiers[tier].clone();
        let redacted = order
            .iter()
            .map(|i| redacted_tracker_url(self.trackers[*i].url()))
            .collect::<Vec<_>>();
        let span = debug_span!(
            parent: None,
            "tracker_tier",
            tier,
            trackers = ?redacted,
            info_hash = ?self.info_hash
        );
        async move {
            trace!("starting monitor");
            let mut events = AnnounceEvents::new();
            let mut consecutive_failures: u32 = 0;
            let mut reannounce_rx = self.reannounce_tx.subscribe();
            // Last resolved addresses per UDP tracker, reused while DNS
            // fails so a resolver hiccup does not silence a tracker that
            // was reachable a minute ago.
            let mut udp_addrs = vec![None; self.trackers.len()];

            loop {
                let tier_event = events.next_event(&self.stats.get());
                let mut answered: Option<Duration> = None;
                let mut last_failure: Option<AnnounceFailure> = None;
                let mut failed: Vec<usize> = Vec::new();

                for pos in 0..order.len() {
                    let index = order[pos];
                    // A tracker contacted for the first time gets `started`
                    // even when the tier already announced elsewhere: it has
                    // never heard of this peer.
                    let event = tier_event.or_else(|| {
                        (!self.contacted[index].load(Ordering::SeqCst))
                            .then_some(TrackerRequestEvent::Started)
                    });
                    self.set_status_updating(index);
                    match self
                        .announce_once(index, event, &mut udp_addrs[index])
                        .await
                    {
                        Ok(ok) => {
                            events.on_success();
                            self.contacted[index].store(true, Ordering::SeqCst);
                            consecutive_failures = 0;
                            let sleep_for = self.force_tracker_interval.unwrap_or(ok.interval);
                            self.set_status_ok(index, &ok, Some(sleep_for));
                            // The tracker that answers moves to the front of
                            // its tier, so the next cycle asks it first.
                            order[..=pos].rotate_right(1);
                            // An answer with no peers is not a dead swarm: a
                            // front proxy synthesises one while the tracker
                            // itself is down.
                            for peer in ok.peers {
                                if self.tx.send(peer).await.is_err() {
                                    return Ok(());
                                }
                            }
                            answered = Some(sleep_for);
                            break;
                        }
                        Err(err) => {
                            debug!(
                                tracker = %redacted_tracker_url(self.trackers[index].url()),
                                "error announcing to tracker: {}",
                                err.message
                            );
                            self.set_status_error(index, err.message.clone(), None);
                            failed.push(index);
                            last_failure = Some(err);
                        }
                    }
                }

                let sleep_for = match answered {
                    Some(d) => d,
                    None => {
                        consecutive_failures += 1;
                        let retry_in = last_failure
                            .and_then(|f| f.retry_after)
                            .unwrap_or_else(|| error_backoff(consecutive_failures));
                        debug!(?retry_in, "every tracker of the tier failed");
                        let next = SystemTime::now() + retry_in;
                        for index in failed {
                            self.with_status(index, |s| s.next_announce = Some(next));
                        }
                        retry_in
                    }
                };

                trace!(?sleep_for, "sleeping until the next announce");
                self.sleep_or_reannounce(sleep_for, &mut reannounce_rx)
                    .await;
            }
        }
        .instrument(span)
        .await
    }

    /// One announce to one tracker, whatever its transport.
    async fn announce_once(
        &self,
        index: usize,
        event: Option<TrackerRequestEvent>,
        udp_addrs: &mut Option<UdpTrackerResolveResult>,
    ) -> Result<AnnounceOk, AnnounceFailure> {
        match &self.trackers[index] {
            SupportedTracker::Http(url) => self.announce_http(url, event).await,
            SupportedTracker::Udp(url) => self.announce_udp_url(url, event, udp_addrs).await,
        }
    }

    async fn announce_http(
        &self,
        tracker_url: &Url,
        event: Option<TrackerRequestEvent>,
    ) -> Result<AnnounceOk, AnnounceFailure> {
        let stats = self.stats.get();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.peer_id,
            port: self.announce_port,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: None,
            key: Some(self.key),
            trackerid: None,
        };

        let mut url = tracker_url.clone();

        let mut queries = request.as_querystring();
        if let Some(url_query) = url.query() {
            queries.push_str(&format!("&{}", url_query));
        }
        url.set_query(Some(&queries));

        let response = self
            .reqwest_client
            .get(url)
            .send()
            .await
            .map_err(|e| AnnounceFailure::new(format!("error calling tracker: {e}")))?;

        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        if !status.is_success() {
            return Err(AnnounceFailure {
                message: format!("tracker responded with {status}"),
                retry_after,
            });
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| AnnounceFailure::new(format!("error reading tracker response: {e}")))?;

        // A private tracker answers 200 with a bencoded failure body for a
        // torrent it does not know, so the body decides, never the status
        // code. The reason becomes the tracker's status message and the task
        // backs off; the torrent is never dropped over it.
        if let Ok((error, _)) =
            bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(&bytes)
        {
            return Err(AnnounceFailure {
                message: format!("tracker failure: {}", error.failure_reason),
                retry_after,
            });
        }

        let response = bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(&bytes)
            .map_err(|e| {
                tracing::trace!("error deserializing TrackerResponse: {e:#}");
                AnnounceFailure {
                    message: format!("error parsing tracker response: {}", e.into_kind()),
                    retry_after,
                }
            })?
            .0;

        // BEP-3: `interval` is the announce period and `min interval`, when
        // the tracker sends one, is the shortest period it accepts.
        let interval_secs = response
            .interval
            .max(response.min_interval.unwrap_or_default());
        let interval = Duration::from_secs(interval_secs).max(MIN_ANNOUNCE_INTERVAL);

        Ok(AnnounceOk {
            interval,
            peers: response.iter_peers().collect(),
            seeders: response.complete,
            leechers: response.incomplete,
            downloaded: response.downloaded,
            message: response
                .warning_message
                .as_ref()
                .map(|w| format!("tracker warning: {w}")),
        })
    }

    /// One UDP announce: resolve the host, then announce, to both address
    /// families when the name has both. `cache` keeps the last resolution
    /// for the next call, and stands in when the resolver fails.
    async fn announce_udp_url(
        &self,
        url: &Url,
        event: Option<TrackerRequestEvent>,
        cache: &mut Option<UdpTrackerResolveResult>,
    ) -> Result<AnnounceOk, AnnounceFailure> {
        let host = url
            .host()
            .ok_or_else(|| AnnounceFailure::new("missing host".to_owned()))?;
        let port = url
            .port()
            .ok_or_else(|| AnnounceFailure::new("missing port".to_owned()))?;
        let addrs = match udp_tracker_to_socket_addrs(host.clone(), port)
            .instrument(trace_span!("resolve", ?host))
            .await
        {
            Ok(addrs) => {
                *cache = Some(addrs);
                addrs
            }
            Err(err) => match *cache {
                Some(addrs) => {
                    debug!("error resolving tracker, reusing the last addresses: {err:#}");
                    addrs
                }
                None => return Err(AnnounceFailure::new(format!("{err:#}"))),
            },
        };
        let result = match addrs {
            UdpTrackerResolveResult::One(addr) => {
                self.announce_udp(addr, event)
                    .instrument(trace_span!("udp request", ?addr))
                    .await
            }
            UdpTrackerResolveResult::Two(v4, v6) => {
                let (r4, r6) = tokio::join!(
                    self.announce_udp(v4.into(), event)
                        .instrument(trace_span!("udp request", addr=?v4)),
                    self.announce_udp(v6.into(), event)
                        .instrument(trace_span!("udp request", addr=?v6))
                );
                r4.or(r6)
            }
        };
        result.map_err(|e| AnnounceFailure::new(format!("{e:#}")))
    }

    async fn announce_udp(
        &self,
        addr: SocketAddr,
        event: Option<TrackerRequestEvent>,
    ) -> anyhow::Result<AnnounceOk> {
        use tracker_comms_udp::*;

        let stats = self.stats.get();
        let request = AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            uploaded: stats.uploaded_bytes,
            event: match event {
                None => EVENT_NONE,
                Some(TrackerRequestEvent::Started) => EVENT_STARTED,
                Some(TrackerRequestEvent::Completed) => EVENT_COMPLETED,
                Some(TrackerRequestEvent::Stopped) => EVENT_STOPPED,
            },
            key: self.key,
            port: self.announce_port,
        };

        match self.udp_client.announce(addr, request).await {
            Ok(response) => {
                trace!(len = response.addrs.len(), "received announce response");
                let interval = Duration::from_secs(response.interval.max(5) as u64);
                Ok(AnnounceOk {
                    interval,
                    peers: response.addrs,
                    seeders: Some(response.seeders as u64),
                    leechers: Some(response.leechers as u64),
                    downloaded: None,
                    message: None,
                })
            }
            Err(e) => {
                debug!(?addr, "error reading announce response: {e:#}");
                Err(e)
            }
        }
    }
}
