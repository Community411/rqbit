use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::bail;
use backon::ExponentialBuilder;
use backon::Retryable;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use parking_lot::RwLock;
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
    fn new(url: String) -> Self {
        Self {
            url,
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
    // TODO: fix too many args
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
    ) -> Option<(BoxStream<'static, SocketAddr>, Arc<TrackerComms>)> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!("unsupported tracker URL: {}", redacted_tracker_url(&t));
                    None
                }
            })
            .collect::<Vec<_>>();
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);
        let (reannounce_tx, _) = watch::channel(0u64);
        let statuses = trackers
            .iter()
            .map(|t| TrackerStatus::new(redacted_tracker_url(t.url())))
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
            statuses: RwLock::new(statuses),
            reannounce_tx,
            stopped_sent: AtomicBool::new(false),
        });

        let stream_comms = comms.clone();
        let s = async_stream::stream! {
            use futures::StreamExt;
            let comms = stream_comms;
            let mut futures = FuturesUnordered::new();
            for (index, tracker) in comms.trackers.iter().enumerate() {
                futures.push(comms.add_tracker(index, tracker))
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
        let announces = self.trackers.iter().enumerate().map(|(index, tracker)| {
            let span = debug_span!(
                parent: None,
                "announce_stopped",
                tracker = %redacted_tracker_url(tracker.url()),
                info_hash = ?self.info_hash
            );
            async move {
                match tracker {
                    SupportedTracker::Http(url) => {
                        self.set_status_updating(index);
                        match self
                            .announce_http(url, Some(TrackerRequestEvent::Stopped))
                            .await
                        {
                            Ok(ok) => self.set_status_ok(index, &ok, None),
                            Err(err) => {
                                debug!("error announcing stopped: {}", err.message);
                                self.set_status_error(index, err.message, None);
                            }
                        }
                    }
                    SupportedTracker::Udp(url) => {
                        if let Err(e) = self
                            .announce_udp_to_url(url, Some(TrackerRequestEvent::Stopped))
                            .await
                        {
                            debug!("error announcing stopped: {e:#}");
                        }
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

    fn add_tracker(
        &self,
        index: usize,
        url: &SupportedTracker,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        let redacted = redacted_tracker_url(url.url());
        match url {
            SupportedTracker::Udp(url) => {
                let span = debug_span!(parent: None, "udp_tracker", tracker = %redacted, info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(index, url.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = debug_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %redacted,
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(index, url.clone())
                    .instrument(span)
                    .left_future()
            }
        }
    }

    async fn task_single_tracker_monitor_http(
        &self,
        index: usize,
        tracker_url: Url,
    ) -> anyhow::Result<()> {
        trace!("starting monitor");
        let mut events = AnnounceEvents::new();
        let mut consecutive_errors: u32 = 0;
        let mut reannounce_rx = self.reannounce_tx.subscribe();

        loop {
            let event = events.next_event(&self.stats.get());
            self.set_status_updating(index);

            let sleep_for = match self.announce_http(&tracker_url, event).await {
                Ok(ok) => {
                    events.on_success();
                    consecutive_errors = 0;
                    let sleep_for = self.force_tracker_interval.unwrap_or(ok.interval);
                    self.set_status_ok(index, &ok, Some(sleep_for));
                    // An answer with no peers is not a dead swarm: a front
                    // proxy synthesises one while the tracker itself is down.
                    for peer in ok.peers {
                        if self.tx.send(peer).await.is_err() {
                            return Ok(());
                        }
                    }
                    sleep_for
                }
                Err(err) => {
                    consecutive_errors += 1;
                    let retry_in = err
                        .retry_after
                        .unwrap_or_else(|| error_backoff(consecutive_errors));
                    debug!(?retry_in, "error announcing to tracker: {}", err.message);
                    self.set_status_error(index, err.message, Some(retry_in));
                    retry_in
                }
            };

            trace!(?sleep_for, "sleeping until the next announce");
            self.sleep_or_reannounce(sleep_for, &mut reannounce_rx)
                .await;
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

    async fn task_single_tracker_monitor_udp(&self, index: usize, url: Url) -> anyhow::Result<()> {
        if url.scheme() != "udp" {
            bail!("expected UDP scheme");
        }
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );

        let mut events = AnnounceEvents::new();
        let mut reannounce_rx = self.reannounce_tx.subscribe();
        let mut sleep_interval: Option<Duration> = None;
        let mut prev_addrs: Option<UdpTrackerResolveResult> = None;
        loop {
            if let Some(i) = sleep_interval {
                trace!(interval=?sleep_interval, "sleeping");
                self.sleep_or_reannounce(i, &mut reannounce_rx).await;
            }

            // This should retry forever until the addrs are resolved.
            let addrs = (async || {
                udp_tracker_to_socket_addrs(host.clone(), port)
                    .instrument(trace_span!("resolve", ?host))
                    .await
                    .or_else(|err| prev_addrs.ok_or(err))
            })
            .retry(
                ExponentialBuilder::new()
                    .without_max_times()
                    .with_max_delay(Duration::from_secs(60))
                    .with_jitter(),
            )
            .notify(|err, retry| debug!(retry_in=?retry, "error resolving tracker: {err:#}"))
            .await
            .context("this shouldn't happen: failed resolving tracker addrs")?;

            prev_addrs = Some(addrs);

            let event = events.next_event(&self.stats.get());
            self.set_status_updating(index);

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

            match result {
                Ok(ok) => {
                    events.on_success();
                    let sleep_for = self.force_tracker_interval.unwrap_or(ok.interval);
                    self.set_status_ok(index, &ok, Some(sleep_for));
                    sleep_interval = Some(sleep_for);
                    for peer in ok.peers {
                        if self.tx.send(peer).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let retry_in = sleep_interval.unwrap_or(Duration::from_secs(60));
                    debug!(?retry_in, "error announcing to tracker: {e:#}");
                    self.set_status_error(index, format!("{e:#}"), Some(retry_in));
                    sleep_interval = Some(retry_in);
                }
            }
        }
    }

    async fn announce_udp_to_url(
        &self,
        url: &Url,
        event: Option<TrackerRequestEvent>,
    ) -> anyhow::Result<()> {
        let host = url.host().context("missing host")?;
        let port = url.port().context("missing port")?;
        match udp_tracker_to_socket_addrs(host, port).await? {
            UdpTrackerResolveResult::One(addr) => {
                self.announce_udp(addr, event).await?;
            }
            UdpTrackerResolveResult::Two(v4, v6) => {
                let (r4, r6) = tokio::join!(
                    self.announce_udp(v4.into(), event),
                    self.announce_udp(v6.into(), event)
                );
                r4.or(r6)?;
            }
        }
        Ok(())
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
