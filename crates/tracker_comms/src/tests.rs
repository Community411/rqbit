//! Announce behaviour against a fake HTTP tracker.
//!
//! The tracker is a plain TCP listener speaking just enough HTTP/1.1 to record
//! every announce and answer a scripted reply, which is what lets these tests
//! assert what goes on the wire (the event, `left`) rather than what the code
//! meant to send.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::TrackerComms;
use crate::TrackerState;
use crate::UdpTrackerClient;
use crate::tracker_comms::TorrentStatsProvider;
use crate::tracker_comms::TrackerCommsStats;

/// One recorded announce, as the tracker saw it.
#[derive(Clone, Debug)]
struct Announce {
    query: HashMap<String, String>,
    at: Instant,
}

impl Announce {
    fn event(&self) -> Option<&str> {
        self.query.get("event").map(|s| s.as_str())
    }

    fn left(&self) -> u64 {
        self.query
            .get("left")
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }
}

/// What the fake tracker answers to one announce.
struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: "200 OK",
            headers: Vec::new(),
            body: body.into(),
        }
    }

    fn status(status: &'static str) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }
}

/// A bencoded answer with no peers and the given intervals.
fn body_empty(interval: u64, min_interval: Option<u64>) -> Vec<u8> {
    let mut s = format!("d8:completei3e10:downloadedi7e10:incompletei1e8:intervali{interval}e");
    if let Some(m) = min_interval {
        s.push_str(&format!("12:min intervali{m}e"));
    }
    s.push_str("5:peers0:e");
    s.into_bytes()
}

/// A bencoded answer carrying one compact peer, 127.0.0.1:6881.
fn body_one_peer(interval: u64) -> Vec<u8> {
    let mut v =
        format!("d8:completei3e10:incompletei1e8:intervali{interval}e5:peers6:").into_bytes();
    v.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
    v.extend_from_slice(b"e");
    v
}

fn body_failure(reason: &str) -> Vec<u8> {
    format!("d14:failure reason{}:{}e", reason.len(), reason).into_bytes()
}

struct FakeTracker {
    url: Url,
    announces: Arc<Mutex<Vec<Announce>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeTracker {
    /// `reply` is called with the zero-based index of the announce.
    async fn start(reply: impl Fn(usize) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A path and a query of its own: the announce URL of a private tracker
        // carries the passkey there, and the client must preserve both.
        let url = Url::parse(&format!("http://{addr}/announce/passkey?tid=7")).unwrap();
        let announces = Arc::new(Mutex::new(Vec::new()));

        let task = tokio::spawn({
            let announces = announces.clone();
            async move {
                loop {
                    let (mut sock, _) = match listener.accept().await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    // The request has no body, so the head is the whole of it.
                    let target = loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..end]).to_string();
                            break head.split_whitespace().nth(1).unwrap_or("").to_string();
                        }
                    };

                    let query = target
                        .split_once('?')
                        .map(|(_, q)| q)
                        .unwrap_or("")
                        .split('&')
                        .filter_map(|kv| kv.split_once('='))
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect::<HashMap<_, _>>();

                    let index = {
                        let mut g = announces.lock();
                        g.push(Announce {
                            query,
                            at: Instant::now(),
                        });
                        g.len() - 1
                    };

                    let reply = reply(index);
                    let mut out = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                        reply.status,
                        reply.body.len()
                    );
                    for (name, value) in &reply.headers {
                        out.push_str(&format!("{name}: {value}\r\n"));
                    }
                    out.push_str("\r\n");
                    let mut out = out.into_bytes();
                    out.extend_from_slice(&reply.body);
                    let _ = sock.write_all(&out).await;
                    let _ = sock.flush().await;
                }
            }
        });

        Self {
            url,
            announces,
            task,
        }
    }

    fn announces(&self) -> Vec<Announce> {
        self.announces.lock().clone()
    }

    fn count(&self) -> usize {
        self.announces.lock().len()
    }

    async fn wait_for(&self, n: usize, within: Duration) {
        let deadline = Instant::now() + within;
        while self.count() < n {
            assert!(
                Instant::now() < deadline,
                "only {} announces after {within:?}, wanted {n}",
                self.count()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A stats provider the test drives.
#[derive(Clone, Default)]
struct FakeStats {
    inner: Arc<Mutex<(u64, u64)>>,
}

impl FakeStats {
    fn new(total: u64, downloaded: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new((total, downloaded))),
        }
    }

    fn set_downloaded(&self, downloaded: u64) {
        self.inner.lock().1 = downloaded;
    }
}

impl TorrentStatsProvider for FakeStats {
    fn get(&self) -> TrackerCommsStats {
        let (total, downloaded) = *self.inner.lock();
        TrackerCommsStats {
            uploaded_bytes: 0,
            downloaded_bytes: downloaded,
            total_bytes: total,
            torrent_state: crate::TrackerCommsStatsState::Live,
        }
    }
}

struct Running {
    comms: Arc<TrackerComms>,
    drain: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

async fn run(tracker: &FakeTracker, stats: FakeStats) -> Running {
    use futures::StreamExt;

    let udp = UdpTrackerClient::new(CancellationToken::new(), None)
        .await
        .unwrap();
    let (mut peer_rx, comms) = TrackerComms::start(
        librqbit_core::hash_id::Id20::new([1u8; 20]),
        librqbit_core::hash_id::Id20::new([2u8; 20]),
        [tracker.url.clone()].into_iter().collect(),
        Box::new(stats),
        None,
        6881,
        reqwest::Client::new(),
        udp,
    )
    .unwrap();

    // The announce tasks live inside the peer stream, so something has to poll
    // it for them to run at all.
    let drain = tokio::spawn(async move { while peer_rx.next().await.is_some() {} });
    Running { comms, drain }
}

#[tokio::test]
async fn started_is_first_then_no_event() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_one_peer(1))).await;
    let _running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(2, Duration::from_secs(5)).await;
    let announces = tracker.announces();
    assert_eq!(announces[0].event(), Some("started"));
    assert_eq!(announces[0].left(), 1000);
    assert_eq!(announces[1].event(), None);
    // The tracker's own query is kept beside the announce parameters.
    assert_eq!(announces[0].query.get("tid").map(|s| s.as_str()), Some("7"));
}

#[tokio::test]
async fn sleeps_on_interval_with_min_interval_as_a_floor() {
    // interval below "min interval": BEP-3 says the shortest period the
    // tracker accepts wins, so the gap must be the larger of the two.
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(1, Some(2)))).await;
    let _running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(2, Duration::from_secs(6)).await;
    let announces = tracker.announces();
    let gap = announces[1].at.duration_since(announces[0].at);
    assert!(gap >= Duration::from_millis(1800), "gap was {gap:?}");
}

#[tokio::test]
async fn completed_is_sent_once_on_the_transition() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(600, None))).await;
    let stats = FakeStats::new(1000, 0);
    let running = run(&tracker, stats.clone()).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    stats.set_downloaded(1000);
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(5)).await;
    running.comms.reannounce();
    tracker.wait_for(3, Duration::from_secs(5)).await;

    let announces = tracker.announces();
    assert_eq!(announces[0].event(), Some("started"));
    assert_eq!(announces[1].event(), Some("completed"));
    assert_eq!(announces[1].left(), 0);
    assert_eq!(announces[2].event(), None);
}

#[tokio::test]
async fn completed_is_never_sent_for_a_torrent_complete_at_start() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(600, None))).await;
    // Already a seed when it was added: BEP-3 forbids "completed" here.
    let running = run(&tracker, FakeStats::new(1000, 1000)).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(5)).await;
    running.comms.reannounce();
    tracker.wait_for(3, Duration::from_secs(5)).await;

    let announces = tracker.announces();
    assert_eq!(announces[0].event(), Some("started"));
    assert_eq!(announces[0].left(), 0);
    assert!(
        announces[1..].iter().all(|a| a.event().is_none()),
        "unexpected event: {:?}",
        announces.iter().map(|a| a.event()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn completed_is_retried_through_a_502() {
    let tracker = FakeTracker::start(|i| match i {
        // The proxy in front of a tracker that is down answers a synthesised
        // success to everything except "completed", which it fails with a 502.
        1 => Reply::status("502 Bad Gateway"),
        _ => Reply::ok(body_empty(600, None)),
    })
    .await;
    let stats = FakeStats::new(1000, 0);
    let running = run(&tracker, stats.clone()).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    stats.set_downloaded(1000);
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(5)).await;

    // The retry is backed off rather than fired in a tight loop.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(tracker.count(), 2);

    running.comms.reannounce();
    tracker.wait_for(3, Duration::from_secs(5)).await;
    running.comms.reannounce();
    tracker.wait_for(4, Duration::from_secs(5)).await;

    let announces = tracker.announces();
    assert_eq!(announces[1].event(), Some("completed"));
    // Still "completed" after the failure, and nothing took its place.
    assert_eq!(announces[2].event(), Some("completed"));
    assert_eq!(announces[3].event(), None);
}

#[tokio::test]
async fn a_failure_reason_on_200_is_surfaced_and_announcing_continues() {
    let tracker = FakeTracker::start(|i| match i {
        0 => Reply::ok(body_failure("torrent not registered with this tracker")),
        _ => Reply::ok(body_empty(600, None)),
    })
    .await;
    let running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    // The status carries the tracker's own words, and the URL it reports has
    // no path: the passkey lives there.
    let status = loop {
        let s = running.comms.tracker_statuses().remove(0);
        if s.state == TrackerState::Error {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        status
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("torrent not registered"),
        "message was {:?}",
        status.message
    );
    assert!(!status.url.contains("passkey"), "url was {}", status.url);

    // The torrent is not dropped: the next announce still goes out, and still
    // carries the "started" the tracker refused.
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(5)).await;
    assert_eq!(tracker.announces()[1].event(), Some("started"));
}

#[tokio::test]
async fn an_empty_peer_list_does_not_stop_announcing() {
    // What nginx synthesises while the tracker itself is down.
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
    let _running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(3, Duration::from_secs(8)).await;
    assert!(
        tracker
            .announces()
            .iter()
            .skip(1)
            .all(|a| a.event().is_none())
    );
}

#[tokio::test]
async fn a_forced_reannounce_cuts_the_sleep() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(3600, None))).await;
    let running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(2)).await;
    assert_eq!(tracker.announces()[1].event(), None);
}

#[tokio::test]
async fn retry_after_is_honoured_on_429() {
    let tracker = FakeTracker::start(|i| match i {
        0 => Reply::status("429 Too Many Requests").with_header("Retry-After", "1"),
        _ => Reply::ok(body_empty(3600, None)),
    })
    .await;
    let _running = run(&tracker, FakeStats::new(1000, 0)).await;

    // 1 second, not the 10-second floor of the error backoff.
    tracker.wait_for(2, Duration::from_secs(4)).await;
    let announces = tracker.announces();
    let gap = announces[1].at.duration_since(announces[0].at);
    assert!(gap >= Duration::from_millis(900), "gap was {gap:?}");
    assert_eq!(announces[1].event(), Some("started"));
}

#[tokio::test]
async fn stopped_is_announced_once_on_stop() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(3600, None))).await;
    let running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    running.comms.announce_stopped(Duration::from_secs(3)).await;
    assert_eq!(tracker.count(), 2);
    assert_eq!(tracker.announces()[1].event(), Some("stopped"));

    // Once per session: a second stop announces nothing.
    running.comms.announce_stopped(Duration::from_secs(3)).await;
    assert_eq!(tracker.count(), 2);
}

#[tokio::test]
async fn the_status_snapshot_reflects_the_last_answer() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(3600, Some(60)))).await;
    let running = run(&tracker, FakeStats::new(1000, 0)).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    let status = loop {
        let s = running.comms.tracker_statuses().remove(0);
        if s.state == TrackerState::Working {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(status.seeders, Some(3));
    assert_eq!(status.leechers, Some(1));
    assert_eq!(status.downloaded, Some(7));
    assert!(status.last_announce.is_some());
    assert!(status.next_announce.is_some());
    assert!(status.url.starts_with("http://127.0.0.1:"));
    assert!(!status.url.contains('?'), "url was {}", status.url);
}

#[tokio::test]
async fn a_tracker_that_was_never_reached_reports_not_contacted() {
    // Nothing listens on this port; the status must still name the tracker.
    let url = Url::parse("http://127.0.0.1:1/announce/passkey").unwrap();
    let udp = UdpTrackerClient::new(CancellationToken::new(), None)
        .await
        .unwrap();
    let (peer_rx, comms) = TrackerComms::start(
        librqbit_core::hash_id::Id20::new([1u8; 20]),
        librqbit_core::hash_id::Id20::new([2u8; 20]),
        [url].into_iter().collect(),
        Box::new(()),
        None,
        6881,
        reqwest::Client::new(),
        udp,
    )
    .unwrap();
    drop(peer_rx);

    let statuses = comms.tracker_statuses();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, TrackerState::NotContacted);
    assert_eq!(statuses[0].url, "http://127.0.0.1:1");
}
