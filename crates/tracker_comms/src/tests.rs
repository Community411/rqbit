//! Announce behaviour against the fake HTTP tracker of `test_support`,
//! which records every announce and answers a scripted reply: these tests
//! assert what goes on the wire (the event, `left`, `downloaded`) rather
//! than what the code meant to send.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::TrackerComms;
use crate::TrackerState;
use crate::UdpTrackerClient;
use crate::test_support::{FakeTracker, Reply, body_empty, body_failure, body_one_peer};
use crate::tracker_comms::TorrentStatsProvider;
use crate::tracker_comms::TrackerCommsStats;

/// A stats provider the test drives. `progress` is the verified bytes on
/// disk (drives `left` and the completion transition), `fetched` the bytes
/// fetched from peers (the announce `downloaded`): the two move separately
/// on purpose, a torrent whose data preexists verifies without fetching.
#[derive(Clone, Default)]
struct FakeStats {
    inner: Arc<Mutex<FakeStatsInner>>,
}

#[derive(Default, Clone, Copy)]
struct FakeStatsInner {
    total: u64,
    progress: u64,
    fetched: u64,
    uploaded: u64,
}

impl FakeStats {
    fn new(total: u64, progress: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeStatsInner {
                total,
                progress,
                ..Default::default()
            })),
        }
    }

    fn set_progress(&self, progress: u64) {
        self.inner.lock().progress = progress;
    }

    fn set_fetched(&self, fetched: u64) {
        self.inner.lock().fetched = fetched;
    }

    fn set_uploaded(&self, uploaded: u64) {
        self.inner.lock().uploaded = uploaded;
    }
}

impl TorrentStatsProvider for FakeStats {
    fn get(&self) -> TrackerCommsStats {
        let inner = *self.inner.lock();
        TrackerCommsStats {
            uploaded_bytes: inner.uploaded,
            downloaded_bytes: inner.fetched,
            progress_bytes: inner.progress,
            total_bytes: inner.total,
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
    stats.set_progress(1000);
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
    stats.set_progress(1000);
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

#[tokio::test]
async fn a_torrent_complete_at_start_announces_downloaded_zero() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(600, None))).await;
    // Added with its data already on disk: everything verified, nothing
    // fetched from a peer. The honest first announce is a seed (left 0)
    // that downloaded nothing, not one charged the full size.
    let stats = FakeStats::new(1000, 1000);
    stats.set_uploaded(25);
    let _running = run(&tracker, stats.clone()).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    let first = &tracker.announces()[0];
    assert_eq!(first.event(), Some("started"));
    assert_eq!(first.downloaded(), 0);
    assert_eq!(first.left(), 0);
    assert_eq!(first.uploaded(), 25);
}

#[tokio::test]
async fn a_real_transfer_announces_the_fetched_bytes() {
    let tracker = FakeTracker::start(|_| Reply::ok(body_empty(600, None))).await;
    let stats = FakeStats::new(1000, 0);
    let running = run(&tracker, stats.clone()).await;

    tracker.wait_for(1, Duration::from_secs(5)).await;
    // Mid-transfer: 300 bytes came off the wire, 200 of them verified so
    // far. `downloaded` reports the transfer, `left` the verified progress.
    stats.set_fetched(300);
    stats.set_progress(200);
    running.comms.reannounce();
    tracker.wait_for(2, Duration::from_secs(5)).await;

    let second = &tracker.announces()[1];
    assert_eq!(second.downloaded(), 300);
    assert_eq!(second.left(), 800);
}
