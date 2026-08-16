//! BEP 12 tiers, from the .torrent to the announcer, driven end to end
//! against the fake HTTP tracker of librqbit-tracker-comms' `test_support`.
//!
//! The trackers of one tier are fallbacks of each other: one of them carries
//! the announces, the others stay standby. Every tier is announced to on its
//! own. A private torrent keeps that shape too, and takes no session-wide
//! tracker on top (BEP 27); a public one gets each session-wide tracker as
//! a tier of its own.

use std::time::Duration;

use tracing::error_span;
use tracker_comms::test_support::{FakeTracker, Reply, body_empty};

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, Session, SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

const FILE_LEN: usize = 128 * 1024;
const PIECE_LEN: u32 = 16384;

/// A .torrent over `dir` whose `announce-list` is exactly `tiers`.
async fn torrent_with_tiers(
    dir: &std::path::Path,
    tiers: &[&[&FakeTracker]],
    private: bool,
) -> bytes::Bytes {
    let mut torrent = create_torrent(
        dir,
        crate::CreateTorrentOptions {
            piece_length: Some(PIECE_LEN),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    torrent.meta.announce_list = tiers
        .iter()
        .map(|tier| {
            tier.iter()
                .map(|t| t.url.as_str().as_bytes().into())
                .collect()
        })
        .collect();
    torrent.meta.info.data.private = private;
    torrent.as_bytes().unwrap()
}

async fn session_with_trackers(
    outdir: &std::path::Path,
    span_name: &'static str,
    trackers: &[&FakeTracker],
) -> std::sync::Arc<Session> {
    Session::new_with_opts(
        outdir.to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                ..Default::default()
            }),
            trackers: trackers.iter().map(|t| t.url.clone()).collect(),
            root_span: Some(error_span!(parent: None, "s", name = span_name)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn add(
    session: &std::sync::Arc<Session>,
    dir: &std::path::Path,
    bytes: bytes::Bytes,
) -> crate::torrent_state::ManagedTorrentHandle {
    let handle = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(dir.to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    handle
}

async fn wait_for_total(trackers: &[&FakeTracker], n: usize, within: Duration) {
    let deadline = std::time::Instant::now() + within;
    while trackers.iter().map(|t| t.count()).sum::<usize>() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "fewer than {n} announces after {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_private_torrent_with_one_tier_of_two_announces_to_one_and_ignores_session_trackers() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(60), async {
        let datadir = create_default_random_dir_with_torrents(1, FILE_LEN, Some("rqbit_tier_"));
        let a = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let b = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let session_wide = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let bytes = torrent_with_tiers(datadir.path(), &[&[&a, &b]], true).await;

        let session = session_with_trackers(datadir.path(), "private", &[&session_wide]).await;
        let handle = add(&session, datadir.path(), bytes).await;

        wait_for_total(&[&a, &b], 3, Duration::from_secs(20)).await;
        let (used, standby) = if a.count() > 0 { (&a, &b) } else { (&b, &a) };
        assert_eq!(
            standby.count(),
            0,
            "both trackers of the tier were announced to"
        );
        assert_eq!(used.announces()[0].event(), Some("started"));
        assert_eq!(
            session_wide.count(),
            0,
            "a session-wide tracker reached a private torrent"
        );

        let statuses = handle.tracker_statuses();
        assert_eq!(statuses.len(), 2);
        assert!(statuses.iter().all(|s| s.tier == 0));
        session.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_public_torrent_announces_to_every_tier_and_to_the_session_trackers() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(60), async {
        let datadir = create_default_random_dir_with_torrents(1, FILE_LEN, Some("rqbit_tier_"));
        let a = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let b = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let session_wide = FakeTracker::start(|_| Reply::ok(body_empty(1, None))).await;
        let bytes = torrent_with_tiers(datadir.path(), &[&[&a], &[&b]], false).await;

        let session = session_with_trackers(datadir.path(), "public", &[&session_wide]).await;
        let handle = add(&session, datadir.path(), bytes).await;

        a.wait_for(1, Duration::from_secs(20)).await;
        b.wait_for(1, Duration::from_secs(20)).await;
        session_wide.wait_for(1, Duration::from_secs(20)).await;

        let mut tiers = handle
            .tracker_statuses()
            .into_iter()
            .map(|s| s.tier)
            .collect::<Vec<_>>();
        tiers.sort_unstable();
        assert_eq!(tiers, vec![0, 1, 2]);
        session.stop().await;
    })
    .await
    .unwrap();
}
