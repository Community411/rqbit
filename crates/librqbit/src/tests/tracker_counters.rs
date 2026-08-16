//! What a whole session announces to its tracker, driven end to end against
//! the fake HTTP tracker of librqbit-tracker-comms' `test_support`.
//!
//! The announce `downloaded` is the bytes fetched from peers this session,
//! never the bytes a hash check verified on disk: a ratio-enforcing tracker
//! charges the account for what it is told, and a torrent added with its
//! data already present downloaded nothing. `left` keeps reading the
//! verified progress, which is what BEP-3 wants there. An embedder that
//! re-adds a torrent in place can seed the counters at add time so the next
//! announce continues the accounting instead of restarting it.

use std::net::Ipv4Addr;
use std::time::Duration;

use tracing::error_span;
use tracker_comms::test_support::{FakeTracker, Reply, body_empty};

use crate::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, Session,
    SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

const FILE_LEN: usize = 128 * 1024;
const PIECE_LEN: u32 = 16384;

async fn create_test_torrent(dir: &std::path::Path) -> bytes::Bytes {
    let torrent_file = create_torrent(
        dir,
        crate::CreateTorrentOptions {
            piece_length: Some(PIECE_LEN),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    torrent_file.as_bytes().unwrap()
}

async fn minimal_session(
    outdir: &std::path::Path,
    span_name: &'static str,
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
            root_span: Some(error_span!(parent: None, "s", name = span_name)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// A torrent added with its data already on disk verifies everything and
/// fetches nothing: the first announce must say `downloaded=0` while
/// `left=0`, not charge the full size to the account.
#[tokio::test(flavor = "multi_thread")]
async fn added_complete_announces_downloaded_zero() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(60), async {
        let datadir = create_default_random_dir_with_torrents(1, FILE_LEN, Some("rqbit_ann_"));
        let torrent_bytes = create_test_torrent(datadir.path()).await;
        let tracker = FakeTracker::start(|_| Reply::ok(body_empty(3600, None))).await;

        let session = minimal_session(datadir.path(), "seed").await;
        let handle = session
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(datadir.path().to_str().unwrap().to_owned()),
                    trackers: Some(vec![tracker.url.to_string()]),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();

        tracker.wait_for(1, Duration::from_secs(15)).await;
        let first = &tracker.announces()[0];
        assert_eq!(first.event(), Some("started"));
        assert_eq!(first.downloaded(), 0);
        assert_eq!(first.left(), 0);
        session.stop().await;
    })
    .await
    .unwrap();
}

/// A real transfer announces the bytes fetched from the peer, on top of the
/// add-time floor `initial_uploaded_bytes` / `initial_downloaded_bytes`
/// seeds, so a re-added torrent continues its accounting.
#[tokio::test(flavor = "multi_thread")]
async fn a_transfer_announces_fetched_bytes_on_top_of_the_seeded_floor() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        let datadir = create_default_random_dir_with_torrents(1, FILE_LEN, Some("rqbit_ann_"));
        let torrent_bytes = create_test_torrent(datadir.path()).await;
        let tracker = FakeTracker::start(|_| Reply::ok(body_empty(3600, None))).await;

        let server_session = Session::new_with_opts(
            datadir.path().to_owned(),
            SessionOptions {
                dht: None,
                disable_local_service_discovery: true,
                listen: Some(ListenerOptions {
                    mode: ListenerMode::TcpOnly,
                    listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                root_span: Some(error_span!(parent: None, "s", name = "server")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let server_handle = server_session
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(datadir.path().to_str().unwrap().to_owned()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        server_handle.wait_until_initialized().await.unwrap();
        let server_addr = server_session.listen_addr().unwrap();

        let outdir = tempfile::TempDir::with_prefix("rqbit_ann_out").unwrap();
        let client_session = minimal_session(outdir.path(), "client").await;
        let response = client_session
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(AddTorrentOptions {
                    initial_peers: Some(vec![server_addr]),
                    trackers: Some(vec![tracker.url.to_string()]),
                    initial_uploaded_bytes: Some(1234),
                    initial_downloaded_bytes: Some(5000),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let client_handle = match response {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected the torrent to be added"),
        };
        client_handle.wait_until_completed().await.unwrap();

        // The completion cuts the announce sleep, so the announce that
        // carries it arrives promptly rather than at the next interval.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let completed = loop {
            if let Some(a) = tracker.announces().into_iter().find(|a| a.left() == 0) {
                break a;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no announce with left=0 in time; announces: {:?}",
                tracker.announces()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        // Every byte of the file was fetched from the peer, and the seeded
        // floor is announced on top of it, never lost and never doubled.
        assert_eq!(completed.downloaded(), 5000 + FILE_LEN as u64);
        assert_eq!(completed.uploaded(), 1234);

        // The first announce of the session already carried the floor.
        let first = &tracker.announces()[0];
        assert_eq!(first.event(), Some("started"));
        assert!(first.downloaded() >= 5000, "was {}", first.downloaded());
        assert_eq!(first.uploaded(), 1234);

        client_session.stop().await;
        server_session.stop().await;
    })
    .await
    .unwrap();
}
