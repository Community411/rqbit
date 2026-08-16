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
