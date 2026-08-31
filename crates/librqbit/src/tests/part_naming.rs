//! The on-disk name of a file follows its state: `<name>.part` until every
//! piece of it is validated, its final name from then on. No partial file
//! ever sits under a final name, whether it is being downloaded, was left
//! by a session that stopped mid-way, or was written before the rule
//! existed. Driven over a real transfer between two sessions, and over the
//! initial check alone.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use tracing::error_span;

use crate::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, Session,
    SessionOptions, SessionPersistenceConfig,
    api::TorrentIdOrHash,
    create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    storage::{StorageFactoryExt, filesystem::FilesystemStorageFactory},
    tests::test_util::{
        create_default_random_dir_with_torrents, create_new_file_with_random_content,
        setup_test_logging, wait_until,
    },
};

const PIECE_LEN: u32 = 16384;
// A multiple of the piece length, so no piece straddles two files and a
// file's progress is exactly the pieces inside it.
const FILE_LEN: usize = 8 * PIECE_LEN as usize;
const NUM_FILES: usize = 3;
const TOTAL_LEN: u64 = (FILE_LEN * NUM_FILES) as u64;
// Slow enough that the transfer is observed in flight, fast enough that a
// whole file goes by in a couple of seconds.
const DOWNLOAD_BPS: u32 = 192 * 1024;

fn final_names() -> Vec<String> {
    (0..NUM_FILES).map(|i| format!("{i}.data")).collect()
}

fn part_names() -> Vec<String> {
    (0..NUM_FILES).map(|i| format!("{i}.data.part")).collect()
}

fn names(root: &Path) -> Vec<String> {
    let mut out = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn assert_same_content(outdir: &Path, datadir: &Path) {
    for name in final_names() {
        assert_eq!(
            std::fs::read(outdir.join(&name)).unwrap(),
            std::fs::read(datadir.join(&name)).unwrap(),
            "{name} differs from the seeded copy"
        );
    }
}

async fn create_test_torrent(dir: &Path) -> (bytes::Bytes, librqbit_core::Id20) {
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
    (torrent_file.as_bytes().unwrap(), torrent_file.info_hash())
}

async fn seed_session(datadir: &Path, torrent_bytes: bytes::Bytes) -> (Arc<Session>, SocketAddr) {
    let session = Session::new_with_opts(
        datadir.to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            root_span: Some(error_span!(parent: None, "s", name = "seed")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent_bytes),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(datadir.to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let addr = session.listen_addr().unwrap();
    (session, addr)
}

async fn client_session(outdir: &Path, persistence: Option<PathBuf>) -> Arc<Session> {
    Session::new_with_opts(
        outdir.to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                ..Default::default()
            }),
            fastresume: persistence.is_some(),
            persistence: persistence.map(|folder| SessionPersistenceConfig::Json {
                folder: Some(folder),
            }),
            root_span: Some(error_span!(parent: None, "s", name = "client")),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

fn client_add_options(outdir: &Path, seed: SocketAddr, paused: bool) -> AddTorrentOptions {
    AddTorrentOptions {
        paused,
        overwrite: true,
        output_folder: Some(outdir.to_str().unwrap().to_owned()),
        initial_peers: Some(vec![seed]),
        ratelimits: LimitsConfig {
            download_bps: NonZeroU32::new(DOWNLOAD_BPS),
            upload_bps: None,
        },
        ..Default::default()
    }
}

fn fetched_bytes(handle: &crate::torrent_state::ManagedTorrentHandle) -> u64 {
    handle
        .stats()
        .live
        .expect("live stats")
        .snapshot
        .fetched_bytes
}

/// A file is `.part` from the add to the moment its last piece lands, and
/// its final name from then on: at no observation does a final name carry
/// an incomplete file, and once the stats call a file complete its final
/// name is what the disk holds.
#[tokio::test(flavor = "multi_thread")]
async fn an_incomplete_file_is_named_part_until_its_last_piece_lands() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        let datadir =
            create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_part_"));
        let (torrent_bytes, _) = create_test_torrent(datadir.path()).await;
        let (seed, seed_addr) = seed_session(datadir.path(), torrent_bytes.clone()).await;

        let outdir = tempfile::TempDir::with_prefix("rqbit_part_out").unwrap();
        let client = client_session(outdir.path(), None).await;
        let handle = client
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(client_add_options(outdir.path(), seed_addr, true)),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        assert_eq!(names(outdir.path()), part_names());

        client.unpause(&handle).await.unwrap();

        let mut saw_a_final_name_beside_a_part = false;
        let mut interval = tokio::time::interval(Duration::from_millis(5));
        loop {
            interval.tick().await;
            let before = names(outdir.path());
            let stats = handle.stats();
            let after = names(outdir.path());

            // A final name seen before the stats were read was renamed
            // after its last piece validated, so the stats must agree.
            for name in before.iter().filter(|n| !n.ends_with(".part")) {
                let idx: usize = name.strip_suffix(".data").unwrap().parse().unwrap();
                assert_eq!(
                    stats.file_progress[idx], FILE_LEN as u64,
                    "{name} sits at its final name while incomplete: {stats:?}"
                );
            }
            // A file the stats call complete was renamed before they could
            // say so, so the listing after them holds its final name.
            for (idx, have) in stats.file_progress.iter().enumerate() {
                if *have == FILE_LEN as u64 {
                    assert!(after.contains(&format!("{idx}.data")), "{after:?}");
                    assert!(!after.contains(&format!("{idx}.data.part")), "{after:?}");
                }
            }
            if before.iter().any(|n| n.ends_with(".part"))
                && before.iter().any(|n| !n.ends_with(".part"))
            {
                saw_a_final_name_beside_a_part = true;
            }
            if stats.finished {
                break;
            }
        }

        assert!(
            saw_a_final_name_beside_a_part,
            "the transfer was never observed in flight"
        );
        // The `finished` flag rides hns, published a beat before the rename
        // of the last file runs, so let the names settle rather than assert
        // them on the instant the flag flips.
        wait_until(
            || {
                let now = names(outdir.path());
                if now == final_names() {
                    Ok(())
                } else {
                    bail!("names not settled yet: {now:?}")
                }
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_same_content(outdir.path(), datadir.path());
        // The seed's complete files were never touched.
        assert_eq!(names(datadir.path()), final_names());

        client.stop().await;
        seed.stop().await;
    })
    .await
    .unwrap();
}

/// A session that stops mid-way leaves `.part` files, and the next session
/// on the same folder adopts them: the pieces they hold validate and are
/// not fetched again, and the files land at their final names when done.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_mid_download_resumes_the_part_files_without_refetching() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        let datadir =
            create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_part_"));
        let (torrent_bytes, _) = create_test_torrent(datadir.path()).await;
        let (seed, seed_addr) = seed_session(datadir.path(), torrent_bytes.clone()).await;
        let outdir = tempfile::TempDir::with_prefix("rqbit_part_out").unwrap();

        let progress_at_stop = {
            let client = client_session(outdir.path(), None).await;
            let handle = client
                .add_torrent(
                    AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
                    Some(client_add_options(outdir.path(), seed_addr, false)),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap();
            wait_until(
                || {
                    let p = handle.stats().progress_bytes;
                    if p < TOTAL_LEN / 3 {
                        bail!("progress {p} < {}", TOTAL_LEN / 3)
                    }
                    Ok(())
                },
                Duration::from_secs(60),
            )
            .await
            .unwrap();
            let progress = handle.stats().progress_bytes;
            assert!(
                progress < TOTAL_LEN,
                "the transfer finished before it could be interrupted"
            );
            client.stop().await;
            progress
        };

        let interrupted = names(outdir.path());
        assert!(
            interrupted.iter().any(|n| n.ends_with(".part")),
            "the interrupted transfer left no .part file: {interrupted:?}"
        );

        let client = client_session(outdir.path(), None).await;
        let handle = client
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(client_add_options(outdir.path(), seed_addr, false)),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_completed().await.unwrap();

        assert_eq!(names(outdir.path()), final_names());
        assert_same_content(outdir.path(), datadir.path());
        let fetched = fetched_bytes(&handle);
        assert!(
            fetched <= TOTAL_LEN - progress_at_stop,
            "fetched {fetched} bytes, more than the {} the first session had not validated",
            TOTAL_LEN - progress_at_stop
        );

        client.stop().await;
        seed.stop().await;
    })
    .await
    .unwrap();
}

/// Data already complete at its final names, with no resume data at all, is
/// adopted as it is: no rename, no fetch. And a session restored on the same
/// folder finds it complete again with no fetch either.
#[tokio::test(flavor = "multi_thread")]
async fn complete_data_at_final_names_is_adopted_and_restored_unrenamed() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(60), async {
        let datadir =
            create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_part_"));
        let (torrent_bytes, info_hash) = create_test_torrent(datadir.path()).await;
        let persistence = tempfile::TempDir::with_prefix("rqbit_part_session").unwrap();

        {
            let session = client_session(datadir.path(), Some(persistence.path().to_owned())).await;
            let handle = session
                .add_torrent(
                    AddTorrent::TorrentFileBytes(torrent_bytes),
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
            handle.wait_until_initialized().await.unwrap();
            let stats = handle.stats();
            assert!(stats.finished, "{stats:?}");
            assert_eq!(names(datadir.path()), final_names());
            assert_eq!(fetched_bytes(&handle), 0);
            session.stop().await;
        }

        let session = client_session(datadir.path(), Some(persistence.path().to_owned())).await;
        let handle = session
            .get(TorrentIdOrHash::Hash(info_hash))
            .expect("the torrent is restored from the session");
        handle.wait_until_initialized().await.unwrap();
        let stats = handle.stats();
        assert!(stats.finished, "{stats:?}");
        assert_eq!(stats.progress_bytes, TOTAL_LEN);
        assert_eq!(names(datadir.path()), final_names());
        assert_eq!(fetched_bytes(&handle), 0);
        session.stop().await;
    })
    .await
    .unwrap();
}

/// A final name holding a truncated file, as every partial written before
/// the rule existed does, is demoted to `.part` by the check, keeps the
/// pieces it holds, and is promoted back once the rest has landed.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_data_at_a_final_name_is_demoted_and_completed_from_there() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        let datadir =
            create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_part_"));
        let (torrent_bytes, _) = create_test_torrent(datadir.path()).await;
        let (seed, seed_addr) = seed_session(datadir.path(), torrent_bytes.clone()).await;

        // File 0 whole, file 1 cut in half, file 2 absent, all at final names.
        let outdir = tempfile::TempDir::with_prefix("rqbit_part_out").unwrap();
        std::fs::copy(datadir.path().join("0.data"), outdir.path().join("0.data")).unwrap();
        let half = std::fs::read(datadir.path().join("1.data")).unwrap();
        std::fs::write(outdir.path().join("1.data"), &half[..FILE_LEN / 2]).unwrap();

        let client = client_session(outdir.path(), None).await;
        let handle = client
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(client_add_options(outdir.path(), seed_addr, true)),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();

        assert_eq!(
            names(outdir.path()),
            vec!["0.data", "1.data.part", "2.data.part"]
        );
        let checked = handle.stats();
        assert_eq!(
            checked.file_progress,
            vec![FILE_LEN as u64, (FILE_LEN / 2) as u64, 0]
        );

        client.unpause(&handle).await.unwrap();
        handle.wait_until_completed().await.unwrap();

        assert_eq!(names(outdir.path()), final_names());
        assert_same_content(outdir.path(), datadir.path());
        let fetched = fetched_bytes(&handle);
        assert!(
            fetched <= TOTAL_LEN - checked.progress_bytes,
            "fetched {fetched} bytes, more than the {} the check had not validated",
            TOTAL_LEN - checked.progress_bytes
        );

        client.stop().await;
        seed.stop().await;
    })
    .await
    .unwrap();
}

/// With the switch off every file opens at its final name from the add,
/// the upstream behaviour, and nothing is renamed on the way.
#[tokio::test(flavor = "multi_thread")]
async fn the_switch_off_keeps_every_file_at_its_final_name() {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        let datadir =
            create_default_random_dir_with_torrents(NUM_FILES, FILE_LEN, Some("rqbit_part_"));
        let (torrent_bytes, _) = create_test_torrent(datadir.path()).await;
        let (seed, seed_addr) = seed_session(datadir.path(), torrent_bytes.clone()).await;

        let outdir = tempfile::TempDir::with_prefix("rqbit_part_out").unwrap();
        let client = client_session(outdir.path(), None).await;
        let response = client
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(AddTorrentOptions {
                    storage_factory: Some(
                        FilesystemStorageFactory::with_part_naming(false).boxed(),
                    ),
                    ..client_add_options(outdir.path(), seed_addr, true)
                }),
            )
            .await
            .unwrap();
        let handle = match response {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected the torrent to be added"),
        };
        handle.wait_until_initialized().await.unwrap();
        assert_eq!(names(outdir.path()), final_names());

        client.unpause(&handle).await.unwrap();
        handle.wait_until_completed().await.unwrap();
        assert_eq!(names(outdir.path()), final_names());
        assert_same_content(outdir.path(), datadir.path());

        client.stop().await;
        seed.stop().await;
    })
    .await
    .unwrap();
}

/// A widened selection leaves every file its pieces already cover at its
/// final name. No piece completes after the widen, so the piece-completion
/// hook cannot be what renames the file: a promote refused earlier (here, a
/// directory squatting the final name) is only ever retried by
/// update_only_files itself.
async fn widen_after_a_refused_promote(pause_first: bool) {
    setup_test_logging();
    tokio::time::timeout(Duration::from_secs(120), async {
        // One piece spans both files, so selecting file 0 downloads and
        // validates file 1's bytes with it.
        let datadir = tempfile::TempDir::with_prefix("rqbit_part_").unwrap();
        create_new_file_with_random_content(&datadir.path().join("0.data"), 10000);
        create_new_file_with_random_content(&datadir.path().join("1.data"), 6384);
        let (torrent_bytes, _) = create_test_torrent(datadir.path()).await;
        let (seed, seed_addr) = seed_session(datadir.path(), torrent_bytes.clone()).await;

        let outdir = tempfile::TempDir::with_prefix("rqbit_part_out").unwrap();
        let client = client_session(outdir.path(), None).await;
        let handle = client
            .add_torrent(
                AddTorrent::TorrentFileBytes(torrent_bytes),
                Some(AddTorrentOptions {
                    only_files: Some(vec![0]),
                    ..client_add_options(outdir.path(), seed_addr, true)
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        assert_eq!(names(outdir.path()), vec!["0.data.part", "1.data.part"]);

        // The directory makes the promote at piece completion refuse.
        std::fs::create_dir(outdir.path().join("1.data")).unwrap();
        client.unpause(&handle).await.unwrap();
        handle.wait_until_completed().await.unwrap();
        assert_eq!(
            names(outdir.path()),
            vec!["0.data", "1.data", "1.data.part"]
        );
        std::fs::remove_dir(outdir.path().join("1.data")).unwrap();

        if pause_first {
            client.pause(&handle).await.unwrap();
        }
        client
            .update_only_files(&handle, &HashSet::from_iter([0usize, 1usize]))
            .await
            .unwrap();

        assert_eq!(names(outdir.path()), vec!["0.data", "1.data"]);
        for name in ["0.data", "1.data"] {
            assert_eq!(
                std::fs::read(outdir.path().join(name)).unwrap(),
                std::fs::read(datadir.path().join(name)).unwrap(),
                "{name} differs from the seeded copy"
            );
        }

        client.stop().await;
        seed.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn widening_the_selection_promotes_covered_files_while_live() {
    widen_after_a_refused_promote(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn widening_the_selection_promotes_covered_files_while_paused() {
    widen_after_a_refused_promote(true).await
}
