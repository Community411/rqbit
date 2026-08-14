use std::{
    collections::VecDeque,
    io::SeekFrom,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Instant,
};

use anyhow::Context;
use dashmap::DashMap;

use librqbit_core::constants::CHUNK_SIZE;
use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::OwnedSemaphorePermit,
};
use tracing::{debug, trace};

use crate::{ManagedTorrent, file_info::FileInfo, storage::TorrentStorage};

use super::{ManagedTorrentHandle, TorrentMetadata};

type StreamId = usize;

// 32 mb lookahead by default, overridable with RQBIT_STREAM_LOOKAHEAD_MB.
const PER_STREAM_BUF_DEFAULT: u64 = 32 * 1024 * 1024;

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => !matches!(v.trim(), "0" | "false" | "no" | ""),
        Err(_) => default,
    }
}

/// Serve chunks that are on disk but belong to a piece whose hash has not been
/// checked yet. On by default in this build: it is what lets playback start on
/// the first 16 KB rather than on a whole 2-8 MB piece. Set
/// RQBIT_STREAM_SUBPIECE=0 to get the upstream behaviour back.
static SUBPIECE_READS: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| env_flag("RQBIT_STREAM_SUBPIECE", true));

/// How much of a file's end holds the container index a player reads before
/// it can open the file. Matroska Cues for a feature-length release measure
/// tens of kilobytes; this is the window whose chunks the last piece is
/// fetched from first.
const TAIL_INDEX_BYTES: u64 = 256 * 1024;

/// Fetch the last piece of a streamed file alongside the read head, for the
/// container index players read before they can open the file.
static TAIL_PRIORITY: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| env_flag("RQBIT_STREAM_TAIL_PRIORITY", true));

static STREAM_LOOKAHEAD: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("RQBIT_STREAM_LOOKAHEAD_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|mb| *mb > 0)
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(PER_STREAM_BUF_DEFAULT)
});

struct StreamState {
    file_id: usize,
    file_len: u64,
    file_abs_offset: u64,
    position: u64,
    waker: Option<Waker>,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        lengths.compute_current_piece(self.position, self.file_abs_offset)
    }

    fn queue<'a>(&self, lengths: &'a Lengths) -> impl Iterator<Item = ValidPieceIndex> + use<'a> {
        let start = self.file_abs_offset + self.position;
        let end = (start + *STREAM_LOOKAHEAD).min(self.file_abs_offset + self.file_len);
        let dpl = lengths.default_piece_length();
        let start_id: u32 = (start / dpl as u64).try_into().unwrap();
        let end_id: u32 = end.div_ceil(dpl as u64).try_into().unwrap();
        let empty = start_id >= end_id;

        // The container index a player reads before it can report the file
        // opened (Matroska Cues, a non-faststart MP4 moov) lives at the end of
        // the file. Ask for that piece right after the one under the read head,
        // so a second peer fetches it in parallel instead of after the whole
        // 32 MB window. Without this the player's own tail request is what
        // first puts the piece in a queue, and opening waits for it.
        let tail_id = if empty || !*TAIL_PRIORITY || self.file_len == 0 {
            None
        } else {
            let id = ((self.file_abs_offset + self.file_len - 1) / dpl as u64) as u32;
            (id < start_id || id >= end_id).then_some(id)
        };

        let head = (!empty).then_some(start_id);
        let rest_start = start_id.saturating_add(1);
        let rest_end = if empty { rest_start } else { end_id };
        head.into_iter()
            .chain(tail_id)
            .chain(rest_start..rest_end)
            .filter_map(|i| lengths.validate_piece_index(i))
    }
}

#[derive(Default)]
pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
}

impl TorrentStreams {
    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let vm = s.value_mut();
            vm.waker = Some(waker);
        }
    }

    // Interleave 1st, 2nd etc pieces from each active stream in turn until they get 1/10th of the file .
    pub(crate) fn iter_next_pieces<'a>(
        &'a self,
        lengths: &'a Lengths,
    ) -> impl Iterator<Item = ValidPieceIndex> + 'a {
        struct Interleave<I> {
            all: VecDeque<I>,
        }

        impl<I: Iterator<Item = ValidPieceIndex>> Iterator for Interleave<I> {
            type Item = ValidPieceIndex;

            fn next(&mut self) -> Option<Self::Item> {
                while let Some(mut it) = self.all.pop_front() {
                    if let Some(piece) = it.next() {
                        self.all.push_back(it);
                        return Some(piece);
                    }
                }
                None
            }
        }

        let mut all: Vec<_> = self.streams.iter().map(|s| s.queue(lengths)).collect();

        // Shuffle to decrease determinism and make queueing fairer.
        use rand::seq::SliceRandom;
        all.shuffle(&mut rand::rng());

        Interleave { all: all.into() }
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        for mut w in self.streams.iter_mut() {
            if w.value().current_piece(lengths).map(|p| p.id) == Some(piece_id)
                && let Some(waker) = w.value_mut().waker.take()
            {
                debug!(
                    stream_id = *w.key(),
                    piece_id = piece_id.get(),
                    "waking stream"
                );
                waker.wake();
            }
        }
    }

    /// Index, within [piece], of the chunk the earliest reader standing in
    /// that piece is waiting for, when any reader is.
    ///
    /// A peer sends the chunks of a piece in index order, so a reader that
    /// entered the piece anywhere but at its start waits for everything
    /// before it: on an 8 MB piece a request for the last 64 KB waits for
    /// 8 MB. That is what a player's index read at the end of the file is,
    /// and what a seek landing mid-piece is. Requesting from the reader's
    /// own chunk first turns both into one chunk of waiting.
    pub(crate) fn first_wanted_chunk(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) -> Option<u32> {
        let dpl = lengths.default_piece_length() as u64;
        let chunk_of = |abs: u64| ((abs % dpl) / CHUNK_SIZE as u64) as u32;
        let mut first: Option<u32> = None;
        let mut tail: Option<u32> = None;
        for s in self.streams.iter() {
            let st = s.value();
            if st.file_len == 0 {
                continue;
            }
            if st.current_piece(lengths).map(|p| p.id) == Some(piece_id) {
                let chunk = chunk_of(st.file_abs_offset + st.position);
                first = Some(first.map_or(chunk, |c: u32| c.min(chunk)));
                continue;
            }
            // No reader stands here, but this is the last piece of a file
            // being streamed, which the queue asks for early precisely
            // because the container index lives at its end. Nothing would
            // otherwise stop a peer from starting that piece at its first
            // chunk and delivering the index last.
            let last_byte = st.file_abs_offset + st.file_len - 1;
            if (last_byte / dpl) as u32 != piece_id.get() {
                continue;
            }
            let window_start = last_byte
                .saturating_sub(TAIL_INDEX_BYTES - 1)
                .max(st.file_abs_offset);
            let chunk = chunk_of(window_start);
            tail = Some(tail.map_or(chunk, |c: u32| c.min(chunk)));
        }
        first.or(tail).filter(|c| *c > 0)
    }

    fn drop_stream(&self, stream_id: StreamId) -> Option<StreamState> {
        debug!(stream_id, "dropping stream");
        self.streams.remove(&stream_id).map(|s| s.1)
    }

    pub(crate) fn streamed_file_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.streams.iter().map(|s| s.value().file_id)
    }
}

pub struct FileStream {
    torrent: ManagedTorrentHandle,
    metadata: Arc<TorrentMetadata>,
    streams: Arc<TorrentStreams>,
    stream_id: usize,
    file_id: usize,
    position: u64,

    // file params
    file_len: u64,
    file_torrent_abs_offset: u64,

    // Serve chunks already on disk inside a piece that is not verified yet.
    subpiece_reads: bool,

    _blocking_permit: OwnedSemaphorePermit,
}

macro_rules! map_io_err {
    ($e:expr) => {
        $e.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    };
}

macro_rules! poll_try_io {
    ($e:expr) => {{
        let e = map_io_err!($e);
        match e {
            Ok(r) => r,
            Err(e) => {
                debug!("stream error {e:#}");
                return Poll::Ready(Err(e));
            }
        }
    }};
}

impl AsyncRead for FileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // if the file is over, return 0
        if self.position == self.file_len {
            debug!(
                stream_id = self.stream_id,
                file_id = self.file_id,
                "stream completed, EOF"
            );
            return Poll::Ready(Ok(()));
        }

        let current = poll_try_io!(
            self.metadata
                .lengths()
                .compute_current_piece(self.position, self.file_torrent_abs_offset)
                .context("invalid position")
        );

        // How many bytes we may serve from the current position.
        //
        // With the whole piece verified, that is the rest of the piece. Without
        // it, and only when sub-piece streaming is on, it is the run of chunks
        // already written to disk under the read head: the bytes a player needs
        // to start are the first few kilobytes of a 2-8 MB piece, and waiting
        // for the whole piece (from the single peer that owns it) is what makes
        // playback start slow.
        let abs_pos = self.file_torrent_abs_offset + self.position;
        let subpiece = self.subpiece_reads;
        let available = poll_try_io!(self.torrent.with_chunk_tracker(|ct| {
            let mut available = if ct.get_have_pieces().as_slice()[current.id.get() as usize] {
                current.piece_remaining as u64
            } else if subpiece {
                ct.contiguous_downloaded_bytes_at(abs_pos)
            } else {
                0
            };
            available = available.min(current.piece_remaining as u64);
            if available == 0 {
                self.streams
                    .register_waker(self.stream_id, cx.waker().clone());
            }
            available
        }));
        if available == 0 {
            debug!(stream_id = self.stream_id, file_id = self.file_id, piece_id = %current.id, "poll pending, not have");
            return Poll::Pending;
        }

        // actually stream the piece
        let buf = tbuf.initialize_unfilled();
        let file_remaining = self.file_len - self.position;
        let bytes_to_read: usize = poll_try_io!(
            (buf.len() as u64)
                .min(available)
                .min(file_remaining)
                .try_into()
        );

        let buf = &mut buf[..bytes_to_read];

        let start = Instant::now();
        poll_try_io!(poll_try_io!(self.torrent.shared.spawner.block_in_place(
            || {
                self.torrent.with_storage_and_file(
                    self.file_id,
                    |files, _fi| {
                        files.pread_exact(self.file_id, self.position, buf)?;
                        Ok::<_, anyhow::Error>(())
                    },
                    &self.metadata,
                )
            }
        )));

        trace!(
            buflen = buf.len(),
            stream_id = self.stream_id,
            file_id = self.file_id,
            read_time = ?start.elapsed(),
            "will write bytes"
        );

        self.as_mut().advance(bytes_to_read as u64);
        tbuf.advance(bytes_to_read);

        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let end_i64 = map_io_err!(TryInto::<i64>::try_into(self.file_len))?;
        let new_pos: i64 = match position {
            SeekFrom::Start(s) => map_io_err!(s.try_into())?,
            SeekFrom::End(e) => map_io_err!(TryInto::<i64>::try_into(self.file_len))? + e,
            SeekFrom::Current(o) => map_io_err!(TryInto::<i64>::try_into(self.position))? + o,
        };

        if new_pos < 0 || new_pos > end_i64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                anyhow::anyhow!("invalid seek"),
            ));
        }

        self.as_mut().set_position(map_io_err!(new_pos.try_into())?);
        debug!(stream_id = self.stream_id, position = self.position, "seek");
        // The window this stream asks for just moved. Peers parked on the 5 s
        // "no pieces to request" timer would otherwise discover it that late.
        self.torrent.notify_new_pieces_available();
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.streams.drop_stream(self.stream_id);
    }
}

impl ManagedTorrent {
    fn with_storage_and_file<F, R>(
        &self,
        file_id: usize,
        f: F,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(&dyn TorrentStorage, &FileInfo) -> R,
    {
        self.with_state(|s| {
            let files = match s {
                crate::ManagedTorrentState::Paused(p) => &*p.files,
                crate::ManagedTorrentState::Live(l) => &*l.files,
                s => anyhow::bail!("with_storage_and_file: invalid state: {}", s.name()),
            };
            let fi = metadata.file_infos.get(file_id).context("invalid file")?;
            Ok(f(files, fi))
        })
    }

    fn streams(&self) -> anyhow::Result<Arc<TorrentStreams>> {
        self.with_state(|s| match s {
            crate::ManagedTorrentState::Paused(p) => Ok(p.streams.clone()),
            crate::ManagedTorrentState::Live(l) => Ok(l.streams.clone()),
            s => anyhow::bail!("streams: invalid state {}", s.name()),
        })
    }

    fn maybe_reconnect_needed_peers_for_file(&self, file_id: usize) -> bool {
        // If we have the full file, don't bother.
        if self.is_file_finished(file_id) {
            return false;
        }
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = &state {
                l.reconnect_all_not_needed_peers();
            }
        });
        true
    }

    /// Wake every peer parked waiting for something to request. Called when a
    /// stream opens or moves, since both change what the torrent needs next.
    pub(crate) fn notify_new_pieces_available(&self) {
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = &state {
                l.notify_new_pieces_available();
            }
        });
    }

    fn is_file_finished(&self, file_id: usize) -> bool {
        let metadata = self.metadata.load();
        let metadata = match metadata.as_ref() {
            Some(r) => r,
            None => return false,
        };
        // TODO: would be nice to remove locking
        self.with_chunk_tracker(|ct| ct.is_file_finished(&metadata.file_infos[file_id]))
            .unwrap_or(false)
    }

    pub async fn stream(self: Arc<Self>, file_id: usize) -> anyhow::Result<FileStream> {
        let metadata = self
            .metadata
            .load_full()
            .context("torrent metadata is not resolved")?;
        let (fd_len, fd_offset) = self.with_storage_and_file(
            file_id,
            |_fd, fi| (fi.len, fi.offset_in_torrent),
            &metadata,
        )?;
        let streams = self.streams()?;
        let blocking_permit = self.shared().spawner.semaphore().acquire_owned().await?;
        let s = FileStream {
            stream_id: streams.next_id(),
            streams: streams.clone(),
            file_id,
            position: 0,

            file_len: fd_len,
            file_torrent_abs_offset: fd_offset,
            subpiece_reads: *SUBPIECE_READS,
            _blocking_permit: blocking_permit,
            torrent: self,
            metadata,
        };
        s.torrent.maybe_reconnect_needed_peers_for_file(file_id);
        streams.streams.insert(
            s.stream_id,
            StreamState {
                file_id,
                position: 0,
                waker: None,
                file_len: fd_len,
                file_abs_offset: fd_offset,
            },
        );

        debug!(stream_id = s.stream_id, file_id, "started stream");
        s.torrent.notify_new_pieces_available();

        Ok(s)
    }
}

impl FileStream {
    pub fn position(&self) -> u64 {
        self.position
    }

    fn advance(&mut self, diff: u64) {
        self.set_position(self.position + diff)
    }

    fn set_position(&mut self, new_pos: u64) {
        self.position = new_pos;
        self.streams
            .streams
            .get_mut(&self.stream_id)
            .unwrap()
            .value_mut()
            .position = new_pos;
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }
}
