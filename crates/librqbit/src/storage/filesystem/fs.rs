use std::{
    collections::HashSet,
    ffi::OsString,
    fs::{File, OpenOptions},
    io::IoSlice,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use tracing::warn;

use crate::{
    storage::{StorageFactoryExt, filesystem::opened_file::OurFileExt},
    torrent_state::{ManagedTorrentShared, TorrentMetadata, env_flag},
};

use crate::storage::{StorageFactory, TorrentStorage};

use super::opened_file::OpenedFile;

/// Keep a file under `<name>.part` until every piece of it is validated, then
/// rename it to its final name. On by default; RQBIT_PART_NAMING=0 opens every
/// file at its final name and renames nothing, the upstream behaviour.
static PART_NAMING: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| env_flag("RQBIT_PART_NAMING", true));

const PART_SUFFIX: &str = ".part";

#[derive(Clone, Copy)]
pub struct FilesystemStorageFactory {
    part_naming: bool,
}

impl Default for FilesystemStorageFactory {
    fn default() -> Self {
        Self {
            part_naming: *PART_NAMING,
        }
    }
}

impl FilesystemStorageFactory {
    pub fn with_part_naming(part_naming: bool) -> Self {
        Self { part_naming }
    }
}

impl StorageFactory for FilesystemStorageFactory {
    type Storage = FilesystemStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<FilesystemStorage> {
        Ok(FilesystemStorage {
            output_folder: shared.options.output_folder.clone(),
            opened_files: Default::default(),
            part_naming: self.part_naming,
        })
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.boxed()
    }
}

pub struct FilesystemStorage {
    pub(crate) output_folder: PathBuf,
    pub(crate) opened_files: Vec<OpenedFile>,
    part_naming: bool,
}

/// One file of a torrent, as `init` reads it off the metadata.
pub(crate) struct FileToOpen<'a> {
    pub relative: &'a Path,
    pub len: u64,
    pub padding: bool,
}

/// `<name>.part`, or `<name>.<file_id>.part` when the torrent itself ships a
/// file under the plain one. The parent is kept, so a rename never crosses a
/// directory.
fn part_name(path: &Path, disambiguator: Option<usize>) -> PathBuf {
    let mut name = path.file_name().map(OsString::from).unwrap_or_default();
    if let Some(id) = disambiguator {
        name.push(format!(".{id}"));
    }
    name.push(PART_SUFFIX);
    path.with_file_name(name)
}

/// Collision key. Case-folded, because the filesystems of two of the three
/// platforms are case-insensitive; on the third the worst case is a needless
/// escalation, deterministic and harmless.
fn folded(path: &Path) -> String {
    path.to_string_lossy().to_lowercase()
}

fn open_rw(path: &Path, create_new: bool) -> anyhow::Result<File> {
    if create_new {
        // create_new does not seem to work with read(true), so calling this twice.
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .with_context(|| {
                format!("error creating a new file (because allow_overwrite = false) {path:?}")
            })?;
        Ok(OpenOptions::new().read(true).write(true).open(path)?)
    } else {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("error opening {path:?} in read/write mode"))
    }
}

impl FilesystemStorage {
    #[allow(dead_code)]
    pub(crate) fn take_fs(&self) -> anyhow::Result<Self> {
        Ok(Self {
            opened_files: self
                .opened_files
                .iter()
                .map(|f| f.take_clone())
                .collect::<anyhow::Result<Vec<_>>>()?,
            output_folder: self.output_folder.clone(),
            part_naming: self.part_naming,
        })
    }

    /// Open every file of the torrent. An incomplete file lives under its
    /// `.part` name and a complete one under its final name, and which of the
    /// two a file is at now is decided here from what the disk holds: a final
    /// name present is adopted as is (the check that follows demotes it if it
    /// is not complete), otherwise the `.part` name is opened or created. A
    /// declared-empty file is complete by definition and opens at its final
    /// name. A file that cannot take a `.part` name at all (the torrent owns
    /// every candidate, or a directory sits at it) opens at its final name,
    /// the upstream behaviour, for that one file. With `allow_overwrite` off,
    /// either name present is a refusal.
    pub(crate) fn open_all<'a>(
        &mut self,
        files: impl IntoIterator<Item = FileToOpen<'a>>,
        allow_overwrite: bool,
    ) -> anyhow::Result<()> {
        let files = files.into_iter().collect::<Vec<_>>();
        // What a .part candidate may not collide with: every final name of
        // the torrent, every directory those names live under, and every
        // .part name already handed out.
        let mut taken = HashSet::new();
        for f in files.iter().filter(|f| !f.padding) {
            taken.insert(folded(f.relative));
            let mut ancestor = f.relative.parent();
            while let Some(dir) = ancestor {
                if dir.as_os_str().is_empty() {
                    break;
                }
                taken.insert(folded(dir));
                ancestor = dir.parent();
            }
        }
        let mut opened = Vec::with_capacity(files.len());
        for (file_id, file) in files.iter().enumerate() {
            if file.padding {
                opened.push(OpenedFile::new_dummy());
                continue;
            }
            let final_path = self.output_folder.join(file.relative);
            std::fs::create_dir_all(final_path.parent().context("bug: no parent")?)?;

            let part_rel = if !self.part_naming || file.len == 0 {
                None
            } else {
                let plain = part_name(file.relative, None);
                if !taken.contains(&folded(&plain)) {
                    Some(plain)
                } else {
                    let escalated = part_name(file.relative, Some(file_id));
                    if !taken.contains(&folded(&escalated)) {
                        warn!(
                            file = ?file.relative,
                            part = ?escalated,
                            "the plain .part name belongs to the torrent, using an escalated one"
                        );
                        Some(escalated)
                    } else {
                        warn!(
                            file = ?file.relative,
                            "the torrent owns every .part candidate for this file, keeping it at its final name"
                        );
                        None
                    }
                }
            };
            let part_rel = match part_rel {
                Some(rel) => {
                    let on_disk = self.output_folder.join(&rel);
                    if on_disk.is_dir() {
                        warn!(
                            part = ?on_disk,
                            "a directory sits at this file's .part name, keeping the file at its final name"
                        );
                        None
                    } else {
                        Some(rel)
                    }
                }
                None => None,
            };

            let (open_path, part_path, at_part) = match part_rel {
                None => (final_path.clone(), final_path.clone(), false),
                Some(rel) => {
                    taken.insert(folded(&rel));
                    let part_path = self.output_folder.join(&rel);
                    let final_exists = final_path.try_exists()?;
                    let part_exists = part_path.try_exists()?;
                    if !allow_overwrite && (final_exists || part_exists) {
                        let existing = if final_exists {
                            &final_path
                        } else {
                            &part_path
                        };
                        bail!(
                            "error creating a new file (because allow_overwrite = false): {existing:?} already exists"
                        );
                    }
                    if final_exists && part_exists {
                        warn!(
                            ?final_path,
                            ?part_path,
                            "both names exist; opening the final one, and no rename will replace the .part copy, so the file keeps its final name until one of the two goes"
                        );
                    }
                    let at_part = !final_exists;
                    let open_path = if at_part {
                        part_path.clone()
                    } else {
                        final_path.clone()
                    };
                    (open_path, part_path, at_part)
                }
            };
            let f = open_rw(&open_path, !allow_overwrite)?;
            opened.push(OpenedFile::new(final_path, part_path, at_part, f));
        }

        self.opened_files = opened;
        Ok(())
    }
}

impl TorrentStorage for FilesystemStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.opened_files
            .get(file_id)
            .context("no such file")?
            .lock_read()?
            .pread_exact(offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let of = self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        return of.try_mark_sparse()?.pwrite_all(offset, buf);
        #[cfg(not(windows))]
        return of.lock_read()?.pwrite_all(offset, buf);
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let of = self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        return of.try_mark_sparse()?.pwrite_all_vectored(offset, bufs);
        #[cfg(not(windows))]
        return of.lock_read()?.pwrite_all_vectored(offset, bufs);
    }

    /// A storage that opened the torrent removes exactly the name the file
    /// sits at. Only a storage built without init, as the delete path can
    /// hold, probes the names the file could be at, final first, and stops
    /// at the first hit, so nothing beyond this torrent's own file is ever
    /// touched.
    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        if let Some(current) = self
            .opened_files
            .get(file_id)
            .and_then(|of| of.current_path_if_any())
        {
            return std::fs::remove_file(&current)
                .with_context(|| format!("error removing {current:?}"));
        }
        let final_path = self.output_folder.join(filename);
        let mut candidates = vec![final_path.clone()];
        if self.part_naming {
            candidates.push(part_name(&final_path, None));
            candidates.push(part_name(&final_path, Some(file_id)));
        }
        for path in &candidates {
            match std::fs::remove_file(path) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("error removing {path:?}")),
            }
        }
        bail!("{final_path:?} is not there under any of its names")
    }

    fn ensure_file_length(&self, file_id: usize, len: u64) -> anyhow::Result<()> {
        let f = &self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        f.try_mark_sparse()?;
        Ok(f.lock_read()?.set_len(len)?)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self {
            opened_files: self
                .opened_files
                .iter()
                .map(|f| f.take_clone())
                .collect::<anyhow::Result<Vec<_>>>()?,
            output_folder: self.output_folder.clone(),
            part_naming: self.part_naming,
        }))
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        let path = self.output_folder.join(path);
        if !path.is_dir() {
            anyhow::bail!("cannot remove dir: {path:?} is not a directory")
        }
        if std::fs::read_dir(&path)?.count() == 0 {
            std::fs::remove_dir(&path).with_context(|| format!("error removing {path:?}"))
        } else {
            warn!("did not remove {path:?} as it was not empty");
            Ok(())
        }
    }

    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.open_all(
            metadata.file_infos.iter().map(|fi| FileToOpen {
                relative: &fi.relative_filename,
                len: fi.len,
                padding: fi.attrs.padding,
            }),
            shared.options.allow_overwrite,
        )
    }

    fn on_file_complete(&self, file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        self.opened_files
            .get(file_id)
            .context("no such file")?
            .promote()
    }

    fn on_file_incomplete(&self, file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        self.opened_files
            .get(file_id)
            .context("no such file")?
            .demote()
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use crate::storage::TorrentStorage;

    use super::{FileToOpen, FilesystemStorage, part_name};

    fn blind(root: &Path, part_naming: bool) -> FilesystemStorage {
        FilesystemStorage {
            output_folder: root.to_path_buf(),
            opened_files: Vec::new(),
            part_naming,
        }
    }

    fn open(
        root: &Path,
        files: &[(&str, u64)],
        allow_overwrite: bool,
        part_naming: bool,
    ) -> anyhow::Result<FilesystemStorage> {
        let mut storage = blind(root, part_naming);
        let specs = files
            .iter()
            .map(|(name, len)| (PathBuf::from(name), *len))
            .collect::<Vec<_>>();
        storage.open_all(
            specs.iter().map(|(relative, len)| FileToOpen {
                relative,
                len: *len,
                padding: false,
            }),
            allow_overwrite,
        )?;
        Ok(storage)
    }

    fn open_default(root: &Path, files: &[(&str, u64)]) -> FilesystemStorage {
        open(root, files, true, true).unwrap()
    }

    fn names(root: &Path) -> Vec<String> {
        fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
                if entry.file_type().unwrap().is_dir() {
                    walk(&entry.path(), &format!("{name}/"), out);
                } else {
                    out.push(name);
                }
            }
        }
        let mut out = Vec::new();
        walk(root, "", &mut out);
        out.sort();
        out
    }

    fn read(storage: &FilesystemStorage, file_id: usize, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        storage.pread_exact(file_id, 0, &mut buf).unwrap();
        buf
    }

    #[test]
    fn part_name_keeps_the_parent_and_appends_the_suffix() {
        assert_eq!(
            part_name(Path::new("a/b/c.mkv"), None),
            PathBuf::from("a/b/c.mkv.part")
        );
        assert_eq!(
            part_name(Path::new("a/b/c.mkv"), Some(7)),
            PathBuf::from("a/b/c.mkv.7.part")
        );
        assert_eq!(part_name(Path::new("c"), None), PathBuf::from("c.part"));
    }

    #[test]
    fn init_creates_part_names() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 10), ("sub/b.bin", 20)]);
        assert_eq!(names(td.path()), vec!["a.mkv.part", "sub/b.bin.part"]);
        assert_eq!(
            storage.opened_files[1].current_path_if_any().unwrap(),
            td.path().join("sub/b.bin.part")
        );
    }

    #[test]
    fn init_adopts_an_existing_final_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"final").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 5)]);
        assert_eq!(names(td.path()), vec!["a.mkv"]);
        assert_eq!(read(&storage, 0, 5), b"final");
    }

    #[test]
    fn init_adopts_an_existing_part_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv.part"), b"half").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 8)]);
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
        assert_eq!(read(&storage, 0, 4), b"half");
    }

    #[test]
    fn init_prefers_the_final_name_when_both_exist() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"final").unwrap();
        std::fs::write(td.path().join("a.mkv.part"), b"stale").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 5)]);
        assert_eq!(read(&storage, 0, 5), b"final");
        assert_eq!(names(td.path()), vec!["a.mkv", "a.mkv.part"]);
    }

    #[test]
    fn promote_renames_and_the_handle_keeps_working() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 8)]);
        storage.pwrite_all(0, 0, b"abcd").unwrap();

        storage.on_file_complete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv"]);
        assert_eq!(read(&storage, 0, 4), b"abcd");

        storage.pwrite_all(0, 4, b"efgh").unwrap();
        assert_eq!(std::fs::read(td.path().join("a.mkv")).unwrap(), b"abcdefgh");
        // Again is a no-op rather than a failed rename.
        storage.on_file_complete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv"]);
    }

    #[test]
    fn demote_moves_a_final_name_to_part() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"trunc").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 10)]);

        storage.on_file_incomplete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
        storage.pwrite_all(0, 5, b"ated").unwrap();
        assert_eq!(read(&storage, 0, 9), b"truncated");
        storage.on_file_incomplete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
    }

    #[test]
    fn demote_refuses_to_replace_a_stale_part_file() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"eighty percent").unwrap();
        std::fs::write(td.path().join("a.mkv.part"), b"stale").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 100)]);

        assert!(storage.on_file_incomplete(0, Path::new("a.mkv")).is_err());
        assert_eq!(
            std::fs::read(td.path().join("a.mkv")).unwrap(),
            b"eighty percent"
        );
        assert_eq!(
            std::fs::read(td.path().join("a.mkv.part")).unwrap(),
            b"stale"
        );
    }

    #[test]
    fn promote_refuses_to_replace_a_file_at_the_final_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("a.mkv", 4)]);
        storage.pwrite_all(0, 0, b"ours").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"them").unwrap();

        assert!(storage.on_file_complete(0, Path::new("a.mkv")).is_err());
        assert_eq!(std::fs::read(td.path().join("a.mkv")).unwrap(), b"them");
        assert_eq!(
            std::fs::read(td.path().join("a.mkv.part")).unwrap(),
            b"ours"
        );
    }

    #[test]
    fn remove_file_removes_the_name_the_file_is_at() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("done.mkv"), b"x").unwrap();
        let storage = open_default(td.path(), &[("done.mkv", 1), ("sub/todo.mkv", 4)]);
        assert_eq!(names(td.path()), vec!["done.mkv", "sub/todo.mkv.part"]);

        storage.remove_file(0, Path::new("done.mkv")).unwrap();
        storage.remove_file(1, Path::new("sub/todo.mkv")).unwrap();
        assert!(names(td.path()).is_empty());
        assert!(storage.remove_file(0, Path::new("done.mkv")).is_err());
    }

    #[test]
    fn a_blind_storage_probes_the_names_in_order_and_stops_at_the_first() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = blind(td.path(), true);
        std::fs::write(td.path().join("x"), b"1").unwrap();
        std::fs::write(td.path().join("x.part"), b"2").unwrap();
        std::fs::write(td.path().join("x.0.part"), b"3").unwrap();

        storage.remove_file(0, Path::new("x")).unwrap();
        assert_eq!(names(td.path()), vec!["x.0.part", "x.part"]);
        storage.remove_file(0, Path::new("x")).unwrap();
        assert_eq!(names(td.path()), vec!["x.0.part"]);
        storage.remove_file(0, Path::new("x")).unwrap();
        assert!(names(td.path()).is_empty());
        assert!(storage.remove_file(0, Path::new("x")).is_err());

        // With the switch off, .part files are not this build's to delete.
        let upstream = blind(td.path(), false);
        std::fs::write(td.path().join("y.part"), b"foreign").unwrap();
        assert!(upstream.remove_file(0, Path::new("y")).is_err());
        assert_eq!(names(td.path()), vec!["y.part"]);
    }

    #[test]
    fn a_declared_empty_file_lands_at_its_final_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("empty.txt", 0), ("a.mkv", 4)]);
        assert_eq!(names(td.path()), vec!["a.mkv.part", "empty.txt"]);
        storage
            .on_file_incomplete(0, Path::new("empty.txt"))
            .unwrap();
        storage.on_file_complete(0, Path::new("empty.txt")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part", "empty.txt"]);
    }

    #[test]
    fn no_overwrite_refuses_either_existing_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::write(td.path().join("a.mkv"), b"x").unwrap();
        assert!(open(td.path(), &[("a.mkv", 4)], false, true).is_err());
        std::fs::remove_file(td.path().join("a.mkv")).unwrap();

        std::fs::write(td.path().join("a.mkv.part"), b"x").unwrap();
        assert!(open(td.path(), &[("a.mkv", 4)], false, true).is_err());
        std::fs::remove_file(td.path().join("a.mkv.part")).unwrap();

        let storage = open(td.path(), &[("a.mkv", 4)], false, true).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
        storage.pwrite_all(0, 0, b"abcd").unwrap();
        assert_eq!(read(&storage, 0, 4), b"abcd");
    }

    #[test]
    fn a_torrent_shipping_the_part_name_escalates_deterministically() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("x", 4), ("x.part", 4)]);
        assert_eq!(names(td.path()), vec!["x.0.part", "x.part.part"]);

        storage.on_file_complete(1, Path::new("x.part")).unwrap();
        assert_eq!(names(td.path()), vec!["x.0.part", "x.part"]);
        storage.on_file_complete(0, Path::new("x")).unwrap();
        assert_eq!(names(td.path()), vec!["x", "x.part"]);
        storage.on_file_incomplete(0, Path::new("x")).unwrap();
        assert_eq!(names(td.path()), vec!["x.0.part", "x.part"]);

        // Each file's removal touches exactly the name it sits at.
        storage.remove_file(0, Path::new("x")).unwrap();
        assert_eq!(names(td.path()), vec!["x.part"]);
        storage.remove_file(1, Path::new("x.part")).unwrap();
        assert!(names(td.path()).is_empty());
    }

    #[test]
    fn a_file_with_no_free_part_name_stays_at_its_final_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("y", 4), ("y.part", 4), ("y.0.part", 4)]);
        assert_eq!(names(td.path()), vec!["y", "y.0.part.part", "y.part.part"]);
        // The fallback file has no .part identity: renames are no-ops.
        storage.on_file_incomplete(0, Path::new("y")).unwrap();
        storage.on_file_complete(0, Path::new("y")).unwrap();
        assert_eq!(names(td.path()), vec!["y", "y.0.part.part", "y.part.part"]);
    }

    #[test]
    fn collisions_are_judged_case_insensitively() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open_default(td.path(), &[("x", 4), ("X.PART", 4)]);
        let got = names(td.path());
        // On a case-insensitive filesystem X.PART.part may list either way;
        // what matters is that file 0 escalated away from x.part.
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got.contains(&"x.0.part".to_owned()), "{got:?}");
        storage.on_file_complete(0, Path::new("x")).unwrap();
        assert!(names(td.path()).contains(&"x".to_owned()));
    }

    #[test]
    fn a_torrent_directory_never_becomes_a_part_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let _storage = open_default(td.path(), &[("x", 4), ("x.part/y", 4)]);
        assert_eq!(names(td.path()), vec!["x.0.part", "x.part/y.part"]);
    }

    #[test]
    fn a_directory_on_disk_at_the_part_name_keeps_the_file_final() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        std::fs::create_dir(td.path().join("z.part")).unwrap();
        let storage = open_default(td.path(), &[("z", 4)]);
        assert_eq!(names(td.path()), vec!["z"]);
        storage.on_file_incomplete(0, Path::new("z")).unwrap();
        assert_eq!(names(td.path()), vec!["z"]);
    }

    #[test]
    fn the_switch_off_keeps_upstream_naming() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let storage = open(td.path(), &[("a.mkv", 8), ("sub/b.bin", 8)], true, false).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv", "sub/b.bin"]);
        storage.on_file_incomplete(0, Path::new("a.mkv")).unwrap();
        storage.on_file_complete(1, Path::new("sub/b.bin")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv", "sub/b.bin"]);
    }

    #[test]
    fn padding_files_take_no_name() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let mut storage = blind(td.path(), true);
        storage
            .open_all(
                [
                    FileToOpen {
                        relative: Path::new("a.mkv"),
                        len: 4,
                        padding: false,
                    },
                    FileToOpen {
                        relative: Path::new(".pad/16"),
                        len: 16,
                        padding: true,
                    },
                ],
                true,
            )
            .unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
        storage.on_file_complete(1, Path::new(".pad/16")).unwrap();
        storage.on_file_incomplete(1, Path::new(".pad/16")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);
    }

    #[test]
    fn take_moves_the_names_with_the_handles() {
        let td = TempDir::with_prefix("part_naming").unwrap();
        let before = open_default(td.path(), &[("a.mkv", 4)]);
        before.pwrite_all(0, 0, b"abcd").unwrap();
        let after = before.take().unwrap();

        // The taken-from storage is inert.
        before.on_file_complete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv.part"]);

        after.on_file_complete(0, Path::new("a.mkv")).unwrap();
        assert_eq!(names(td.path()), vec!["a.mkv"]);
        let mut buf = [0u8; 4];
        after.pread_exact(0, 0, &mut buf).unwrap();
        assert_eq!(&buf, b"abcd");
    }
}
