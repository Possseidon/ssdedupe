use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    hash::{BuildHasher, Hash, Hasher},
    io::{BufRead, BufReader},
    iter::once,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicBool, AtomicU64},
    },
};

use compact_str::CompactString;
use itertools::Itertools;
use rayon::iter::{ParallelBridge, ParallelIterator};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Entry {
    Dir(Dir),
    File(EntryInfo),
}

impl Entry {
    pub fn dir(entries: BTreeMap<CompactString, Entry>) -> Self {
        Self::Dir(Dir {
            info: EntryInfo::dir(entries.values().map(|entry| entry.info())),
            dirs: 1 + entries.values().map(|entry| entry.dirs()).sum::<u64>(),
            files: entries.values().map(|entry| entry.files()).sum(),
            entries,
        })
    }

    pub fn scan(path: impl AsRef<Path>, state: &ScanState) -> Option<Self> {
        if state.canceled() {
            return None;
        }

        let path = path.as_ref();
        let metadata = match path.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                state.log(format!(
                    "failed to read metadata of {}: {error}",
                    path.display()
                ));
                return None;
            }
        };

        if metadata.is_file() {
            let file = match File::open(path) {
                Ok(file) => file,
                Err(error) => {
                    state.log(format!("failed to open {}: {error}", path.display()));
                    return None;
                }
            };

            let mut buf_reader = BufReader::new(file);

            let mut hasher = FIXED_RANDOM_STATE.build_hasher();
            let mut bytes = 0;
            while let buf = match buf_reader.fill_buf() {
                Ok(buf) => buf,
                Err(error) => {
                    state.log(format!("failed to read {}: {error}", path.display()));
                    return None;
                }
            } && !buf.is_empty()
            {
                if state.canceled() {
                    return None;
                }

                hasher.write(buf);
                let buf_len = buf.len();
                let buf_len_u64 = buf_len as u64;
                bytes += buf_len_u64;
                state.add_bytes(buf_len_u64);
                buf_reader.consume(buf_len);
            }

            state.inc_files();

            Some(Self::File(EntryInfo {
                kind: EntryKind::File,
                bytes,
                hash: hasher.finish(),
            }))
        } else if metadata.is_dir() {
            let entries = path
                .read_dir()
                .ok()?
                .filter_map(|dir_entry| match dir_entry {
                    Ok(dir_entry) => Some(dir_entry),
                    Err(error) => {
                        state.log(format!("failed to read dir {}: {error}", path.display()));
                        None
                    }
                })
                .par_bridge()
                .filter_map(|dir_entry| {
                    let file_name = dir_entry.file_name().to_string_lossy().into();
                    Some((file_name, Self::scan(dir_entry.path(), state)?))
                })
                .collect::<BTreeMap<_, _>>();
            state.inc_dirs();
            Some(Self::dir(entries))
        } else {
            state.log(format!("skipped (neither file/dir): {}", path.display()));
            None
        }
    }

    pub fn info(&self) -> EntryInfo {
        match self {
            Self::File(info) => *info,
            Self::Dir(Dir { info, .. }) => *info,
        }
    }

    pub fn dirs(&self) -> u64 {
        match self {
            Self::File(..) => 0,
            Self::Dir(Dir { dirs, .. }) => *dirs,
        }
    }

    pub fn files(&self) -> u64 {
        match self {
            Self::File(..) => 1,
            Self::Dir(Dir { files, .. }) => *files,
        }
    }

    pub fn redundant_bytes(unfiltered_duplicates: &BTreeMap<EntryInfo, BTreeSet<PathBuf>>) -> u64 {
        unfiltered_duplicates
            .iter()
            .filter(|(info, _)| (info.kind == EntryKind::File))
            .map(|(info, paths)| info.bytes * (paths.len() as u64 - 1))
            .sum::<u64>()
    }

    /// Returns all sets of duplicate paths, keyed by their [`EntryInfo`].
    ///
    /// If a duplicate set is already implied by a higher-level set of duplicates, it is omitted.
    ///
    /// For example:
    ///
    /// - Given the sets `a/x.txt; b/y.txt` and `a; b`, the former is omitted, since `a; b` already
    ///   implies that all of their contents match.
    /// - However, `a/x.txt; b/y.txt; z.txt` is **not** omitted, since `z.txt` is not covered by the
    ///   `a; b` prefix and therefore adds new information.
    pub fn filter_duplicates_by_prefix(
        mut duplicates: BTreeMap<EntryInfo, BTreeSet<PathBuf>>,
    ) -> BTreeMap<EntryInfo, BTreeSet<PathBuf>> {
        let prefixes = duplicates.values().cloned().collect::<HashSet<_>>();

        duplicates.retain(|_, paths| {
            let mut paths = paths.clone();
            loop {
                let Some(new_paths) = paths
                    .into_iter()
                    .map(|mut path| (path.pop() && path.file_name().is_some()).then_some(path))
                    .collect()
                else {
                    // at least one path is now root and no prefix was found yet; keep this entry
                    break true;
                };

                if prefixes.contains(&new_paths) {
                    // this prefix already exists; remove it
                    break false;
                }

                paths = new_paths;
            }
        });

        duplicates
    }

    pub fn unfiltered_duplicates(&self) -> BTreeMap<EntryInfo, BTreeSet<PathBuf>> {
        self.hashes()
            .into_grouping_map()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|(_, paths)| paths.len() > 1)
            .collect::<BTreeMap<_, _>>()
    }

    fn hashes(&self) -> impl Iterator<Item = (EntryInfo, PathBuf)> + '_ {
        self.hashes_with_root(PathBuf::new())
    }

    fn hashes_with_root(
        &self,
        path: PathBuf,
    ) -> Box<dyn Iterator<Item = (EntryInfo, PathBuf)> + '_> {
        Box::new(
            once((self.info(), path.clone())).chain(
                match self {
                    Self::File(EntryInfo { .. }) => None,
                    Self::Dir(dir) => Some(dir.entries.iter().map(move |(file_name, entry)| {
                        let mut path = path.clone();
                        path.push(file_name.clone());
                        entry.hashes_with_root(path)
                    })),
                }
                .into_iter()
                .flatten()
                .flatten(),
            ),
        )
    }
}

#[derive(Default)]
pub struct ScanState {
    canceled: AtomicBool,
    bytes: AtomicU64,
    dirs: AtomicU64,
    files: AtomicU64,
    error_log: Mutex<Vec<String>>,
}

impl ScanState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn cancel(&self) {
        self.canceled.store(true, atomic::Ordering::Relaxed);
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(atomic::Ordering::Relaxed)
    }

    pub fn dirs(&self) -> u64 {
        self.dirs.load(atomic::Ordering::Relaxed)
    }

    pub fn files(&self) -> u64 {
        self.files.load(atomic::Ordering::Relaxed)
    }

    /// Returns the last error and how many additional errors there were.
    pub fn last_error_plus(&self) -> Option<(String, usize)> {
        let error_log = self.error_log.lock().unwrap();
        Some((error_log.last()?.clone(), error_log.len() - 1))
    }

    pub fn clone_error_log(&self) -> Vec<String> {
        self.error_log.lock().unwrap().clone()
    }

    fn add_bytes(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, atomic::Ordering::Relaxed);
    }

    fn inc_dirs(&self) {
        self.dirs.fetch_add(1, atomic::Ordering::Relaxed);
    }

    fn inc_files(&self) {
        self.files.fetch_add(1, atomic::Ordering::Relaxed);
    }

    fn log(&self, message: String) {
        self.error_log.lock().unwrap().push(message);
    }

    fn canceled(&self) -> bool {
        self.canceled.load(atomic::Ordering::Relaxed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dir {
    pub info: EntryInfo,
    pub dirs: u64,
    pub files: u64,
    pub entries: BTreeMap<CompactString, Entry>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntryInfo {
    pub bytes: u64,
    pub kind: EntryKind,
    pub hash: u64,
}

impl EntryInfo {
    fn dir(entries: impl Iterator<Item = Self> + Clone) -> Self {
        let mut hashes = entries.clone().map(|x| x.hash).collect_vec();
        // sort hashes so that the order of hashes (order of files) doesn't matter
        hashes.sort();
        Self {
            kind: EntryKind::Dir,
            bytes: entries.map(|x| x.bytes).sum(),
            // marker to prevent empty directories from leading to the same hash as empty files
            hash: FIXED_RANDOM_STATE.hash_one((hashes, 0xBEEE38829F9F8197_u64)),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EntryKind {
    Dir,
    File,
}

const FIXED_RANDOM_STATE: ahash::RandomState = ahash::RandomState::with_seeds(0, 0, 0, 0);
