//! A store with a tee: a file larger than one chunk goes to the tee as it is hashed, is read
//! once, and is never handed to `put_file`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use kbf_outputs::{
    CHUNK_BYTES, Exceeded, OutputLimits, OutputsError, Store, StoreError, Tee, TeeFile, collect,
    digest_of, store_file,
};
use kbf_proto::reapi::{ActionResult, Digest};

/// What the tee saw: each ended file's bytes by digest, and the files begun and abandoned.
#[derive(Default)]
struct Seen {
    ended: BTreeMap<String, Vec<u8>>,
    opened: usize,
}

/// Where the tee fails, if anywhere.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    Open,
    Write,
    /// The second write fails: the first chunk after the head.
    WriteLater,
    End,
}

struct Spool {
    seen: Arc<Mutex<Seen>>,
    fault: Fault,
}

struct One {
    seen: Arc<Mutex<Seen>>,
    bytes: Vec<u8>,
    fault: Fault,
    writes: usize,
}

impl Tee for Spool {
    fn open(&self) -> std::io::Result<Box<dyn TeeFile>> {
        if self.fault == Fault::Open {
            return Err(std::io::Error::other("no room to open"));
        }
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .opened += 1;
        Ok(Box::new(One {
            seen: Arc::clone(&self.seen),
            bytes: Vec::new(),
            fault: self.fault,
            writes: 0,
        }))
    }
}

impl TeeFile for One {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.writes += 1;
        let refused = match self.fault {
            Fault::Write => true,
            Fault::WriteLater => self.writes == 2,
            _ => false,
        };
        if refused {
            return Err(std::io::Error::other("no room to write"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn end(self: Box<Self>, digest: &Digest) -> std::io::Result<()> {
        if self.fault == Fault::End {
            return Err(std::io::Error::other("no room to end"));
        }
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ended
            .insert(digest.hash.clone(), self.bytes);
        Ok(())
    }
}

/// A store that keeps small blobs, counts `put_file`, and tees large files.
struct TeeStore {
    spool: Arc<Spool>,
    seen: Arc<Mutex<Seen>>,
    small: Mutex<BTreeMap<String, Vec<u8>>>,
    put_files: Mutex<usize>,
}

impl TeeStore {
    fn new(fault: Fault) -> Self {
        let seen = Arc::new(Mutex::new(Seen::default()));
        Self {
            spool: Arc::new(Spool {
                seen: Arc::clone(&seen),
                fault,
            }),
            seen,
            small: Mutex::default(),
            put_files: Mutex::new(0),
        }
    }
}

impl Store for TeeStore {
    async fn put(&self, bytes: Vec<u8>) -> Result<Digest, StoreError> {
        let digest = digest_of(&bytes);
        self.small
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(digest.hash.clone(), bytes);
        Ok(digest)
    }

    async fn put_file(&self, _: std::fs::File, digest: Digest) -> Result<Digest, StoreError> {
        *self
            .put_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        Ok(digest)
    }

    fn in_flight(&self) -> usize {
        4
    }

    fn tee(&self) -> Option<Arc<dyn Tee>> {
        Some(Arc::clone(&self.spool) as Arc<dyn Tee>)
    }
}

fn scratch(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("test binary");
    let dir = exe
        .parent()
        .expect("deps directory")
        .join("kbf-outputs-tee-tests")
        .join(name);
    if dir.exists() {
        kbf_outputs::remove_tree(&dir).expect("clear old scratch");
    }
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

fn large(seed: u8) -> Vec<u8> {
    (0..(CHUNK_BYTES * 2 + 7))
        .map(|i| (i % 251) as u8 ^ seed)
        .collect()
}

async fn run(
    store: &TeeStore,
    root: &std::path::Path,
    paths: &[&str],
    limits: OutputLimits,
) -> Result<ActionResult, OutputsError> {
    let paths: Vec<String> = paths.iter().map(|p| (*p).to_owned()).collect();
    let mut result = ActionResult::default();
    collect(store, root, "", &paths, limits, &mut result).await?;
    Ok(result)
}

#[tokio::test]
async fn a_large_file_goes_to_the_tee_whole_and_is_not_put_as_a_file() {
    let root = scratch("whole");
    let (one, two) = (large(1), large(2));
    std::fs::create_dir(root.join("dir")).expect("mkdir");
    std::fs::write(root.join("big"), &one).expect("write");
    std::fs::write(root.join("dir/big"), &two).expect("write");
    std::fs::write(root.join("dir/small"), b"small").expect("write");
    let store = TeeStore::new(Fault::None);
    let result = run(&store, &root, &["big", "dir"], OutputLimits::DEFAULT)
        .await
        .expect("collect");
    assert_eq!(result.output_files[0].digest, Some(digest_of(&one)));
    let seen = store.seen.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(seen.ended[&digest_of(&one).hash], one);
    assert_eq!(seen.ended[&digest_of(&two).hash], two);
    assert_eq!(seen.ended.len(), 2, "the small file did not go to the tee");
    assert_eq!(seen.opened, 2);
    assert_eq!(
        *store
            .put_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
        0
    );
    let small = store.small.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(small.contains_key(&digest_of(b"small").hash));
}

#[tokio::test]
async fn store_file_tees_a_large_log() {
    let dir = scratch("log");
    let log = large(3);
    std::fs::write(dir.join("log"), &log).expect("write");
    let store = TeeStore::new(Fault::None);
    let digest = store_file(&store, &dir.join("log"), u64::MAX)
        .await
        .expect("stored");
    assert_eq!(digest, digest_of(&log));
    let seen = store.seen.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(seen.ended[&digest.hash], log);
    assert_eq!(
        *store
            .put_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
        0
    );
}

#[tokio::test]
async fn a_tee_that_fails_fails_the_collection() {
    for fault in [Fault::Open, Fault::Write, Fault::WriteLater, Fault::End] {
        let root = scratch("fault");
        std::fs::write(root.join("big"), large(4)).expect("write");
        let store = TeeStore::new(fault);
        let why = run(&store, &root, &["big"], OutputLimits::DEFAULT)
            .await
            .expect_err("the tee failed");
        assert!(matches!(why, OutputsError::Io { .. }), "{why}");
    }
}

#[tokio::test]
async fn a_file_past_the_byte_limit_fails_even_after_it_was_teed() {
    let root = scratch("limit");
    std::fs::write(root.join("big"), large(5)).expect("write");
    let store = TeeStore::new(Fault::None);
    let limits = OutputLimits {
        max_bytes: CHUNK_BYTES as u64 + 10,
        ..OutputLimits::DEFAULT
    };
    let why = run(&store, &root, &["big"], limits)
        .await
        .expect_err("over the limit");
    assert!(
        matches!(
            why,
            OutputsError::Limit {
                what: Exceeded::Bytes,
                ..
            }
        ),
        "{why}"
    );
}
