//! The spool: blobs from memory and teed files in one object, and what it says when it cannot.

use std::sync::{Arc, Mutex};

use kbf_segments::sha256;
use kbf_task_pack::one_pass::{TRAILER_LEN, Tail, Trailer};

use super::*;
use crate::digest::to_reapi;

/// A file the test can read back.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A file that refuses every write.
struct Refusing;

impl Write for Refusing {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(libc::ENOSPC))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSPC))
    }
}

/// The entries of the finished object `bytes`.
fn entries(bytes: &[u8]) -> Vec<kbf_segments::IndexEntry> {
    let len = bytes.len() as u64;
    let trailer = Trailer::parse(&bytes[bytes.len() - TRAILER_LEN..], len).expect("trailer");
    let tail = &bytes[bytes.len() - usize::try_from(trailer.tail_len()).expect("fits")..];
    Tail::parse(&trailer, tail).expect("tail").entries
}

/// A manifest whose standard output is `stdout`.
fn manifest_naming(stdout: kbf_types::Digest) -> TaskManifest {
    TaskManifest {
        lease: "lease-1".to_owned(),
        action: sha256(b"action"),
        exit_code: 0,
        stdout: Some(stdout),
        stderr: None,
        output_files: Vec::new(),
        output_directories: Vec::new(),
        output_symlinks: Vec::new(),
        timing: kbf_task_pack::Timing::default(),
        usage: kbf_task_pack::Usage::default(),
        packs: 1,
        large: Vec::new(),
        inputs: None,
        objects: None,
    }
}

/// Opens a tee file on the blocking pool, as the walk does.
async fn open(spool: &Arc<Spool>) -> io::Result<Box<dyn TeeFile>> {
    let spool = Arc::clone(spool);
    tokio::task::spawn_blocking(move || spool.open())
        .await
        .expect("joined")
}

#[tokio::test]
async fn blobs_from_memory_and_a_teed_file_make_one_object() {
    let file = Shared::default();
    let spool = Spool::new(Box::new(file.clone()));
    spool
        .put(&to_reapi(&sha256(b"small")), b"small".to_vec())
        .await
        .expect("put");
    // The same blob again is not written twice.
    spool
        .put(&to_reapi(&sha256(b"small")), b"small".to_vec())
        .await
        .expect("put again");
    let mut tee = open(&spool).await.expect("open");
    tee.write(b"big ").expect("write");
    tee.write(b"file").expect("write");
    tee.end(&to_reapi(&sha256(b"big file"))).expect("end");
    spool.finish(b"{}").expect("finished");
    let bytes = file.0.lock().expect("lock").clone();
    assert_eq!(&bytes[..13], b"smallbig file");
    let held: Vec<_> = entries(&bytes).iter().map(|e| e.digest).collect();
    assert_eq!(held.len(), 2);
    assert!(held.contains(&sha256(b"small")) && held.contains(&sha256(b"big file")));
}

#[tokio::test]
async fn a_blob_put_while_a_file_is_teed_waits_and_lands_after_it() {
    let file = Shared::default();
    let spool = Spool::new(Box::new(file.clone()));
    let mut tee = open(&spool).await.expect("open");
    tee.write(b"aaaa").expect("write");
    let waiting = {
        let spool = Arc::clone(&spool);
        tokio::spawn(async move { spool.put(&to_reapi(&sha256(b"zz")), b"zz".to_vec()).await })
    };
    // `put` cannot take the writer while the file is open: it is still pending when the file's
    // bytes are all in.
    tee.write(b"bbbb").expect("write");
    tee.end(&to_reapi(&sha256(b"aaaabbbb"))).expect("end");
    waiting.await.expect("joined").expect("put");
    spool.finish(b"{}").expect("finished");
    let bytes = file.0.lock().expect("lock").clone();
    assert_eq!(&bytes[..10], b"aaaabbbbzz");
}

#[tokio::test]
async fn what_the_manifest_names_that_the_object_lacks_is_missing() {
    let spool = Spool::new(Box::new(Shared::default()));
    spool
        .put(&to_reapi(&sha256(b"here")), b"here".to_vec())
        .await
        .expect("put");
    let mut manifest = manifest_naming(sha256(b"here"));
    assert_eq!(spool.missing(&manifest).await, None);
    manifest.stdout = Some(sha256(b"gone"));
    assert_eq!(spool.missing(&manifest).await, Some(sha256(b"gone")));
}

#[tokio::test]
async fn a_digest_that_is_not_sha256_text_is_refused_by_put_and_end() {
    let spool = Spool::new(Box::new(Shared::default()));
    let bad = reapi::Digest {
        hash: "NOT-HEX".to_owned(),
        size_bytes: 3,
    };
    assert!(spool.put(&bad, b"abc".to_vec()).await.is_err());
    let mut tee = open(&spool).await.expect("open");
    tee.write(b"abc").expect("write");
    assert!(tee.end(&bad).is_err());
}

#[tokio::test]
async fn a_file_that_refuses_a_write_is_remembered_whichever_step_it_refused() {
    // A refusal while a blob is put.
    let spool = Spool::new(Box::new(Refusing));
    let big = vec![1u8; BUFFER_BYTES + 1];
    let put = spool.put(&to_reapi(&sha256(&big)), big).await;
    assert!(put.is_err());
    assert_eq!(
        spool.write_failure().expect("remembered").kind(),
        io::ErrorKind::StorageFull
    );
    // A refusal while a teed file's bytes are written, and then at the end of the object.
    let spool = Spool::new(Box::new(Refusing));
    let mut tee = open(&spool).await.expect("open");
    assert!(tee.write(&vec![2u8; BUFFER_BYTES + 1]).is_err());
    drop(tee);
    assert!(spool.write_failure().is_some());
    // A refusal while the manifest is written, and while the file is flushed.
    for manifest in [vec![0u8; BUFFER_BYTES + 1], b"{}".to_vec()] {
        let spool = Spool::new(Box::new(Refusing));
        let kept = Arc::clone(&spool.failure);
        assert!(spool.finish(&manifest).is_err());
        assert!(kept.lock().expect("lock").is_some());
    }
}

#[test]
fn a_spool_that_is_still_shared_cannot_be_finished() {
    let spool = Spool::new(Box::new(Shared::default()));
    let other = Arc::clone(&spool);
    let error = spool.finish(b"{}").expect_err("shared");
    assert_eq!(error.to_string(), "the object is still being written");
    drop(other);
}

#[test]
fn a_spool_with_no_failure_reports_none() {
    let spool = Spool::new(Box::new(Shared::default()));
    assert!(spool.write_failure().is_none());
}
