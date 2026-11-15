//! The one-pass object being written while the outputs are collected (`--object-format
//! one-pass`, docs/design/one-pass-task-object.md).
//!
//! A [`Spool`] owns the open `pack.part` file as a [`kbf_task_pack::one_pass::ObjectWriter`].
//! Collection feeds it two ways, both under one lock so the object is one stream of whole blobs:
//!
//! - a blob held in memory (a Directory, a Tree, a file of at most one chunk, stdout) comes
//!   through [`Spool::put`], and is written only if its digest is new;
//! - a larger file comes through the [`Tee`] the spool is for `kbf-outputs`: its bytes are
//!   written as they are hashed ([`TeeFile::write`]) and it is indexed when its digest is
//!   known ([`TeeFile::end`]). The tee holds the lock from its open to its end, so no other
//!   blob is written between the bytes of one.
//!
//! [`Spool::finish`] then writes the manifest, the index and the trailer. A write the file
//! refused is remembered ([`Spool::write_failure`]) so the run fails as a put (exit 13), not as
//! an unreadable output.

use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use kbf_outputs::{Tee, TeeFile};
use kbf_proto::reapi;
use kbf_task_pack::TaskManifest;
use kbf_task_pack::one_pass::{ObjectWriter, Streaming, Written};
use kbf_types::Digest;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::digest;

/// The bytes buffered for the file at one time.
const BUFFER_BYTES: usize = 1 << 20;

/// What the object's file is written through.
type Sink = BufWriter<Box<dyn Write + Send>>;

type Writer = ObjectWriter<Sink>;

/// The first error the file gave a write, shared with the files being teed.
type Failure = Arc<StdMutex<Option<io::Error>>>;

/// Remembers `error` if it is the first, and returns it.
fn record(failure: &Failure, error: io::Error) -> io::Error {
    let mut first = failure.lock().unwrap_or_else(PoisonError::into_inner);
    if first.is_none() {
        *first = Some(io::Error::new(error.kind(), error.to_string()));
    }
    error
}

/// The object being written.
pub struct Spool {
    writer: Arc<Mutex<Writer>>,
    failure: Failure,
}

impl Spool {
    /// A spool writing to `file`, which is empty.
    #[must_use]
    pub fn new(file: Box<dyn Write + Send>) -> Arc<Self> {
        let sink = BufWriter::with_capacity(BUFFER_BYTES, file);
        Arc::new(Self {
            writer: Arc::new(Mutex::new(ObjectWriter::new(sink))),
            failure: Failure::default(),
        })
    }

    /// The first error the file gave a write, if any.
    #[must_use]
    pub fn write_failure(&self) -> Option<io::Error> {
        let first = self.failure.lock().unwrap_or_else(PoisonError::into_inner);
        first
            .as_ref()
            .map(|error| io::Error::new(error.kind(), error.to_string()))
    }

    /// Writes `bytes`, the blob `digest` names, unless it is empty or already in the object.
    /// The file is written on the blocking pool.
    ///
    /// # Errors
    /// The file refused the bytes, or the digest is not a SHA-256 digest.
    pub async fn put(&self, digest: &reapi::Digest, bytes: Vec<u8>) -> io::Result<()> {
        let digest = digest::from_reapi(digest).map_err(io::Error::other)?;
        let (writer, failure) = (Arc::clone(&self.writer), Arc::clone(&self.failure));
        let done = tokio::task::spawn_blocking(move || {
            let mut writer = writer.blocking_lock();
            (writer.blob(&digest, &bytes).map(drop)).map_err(|error| record(&failure, error))
        })
        .await;
        done.map_err(io::Error::other)?
    }

    /// The first digest `manifest` names that the object does not hold, if any. The empty blob is
    /// implicit and never missing.
    pub async fn missing(&self, manifest: &TaskManifest) -> Option<Digest> {
        let writer = self.writer.lock().await;
        manifest
            .output_digests()
            .find(|digest| digest.size_bytes != 0 && !writer.holds(digest))
    }

    /// Ends the object with `manifest` (its JSON), the index and the trailer, flushes it, and
    /// returns what was written. Every other holder of the spool must be gone.
    ///
    /// # Errors
    /// The file refused a write, or something still holds the spool.
    pub fn finish(self: Arc<Self>, manifest: &[u8]) -> io::Result<Written> {
        let busy = || io::Error::other("the object is still being written");
        let this = Arc::try_unwrap(self).map_err(|_| busy())?;
        let writer = Arc::try_unwrap(this.writer)
            .map_err(|_| busy())?
            .into_inner();
        let record = |error| record(&this.failure, error);
        let (mut sink, written) = writer.finish(manifest).map_err(record)?;
        sink.flush().map_err(record)?;
        Ok(written)
    }
}

impl Tee for Spool {
    fn open(&self) -> io::Result<Box<dyn TeeFile>> {
        // Called on the blocking pool; waits for a blob being written by `put`.
        let guard = Arc::clone(&self.writer).blocking_lock_owned();
        let at = guard.begin();
        Ok(Box::new(Streamed {
            guard,
            at,
            failure: Arc::clone(&self.failure),
        }))
    }
}

/// One file being teed into the object: it holds the writer from its open to its end.
struct Streamed {
    guard: OwnedMutexGuard<Writer>,
    at: Streaming,
    failure: Failure,
}

impl TeeFile for Streamed {
    fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        (self.guard.chunk(&mut self.at, bytes)).map_err(|error| record(&self.failure, error))
    }

    fn end(self: Box<Self>, digest: &reapi::Digest) -> io::Result<()> {
        let digest = digest::from_reapi(digest).map_err(io::Error::other)?;
        let Self { mut guard, at, .. } = *self;
        guard.end(at, &digest).map(drop)
    }
}

#[cfg(test)]
#[path = "spool_tests.rs"]
mod tests;
