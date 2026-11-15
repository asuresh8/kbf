//! Ingest of a one-pass object (`kbf_pack::one_pass`, docs/design/one-pass-task-object.md): the
//! one object a put-directory task writes with `--object-format one-pass`, in place of packs and
//! large objects. It is an uncompressed ZIP64 archive. [`Ingest::run`] hands it here when the end
//! of pack 0 is the format's end records.
//!
//! What it reads: the last 138 bytes (the end records), then once from the manifest member to
//! the end (the manifest, the file index and the central directory), and no record. In order,
//! each step refusing before the next reads:
//!
//! 1. the end records' offsets must describe the object's length, and the tail one range holds
//!    must be within [`MAX_UNTRUSTED_MANIFEST_BYTES`] and [`UntrustedLimits::max_object_entries`]
//!    records (`Malformed`, `TooLarge`), before it is read;
//! 2. the members' CRC-32s, that every entry is stored (an entry of any other method is
//!    `Malformed`), has its SHA-256 and lies inside the records, and none overlaps (`Malformed`);
//! 3. the manifest, for this lease and action, naming one pack and no large object
//!    (`BadManifest`, `WrongKey`), and only paths the Command declares ([`Ingest::declared`]);
//! 4. every digest the manifest names (the empty one aside) is a record (`Incomplete`);
//! 5. the object is recorded in the pack index with a row for every record, a blob over
//!    [`Packs::max_blob_bytes`] included: the row says where it is, and a read of one, from a
//!    frontend that serves large blobs from the index, is a ranged read of this object;
//! 6. a frontend whose reads of a large blob go to storage nodes has each such blob copied to
//!    them by a ranged read of the object ([`Large::at`]); where the index serves it, the
//!    copy finds the row and reads nothing;
//! 7. the result, written to the action cache last, as in [`Ingest::run`].
//!
//! The proof is [`Proof::Manifest`]'s: a record that is not its digest is dropped by the first
//! read that hashes it.

use kbf_pack::TaskManifest;
use kbf_pack::one_pass::{Entry, Tail, Trailer};
use kbf_pack_index::{BlobDigest, NewPack, PackBlob};

use super::large::Large;
use super::task::{Ingest, Ran, RejectReason, Step, rebuilt};
use super::{IssuedPackKey, MAX_UNTRUSTED_MANIFEST_BYTES};

/// The most bytes of the tail one record may account for (its central directory entry of about
/// 130 bytes and its line of the file index, which names a path).
const BYTES_PER_ENTRY: u64 = 1024;

/// The length of the end of a one-pass object, which is how many bytes of the end of pack 0
/// [`Ingest::run`] reads to tell the formats apart.
pub(super) const TAIL_LEN: usize = kbf_pack::one_pass::TAIL_LEN;

/// Whether `end`, the last bytes of an object, is the end of a one-pass object.
pub(super) fn is_one_pass(end: &[u8]) -> bool {
    kbf_pack::one_pass::looks_like(end)
}

impl Ingest<'_> {
    /// Ingests the one-pass object `first` of `size` bytes, whose last bytes are `end`.
    pub(super) async fn one_pass(&self, first: &IssuedPackKey, size: u64, end: &[u8]) -> Step<Ran> {
        let malformed = |_| RejectReason::Malformed;
        let trailer = Trailer::parse(end, size).map_err(malformed)?;
        if trailer.manifest_len > MAX_UNTRUSTED_MANIFEST_BYTES {
            return Err(RejectReason::TooLarge.into());
        }
        let max_entries = u64::try_from(self.limits.max_object_entries).unwrap_or(u64::MAX);
        if trailer.entry_count > max_entries {
            return Err(RejectReason::TooLarge.into());
        }
        // What one range may hold: the manifest, and for each record at most this many bytes of
        // its central directory entry and its line of the file index.
        let len = trailer.tail_len();
        let most = MAX_UNTRUSTED_MANIFEST_BYTES
            .saturating_add(max_entries.saturating_mul(BYTES_PER_ENTRY));
        if len > most {
            return Err(RejectReason::TooLarge.into());
        }
        let bytes = self.read(&first.object, size - len, len).await?;
        let tail = Tail::parse(&trailer, &bytes).map_err(malformed)?;

        let manifest = TaskManifest::parse_with(&tail.manifest, self.limits.max_large_bytes)
            .map_err(|_| RejectReason::BadManifest)?;
        if manifest.lease != self.lease || manifest.action != *self.action {
            return Err(RejectReason::WrongKey.into());
        }
        self.issued(&manifest)?;
        if manifest.packs != 1 || !manifest.large.is_empty() {
            return Err(RejectReason::BadManifest.into());
        }
        self.declared(&manifest).await?;
        let held = |digest: &kbf_types::Digest| {
            tail.entries
                .binary_search_by(|entry| entry.digest.cmp(digest))
                .is_ok()
        };
        if (manifest.output_digests()).any(|digest| digest.size_bytes != 0 && !held(&digest)) {
            return Err(RejectReason::Incomplete.into());
        }

        self.packs
            .record_put_dir_pack(first, new_pack(first, &tail.entries, size))
            .await?;
        let cap = self.packs.max_blob_bytes();
        let mut copied = 0;
        for entry in tail.entries.iter().filter(|e| e.digest.size_bytes > cap) {
            Large::at(self.packs, first, entry.offset, entry.digest)
                .store(self.backend)
                .await?;
            copied += entry.digest.size_bytes;
        }
        let result = rebuilt(&manifest, self.packs.now());
        let cached = self.cached(&result, manifest.exit_code).await?;
        Ok(Ran {
            result,
            cached,
            packs: 1,
            large_bytes: copied,
        })
    }
}

/// The pack index's record of the object: a row for each entry of its index.
fn new_pack(key: &IssuedPackKey, entries: &[Entry], size: u64) -> NewPack {
    let blob = |entry: &Entry| PackBlob {
        digest: BlobDigest {
            hash: entry.digest.hash,
            size: entry.digest.size_bytes,
        },
        offset: entry.offset,
    };
    NewPack {
        key: key.key().to_owned(),
        writer: key.build().to_owned(),
        size,
        blobs: entries.iter().map(blob).collect(),
    }
}
