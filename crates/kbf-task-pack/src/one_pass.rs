//! The one-pass task object: one object per action, written in one pass over the outputs, with
//! the manifest and the index at the end (docs/design/one-pass-task-object.md).
//!
//! ```text
//! blobs      each blob's bytes, in the order they were written (a duplicate's bytes may sit
//!            between two records, unindexed)
//! manifest   the manifest JSON
//! files      the neutral file index (JSON, may be empty): see [`Tail::files`]
//! index      N x 52 bytes, strictly ascending by digest:
//!              hash [32] | size u64 | offset u64 | crc32c u32
//! trailer    64 bytes, little endian:
//!              manifest_offset u64 | manifest_len u64 | index_offset u64 | entry_count u64
//!              | index_crc32c u32 | manifest_crc32c u32 | digest_function u8 | reserved [3]
//!              | version u32 | files_crc32c u32 | trailer_crc32c u32 (of the first 52 bytes)
//!              | magic [8]
//! ```
//!
//! [`ObjectWriter`] writes it through any [`Write`], blob by blob, and ends with the manifest,
//! the index and the trailer. [`Trailer::parse`] and [`Tail::parse`] are the reader: the first
//! from the last [`TRAILER_LEN`] bytes of the object, the second from the bytes the trailer
//! says to fetch next (`manifest_offset` to the end), which hold the manifest and the index.
//! Neither reads a blob. Every rule a reader needs before it trusts the index is checked there.

use std::collections::BTreeMap;
use std::io::{self, Write};

use kbf_segments::{Crc32c, IndexEntry, crc32c};
use kbf_types::{Digest, DigestFunction};
use sha2::{Digest as _, Sha256};

/// The last eight bytes of a one-pass object. Not the magic of a segment (`KBFSEG\r\n`), so
/// neither reader takes the other's object.
pub const MAGIC: [u8; 8] = *b"KBFTASK\n";

/// The layout version this crate writes and reads.
pub const VERSION: u32 = 1;

/// The length of the trailer, the last bytes of every one-pass object.
pub const TRAILER_LEN: usize = 64;

/// The length of one index entry, that of a segment's.
pub const ENTRY_LEN: usize = 52;

/// The trailer's code for SHA-256, that of a segment's.
const SHA256_CODE: u8 = 1;

/// Why an object is not a well-formed one-pass object.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ObjectError {
    /// Fewer bytes than a trailer, or than the trailer says follow the records.
    #[error("one-pass object truncated: needed {needed} bytes, had {available}")]
    Truncated {
        /// Bytes needed.
        needed: u64,
        /// Bytes supplied.
        available: u64,
    },
    /// The last eight bytes are not the magic.
    #[error("not a one-pass object: wrong magic")]
    BadMagic,
    /// A layout version this build does not read.
    #[error("unsupported one-pass version {0}")]
    UnsupportedVersion(u32),
    /// A digest function code this build does not read.
    #[error("unsupported digest function code {0}")]
    UnsupportedDigestFunction(u8),
    /// A reserved field is not zero.
    #[error("one-pass trailer reserved bytes are not zero")]
    ReservedNotZero,
    /// The trailer's own CRC-32C does not match its bytes.
    #[error("one-pass trailer CRC mismatch")]
    TrailerCrc,
    /// The files' CRC-32C does not match their bytes.
    #[error("one-pass files CRC mismatch")]
    FilesCrc,
    /// The offsets of the trailer do not add up to the object's length.
    #[error("one-pass offsets do not describe an object of {actual} bytes")]
    Layout {
        /// The object's length.
        actual: u64,
    },
    /// The manifest's CRC-32C does not match its bytes.
    #[error("one-pass manifest CRC mismatch")]
    ManifestCrc,
    /// The index's CRC-32C does not match its bytes.
    #[error("one-pass index CRC mismatch")]
    IndexCrc,
    /// The entry at `position` does not sort strictly after the one before it.
    #[error("one-pass index out of order at entry {position}")]
    IndexOrder {
        /// The entry's position.
        position: usize,
    },
    /// A record of size 0 (the empty blob is implicit in every store and is never a record).
    #[error("the record at offset {offset} has size 0")]
    ZeroSize {
        /// Where the record starts.
        offset: u64,
    },
    /// A record that is not inside the records.
    #[error("the record {digest} lies outside the records")]
    OutOfBounds {
        /// The record's digest.
        digest: Digest,
    },
    /// Two records share bytes.
    #[error("the records {first} and {second} overlap")]
    Overlap {
        /// The record that starts first.
        first: Digest,
        /// The record that starts inside it.
        second: Digest,
    },
}

/// The fixed end of a one-pass object: where the manifest and the index are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trailer {
    /// Where the manifest starts, which is where the records end.
    pub manifest_offset: u64,
    /// The manifest's length.
    pub manifest_len: u64,
    /// How many bytes of files follow the manifest (0 for none).
    pub files_len: u64,
    /// Where the index starts.
    pub index_offset: u64,
    /// How many entries the index holds.
    pub entry_count: u64,
    index_crc32c: u32,
    manifest_crc32c: u32,
    files_crc32c: u32,
}

impl Trailer {
    fn encode(&self) -> [u8; TRAILER_LEN] {
        let mut out = [0u8; TRAILER_LEN];
        out[0..8].copy_from_slice(&self.manifest_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.manifest_len.to_le_bytes());
        out[16..24].copy_from_slice(&self.index_offset.to_le_bytes());
        out[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        out[32..36].copy_from_slice(&self.index_crc32c.to_le_bytes());
        out[36..40].copy_from_slice(&self.manifest_crc32c.to_le_bytes());
        out[40] = SHA256_CODE;
        out[44..48].copy_from_slice(&VERSION.to_le_bytes());
        out[48..52].copy_from_slice(&self.files_crc32c.to_le_bytes());
        let crc = crc32c(&out[..52]);
        out[52..56].copy_from_slice(&crc.to_le_bytes());
        out[56..64].copy_from_slice(&MAGIC);
        out
    }

    /// Parses `trailer`, the last [`TRAILER_LEN`] bytes of an object of `object_len` bytes, and
    /// checks that its offsets describe that object: the manifest then the index then the
    /// trailer, ending at `object_len`.
    ///
    /// # Errors
    /// [`ObjectError`] for the first rule that fails.
    pub fn parse(trailer: &[u8], object_len: u64) -> Result<Self, ObjectError> {
        if trailer.len() != TRAILER_LEN {
            return Err(ObjectError::Truncated {
                needed: TRAILER_LEN as u64,
                available: trailer.len() as u64,
            });
        }
        if trailer[56..64] != MAGIC {
            return Err(ObjectError::BadMagic);
        }
        let u32_at =
            |at: usize| u32::from_le_bytes(trailer[at..at + 4].try_into().unwrap_or([0; 4]));
        let u64_at =
            |at: usize| u64::from_le_bytes(trailer[at..at + 8].try_into().unwrap_or([0; 8]));
        let version = u32_at(44);
        if version != VERSION {
            return Err(ObjectError::UnsupportedVersion(version));
        }
        if trailer[40] != SHA256_CODE {
            return Err(ObjectError::UnsupportedDigestFunction(trailer[40]));
        }
        if trailer[41..44] != [0; 3] {
            return Err(ObjectError::ReservedNotZero);
        }
        if crc32c(&trailer[..52]) != u32_at(52) {
            return Err(ObjectError::TrailerCrc);
        }
        let mut parsed = Self {
            manifest_offset: u64_at(0),
            manifest_len: u64_at(8),
            files_len: 0,
            index_offset: u64_at(16),
            entry_count: u64_at(24),
            index_crc32c: u32_at(32),
            manifest_crc32c: u32_at(36),
            files_crc32c: u32_at(48),
        };
        let layout = || ObjectError::Layout { actual: object_len };
        let manifest_end = parsed
            .manifest_offset
            .checked_add(parsed.manifest_len)
            .ok_or_else(layout)?;
        let index_len = parsed
            .entry_count
            .checked_mul(ENTRY_LEN as u64)
            .ok_or_else(layout)?;
        let end = parsed
            .index_offset
            .checked_add(index_len)
            .and_then(|end| end.checked_add(TRAILER_LEN as u64))
            .ok_or_else(layout)?;
        if manifest_end > parsed.index_offset || end != object_len {
            return Err(layout());
        }
        parsed.files_len = parsed.index_offset - manifest_end;
        Ok(parsed)
    }

    /// How many bytes from the end of the object hold the manifest, the files, the index and
    /// the trailer: what a reader fetches next.
    #[must_use]
    pub const fn tail_len(&self) -> u64 {
        self.manifest_len
            + self.files_len
            + self.entry_count * ENTRY_LEN as u64
            + TRAILER_LEN as u64
    }
}

/// The manifest and the index of a one-pass object, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tail {
    /// The manifest record's bytes.
    pub manifest: Vec<u8>,
    /// The neutral file index: every output file and symlink with its size, mode, SHA-256 and
    /// offset in this object (docs/design/one-pass-task-object.md the end of section 3). Opaque here;
    /// empty for none.
    pub files: Vec<u8>,
    /// Every record, in digest order.
    pub entries: Vec<IndexEntry>,
}

impl Tail {
    /// Parses `tail`, the last [`Trailer::tail_len`] bytes of the object `trailer` is the end of.
    ///
    /// Checks both CRCs; that entries ascend strictly by digest, none is of size 0, each lies
    /// inside the records (before the manifest) and no two overlap. Gaps between records are
    /// allowed. Reads no blob.
    ///
    /// # Errors
    /// [`ObjectError`] for the first rule that fails.
    pub fn parse(trailer: &Trailer, tail: &[u8]) -> Result<Self, ObjectError> {
        if (tail.len() as u64) != trailer.tail_len() {
            return Err(ObjectError::Truncated {
                needed: trailer.tail_len(),
                available: tail.len() as u64,
            });
        }
        // The lengths were checked against the object by `Trailer::parse` and against `tail`
        // just now, so both fit `usize` on any target that could hold the bytes.
        let manifest_len = usize::try_from(trailer.manifest_len).unwrap_or(usize::MAX);
        let manifest = &tail[..manifest_len];
        let files_len = usize::try_from(trailer.files_len).unwrap_or(usize::MAX);
        let files = &tail[manifest_len..manifest_len + files_len];
        let index = &tail[manifest_len + files_len..tail.len() - TRAILER_LEN];
        if crc32c(manifest) != trailer.manifest_crc32c {
            return Err(ObjectError::ManifestCrc);
        }
        if crc32c(files) != trailer.files_crc32c {
            return Err(ObjectError::FilesCrc);
        }
        if crc32c(index) != trailer.index_crc32c {
            return Err(ObjectError::IndexCrc);
        }
        let mut entries: Vec<IndexEntry> = Vec::with_capacity(index.len() / ENTRY_LEN);
        for (position, raw) in index.chunks_exact(ENTRY_LEN).enumerate() {
            let entry = decode_entry(raw);
            if entries
                .last()
                .is_some_and(|prev| prev.digest >= entry.digest)
            {
                return Err(ObjectError::IndexOrder { position });
            }
            if entry.digest.size_bytes == 0 {
                return Err(ObjectError::ZeroSize {
                    offset: entry.offset,
                });
            }
            let end = entry.offset.checked_add(entry.digest.size_bytes);
            if end.is_none_or(|end| end > trailer.manifest_offset) {
                return Err(ObjectError::OutOfBounds {
                    digest: entry.digest,
                });
            }
            entries.push(entry);
        }
        let mut by_offset: Vec<&IndexEntry> = entries.iter().collect();
        by_offset.sort_by_key(|e| e.offset);
        for pair in by_offset.windows(2) {
            if pair[1].offset < pair[0].range().end {
                return Err(ObjectError::Overlap {
                    first: pair[0].digest,
                    second: pair[1].digest,
                });
            }
        }
        Ok(Self {
            manifest: manifest.to_vec(),
            files: files.to_vec(),
            entries,
        })
    }
}

fn encode_entry(out: &mut Vec<u8>, entry: &IndexEntry) {
    out.extend_from_slice(&entry.digest.hash);
    out.extend_from_slice(&entry.digest.size_bytes.to_le_bytes());
    out.extend_from_slice(&entry.offset.to_le_bytes());
    out.extend_from_slice(&entry.crc32c.to_le_bytes());
}

fn decode_entry(raw: &[u8]) -> IndexEntry {
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&raw[..32]);
    let u64_at = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap_or([0; 8]));
    IndexEntry {
        digest: Digest::new(DigestFunction::Sha256, hash, u64_at(32)),
        offset: u64_at(40),
        crc32c: u32::from_le_bytes(raw[48..52].try_into().unwrap_or([0; 4])),
    }
}

/// A blob being streamed into the object: its bytes are written as they come, and it is
/// indexed only by [`ObjectWriter::end`], once its digest is known.
#[derive(Debug)]
pub struct Streaming {
    offset: u64,
    crc: Crc32c,
    len: u64,
}

/// What a finished object is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    /// The SHA-256 digest of the whole object, hashed as it was written.
    pub object: Digest,
    /// How many distinct blobs the index holds.
    pub blobs: u64,
    /// The bytes written for a streamed blob that turned out to be a duplicate of an earlier
    /// one: in the object, in no entry.
    pub dead_bytes: u64,
}

/// Writes a one-pass object through `W`, one blob at a time; see the module documentation.
///
/// The writer holds the index (one entry per distinct blob) and nothing of a blob. It is
/// single-writer: a streamed blob ([`ObjectWriter::begin`]) is ended before the next blob
/// starts.
pub struct ObjectWriter<W: Write> {
    out: W,
    len: u64,
    object: Sha256,
    entries: BTreeMap<Digest, (u64, u32)>,
    dead: u64,
}

impl<W: Write> ObjectWriter<W> {
    /// A writer at the start of an empty `out`.
    pub fn new(out: W) -> Self {
        Self {
            out,
            len: 0,
            object: Sha256::new(),
            entries: BTreeMap::new(),
            dead: 0,
        }
    }

    /// Whether a blob of this digest is in the index.
    #[must_use]
    pub fn holds(&self, digest: &Digest) -> bool {
        self.entries.contains_key(digest)
    }

    /// Where the blob `digest` starts in the object, if it is held.
    #[must_use]
    pub fn offset_of(&self, digest: &Digest) -> Option<u64> {
        self.entries.get(digest).map(|(offset, _)| *offset)
    }

    /// The sink, for a caller that must flush it before it reads what was written.
    pub fn sink(&mut self) -> &mut W {
        &mut self.out
    }

    /// The bytes written so far.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether nothing has been written.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.out.write_all(bytes)?;
        self.object.update(bytes);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Writes `bytes`, the blob `digest` names, unless the empty blob or one already written.
    /// Returns whether it was written. Nothing is written for a blob that is not new, so a blob
    /// held in memory never costs a dead byte.
    ///
    /// # Errors
    /// The sink's error.
    pub fn blob(&mut self, digest: &Digest, bytes: &[u8]) -> io::Result<bool> {
        if digest.size_bytes == 0 || self.holds(digest) {
            return Ok(false);
        }
        let offset = self.len;
        self.put(bytes)?;
        self.entries.insert(*digest, (offset, crc32c(bytes)));
        Ok(true)
    }

    /// Starts a blob whose digest is not yet known: its bytes follow in [`ObjectWriter::chunk`]
    /// calls and [`ObjectWriter::end`] names it.
    pub fn begin(&self) -> Streaming {
        Streaming {
            offset: self.len,
            crc: Crc32c::new(),
            len: 0,
        }
    }

    /// Writes the next bytes of the streamed blob `at`.
    ///
    /// # Errors
    /// The sink's error.
    pub fn chunk(&mut self, at: &mut Streaming, bytes: &[u8]) -> io::Result<()> {
        self.put(bytes)?;
        at.crc.update(bytes);
        at.len += bytes.len() as u64;
        Ok(())
    }

    /// Ends the streamed blob `at`, whose bytes hash to `digest`. A new blob is indexed where it
    /// began. One already held (or empty) is not indexed and its bytes are dead: they stay in
    /// the object. Returns whether it was indexed.
    ///
    /// # Errors
    /// `InvalidData` when the bytes written are not `digest.size_bytes` long.
    pub fn end(&mut self, at: Streaming, digest: &Digest) -> io::Result<bool> {
        if at.len != digest.size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a streamed blob is not as long as its digest says",
            ));
        }
        if digest.size_bytes == 0 || self.holds(digest) {
            self.dead += at.len;
            return Ok(false);
        }
        self.entries.insert(*digest, (at.offset, at.crc.finish()));
        Ok(true)
    }

    /// Ends the object: writes `manifest`, `files` (may be empty), the index and the trailer,
    /// and returns the sink with what was written. The sink is not flushed.
    ///
    /// # Errors
    /// The sink's error.
    pub fn finish(mut self, manifest: &[u8], files: &[u8]) -> io::Result<(W, Written)> {
        let manifest_offset = self.len;
        self.put(manifest)?;
        self.put(files)?;
        let mut index = Vec::with_capacity(self.entries.len() * ENTRY_LEN);
        for (digest, (offset, crc32c)) in &self.entries {
            let entry = IndexEntry {
                digest: *digest,
                offset: *offset,
                crc32c: *crc32c,
            };
            encode_entry(&mut index, &entry);
        }
        let index_offset = self.len;
        self.put(&index)?;
        let trailer = Trailer {
            manifest_offset,
            manifest_len: manifest.len() as u64,
            files_len: files.len() as u64,
            index_offset,
            entry_count: self.entries.len() as u64,
            index_crc32c: crc32c(&index),
            manifest_crc32c: crc32c(manifest),
            files_crc32c: crc32c(files),
        };
        self.put(&trailer.encode())?;
        let object = Digest::new(
            DigestFunction::Sha256,
            self.object.finalize().into(),
            self.len,
        );
        let written = Written {
            object,
            blobs: self.entries.len() as u64,
            dead_bytes: self.dead,
        };
        Ok((self.out, written))
    }
}
