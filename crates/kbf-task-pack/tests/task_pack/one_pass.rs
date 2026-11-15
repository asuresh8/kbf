//! The one-pass task object: what the writer writes, what the reader accepts, and what it
//! refuses before it trusts the index.

use std::io::{self, Write};

use kbf_segments::{crc32c, sha256};
use kbf_task_pack::one_pass::{
    ENTRY_LEN, MAGIC, ObjectError, ObjectWriter, TRAILER_LEN, Tail, Trailer,
};
use kbf_types::{Digest, DigestFunction};

/// A blob of `len` bytes that differs with `seed`.
fn blob(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// Writes `blobs` (each streamed in pieces of `piece` bytes when `piece` is not 0, else given
/// whole) then `manifest`; the object's bytes and what the writer said.
fn write(
    blobs: &[&[u8]],
    manifest: &[u8],
    piece: usize,
) -> (Vec<u8>, kbf_task_pack::one_pass::Written) {
    let mut writer = ObjectWriter::new(Vec::new());
    for bytes in blobs {
        let digest = sha256(bytes);
        if piece == 0 {
            writer.blob(&digest, bytes).expect("written");
        } else {
            let mut at = writer.begin();
            for part in bytes.chunks(piece) {
                writer.chunk(&mut at, part).expect("chunk");
            }
            writer.end(at, &digest).expect("ended");
        }
    }
    writer.finish(manifest, b"listed").expect("finished")
}

/// The reader's two steps over `object`.
fn read(object: &[u8]) -> Result<Tail, ObjectError> {
    let len = object.len() as u64;
    let trailer = Trailer::parse(&object[object.len() - TRAILER_LEN..], len)?;
    let tail = &object[object.len() - usize::try_from(trailer.tail_len()).expect("fits")..];
    Tail::parse(&trailer, tail)
}

#[test]
fn objects_round_trip_each_blob_at_its_entry() {
    let blobs = [blob(1, 10), blob(2, 3000), blob(3, 1)];
    let refs: Vec<&[u8]> = blobs.iter().map(Vec::as_slice).collect();
    let (object, written) = write(&refs, b"{\"manifest\":1}", 0);
    let tail = read(&object).expect("read");
    assert_eq!(tail.manifest, b"{\"manifest\":1}");
    assert_eq!(tail.files, b"listed");
    assert_eq!(tail.entries.len(), 3);
    assert_eq!(written.blobs, 3);
    assert_eq!(written.dead_bytes, 0);
    for bytes in &blobs {
        let entry = tail
            .entries
            .iter()
            .find(|e| e.digest == sha256(bytes))
            .expect("entry");
        let range = entry.range();
        assert_eq!(&object[range.start as usize..range.end as usize], bytes);
        entry.verify(bytes).expect("crc and digest");
    }
    assert!(tail.entries.windows(2).all(|p| p[0].digest < p[1].digest));
    assert_eq!(written.object, sha256(&object));
}

#[test]
fn a_streamed_blob_is_the_bytes_of_a_whole_one() {
    let big = blob(9, 5000);
    let (whole, _) = write(&[&big], b"m", 0);
    let (pieces, _) = write(&[&big], b"m", 777);
    assert_eq!(whole, pieces);
}

#[test]
fn a_held_blob_is_not_written_twice() {
    let a = blob(1, 100);
    let (object, written) = write(&[&a, &a], b"m", 0);
    assert_eq!(written.blobs, 1);
    assert_eq!(written.dead_bytes, 0);
    assert_eq!(object.len(), 100 + 1 + 6 + ENTRY_LEN + TRAILER_LEN);
}

#[test]
fn a_streamed_duplicate_is_dead_bytes_in_no_entry() {
    let a = blob(1, 100);
    let b = blob(2, 50);
    let (object, written) = write(&[&a, &b, &a], b"m", 33);
    assert_eq!(written.blobs, 2);
    assert_eq!(written.dead_bytes, 100);
    assert_eq!(
        object.len(),
        100 + 50 + 100 + 1 + 6 + 2 * ENTRY_LEN + TRAILER_LEN
    );
    let tail = read(&object).expect("gaps are allowed");
    assert_eq!(tail.entries.len(), 2);
    let first = tail
        .entries
        .iter()
        .find(|e| e.digest == sha256(&a))
        .expect("a");
    assert_eq!(first.offset, 0);
}

#[test]
fn the_empty_blob_is_never_a_record() {
    let empty = sha256(b"");
    let mut writer = ObjectWriter::new(Vec::new());
    assert!(!writer.blob(&empty, b"").expect("ok"));
    let at = writer.begin();
    assert!(!writer.end(at, &empty).expect("ok"));
    let (object, written) = writer.finish(b"m", b"").expect("finished");
    assert_eq!(written.blobs, 0);
    assert!(read(&object).expect("read").entries.is_empty());
}

#[test]
fn a_streamed_blob_of_another_length_than_its_digest_is_refused() {
    let mut writer = ObjectWriter::new(Vec::new());
    let mut at = writer.begin();
    writer.chunk(&mut at, b"abc").expect("chunk");
    let error = writer.end(at, &sha256(b"abcd")).expect_err("short");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn the_writer_reports_what_it_holds_and_has_written() {
    let mut writer = ObjectWriter::new(Vec::new());
    assert!(writer.is_empty());
    let a = blob(1, 5);
    writer.blob(&sha256(&a), &a).expect("written");
    assert_eq!(writer.len(), 5);
    assert!(writer.holds(&sha256(&a)));
    assert!(!writer.holds(&sha256(b"other")));
}

/// A sink that takes `room` bytes and then refuses.
struct Full {
    room: usize,
}

impl Write for Full {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.room == 0 {
            return Err(io::Error::other("full"));
        }
        let n = bytes.len().min(self.room);
        self.room -= n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_sink_that_refuses_fails_every_step_of_the_writer() {
    let a = blob(1, 8);
    let digest = sha256(&a);
    let mut writer = ObjectWriter::new(Full { room: 4 });
    assert!(writer.blob(&digest, &a).is_err());
    let mut writer = ObjectWriter::new(Full { room: 4 });
    let mut at = writer.begin();
    assert!(writer.chunk(&mut at, &a).is_err());
    let writer = ObjectWriter::new(Full { room: 0 });
    assert!(writer.finish(b"m", b"").is_err());
}

/// An object with one blob, as bytes to damage.
fn sample() -> Vec<u8> {
    let a = blob(1, 100);
    write(&[&a], b"manifest", 0).0
}

/// Sets the trailer's own CRC to match its bytes, so a damaged field is the only fault.
fn reseal(object: &mut [u8]) {
    let at = object.len() - TRAILER_LEN;
    let crc = crc32c(&object[at..at + 52]);
    object[at + 52..at + 56].copy_from_slice(&crc.to_le_bytes());
}

fn trailer_error(mut object: Vec<u8>, damage: impl FnOnce(&mut [u8])) -> ObjectError {
    let at = object.len() - TRAILER_LEN;
    damage(&mut object[at..]);
    reseal(&mut object);
    read(&object).expect_err("refused")
}

#[test]
fn a_trailer_with_a_wrong_field_is_refused_for_that_field() {
    let n = TRAILER_LEN;
    assert_eq!(
        Trailer::parse(&[0; 10], 10),
        Err(ObjectError::Truncated {
            needed: n as u64,
            available: 10
        })
    );
    let mut object = sample();
    let at = object.len() - 8;
    object[at] ^= 1;
    assert_eq!(read(&object), Err(ObjectError::BadMagic));
    assert_eq!(
        trailer_error(sample(), |t| t[44] = 2),
        ObjectError::UnsupportedVersion(2)
    );
    assert_eq!(
        trailer_error(sample(), |t| t[40] = 9),
        ObjectError::UnsupportedDigestFunction(9)
    );
    assert_eq!(
        trailer_error(sample(), |t| t[41] = 1),
        ObjectError::ReservedNotZero
    );
    let mut object = sample();
    let at = object.len() - n + 3;
    object[at] ^= 1;
    assert_eq!(read(&object), Err(ObjectError::TrailerCrc));
}

#[test]
fn offsets_that_do_not_add_up_to_the_object_are_refused() {
    let len = sample().len() as u64;
    for (what, at) in [("index_offset", 16), ("entry_count", 24)] {
        let error = trailer_error(sample(), |t| t[at] = t[at].wrapping_add(1));
        assert_eq!(error, ObjectError::Layout { actual: len }, "{what}");
    }
    // Sums that pass u64 are refused, not wrapped.
    for at in [0, 8, 16, 24] {
        let error = trailer_error(sample(), |t| {
            t[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes())
        });
        assert_eq!(
            error,
            ObjectError::Layout { actual: len },
            "overflow at {at}"
        );
    }
}

#[test]
fn damaged_manifest_or_index_bytes_are_refused() {
    let mut object = sample();
    object[100] ^= 1; // the manifest's first byte
    assert_eq!(read(&object), Err(ObjectError::ManifestCrc));
    let mut object = sample();
    object[108] ^= 1; // the first byte of the files, after the manifest
    assert_eq!(read(&object), Err(ObjectError::FilesCrc));
    let mut object = sample();
    object[114] ^= 1; // the index's first byte
    assert_eq!(read(&object), Err(ObjectError::IndexCrc));
}

#[test]
fn a_tail_of_another_length_than_the_trailer_says_is_truncated() {
    let object = sample();
    let len = object.len() as u64;
    let trailer = Trailer::parse(&object[object.len() - TRAILER_LEN..], len).expect("trailer");
    let short = &object[object.len() - trailer.tail_len() as usize + 1..];
    assert!(matches!(
        Tail::parse(&trailer, short),
        Err(ObjectError::Truncated { .. })
    ));
}

/// An object whose index is `entries` as (digest, offset), over `records` bytes of records,
/// with every CRC right, so the entries are the only fault.
fn forged(records: usize, entries: &[(Digest, u64)]) -> Vec<u8> {
    let mut object = vec![7u8; records];
    let manifest = b"m";
    object.extend_from_slice(manifest);
    let index_offset = object.len() as u64;
    let mut index = Vec::new();
    for (digest, offset) in entries {
        index.extend_from_slice(&digest.hash);
        index.extend_from_slice(&digest.size_bytes.to_le_bytes());
        index.extend_from_slice(&offset.to_le_bytes());
        index.extend_from_slice(&0u32.to_le_bytes());
    }
    object.extend_from_slice(&index);
    let mut trailer = [0u8; TRAILER_LEN];
    trailer[0..8].copy_from_slice(&(records as u64).to_le_bytes());
    trailer[8..16].copy_from_slice(&1u64.to_le_bytes());
    trailer[16..24].copy_from_slice(&index_offset.to_le_bytes());
    trailer[24..32].copy_from_slice(&(entries.len() as u64).to_le_bytes());
    trailer[32..36].copy_from_slice(&crc32c(&index).to_le_bytes());
    trailer[36..40].copy_from_slice(&crc32c(manifest).to_le_bytes());
    trailer[40] = 1;
    trailer[44..48].copy_from_slice(&1u32.to_le_bytes());
    let crc = crc32c(&trailer[..52]);
    trailer[52..56].copy_from_slice(&crc.to_le_bytes());
    trailer[56..64].copy_from_slice(&MAGIC);
    object.extend_from_slice(&trailer);
    object
}

fn digest(first: u8, size: u64) -> Digest {
    let mut hash = [0u8; 32];
    hash[0] = first;
    Digest::new(DigestFunction::Sha256, hash, size)
}

#[test]
fn an_index_that_lies_about_its_records_is_refused() {
    let ok = forged(100, &[(digest(1, 10), 0), (digest(2, 10), 10)]);
    assert_eq!(read(&ok).expect("fine").entries.len(), 2);

    let unordered = forged(100, &[(digest(2, 10), 0), (digest(1, 10), 10)]);
    assert_eq!(
        read(&unordered),
        Err(ObjectError::IndexOrder { position: 1 })
    );
    let twice = forged(100, &[(digest(1, 10), 0), (digest(1, 10), 10)]);
    assert_eq!(read(&twice), Err(ObjectError::IndexOrder { position: 1 }));
    let empty = forged(100, &[(digest(1, 0), 5)]);
    assert_eq!(read(&empty), Err(ObjectError::ZeroSize { offset: 5 }));
    let past = forged(100, &[(digest(1, 10), 95)]);
    assert_eq!(
        read(&past),
        Err(ObjectError::OutOfBounds {
            digest: digest(1, 10)
        })
    );
    let wraps = forged(100, &[(digest(1, 10), u64::MAX)]);
    assert!(matches!(read(&wraps), Err(ObjectError::OutOfBounds { .. })));
    let overlap = forged(100, &[(digest(1, 10), 0), (digest(2, 10), 9)]);
    assert_eq!(
        read(&overlap),
        Err(ObjectError::Overlap {
            first: digest(1, 10),
            second: digest(2, 10)
        })
    );
    // Entries in digest order need not be in offset order, and gaps are fine.
    let shuffled = forged(100, &[(digest(1, 10), 50), (digest(2, 10), 0)]);
    assert!(read(&shuffled).is_ok());
}
