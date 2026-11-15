# One output object per action, written in one pass

Status: **DESIGN, then built in the same pull request** (branch `task-output-one-pass`). Owner approval of the direction
(2026-11-15): one object per action, outputs read once, "the single pack per action". Linux job mode with a put directory
(`--stage-and-commit`) only; the presigned mode is untouched. Linked from [actions-as-jobs.md](actions-as-jobs.md)
section 5.3 and [jobs-on-jobscheduler-and-tierstore.md](jobs-on-jobscheduler-and-tierstore.md).

## 1. The problem

A put-directory run today (`crates/kbf-task`, `out_dir.rs`) works like a presigned run, which had to be planned before any
output was known:

1. `kbf-outputs` reads and hashes every file, and builds the Directory and Tree records (first read).
2. `kbf-task` plans the layout (`PackLayout::plan`, `plan.rs`): at most 8 packs of 128 MiB and 16,384 records, at most 8
   large objects for blobs over 64 MiB, the manifest as record 0 at offset 0.
3. It reads every file again into the pack and checks each digest (second read), then writes the footer.

The caps exist because a presigned URL has to be minted for a key before the outputs are known. A put directory has no such
need. So: a run whose outputs pass 128 MiB or 16,384 files writes several objects, an output over 64 MiB is a separate
object the frontend copies into the CAS, and every large file is read twice.

## 2. The target

- One object per action: `pack` in the put directory. No second pack, no large object, no record cap, no size cap
  (AGENTS.md B16).
- Each output file is read once. Its bytes are written to the object while they are hashed.
- The manifest and the index come after the last blob, at the end, found through a fixed trailer.
- The frontend reads the trailer, then the manifest and the index in one ranged read, records every blob (large ones too) as
  `digest -> (object key, offset, length)` in its pack index, and builds the ActionResult as today.

## 3. The format (`KBFTASK`, version 1)

```text
blobs      each blob's bytes, back to back in the order the task produced them (see 4: gaps are possible)
manifest   the TaskManifest JSON (the bytes `to_json_with` writes today), packs 1 and no large object
index      N entries of 52 bytes, ascending by (hash, size): hash [32] | size u64 | offset u64 | crc32c u32
trailer    64 bytes, little endian:
             manifest_offset u64 | manifest_len u64 | index_offset u64 | entry_count u64
             | index_crc32c u32 | manifest_crc32c u32 | digest_function u8 = 1 | reserved [3] = 0
             | version u32 = 1 | trailer_crc32c u32 (of the first 48 bytes) | reserved u32 = 0 | magic [8] = "KBFTASK\n"
```

The entry is the entry of a `kbf-segments` footer, so the code that reads digests and offsets is shared in meaning. The
magic differs from a segment's (`KBFSEG\r\n`), so no reader takes one format for the other: an old frontend refuses the
object as `Malformed` and an old tool refuses it at its trailer.

Rules a reader checks before it trusts the index: `manifest_offset + manifest_len == index_offset`;
`index_offset + 52 * entry_count + 64 == object length`; both CRCs; entries strictly ascending and of size at least 1; every
entry inside `[0, manifest_offset)`; no two entries overlap. Gaps between entries are allowed (4). Order of the blobs and
their contiguity carry no meaning, so the writer needs no plan.

### Tar: recommended against

A valid uncompressed tar (pax headers, index as a final entry) would let `tar -t` list the object. Against:

- A tar header comes **before** its data and needs the size, and the SHA-256 in a pax header needs the whole file read
  first. One pass puts the digest after the data, so the pax digest could only be in the index at the end, which is what
  this format already has. A tar would carry the same index plus headers that say less.
- 512-byte header and padding per entry: 2,000 small files cost about 1.5 MB of overhead and an equal number of header
  reads for a reader that walks the object. This format costs 52 bytes per blob, in one place.
- A blob is content-addressed and appears once; a tar entry is a path. Dedup (4) would leave named entries for blobs that
  are never named.
- Every consumer here reads by `(offset, length)` from the pack index; none lists, so nothing needs `tar`. The format is
  small enough to read by hand with the offsets of 3 when an operator must.

## 4. The one-pass algorithm, and dedup

`kbf-outputs` walks the outputs one file at a time. With a **tee** installed (`Store::tee`, default none: every other user
of the crate is unchanged) a file is handled by size:

- At most 1 MiB (`CHUNK_BYTES`): read whole into memory, hashed, then **written only if its digest is new**. No waste.
- Larger: each chunk is written to the object as it is read and hashed; when the file ends its digest is known. If the
  digest was already written, the bytes just written are **dead**: the writer does not index them and does not move on from
  them. They stay in the object. This is the one tolerated waste, and it is the whole of it: a duplicate of an earlier
  file larger than 1 MiB. We tolerate it and do not truncate because the put directory is a write-once stream of the
  node's store and a `set_len` on it is not something tierstore promises. The count of dead bytes is logged by the task; a
  build whose outputs repeat large files pays their size once more in the object (never in the CAS: dead bytes are in no
  index entry).
- Directory and Tree messages and stdout/stderr go through the same writer (`Store::put`), deduplicated before they are
  written.
- After the walk: manifest, index, trailer. The object's own SHA-256 is taken as the bytes go out, for the store's
  `<name>.sha256-<hex>.part` rename. Then the commit rename, as today.

The only reads of an output are the one that hashes and writes it. A file that changes while it is read is not detected by a
second read; its bytes are hashed as they are written, and the digest is that of the bytes in the object, so the object is
always self-consistent. (The Command has ended and the sandbox hid the out directory; this matches what `kbf-outputs`
already says of files changing during the walk.)

## 5. The frontend

`ingest_put_dir` reads the last 64 bytes of `pack`. If they end in the new magic it follows 5.1; otherwise the old path runs
unchanged (the same bytes are reused for its 32-byte trailer, so an old object costs no extra request).

5.1 One-pass object: parse and check the trailer (entry count at most `max_object_entries`, manifest at most 4 MiB, both
before anything is read); read `[manifest_offset, end)` once; check CRCs and the rules of 3; parse the manifest
(`packs` must be 1, `large` empty, for this lease and action; the declared-outputs check is unchanged); every digest the
manifest names must be an entry (else `Incomplete`); record the object in the pack index with one row per entry, large
ones included. Blobs over `max_blob_bytes` are then stored as today for a frontend whose reads go to storage nodes, by a
ranged read of the object (`Large` with a base offset); a frontend that serves large blobs from the index
(`Packs::read_large`) needs nothing more, since the row says where they are. The action-cache write is last, as before.

The proof is the lighter one of a put directory (`Proof::Manifest`): a record whose bytes are not its digest is dropped by
the first read that hashes it. Nothing about that changes.

## 6. Compatibility (AGENTS.md B8)

Additive: a new object kind under the same key, a new flag, no field of any shipped format changes.

- **Reader first.** The frontend reads both formats from the release that adds this. It does not need telling which.
- **Writer gated by the frontend.** `--task-out-format` (config key `task-out-format`) is `packs` (default) or
  `one-pass`. Only when it is `one-pass` does the executor add `--object-format one-pass` to the task's arguments;
  otherwise the arguments are byte for byte today's, so an old `kbf-task` never sees a flag it does not know.
- **Rollout.** (1) Ship the release to every frontend and the base tree that holds `kbf-task`. (2) When every frontend and
  every task image is that release, set `task-out-format = "one-pass"` and restart the frontends. Rollback: set it back
  to `packs`; objects already written stay readable by the new frontend, and a frontend rolled back to a release before
  this one refuses a one-pass object, so roll the config back before the binary.
- **The test** that runs the previous release on new data and the reverse: `one_pass_and_packs_ingest_side_by_side` (frontend
  reads an old pack and a one-pass object in the same farm) and the task test that, with no flag, writes the bytes the
  previous writer wrote.

## 7. Failure modes

| Failure | Result |
|---|---|
| A file cannot be read or the object cannot be written | task exit 12/13 as today; the `.part` file is removed; nothing committed |
| Commit rename fails | the retries of today's `commit` |
| A task is killed mid-write | a `.part` file the store's sweep removes; no `pack`, so ingest answers `Missing` |
| A duplicate large file | dead bytes in the object (4), not an error |
| A lying object (index claims bytes that are not the digest, overlapping entries, entries past the manifest) | refused by the rules of 3 as `Malformed`, or served until the first read hashes the record, as for any put-directory pack |
| An index with more than `max_object_entries` entries | `TooLarge` before the index is read |
| The old frontend meets a one-pass object | `Malformed`; a configuration error caught by 6's order |

## 8. Measured

See section 9 of the pull request; numbers are repeated here once measured.
