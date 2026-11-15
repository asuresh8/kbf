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
files      the neutral file index (JSON, below; may be empty)
index      N entries of 52 bytes, ascending by (hash, size): hash [32] | size u64 | offset u64 | crc32c u32
trailer    64 bytes, little endian:
             manifest_offset u64 | manifest_len u64 | index_offset u64 | entry_count u64
             | index_crc32c u32 | manifest_crc32c u32 | digest_function u8 = 1 | reserved [3] = 0
             | version u32 = 1 | files_crc32c u32 | trailer_crc32c u32 (of the first 52 bytes)
             | magic [8] = "KBFTASK\n"
```

The entry is the entry of a `kbf-segments` footer, so the code that reads digests and offsets is shared in meaning. The
magic differs from a segment's (`KBFSEG\r\n`), so no reader takes one format for the other: an old frontend refuses the
object as `Malformed` and an old tool refuses it at its trailer.

Rules a reader checks before it trusts the index: `manifest_offset + manifest_len <= index_offset` (the bytes between are the file index);
`index_offset + 52 * entry_count + 64 == object length`; the three CRCs; entries strictly ascending and of size at least 1; every
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

### The neutral file index (the `files` section)

The footer carries one index in no consumer's format. The manifest already has the exit code, stdout, stderr, the timing
and the top-level output files and symlinks; the `files` section adds, for every output file and symlink **including
those below an output directory**, its full path, size, executable bit, SHA-256 and the offset of its bytes in this
object, and every directory (so an empty one is not lost):

```json
{"v":1,"files":[{"path":"out/d/z","size":3,"exec":false,"sha256":"<hex>","offset":20}],
 "symlinks":[{"path":"out/d/l","target":"z"}],"dirs":["out/d","out/d/sub"]}
```

Each list is sorted bytewise by path; `offset` is absent for the empty file (no record). The task builds it from the Trees
it wrote itself (read back from its own file after a flush: they are small records of its own, not outputs) and the
manifest. It is a footer of facts about this object, and nothing in it is tierstore's or REAPI's.

**Layering.** Two systems name the same folder, and they number it differently:

| | REAPI / Buck2 | tierstore |
|---|---|---|
| a folder | `Directory` proto, digest = SHA-256 of the proto | tree manifest v1 JSON, digest = SHA-256 of the manifest |
| a file | SHA-256 and size | SHA-256 and size, plus `source` = {key, offset} |

The two folder digests are different numbers for the same folder; the file digests are the same. kbf-task writes neither
format into the object. **The frontend is the only translator**: from the manifest and the file index it derives (a) the
REAPI ActionResult and the Directory and Tree records (the Buck2 side), and (b) tierstore tree manifests, with `source` =
{this object's key, offset}, when it publishes an input tree (`crates/kbf-front/src/action_tree/encode.rs`). The key is the
frontend's own (it minted it), which is why the task could not write a tierstore manifest even if it were to: the manifest's
digest covers the key. Because (b) uses the frontend's own encoder, a manifest built from the index has the bytes, and so
the digest, the frontend would have built from the Trees: publishing stays deduplicated against any earlier publication. A
reused subtree (a later action's input equal to an earlier output directory) stays cheap because the frontend memoises the
manifest it built for each output directory (`action_tree/memo.rs`), so only newly composed top-level directories are
written. The frontend still builds the ActionResult from the manifest and the footer in the one ranged read of section 5.

**Why whole-tar mounting of dependencies is not used** (mounting each dependency's output, as a tar or one overlay layer,
into the action's root):

- Buck2 composes an input root from parts of many outputs at paths it chooses; a dependency's output is rarely mounted
  whole, at the place it was written.
- Hermeticity: an action must not see a file it did not declare, and a whole-output mount shows every undeclared sibling.
- Overlay layers have a limit and an action may have hundreds of dependencies.

So the input tree is composed per action from digests, and the object is only where file bytes live: each manifest entry
points at a span of it.

**Status.** This pull request writes the object and the index and reads the index back in its tests. The frontend's
derivation of tierstore manifests from it, and of Directory/Tree records without reading the Trees, is the next change; the
ingest here still builds the result from the manifest and records the entries, as for a pack.

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
- After the walk: manifest, file index, index, trailer. The object's own SHA-256 is taken as the bytes go out, for the store's
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
  reads an old pack and a one-pass object in the same farm) and the task test `without_the_one_pass_format_the_pack_is_the_one_of_before`, which with no flag reads the pack the
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

A Command writes `big` (170 MiB of random bytes) and `d/f0..f1999` (1,000 to 2,999 bytes each), outputs `big` and `d`, run
through `kbf_task::tree::execute_out_dir` with a plain directory as the put directory, release build, one Linux host,
page cache warm, three runs of each format (a throwaway harness, not committed). Bytes read are `rchar` of
`/proc/self/io` around the call; it also counts the reaped Command's own reads (170 MiB of `/dev/urandom` and about 36 MiB
for the shell and the 2,000 `head` runs), which are the same for both formats.

| | `--object-format` default (packs) | `one-pass` |
|---|---|---|
| bytes read (rchar, 3 runs identical) | 545.5 MiB | 375.6 MiB |
| of which the outputs | 2 x 170 MiB + small files | 1 x 170 MiB + small files |
| objects written | `pack` + `L0` (the 170 MiB file is a large object) | `pack` |
| task wall minus Command wall (ms, 3 runs) | 267, 281, 284 | 250, 238, 239 |
| object bytes | 182,680,914 | 182,952,316 (the file index and the 2,001 entries' offsets) |

The difference in bytes read is 169.9 MiB, one read of the large file. The wall time is 12% lower with a warm cache; a cold
cache pays the second read in disk time. The output phase is 0.24 s of a 3.9 s task here, so the gain is small for this
Command and grows with output size. Found on the way: in the packs format a large file inside an output *directory* is
refused (`large slot 0 holds ..., which no output names`); the one-pass object has no such case, since a large blob is a
record like any other.
