# s3m-cp — parallel copy (server-side, upload and download)

`s3m-cp` copies in parallel across three directions:

| SRC | DST | Mode |
|-----|-----|------|
| `s3://…` | `s3://…` | **server-side copy** — no object data flows through the client; throughput is bounded by the storage service, not your network |
| local file/dir | `s3://…` | **upload** (streaming; multipart above 128 MiB) |
| `s3://…` | local dir/path | **download** (atomic temp-file + rename) |
| local | local | refused — that's [p3m-cp](../../p3m/docs/p3m-cp.md)'s job |

With `--move` it becomes a parallel rename/relocate: sources are
deleted after their successful copy (batched `DeleteObjects` for S3
sources, `unlink` for local files). **By default nothing is copied**:
the tool performs a dry run listing everything that would be copied and
flagging destination conflicts; add `--apply` to copy.

## Synopsis

```
s3m-cp [OPTIONS] SOURCE... DEST
```

Each `SOURCE` is an `s3://` URI (exact key, or `PREFIX/` for its
contents) or a local file/directory. Sources may be mixed — local and
S3 in one run — but there is one `DEST` and everything lands under it.

## Source and destination rules

aws-cli-style (note: this differs from `p3m-cp`, which copies a
directory *into* the destination — here prefix and directory sources
copy their *contents*):

| Source form | Destination form | Result |
|-------------|------------------|--------|
| `s3://b/a/sub/` (prefix) or `./dir` | `s3://c/dst/` or `./out/` | contents: `X` → `dst/X` |
| `s3://b/a/f.txt` (key) or `./f.txt` | `s3://c/dst/` or existing dir | `dst/f.txt` (basename) |
| `s3://b/a/f.txt` (key) or `./f.txt` | `s3://c/dst/new.txt` or new local path | exactly that key/path (rename-style; single source only) |

A local destination is treated as a directory when it exists as one,
ends with `/`, or the copy has multiple/prefix sources; missing
directories are created on demand.

## Options

| Option | Description |
|--------|-------------|
| `--apply` | Actually copy. Without it the run is a dry run — per s3m convention there is no `--dry-run` flag, because that is the default state. |
| `--overwrite` | Replace existing destination objects. Without it they are **skipped** and reported (`exists`), never clobbered — and the summary says so. |
| `--move` | Delete each source after its successful copy — a parallel rename. S3 deletes are batched (`DeleteObjects`), local sources are unlinked (directories are left behind); a failed copy never deletes its source. |
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallel listing, 0–9 (default 2). |
| `-o, --output FILE` | Write the CSV plan/report to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the console listing (progress and the summary are still shown). |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## Safety

- **Dry run by default** — the plan (including `exists` conflicts) is
  known before anything is written.
- **Existing destinations are never overwritten silently**: without
  `--overwrite` they are skipped, counted, and called out in the
  summary.
- **Copying a prefix into itself is refused** before any request is
  made (`s3m-cp s3://b/a/ s3://b/a/deeper/` → exit 2), as is copying an
  object onto itself.
- **`--move` deletes only confirmed copies.** Sources whose copy failed
  are left in place.
- Overlapping sources that map to the same destination key are
  de-duplicated in the plan; the first mapping wins.

## Output

```csv
src,dst,size,result
logs/2024/file01.bin,cptest/file01.bin,4996,pending
```

`src`/`dst` are object keys or local paths, depending on the
direction.

`result` is `pending` (dry run), `exists` (skipped), `copied`, `moved`,
or `failed: <reason>`. Row order is non-deterministic.

## Large objects and transfers

- Server-side: objects up to 5 GiB are copied with a single
  `CopyObject`; larger ones automatically use multipart
  `UploadPartCopy` in 1 GiB parts (still fully server-side), with the
  upload aborted server-side if any part fails — no billable part
  debris. Object metadata and content type are carried over by the
  service (`x-amz-metadata-directive: COPY` semantics).
- Uploads stream straight from disk with a Content-Type guessed from
  the file extension; files over 128 MiB use multipart upload (64 MiB
  parts), also aborted server-side on failure.
- Downloads land in a temporary file next to the target and are renamed
  into place — readers never see a half-written file. Unlike
  `s3m-sync`, timestamps are fresh (`cp` semantics, not `cp -p`); use
  `s3m-sync` when mtime-based convergence matters.

## Summary

```
✓ s3m-cp complete — copy apply · 25 scanned · 25 copied · 0 errors
  103.17 KiB in 0.1s (1.2 GiB/s server-side)
```

Exit status: `0` success · `1` completed with errors · `2` usage error
or self-copy refusal.

## Examples

```sh
# What would this copy? (dry run)
s3m-cp s3://prod/assets/ s3://staging/assets/

# Copy a prefix between buckets, 32 workers
s3m-cp --apply -j 32 s3://prod/assets/ s3://staging/assets/

# Top up an earlier copy without touching what's already there
s3m-cp --apply s3://prod/assets/ s3://staging/assets/   # skips existing

# Force a full refresh
s3m-cp --apply --overwrite s3://prod/assets/ s3://staging/assets/

# Rename a single object
s3m-cp --apply --move s3://b/reports/draft.pdf s3://b/reports/final.pdf

# "Rename" a whole prefix (copy + delete sources)
s3m-cp --apply --move s3://b/2024-logs/ s3://b/archive/2024/

# Consolidate several prefixes into one place, with an audit CSV
s3m-cp --apply -o copied.csv s3://b/in1/ s3://b/in2/ s3://b/all/

# Upload a directory (contents land under the prefix)
s3m-cp --apply ./website s3://prod-site/static/

# Download a prefix to disk
s3m-cp --apply s3://prod-site/static/ ./website-backup/

# Upload a file under a new name, then archive-and-delete local logs
s3m-cp --apply ./report-draft.pdf s3://docs/reports/final.pdf
s3m-cp --apply --move ./old-logs s3://archive/logs/2026/

# Mixed sources into one destination
s3m-cp --apply ./hotfix.tar.gz s3://builds/v2/ s3://releases/staging/
```

## Behaviour notes

- For server-side copies, both buckets must be reachable on the same
  endpoint with the same credentials (that is what the S3 copy API
  requires).
- Local directory sources are walked without following symbolic links;
  only regular files are copied.
- On a versioned destination every copy creates a new version;
  `--move`'s source deletes write delete markers (recoverable). Use
  `s3m-ver` to prune either.
- The destination index (for `exists` detection) is one parallel
  listing of the destination prefix; `--overwrite` skips that phase
  entirely.
- A copy is atomic per object: readers never see partial objects.
