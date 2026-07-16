# s3m-sync — parallel synchroniser

`s3m-sync` makes a destination match a source, copying only what is new
or changed, in any of three directions:

| SRC | DST | Mode |
|-----|-----|------|
| local directory | `s3://…` | upload |
| `s3://…` | local directory | download |
| `s3://…` | `s3://…` | server-side copy (same endpoint) |

**By default nothing is changed**: the tool prints the plan as CSV and
stops; `--apply` executes it.

## Synopsis

```
s3m-sync [OPTIONS] SRC DST
```

## Options

| Option | Description |
|--------|-------------|
| `--apply` | Actually copy/delete. Without it the run is a dry run — per s3m convention there is no `--dry-run` flag, because that is the default state. |
| `--delete` | Also remove destination entries that do not exist in the source (shown explicitly in the dry run as `delete` / `delete-local` rows). |
| `--size-only` | Compare by size alone; ignore timestamps. |
| `--checksum` | Compare content: local MD5 against the object etag where the etag is conclusive (single-part uploads); multipart etags fall back to size+mtime. S3→S3 compares etags directly. |
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallel listing, 0–9 (default 2). |
| `-o, --output FILE` | Write the CSV plan/report to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the console listing (progress and the summary are still shown). |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## How it decides

The run has three phases — index the destination (parallel listing or
local walk), diff the source against the index, then execute the plan
across the worker pool.

An entry is **copied** when:

1. it does not exist at the destination, or
2. its size differs, or
3. the source is newer than the destination (skipped with
   `--size-only`; refined by `--checksum`).

Copies make the comparison converge: uploads leave the object's
LastModified newer than the file, downloads set the local file's mtime
to the object's LastModified, so an immediately repeated sync copies
nothing.

With `--delete`, destination entries the source diff never touched are
removed — batched `DeleteObjects` on S3, `unlink` locally. If the
destination indexing phase hit any errors, the delete pass is **skipped
entirely** (deleting against an incomplete index would be guesswork);
copies still proceed.

## Output

```csv
key,action,size,result
logs/2025/file014.bin,upload,5127,pending
old/stale.bin,delete,0,pending
```

`action` is `upload`, `download`, `copy`, `delete` or `delete-local`;
`result` is `pending` (dry run), `done`, or `failed: <reason>`. Keys
are shown relative to the SRC/DST roots. Row order is
non-deterministic in apply mode.

## Transfers

- Uploads stream straight from disk with a Content-Type guessed from
  the file extension. Files over 128 MiB use **multipart upload**
  (64 MiB parts, growing for very large files so the 10,000-part limit
  is never hit); a failed multipart upload is aborted server-side so
  no billable part debris is left behind.
- Downloads write to a temporary file next to the target
  (`name.s3m-tmp-XXXXXX`) and are renamed into place — readers never
  see a half-written file — with mtime set to the object's
  LastModified. Missing directories are created.
- S3→S3 copies are server-side (`CopyObject`): no data flows through
  the client. Objects over 5 GiB exceed the single-request copy limit
  and are reported as errors. Both buckets must be on the same
  endpoint/credentials.
- Symbolic links are never followed or synced; special files are
  skipped.

## Progress and summary

```
⠼ s3m-sync — parallel synchronise (upload, apply)
  key       logs/2025/file014.bin
  phase     transfer       threads  16
  indexed   10,000         scanned  9,800
  copied    1,214 / 3,000  deleted  12 / 40
  bytes     1.2 GiB / 3.9 GiB  errors 0
  rate      116.13 MiB/s   elapsed  12.3s
```

```
✓ s3m-sync complete — upload apply · 312 scanned · 310 in sync · 2 copied · 1 deleted · 0 errors
  27 B in 0.1s (350 B/s)
```

The summary always prints; a dry run with pending work also prints a
reminder that `--apply` is required.

## Error handling

Per-entry failures (unreadable file, permission denied, throttling
after retries) mark that row `failed`, are counted, and never stop the
run. Exit status: `0` success · `1` completed with errors · `2` usage
or startup error.

## Examples

```sh
# What would an upload do? (dry run)
s3m-sync ./website s3://prod-site/static

# Publish, removing files deleted locally, with an audit CSV
s3m-sync --apply --delete -o publish.csv ./website s3://prod-site/static

# Fast big-tree mirror to MinIO, 32 workers
s3m-sync --apply -j 32 --endpoint http://minio:9000 /data s3://backup/data

# Restore a prefix to disk
s3m-sync --apply s3://backup/data/reports ./reports

# Verify a mirror by content, then repair drift
s3m-sync --checksum s3://prod/assets ./assets
s3m-sync --checksum --apply s3://prod/assets ./assets

# Replicate between buckets, server-side
s3m-sync --apply s3://prod-site/static s3://staging-site/static
```

## Behaviour notes

- `SRC`/`DST` prefixes are treated as directory-like: a missing
  trailing `/` is added. A missing local destination directory is
  created on first download.
- `--delete` with an empty source removes everything at the
  destination — the dry run makes this visible before it can happen.
- Timestamp comparison is in whole seconds (object stores do not
  return sub-second mtimes).
- `--checksum` costs one local MD5 read per compared file; it never
  makes extra requests.
- Zero-byte "directory placeholder" objects (keys ending in `/`) are
  ignored on both sides.
