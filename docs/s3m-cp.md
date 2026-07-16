# s3m-cp — parallel server-side copy

`s3m-cp` copies objects between buckets and prefixes **entirely
server-side** — no object data ever flows through the client, so copy
throughput is bounded by the storage service, not your network. With
`--move` it becomes a parallel rename (copy, then batched delete of the
sources). **By default nothing is copied**: the tool performs a dry run
listing everything that would be copied and flagging destination
conflicts; add `--apply` to copy.

## Synopsis

```
s3m-cp [OPTIONS] s3://SRC/KEY|PREFIX/... s3://DST[/PREFIX/]
```

## Source and destination rules

aws-cli-style (note: this differs from `p3m-cp`, which copies a
directory *into* the destination — S3 prefix sources copy their
*contents*):

| Source form | Destination form | Result |
|-------------|------------------|--------|
| `s3://b/a/sub/` (prefix) | `s3://c/dst/` | `a/sub/X` → `dst/X` |
| `s3://b/a/file.txt` (key) | `s3://c/dst/` | `dst/file.txt` (basename) |
| `s3://b/a/file.txt` (key) | `s3://c/dst/new.txt` | exactly `dst/new.txt` (rename-style; single source only) |

Multiple sources are allowed; they may come from different buckets, but
there is one destination and everything lands under it.

## Options

| Option | Description |
|--------|-------------|
| `--apply` | Actually copy. Without it the run is a dry run — per s3m convention there is no `--dry-run` flag, because that is the default state. |
| `--overwrite` | Replace existing destination objects. Without it they are **skipped** and reported (`exists`), never clobbered — and the summary says so. |
| `--move` | Delete each source object after its successful copy — a parallel rename. Deletes are batched (`DeleteObjects`); a failed copy never deletes its source. |
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
src_key,dst_key,size,result
logs/2024/file01.bin,cptest/file01.bin,4996,pending
```

`result` is `pending` (dry run), `exists` (skipped), `copied`, `moved`,
or `failed: <reason>`. Row order is non-deterministic.

## Large objects

Objects up to 5 GiB are copied with a single `CopyObject`; larger ones
automatically use multipart `UploadPartCopy` in 1 GiB parts (still
fully server-side), with the upload aborted server-side if any part
fails — no billable part debris. Object metadata and content type are
carried over by the service (`x-amz-metadata-directive: COPY`
semantics).

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
```

## Behaviour notes

- Both buckets must be reachable on the same endpoint with the same
  credentials (that is what the S3 copy API requires).
- On a versioned destination every copy creates a new version;
  `--move`'s source deletes write delete markers (recoverable). Use
  `s3m-ver` to prune either.
- The destination index (for `exists` detection) is one parallel
  listing of the destination prefix; `--overwrite` skips that phase
  entirely.
- A copy is atomic per object: readers never see partial objects.
