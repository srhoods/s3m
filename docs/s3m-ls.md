# s3m-ls — parallel bucket lister

`s3m-ls` lists one or more buckets or prefixes with a pool of worker
threads and emits every object found as CSV. It is a high-performance
alternative to `aws s3 ls --recursive` / `mc ls -r` for cataloguing
large buckets: a single listing stream is limited to 1,000 keys per
round trip, so on a bucket of millions of objects a single-threaded
lister spends almost all of its time waiting on the network. `s3m-ls`
shards the key space by prefix and keeps many listing requests in
flight at once.

## Synopsis

```
s3m-ls [OPTIONS] s3://BUCKET[/PREFIX]...
```

Multiple URIs may be given; they are listed concurrently as part of the
same run, and may name different buckets.

## Options

| Option | Description |
|--------|-------------|
| `-m, --mode LEVEL` | Detail level: `basic` (default), `standard` or `full`. Single-letter abbreviations (`b`, `s`, `f`) are accepted. |
| `-j, --threads N` | Number of worker threads, 1–256. Default: 16. |
| `--class CLASSES` | Only output objects of these storage classes (comma-separated, case-insensitive), e.g. `--class STANDARD,GLACIER`. |
| `--shard-depth N` | Prefix levels to expand for parallelism, 0–9 (default 2). See [Sharding](#sharding-and-performance). |
| `-o, --output FILE` | Write the CSV to `FILE` instead of stdout, and show a live progress display on the terminal. |
| `-q, --quiet` | Suppress the console listing. Progress (on a terminal) and the end-of-run summary are still shown, and a `-o` file is still written in full — `-q` only silences stdout. |
| `-h, --help` | Show usage and exit. |
| `-V, --version` | Show version and exit. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## Detail levels

### `basic` (default)

One column: the key of every object.

```csv
key
logs/2024/file01.bin
logs/2024/file02.bin
```

### `standard`

```csv
key,size,mtime,etag,class
logs/2024/file01.bin,5127,2026-07-16T07:18:55Z,2df19d7d8ba7a3ef945d866bc68a6518,STANDARD
```

| Column | Meaning |
|--------|---------|
| `key` | Object key |
| `size` | Size in bytes |
| `mtime` | Last modified, ISO 8601 UTC |
| `etag` | Entity tag, quotes stripped (an MD5 for single-part uploads; multipart etags contain a `-`) |
| `class` | Storage class (`STANDARD`, `GLACIER`, …) |

Unlike `p3m-ls`, `standard` costs the same as `basic`: every field
comes free in the listing response, so the mode choice is purely about
output shape.

### `full`

Adds fields the listing cannot provide, at the cost of **one HEAD
request per object** — expect the run to be slower by roughly
(objects ÷ threads) round trips:

```csv
key,size,mtime,etag,class,owner,content_type,version_id,sse
```

`owner` comes from the listing (requested with `fetch-owner`);
`content_type`, `version_id` and `sse` (server-side encryption) come
from the per-object HEAD. `version_id` is empty on unversioned buckets.

## Sharding and performance

A bucket listing is inherently serial: each page's continuation token
comes from the previous page. `s3m-ls` gets its parallelism by walking
the key space like a directory tree: prefixes up to `--shard-depth`
levels below the root are discovered with delimiter (`/`) listings and
each discovered prefix is then listed independently — those listings
run concurrently across the worker pool.

- The default (`--shard-depth 2`) suits typical layouts such as
  `logs/2025/06/…` where second-level prefixes hold many objects.
- `--shard-depth 0` disables sharding: one flat, serial listing. Use it
  for buckets with no `/` structure, or when prefixes hold only a few
  objects each (there, per-prefix listing requests outnumber flat
  pages and sharding costs more than it buys).
- Results are identical at any depth; only request patterns change.

Threads sit in HTTPS round trips, not on the CPU, so thread counts far
above the core count are effective. AWS S3 sustains thousands of
requests per second per prefix; against production object stores
`-j 32` or `-j 64` is reasonable for very large buckets.

## Progress display

When `-o` or `-q` is used and stderr is a terminal, a live status block
is shown and refreshed 8 times per second:

```
⠼ s3m-ls — parallel bucket listing
  key       logs/2025/file014.bin
  output    catalogue.csv
  threads   16             mode     standard
  objects   48,214         prefixes 12 pending
  rate      21,032 obj/s   errors   0
  size      8.02 GiB       elapsed  2.3s (req 55 (0 retried))
```

When the run completes the block is replaced by a one-line summary:

```
✓ s3m-ls complete — 312 objects · 0 errors · 1.14 MiB
  standard in 0.0s (42,286 obj/s, 16 requests) → catalogue.csv
```

If stderr is not a terminal (e.g. inside a cron job) the live display
is suppressed and only the final summary is printed.

## CSV format

- The first line is always a header row.
- Fields containing commas, double quotes or newlines are quoted per
  RFC 4180, with embedded quotes doubled — safe to load into any
  spreadsheet, `sqlite3 .import`, pandas, etc.
- **Row order is non-deterministic** because prefixes are listed in
  parallel. Pipe through `sort` (or sort after import) if you need a
  stable order.

## Error handling

An unlistable prefix or failed HEAD does not stop the run: the error is
counted, the first 24 messages are collected, and everything is
reported on stderr after the run finishes. The exit status is:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Completed, but errors were encountered (or the output could not be fully written) |
| 2 | Usage or startup error (bad option, no credentials, cannot open output file) |

## Examples

```sh
# Fast inventory of a bucket, one key per line
s3m-ls s3://mybucket

# Full metadata catalogue written to a file, with live progress
s3m-ls -m standard -o catalogue.csv s3://mybucket

# One prefix only, against MinIO
s3m-ls --endpoint http://minio.local:9000 s3://data/logs/2025/

# Total size of everything under two prefixes
s3m-ls -m standard s3://b/staging/ s3://b/prod/ \
  | awk -F, 'NR>1 {s+=$2} END {print s}'

# Everything not in STANDARD storage
s3m-ls -m standard --class GLACIER,DEEP_ARCHIVE,GLACIER_IR s3://archive

# Stable sorted listing
s3m-ls s3://mybucket | tail -n +2 | sort
```

## Behaviour notes

- Only current objects are listed; delete markers and non-current
  versions are invisible here — use `s3m-ver` for those.
- A prefix URI matches by string prefix, exactly as S3 does:
  `s3://b/log` matches `logs/x` and `log.txt` both. End the prefix
  with `/` for directory-like behaviour.
- Keys are emitted as raw UTF-8 bytes, unmodified.
- The aggregate size counter in the progress display and summary sums
  every listed object's size, whatever the mode.
