# s3m-diff — parallel comparison (buckets, prefixes, local trees)

`s3m-diff` compares two sides — two buckets/prefixes, or a **local
directory against a bucket/prefix** — and reports the likelihood that
their contents are the same. By default it compares **key presence**,
**sizes**, and **etags where etags are conclusive** — a single-part
upload's etag is its content MD5, so most S3↔S3 pairs get real content
comparison for free. With `-c` it additionally verifies **contents**.
Every difference is listed as CSV and the run ends with a summary and
a plain-language verdict.

The tool is read-only: nothing on either side is ever modified.

## Synopsis

```
s3m-diff [OPTIONS] LEFT RIGHT
```

Each side is an `s3://BUCKET[/PREFIX]` URI or a local directory. Two
local directories are refused — that's
[p3m-diff](../../p3m/docs/p3m-diff.md)'s job.

Exit status is diff-like: `0` no differences · `1` differences found ·
`2` usage error, or the comparison hit errors (the verdict is withheld
— coverage was incomplete, so "no differences found" would be
misleading).

## Options

| Option | Description |
|--------|-------------|
| `-c, --checksum` | Verify contents where the cheap checks are inconclusive. In a local-vs-S3 run, a file whose object has a conclusive etag is verified by **hashing the local file — no network I/O at all**; every other pair is compared byte-for-byte in 8 MiB chunks (local reads / ranged GETs), **stopping at the first differing byte** and reporting that offset. Sizes gate everything: size-mismatched objects are never read. |
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallel listing, 0–9 (default 2). |
| `--rrdns` | Resolve the endpoint hostname to every A/AAAA address it has and spread worker threads across them. See [connection.md](connection.md#load-balancing-across-multiple-endpoint-addresses). |
| `-o, --output FILE` | Write the CSV listing to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the listing. The summary and **verdict** still print, making `-q` a fast "are these the same?" check. |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## What is compared

| Check | When |
|-------|------|
| key presence | always — `only-left` / `only-right` rows |
| size | always — a size mismatch stops further comparison of that key |
| etag | when **both** sides' etags are conclusive (no `-`, i.e. single-part uploads: the etag is the content MD5) — never in local-vs-S3 runs, since files have no etag |
| content | with `-c`, for pairs whose sizes match but that no cheaper check settled: local MD5 vs a conclusive etag where possible, chunked byte comparison otherwise |

Timestamps are deliberately **not** compared: an object's LastModified
is its upload time, so two perfect copies always differ there.
Storage class is metadata, not content, and is also not compared.
Local sides walk regular files only; symbolic links are never
followed.

## Output

CSV columns: `key,difference,left,right`, keys relative to the two
prefixes:

| `difference` | Meaning | left / right columns |
|--------------|---------|----------------------|
| `only-left`, `only-right` | key exists on one side only | size / empty (or vice versa) |
| `size` | sizes differ | the two sizes in bytes |
| `etag` | conclusive etags differ (content proven different) | the two etags |
| `content` | contents differ (`-c`) | first differing byte offset, or `md5 differs from etag` / empty |

## Summary and verdict

```
✓ s3m-diff complete — 100 left · 100 right · 0 differences · 0 errors
  0.0s
  prefixes are identical — keys, sizes and contents all match (every etag was conclusive)
```

- **no differences, every etag conclusive** — *"identical"*: matching
  MD5 etags are content verification.
- **no differences, some multipart etags** — *"very likely identical"*,
  with a count of the unverifiable objects and a pointer to `-c`.
- **no differences with `-c`** — *"identical (contents verified)"*.
- **differences** — *"prefixes differ"*, with the per-category
  breakdown:

```
✗ s3m-diff complete — 100 left · 101 right · 2 differences · 0 errors
  0 only in left · 1 only in right · 1 size · 0 etag · 0 content
  0.0s
  prefixes differ — 2 differences are listed above
```

- **errors occurred** — the verdict is withheld and the exit status is
  2: an unlistable prefix means the sides were not fully compared, so
  no equality claim is made.

## Examples

```sh
# Quick verdict: did the replication get everything?
s3m-diff -q s3://prod/assets/ s3://dr-site/assets/

# Verify a migration before deleting the source (content-verified)
s3m-diff -c -q s3://old-bucket s3://new-bucket

# Full difference report to CSV with live progress
s3m-diff -c -o drift.csv s3://prod/config/ s3://backup/config/

# Audit an s3m-cp copy
s3m-cp --apply s3://a/data/ s3://b/data/ && s3m-diff -cq s3://a/data/ s3://b/data/

# Does my disk backup still match the bucket? (keys + sizes, instant)
s3m-diff -q ./backup s3://prod/data/

# …and byte-for-byte (local MD5 against etags where possible)
s3m-diff -c -q ./backup s3://prod/data/

# Verify a download/upload before deleting the other side
s3m-sync --apply s3://b/reports ./reports && s3m-diff -cq ./reports s3://b/reports/

# Compare across services (same credentials/endpoint required per run —
# for cross-endpoint comparison, s3m-ls both sides and diff the CSVs)
s3m-ls -m standard s3://b1 | sort > left.csv
s3m-ls -m standard s3://b2 | sort > right.csv
diff left.csv right.csv
```

## Performance notes

- The default mode costs two parallel listings and nothing else —
  cheap enough to run constantly.
- `-c` reads only the objects that need it (equal sizes, inconclusive
  etags), from both sides, stopping at the first differing byte —
  objects that differ early cost almost nothing.
- Verified on MinIO: a single flipped byte in the middle of a 130 MiB
  multipart object is reported at its exact offset
  (`content,differ at byte 70000000`).
- Zero-byte objects compare equal without any GET.

## Behaviour notes

- S3 sides must be reachable with the same endpoint and credentials.
- In a local-vs-S3 run without `-c`, equal-size pairs are not
  content-checked at all (files have no etag) — the verdict says how
  many pairs that covers.
- Keys are compared relative to the given prefixes; "directory
  placeholder" objects (keys ending `/`) are ignored.
- Comparing a prefix with itself reports identical (and is harmless).
- If an object changes size mid-comparison (`-c`), the short read is
  reported as a content difference at the truncation point.
