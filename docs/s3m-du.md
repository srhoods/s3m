# s3m-du — parallel object storage usage reporter

`s3m-du` aggregates object sizes and counts by prefix, in parallel —
`du` for buckets. It answers "what is taking the space" (and, with
`--versions`, "why is the bill bigger than the data": non-current
versions and delete markers are invisible to ordinary listings but
still billed).

## Synopsis

```
s3m-du [OPTIONS] s3://BUCKET[/PREFIX]...
```

Output is du-style: the size, a tab, then the prefix — and a third
tab-separated column (the storage class) with `--by-class`. By default
every prefix level is printed, like `du`; rows are sorted
lexicographically.

```
     1198983	s3://testdata/
      416183	s3://testdata/backup/
      108935	s3://testdata/backup/2024/
```

## Options

| Option | Description |
|--------|-------------|
| `-s, --summarize` | Only print a total for each URI argument. |
| `-d, --max-depth N` | Print prefixes at most `N` levels below each URI argument (`-d 0` ≡ `-s`). |
| `-c, --total` | Also print a grand total across all arguments. |
| `-h, --human-readable` | Sizes in powers of 1024 (`5.1M`, `2.3G`). |
| `--si` | Sizes in powers of 1000. |
| `--by-class` | Break every output line down by storage class. |
| `--versions` | Include non-current versions and delete markers in the totals. |
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallelism, 0–9 (default 2; 0 = one flat serial listing). See [s3m-ls](s3m-ls.md#sharding-and-performance). |
| `--rrdns` | Resolve the endpoint hostname to every A/AAAA address it has and spread worker threads across them. See [connection.md](connection.md#load-balancing-across-multiple-endpoint-addresses). |
| `-o, --output FILE` | Write the report to `FILE`. |
| `-q, --quiet` | Suppress the report on stdout (the summary is still shown). |
| `--help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

Note: `-h` is `--human-readable` (as in `du`), so help is `--help`.

## Version accounting

By default `s3m-du` sizes what a listing shows: current objects only.
With `--versions` it walks `ListObjectVersions` instead and counts
**every stored version** plus delete markers (size 0, counted in the
summary) — the number that corresponds to what the service actually
stores and bills. Comparing the two runs shows the version overhead
directly; `s3m-ver` can then reclaim it.

## Progress and summary

The report only prints once the scan completes, so the live progress
block is shown whenever stderr is a terminal — no `-o` needed:

```
⠼ s3m-du — parallel usage scan
  key       logs/2025/file014.bin
  threads   16             listing  objects
  objects   48,214         requests 55
  rate      21,032 obj/s   errors   0
  size      8.02 GiB       elapsed  2.3s
```

```
✓ s3m-du complete — 312 objects · 0 errors · 1.14 MiB
  in 0.1s
```

## Error handling

Per-prefix listing failures are counted, reported on stderr at the end,
and do not stop the run — but note that a failed prefix's objects are
missing from the totals, so treat exit status 1 as "the numbers are a
lower bound".

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Completed with errors (totals may be incomplete) |
| 2 | Usage or startup error |

## Examples

```sh
# Where is the space? Top-level breakdown, human sizes
s3m-du -d 1 -h s3://datalake

# Billing view: everything stored, including old versions and markers
s3m-du -s -h --versions s3://datalake

# Compare current vs stored (version overhead)
s3m-du -s s3://b ; s3m-du -s --versions s3://b

# Storage class breakdown per top-level prefix
s3m-du -d 1 -h --by-class s3://archive

# Totals for several prefixes plus a grand total
s3m-du -s -c -h s3://b/raw/ s3://b/staging/ s3://b/prod/
```

## Behaviour notes

- Sizes are exact byte totals from the listing (no block rounding —
  object stores have no blocks).
- Overlapping URI arguments (e.g. a bucket and a prefix inside it) are
  each credited independently; the grand total (`-c`) sums every object
  seen, so overlaps double-count, as with `du`.
- With `--by-class`, an object whose listing omits the storage class is
  counted as `STANDARD`.
- Prefixes are string prefixes: `s3://b/log` covers `logs/…` and
  `log.txt`. End with `/` for directory-like behaviour.
