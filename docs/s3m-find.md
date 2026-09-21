# s3m-find — parallel find for object storage

`s3m-find` searches buckets with a pool of worker threads, evaluating a
`find(1)`-style expression against every object. Everything a test
needs — key, size, mtime, etag, storage class — comes free in the
listing, so even complex searches over millions of objects cost only
the listing requests, made in parallel.

There is deliberately **no `-delete` and no `-exec`**: s3m-find only
reads. Pipe matches to `xargs -0` (via `-print0`) or feed them to
`s3m-rm`, which has its own dry-run safety net.

## Synopsis

```
s3m-find [S3M-OPTIONS] s3://BUCKET[/PREFIX]... [EXPRESSION]
```

s3m options come first, then URIs, then the expression — the same shape
as find (an option placed after the URIs is taken as an expression
token and rejected). With no expression, everything matches.

## s3m options

| Option | Description |
|--------|-------------|
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallelism, 0–9 (default 2). |
| `--versions` | Search every stored version and delete marker instead of just current objects. Output gains a `version_id` column and the `-latest` / `-marker` tests become available. |
| `--rrdns` | Resolve the endpoint hostname to every A/AAAA address it has and spread worker threads across them. See [connection.md](connection.md#load-balancing-across-multiple-endpoint-addresses). |
| `-o, --output FILE` | Write matches to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the listing; the summary still shows the **match count**, making `-q` a fast "how many?" mode. |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## Tests

| Test | Matches objects whose… |
|------|------------------------|
| `-name G`, `-iname G` | last path segment of the key matches the glob `G` (case-insensitive with `i`) |
| `-key G`, `-ikey G` | whole key matches the glob (`*` also spans `/`); `-path`/`-ipath` are accepted as aliases |
| `-regex R`, `-iregex R` | whole key matches the POSIX **extended** regex, anchored to the whole key |
| `-size [+-]N[ckMGT]` | size, **rounded up** to the unit before comparing, exactly as find does. Default unit is `c` (exact bytes) — objects have no 512-byte blocks |
| `-mtime [+-]N` | age in 24-hour units (find rounding rules: `-mtime 1` = age in [24h, 48h), `+1` = older, `-1` = younger) |
| `-mmin [+-]N` | age in minutes |
| `-class C` | storage class equals `C` (case-insensitive; a listing that omits the class means `STANDARD`) |
| `-etag E` | etag equals `E` (surrounding quotes optional) |
| `-empty` | zero-byte object |
| `-latest` | is the current version (`--versions` only) |
| `-marker` | is a delete marker (`--versions` only) |
| `-true`, `-false` | constants |

`-print` (a no-op, always true) and `-print0` (switches the output to
NUL-separated raw keys) are accepted inside the expression, as in find.

## Operators

Highest precedence first, identical to find:

```
( EXPR )                grouping (quote the parens from your shell)
! EXPR, -not EXPR       negation
EXPR EXPR, EXPR -a EXPR and (implicit between adjacent tests)
EXPR -o EXPR            or
```

Short-circuit evaluation is preserved left to right.

## Output

Default: CSV with a `key` header (`key,version_id` with `--versions`),
RFC 4180 quoted. With `-print0`: NUL-separated raw keys, no header —
drop-in for `xargs -0` (the version id is not emitted in this mode).

**Match order is non-deterministic** (parallel listing); sort if you
need stability. The summary always reports the total match count:

```
✓ s3m-find complete — 2,481 matched · 48,214 objects scanned · 0 errors
  1.2s (40,178 obj/s) → matches.csv
```

## Error handling

Unlistable prefixes are counted and reported on stderr after the run.
Exit status: `0` success · `1` completed with errors · `2` usage or
expression error.

## Examples

```sh
# Classic cleanups — see what's old and large
s3m-find s3://logs -name '*.log' -mtime +30
s3m-find s3://data -size +1G
s3m-find s3://data -empty

# Complex: recent build artifacts, excluding sources
s3m-find s3://build '(' -name '*.o' -o -name '*.so' ')' -size +10M -mmin -120

# Everything that has left STANDARD storage
s3m-find s3://archive ! -class STANDARD

# Count without listing (summary shows the total)
s3m-find -q s3://data -key 'staging/*' -mtime +90

# Which objects are currently "deleted" (latest version is a marker)?
s3m-find --versions s3://data -latest -marker

# Find duplicates of a known object by etag
s3m-find s3://data -etag 5d41402abc4b2a76b9719d911017c592

# Feed another tool safely
s3m-find s3://scratch -name 'tmp-*' -mtime +7 -print0 | xargs -0 -r -n1 echo
```

## Behaviour notes

- No per-object requests are ever made: tests use listing data only,
  so cost and speed are identical to `s3m-ls` over the same prefix.
- Time tests take their reference time once at startup, like find.
- `-name` on a key with no `/` matches the whole key.
- In `--versions` mode every stored version is evaluated independently;
  a delete marker has size 0 but does not match `-empty`.
- `-print0` is a global output mode, not an ordered action (same caveat
  as p3m-find).
