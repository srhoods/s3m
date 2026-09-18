# s3m-ver — version & delete-marker manager

On a versioned bucket every overwrite stores a new version and every
delete just writes a *delete marker* — the old bytes remain, invisible
to listings but very visible on the bill. `s3m-ver` lists **everything
that is actually stored** — every version and marker — and prunes it:
keep the newest N versions per key, drop versions past an age, clean up
tombstones, or undelete.

Pruning is **a dry run by default**; `--apply` is the only way to
delete.

## Synopsis

```
s3m-ver [OPTIONS] s3://BUCKET[/PREFIX]...
```

With no pruning options, `s3m-ver` is a pure lister (nothing to apply).
With pruning options it emits the rows that would be — or were —
deleted.

## Pruning options

| Option | Description |
|--------|-------------|
| `--keep N` | Per key, keep the newest `N` non-current versions **in addition to the current version**, delete the rest. `--keep 0` keeps only the current version; `--keep 3` deletes the 4th-newest non-current version and older. |
| `--older-than DUR` | Only delete versions older than `DUR` (`30d`, `12h`, `45m`, `4w`, `90s`). Combined with `--keep`, a version must fail **both** tests — nothing inside the keep window is ever deleted. |
| `--markers` | Additionally delete a key's delete markers when the run leaves the key with no versions at all (tombstone cleanup — also collects markers already orphaned). |
| `--only-markers` | Delete nothing but delete markers. Removing a *latest* marker **undeletes** the object — its newest version becomes visible again. Cannot be combined with `--keep`/`--older-than`. |
| `--latest` | Allow `--older-than` to delete the *current* version too (the next-newest version becomes current). Ignored for `--keep`, which never touches the current version. |
| `--apply` | Actually delete. Per s3m convention there is no `--dry-run` flag — that is the default state. |

## Other options

| Option | Description |
|--------|-------------|
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallelism, 0–9 (default 2). |
| `--rrdns` | Resolve the endpoint hostname to every A/AAAA address it has and spread worker threads across them. See [connection.md](connection.md#load-balancing-across-multiple-endpoint-addresses). |
| `-o, --output FILE` | Write the CSV to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the console listing (progress and summary still shown). |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## Output

List mode:

```csv
key,version_id,latest,marker,size,mtime,etag,class
docs/doc1.txt,04297398-…,y,n,15,2026-07-16T07:18:57Z,b13bb0f…,STANDARD
docs/doc1.txt,a3112e0f-…,n,n,15,2026-07-16T07:18:57Z,6a07261…,STANDARD
```

Prune mode (only affected rows are emitted):

```csv
key,version_id,latest,marker,size,mtime,result
docs/doc5.txt,baa0d24b-…,n,n,15,2026-07-16T07:18:59Z,pending
```

`latest`/`marker` are `y`/`n`. `result` is `pending` (dry run),
`deleted`, or `failed: <Code>: <message>`. A key's versions arrive
newest-first and contiguously, but the order of keys across the file is
non-deterministic (parallel listing).

## How pruning decides

Versions of each key are ranked newest-first (the current version and
delete markers are not ranked):

1. `--keep N` marks ranks ≥ N.
2. `--older-than` marks versions older than the duration.
   If both options are given, a version must be marked by **both**.
3. The current version is only ever marked by `--older-than --latest`.
4. Markers: `--only-markers` takes them all; `--markers` takes a key's
   markers only when steps 1–3 (or prior history) leave the key with
   zero versions.

Deletions go through batched `DeleteObjects` requests with explicit
version ids — a million-version cleanup is roughly a thousand round
trips per worker thread.

## Summary

```
✓ s3m-ver complete — dry run · 6 keys · 21 versions · 1 markers
  6 to delete · 90 B reclaimable · 0 errors · in 0.0s
  dry run — nothing was removed; add --apply to delete
```

Exit status: `0` success · `1` completed with errors · `2` usage error.

## Examples

```sh
# What is actually stored under a prefix?
s3m-ver s3://data/docs/

# Retention: keep current + 3 newest old versions per key — first look…
s3m-ver --keep 3 s3://data

# …then do it, with an audit CSV
s3m-ver --keep 3 --apply -o pruned.csv s3://data

# Keep 30 days of history, but never fewer than 5 versions per key
s3m-ver --keep 5 --older-than 30d --apply s3://data

# Purge history AND tombstones of previously deleted keys
s3m-ver --keep 0 --markers --apply s3://data/scratch/

# Undelete: remove the delete markers under a prefix
s3m-ver --only-markers --apply s3://data/restore-me/

# Age out abandoned objects entirely, current versions included
s3m-ver --older-than 365d --latest --apply s3://data/tmp/
```

## Behaviour notes

- On an unversioned bucket the versions listing reports each object as
  its single "version"; `--keep N` (N ≥ 0) then deletes nothing and
  `--older-than --latest` behaves like an age-based `s3m-rm`.
- `--keep`/`--older-than` never delete the current version (without
  `--latest`), so a prune pass is invisible to readers of the bucket.
- Deleting a *latest version* permanently (via `--older-than --latest`)
  promotes the previous version — readers see older content. That is
  the point, but it is why `--latest` is a separate, explicit flag.
- Overlapping URI arguments are de-duplicated before listing, so a
  key's version stack is never processed twice in one run.
- Timestamps come from the service; age is measured against the local
  clock at startup.
