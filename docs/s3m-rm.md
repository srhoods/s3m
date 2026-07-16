# s3m-rm — parallel object remover

`s3m-rm` removes objects using batched `DeleteObjects` requests (1,000
keys per round trip) fed by the parallel listing engine — with a
safety-first design. **By default nothing is removed**: the tool
performs a dry run listing everything that would be deleted; add
`--apply` to delete.

## Synopsis

```
s3m-rm [OPTIONS] s3://BUCKET/KEY|PREFIX/|MASK...
```

## Target forms

| Form | Meaning |
|------|---------|
| `s3://bucket/some/key` | Exactly that key — nothing else, even keys it prefixes. |
| `s3://bucket/some/prefix/` | Everything under the prefix (the trailing `/` is what makes it a prefix). |
| `s3://bucket/logs/2024-*` | Glob mask (`*`, `?`, `[…]`), matched against whole keys; `*` also spans `/`. **Quote masks** so your shell doesn't try to expand them. |

## Options

| Option | Description |
|--------|-------------|
| `--apply` | Actually remove. Without it the run is a dry run — per s3m convention there is no `--dry-run` flag, because that is the default state. |
| `--permanent` | On versioned buckets, delete **every stored version and delete marker** of the matched keys, instead of writing a delete marker. Unrecoverable. |
| `--entire-bucket` | Allow a target that spans a whole bucket (see [Safety](#safety)). |
| `-j, --threads N` | Worker threads, 1–256 (default 16). |
| `--shard-depth N` | Prefix levels expanded for parallelism, 0–9 (default 2). |
| `-o, --output FILE` | Write the CSV listing to `FILE` and show a live progress display. |
| `-q, --quiet` | Suppress the console listing. Progress (on a terminal) and the end-of-run summary are still shown. |
| `-h, --help`, `-V, --version` | Usage / version. |

Connection options (`--endpoint`, `--profile`, `--path-style`, …) are
shared by all s3m tools — see [connection.md](connection.md).

## Safety

- **Dry run by default.** `--apply` is the only way to delete anything.
- **Whole-bucket guard.** A target that resolves to the entire bucket —
  `s3://bucket`, `s3://bucket/`, or a mask like `s3://bucket/*` whose
  literal prefix is empty — is refused with exit status 2 before any
  request is made, unless the explicit long flag `--entire-bucket` is
  given. There is deliberately no short form.
- **All guards run before any network traffic.** A refused target means
  nothing at all happened.
- **Exact keys are exact.** `s3://b/data` never touches `data.bak` —
  prefix behaviour requires the trailing `/` or an explicit mask.
- **Recoverable by default on versioned buckets.** A plain `--apply`
  writes delete markers (S3 semantics), so objects are recoverable
  until versions are pruned; destroying history requires the separate,
  explicit `--permanent` flag.
- **Overlapping targets are safe.** Overlapping prefixes, masks and
  keys are de-duplicated before listing starts, so no key is deleted —
  or double-marked — twice.

## Output

CSV, one row per matched object (or per stored version with
`--permanent`):

```csv
key,version_id,size,result
scratch/backup/2024/file01.bin,,4996,pending
```

| Column | Meaning |
|--------|---------|
| `key` | Object key |
| `version_id` | Version deleted (`--permanent` only; empty otherwise) |
| `size` | Object size (dry-run rows; empty on apply rows) |
| `result` | `pending` (dry run) · `deleted` · `failed: <Code>: <message>` |

Row order is non-deterministic; fields with commas/quotes are RFC 4180
quoted.

## Progress display

With `-o` or `-q` on a terminal, the s3m live status block is shown and
replaced by a summary when done. The summary always prints; a dry run
that matched anything also prints a reminder that `--apply` is required
to delete:

```
✓ s3m-rm complete — dry run · 100 scanned · 8 matched · 0 errors · 30.24 KiB
  in 0.0s
  dry run — nothing was removed; add --apply to delete
```

## Error handling

Per-object failures (permissions, locked objects) are recorded in the
CSV `result` column, counted, and summarised on stderr; the run never
aborts part-way. Exit status: `0` success · `1` completed with errors ·
`2` usage error or guard refusal.

## Examples

```sh
# See what would go (dry run, CSV to stdout)
s3m-rm s3://data/scratch/old-builds/

# Remove it, with an audit trail and live progress
s3m-rm --apply -o removed.csv s3://data/scratch/old-builds/

# Masks: clean up matching objects only (quote the mask!)
s3m-rm --apply 's3://data/tmp-*' 's3://data/cache/sess_??.json'

# Count what a cleanup would remove before doing it
s3m-rm 's3://data/logs/2023-*' | tail -n +2 | wc -l

# Destroy all history of a prefix on a versioned bucket
s3m-rm --apply --permanent s3://data/leaked-secrets/

# Empty an entire bucket (explicitly)
s3m-rm --apply --entire-bucket s3://scratch-bucket/
```

## Behaviour notes

- Deleting a key that has already vanished is treated as deleted, not
  as an error — `DeleteObjects` is idempotent, matching `rm -f`.
- On an unversioned bucket, `--apply` deletes are immediate and
  permanent; `--permanent` is redundant but harmless there.
- On a versioned bucket, a plain `--apply` makes objects *look* deleted
  (a marker becomes the latest version) while every byte remains
  stored. Use `s3m-ver` to inspect and prune what remains, or
  `--permanent` here to remove key + history in one pass.
- Batches are flushed per worker; with `-j 16` up to 16 delete batches
  are in flight at once, i.e. a sustained rate of thousands of
  deletions per second against production object stores.
