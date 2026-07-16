# s3m — Parallel S3 Object Manager

A set of high-performance command line tools that parallelise
traditionally single-threaded object storage operations against S3 and
S3-compatible services (AWS S3, MinIO, Ceph RGW, Wasabi, Backblaze B2,
Cloudflare R2, …).

The object-storage sibling of [p3m](../p3m/): same conventions, same
look and feel, with the directory walk replaced by paginated bucket
listings and the system calls replaced by REST round trips.

## Why

Every S3 operation is an HTTPS round trip, and a listing stream is
capped at 1,000 keys per request — so single-threaded tools spend
almost all of their wall-clock time waiting on the network, and the
service itself is explicitly built for massive request concurrency.
The s3m tools shard the key space by prefix, keep many requests in
flight across a pool of worker threads, and batch mutations
(`DeleteObjects`, 1,000 keys per round trip), turning hours-long bucket
operations into minutes.

## Tools

| Tool | Description | Docs |
|------|-------------|------|
| `s3m-ls` | Parallel bucket lister with CSV output, three detail levels, storage-class filtering and live progress | [docs/s3m-ls.md](docs/s3m-ls.md) |
| `s3m-du` | Parallel usage reporter: du-style prefix rollups (`-s -c -d -h --si`), storage-class breakdown, `--versions` for what is actually stored and billed | [docs/s3m-du.md](docs/s3m-du.md) |
| `s3m-rm` | Parallel remover: exact keys, prefixes and glob masks, batched deletes, dry-run by default, non-overridable whole-bucket guard, `--permanent` version destruction | [docs/s3m-rm.md](docs/s3m-rm.md) |
| `s3m-ver` | Version & delete-marker manager: list everything stored, prune with `--keep N` / `--older-than`, tombstone cleanup, undelete; dry-run by default | [docs/s3m-ver.md](docs/s3m-ver.md) |
| `s3m-sync` | Parallel synchroniser: local↔S3, S3↔S3 and local↔local, size/mtime/checksum comparison, `--delete`, multipart uploads, atomic copies; dry-run by default | [docs/s3m-sync.md](docs/s3m-sync.md) |
| `s3m-cp` | Parallel copy: server-side S3↔S3 (multipart `UploadPartCopy` >5 GiB), local→S3 upload, S3→local download; skip-existing unless `--overwrite`, `--move` renames; dry-run by default | [docs/s3m-cp.md](docs/s3m-cp.md) |
| `s3m-find` | Parallel find: classic expression grammar (`! ( ) -a -o`), object tests (`-name -key -regex -size -mtime -class -etag -empty`), `--versions` with `-latest`/`-marker`, `-print0`; deliberately no `-delete`/`-exec` | [docs/s3m-find.md](docs/s3m-find.md) |
| `s3m-diff` | Parallel comparison of buckets/prefixes or a local directory vs a bucket: keys, sizes, conclusive etags by default; `-c` content verification (local MD5 vs etag, or chunked reads stopping at the first differing byte); diff-like exit codes and a verdict | [docs/s3m-diff.md](docs/s3m-diff.md) |

Shared connection behaviour (endpoints, credentials, addressing,
retries) is documented in [docs/connection.md](docs/connection.md).

## Conventions

Inherited from p3m and uniform across every tool:

- **CSV output** with a header row and RFC 4180 quoting; row order is
  non-deterministic because work is parallel.
- **Dry run by default** for anything that changes the object store;
  `--apply` is the only way to mutate. There is deliberately no
  `--dry-run` flag — that is the default state.
- `-j/--threads` worker pool (default 16 — S3 concurrency is
  latency-bound, not CPU-bound, so raise it freely for big jobs),
  `-o FILE` output with a live progress block on the terminal,
  `-q` to silence stdout only.
- Errors are per-entry: counted, first 24 collected, reported on
  stderr at the end; runs never abort part-way.
- Exit codes: `0` success · `1` completed with errors · `2` usage,
  startup or safety-guard refusal.

## Building

```sh
make            # builds all tools into ./bin
make clean      # removes build artifacts
```

Requirements: GCC (or Clang), GNU Make, glibc with POSIX threads, and
development headers for libcurl and OpenSSL (`libcurl-devel` +
`openssl-devel` on EL, `libcurl4-openssl-dev` + `libssl-dev` on
Debian/Ubuntu). Requests are signed with an in-tree SigV4
implementation, so any libcurl ≥ 7.32 is fine.

## Testing

The tools are validated against a local [MinIO](https://min.io) server:
seed a versioned and an unversioned bucket, then exercise every tool —
the docs' examples all run as-is with
`--endpoint http://127.0.0.1:9000`. No default AWS account is ever
touched by tests. A git-ignored `testenv/` directory is the suggested
home for the server binary and its data.

## Documentation

Per-tool user documentation lives in [`docs/`](docs/); the project
brief and design rationale in [BRIEF.md](BRIEF.md).

## Licence

MIT — see [LICENSE.md](LICENSE.md).
