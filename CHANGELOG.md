# Changelog

All notable changes to s3m are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the suite
uses [Semantic Versioning](https://semver.org/): see
[Versioning](README.md#versioning).

## [Unreleased]

## [1.5.0] - 2026-10-02

### Added
- A single suite-wide version, kept in the `VERSION` file. Every tool
  reports it with `-V` / `--version`.
- Man pages: `s3m(7)` (conventions, connection options, environment)
  and one section-1 page per tool.
- `make install` (honours `DESTDIR` / `PREFIX`), `make dist` (source
  tarball) and `make rpm` (binary + source RPM from
  `packaging/s3m.spec.in`).
- This changelog.

### Changed
- `--version` output is now `s3m-<tool> <suite version> (s3m: Parallel
  S3 Object Manager)`. The per-tool version numbers used before
  (s3m-sync 1.2.0, s3m-cp 1.1.0, s3m-diff 1.1.0, the rest 1.0.0) are
  retired.

## [1.4.1] - 2026-10-02

### Fixed
- The live progress rate flapped wildly when object sizes varied: it
  was an exponential average of 125 ms samples (about the last 0.4 s),
  so a large object finishing or a server-side copy landing swung it.
  It is now measured over a sliding 10 second window.

## [1.4.0] - 2026-09-29

### Added
- `s3m-sync` and `s3m-cp`: `--s3fs-meta` stores each uploaded file's
  mode, mtime, uid and gid as the `x-amz-meta-*` attributes s3fs reads,
  so buckets mounted with s3fs show the original permissions and mtime.
  Multipart uploads attach them to the initiate request. Off by
  default.

## [1.3.0] - 2026-09-21

### Added
- `--rrdns` on `s3m-ls`, `s3m-du`, `s3m-rm`, `s3m-ver`, `s3m-find`,
  `s3m-cp` and `s3m-diff` (previously only `s3m-sync`; see 1.2.0).
- The explanation of `--rrdns` now lives once in `docs/connection.md`.

## [1.2.0] - 2026-09-18

### Added
- `s3m-sync --rrdns`: resolve the endpoint hostname to every A/AAAA
  address and spread worker threads across them, round robin. The
  connection is pinned to a thread's address while the `Host` header,
  TLS SNI and SigV4 signature keep the original hostname. A thread
  moves to the next address when its current one fails (transport
  errors, or 429/408/5xx after the retry budget is spent).
- Core: endpoint pool and per-handle pinning API in `s3mcore`.

## [1.1.0] - 2026-07-16

### Added
- `s3m-cp`: local sources and destinations. Local files and directories
  upload (multipart above 128 MiB); a bucket downloads to a local
  directory (atomic temp file + rename); mixed local and S3 sources
  work in one run. `--move` unlinks local sources only after a
  confirmed copy. Local-to-local is refused in favour of p3m-cp.
- `s3m-diff`: either side may be a local directory. `-c` hashes the
  local file against a conclusive etag (no network I/O) or compares in
  chunks, stopping at the first differing byte. Both-local is refused
  in favour of p3m-diff.
- `s3m-sync`: local-to-local parallel mirror with the same diff engine,
  `--delete` and dry-run default. Copies go through a temp file renamed
  into place, use `copy_file_range` with a read/write fallback, and
  preserve source mtime so re-runs converge. Syncing a directory into
  itself or its own subtree is refused up front.

### Changed
- Core: upload, download, MIME-type and MD5 helpers moved out of
  `s3m-sync` into `s3mcore` so the other tools share them.

## [1.0.0] - 2026-07-16

First release. The suite as first committed (no tags existed at the
time; this and later versions up to 1.4.1 are reconstructed from the git
history).

### Added
- `s3mcore`, the shared engine: libcurl transport with one persistent
  handle per worker thread, in-tree SigV4 signing on OpenSSL, retries
  with jittered exponential backoff (408/429/5xx and transport errors),
  a minimal S3 XML reader, AWS-compatible endpoint and credential
  resolution, the prefix-sharded parallel listing engine, the batched
  `DeleteObjects` pump, and shared CSV output, error accounting and
  live progress display.
- `s3m-ls`: parallel lister with basic / standard / full detail levels,
  storage-class filtering, `--shard-depth`, CSV output.
- `s3m-du`: du-style prefix rollups (`-s -d -c -h --si`), `--by-class`,
  `--versions`.
- `s3m-rm`: parallel remover for exact keys, prefixes and glob masks;
  batched deletes; dry run by default; whole-bucket guard
  (`--entire-bucket`); `--permanent` version destruction; overlapping
  targets de-duplicated so no key is deleted twice.
- `s3m-ver`: version and delete-marker manager: list every version,
  prune with `--keep N` / `--older-than`, `--markers` tombstone cleanup,
  `--only-markers` undelete, `--latest`; dry run by default.
- `s3m-sync`: upload, download and server-side S3-to-S3 sync with a
  three-phase design (index destination, diff source, execute plan);
  `--size-only`, `--checksum`, `--delete` (skipped when the destination
  index is incomplete); multipart uploads above 128 MiB aborted
  server-side on failure; atomic downloads restoring object mtime.
- `s3m-cp`: parallel server-side copy with aws-cli-style destination
  rules, skip-existing unless `--overwrite`, self-copy guards and
  `--move`; server-side `CopyObject` with multipart `UploadPartCopy`
  above 5 GiB.
- `s3m-find`: find(1)-style expression grammar (`! ( ) -a -o`) with
  object tests (`-name -iname -key -ikey -regex -size -mtime -mmin
  -class -etag -empty`), `-latest` / `-marker` under `--versions`,
  `-print0`. No `-delete` or `-exec` by design.
- `s3m-diff`: compare buckets or prefixes by key, size and conclusive
  etag; `-c` verifies contents with ranged GETs stopping at the first
  differing byte; diff-like exit codes and a verdict that is withheld
  when errors left coverage incomplete.
- Documentation for every tool and the shared connection options.

[Unreleased]: https://github.com/srhoods/s3m/compare/v1.5.0...HEAD
[1.5.0]: https://github.com/srhoods/s3m/releases/tag/v1.5.0
