# s3m — Parallel S3 Object Manager

A set of high-performance command line tools that parallelise
traditionally single-threaded object storage operations against S3 and
S3-compatible services (AWS S3, MinIO, Ceph RGW, Wasabi, Backblaze B2,
Cloudflare R2, …).

The object-storage sibling of [p3m](../p3m/): same conventions, same
look and feel, with the directory walk replaced by paginated bucket
listings and the system calls replaced by REST round trips.

## Status

Under construction — see [BRIEF.md](BRIEF.md) for the approved project
brief and tool roadmap.

## Building

```sh
make            # builds all tools into ./bin
make clean      # removes build artifacts
```

Requirements: GCC (or Clang), GNU Make, glibc with POSIX threads,
libcurl and OpenSSL development headers (`libcurl-devel`,
`openssl-devel` on EL; `libcurl4-openssl-dev`, `libssl-dev` on Debian).

## Documentation

Per-tool user documentation lives in [`docs/`](docs/).

## Licence

MIT — see [LICENSE.md](LICENSE.md).
