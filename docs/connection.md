# Connecting to S3 and S3-compatible services

Every s3m tool takes the same connection options and resolves endpoint,
region and credentials the same way. Targets are always `s3://` URIs:

```
s3://BUCKET            the whole bucket
s3://BUCKET/PREFIX     everything whose key starts with PREFIX
```

## Options

| Option | Description |
|--------|-------------|
| `--endpoint URL` | S3 endpoint, e.g. `https://minio.example.com:9000`. Default: AWS (`https://s3.<region>.amazonaws.com`). |
| `--region R` | SigV4 signing region. Non-AWS services usually accept any value; the default is `us-east-1`. |
| `--profile NAME` | Profile to read from `~/.aws/credentials` / `~/.aws/config`. |
| `--path-style` | Path-style addressing (`host/bucket/key`) instead of virtual-hosted (`bucket.host/key`). Required by most self-hosted services without wildcard DNS. |
| `--no-sign` | Anonymous (unsigned) access, for public buckets. |
| `--insecure` | Skip TLS certificate verification. |
| `--ca-bundle FILE` | CA bundle for endpoints with a private CA. |

## Resolution order

Flags always win, then environment, then profile files, then defaults:

| Setting | Order |
|---------|-------|
| Endpoint | `--endpoint` → `S3M_ENDPOINT` → `AWS_ENDPOINT_URL_S3` → `AWS_ENDPOINT_URL` → profile `endpoint_url` → AWS regional endpoint |
| Region | `--region` → `AWS_REGION` → `AWS_DEFAULT_REGION` → profile `region` → `us-east-1` |
| Profile | `--profile` → `AWS_PROFILE` → `default` |
| Credentials | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`) → profile in `~/.aws/credentials` → profile in `~/.aws/config` |

`AWS_SHARED_CREDENTIALS_FILE` and `AWS_CONFIG_FILE` relocate the profile
files, as with the AWS CLI. If no credentials are found the tool refuses
to start; pass `--no-sign` explicitly for anonymous access.

Requests are signed with AWS Signature Version 4 only.

## Addressing style

Virtual-hosted addressing (`bucket.host`) is the default, matching AWS.
Path-style is enabled automatically when the endpoint host is an IP
address, `localhost`, or a single-label hostname — so
`--endpoint http://127.0.0.1:9000` works against MinIO with no further
flags. Anything else that needs it (e.g. a self-hosted service behind a
DNS name without a wildcard record) should pass `--path-style`.

## Load balancing across multiple endpoint addresses

Every tool that runs a worker pool against S3 (`s3m-ls`, `s3m-du`,
`s3m-rm`, `s3m-ver`, `s3m-find`, `s3m-cp`, `s3m-diff`, `s3m-sync`)
accepts `--rrdns`. It resolves the endpoint's hostname (via normal
DNS — A and AAAA records) once at startup and assigns each worker
thread its own address from the list, round robin: with 16 threads and
4 resolved addresses, each address gets 4 threads. If there are more
threads than resolved addresses, several threads simply share an
address.

The TCP connection is pinned to a thread's assigned address, but the
`Host` header, TLS SNI and the SigV4 signature always use the original
hostname, so this is transparent to certificates and to any endpoint
that validates the `Host` header — it changes only which address a
thread's traffic goes to, not how requests look on the wire.

If a thread's current address starts failing — connection errors,
timeouts, or repeated 429/408/5xx after the normal retry budget is
exhausted — that thread moves to the next address in the list (round
robin) for its subsequent requests; it does not automatically return
to the failed one.

Requires the endpoint to be a hostname, not a literal IP address
(nothing to round-robin).

## Compatibility notes

- Tested against MinIO; the tools speak plain S3 REST (ListObjectsV2,
  ListObjectVersions, DeleteObjects, CopyObject, multipart upload) and
  work with any service implementing those.
- Throttling responses (HTTP 429/503 `SlowDown`) and transient transport
  errors are retried up to 8 times with exponential backoff and jitter;
  retry counts are visible in each tool's progress display.
- Uploads use `UNSIGNED-PAYLOAD` streaming (the AWS CLI does the same);
  use HTTPS endpoints if payload signing matters to your threat model.
