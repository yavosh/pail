# pail

pail is a small S3-compatible server written in pure Go. It targets local testing and personal projects. The goal is S3 API compatibility, not scale. pail is a work in progress.

## Run

```bash
make build
PAIL_ACCESS_KEY_ID=... PAIL_SECRET_ACCESS_KEY=... ./pail
```

pail stops cleanly on SIGINT or SIGTERM.

## Requests

- Path-style requests always work: `http://127.0.0.1:9000/<bucket>/<key>`.
- Virtual-hosted-style requests work when `--domain` is set. For example, `--domain localhost` serves `http://<bucket>.localhost:9000/<key>`.
- An operation that pail does not support returns `501 NotImplemented` with an S3 XML error.
- `GET /_pail/health` returns 200 and needs no credentials.

Supported operations:

- Buckets: `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, and `GetBucketLocation`. Bucket names follow the AWS general-purpose naming rules.
- Listing: `ListObjectsV2` and `ListObjects`, with `prefix`, `delimiter`, pagination, `fetch-owner`, and `encoding-type=url`. aws-sdk-go-v2 leaves `encoding-type=url` keys encoded; decode them with `url.QueryUnescape`. Use `encoding-type=url` for keys with control characters: XML cannot carry them, so a plain listing replaces them with U+FFFD.
- Objects: `PutObject`, `GetObject`, `HeadObject`, and `DeleteObject`. They support system and `x-amz-meta-*` metadata, `Content-MD5`, a single `Range`, the `If-*` read conditions, `If-None-Match: *` and `If-Match` on writes, and the `response-*` overrides. Objects are limited to 5 GiB, keys to 1024 bytes, and user metadata to 2 KB, as on AWS.
- Checksums: CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256, sent as an `x-amz-checksum-*` header or an `aws-chunked` trailer. pail verifies and stores one per object, computes CRC64NVME when a client sends none, as AWS does, and returns it on reads with `x-amz-checksum-mode: ENABLED` and in listings.

pail serves one region, `--region`. A `CreateBucket` with another region's `LocationConstraint` fails. Re-creating a bucket you already own succeeds in `us-east-1`, as AWS's legacy behavior there, and answers `409 BucketAlreadyOwnedByYou` in other regions.

As on AWS, a request path with a literal `..` segment gets a bare `400 Bad Request`. Percent-encoded dots, such as `%2E%2E`, are not checked. An object with a `..` key that an older pail stored is now reachable only through the encoded form.

## Authentication

Every S3 request must carry an AWS Signature Version 4 `Authorization` header, signed with the configured access key pair. pail accepts any region in the signature. A body with a signed SHA-256 payload hash is checked as it is read. Every `x-amz-*` header must be signed.

A captured signed request can be replayed for up to 15 minutes, as on AWS. Keep pail on `127.0.0.1`, or behind TLS, when the network is not trusted.

Streaming uploads (`aws-chunked`) work in the three SigV4 modes: signed chunks, unsigned chunks with a trailer, and signed chunks with a signed trailer. The AWS CLI and the SDKs send them by default over HTTPS. pail checks each chunk signature and the decoded length as it reads the body, and stores the object under its `x-amz-decoded-content-length`. It removes `aws-chunked` from the stored `Content-Encoding`.

Presigned URLs are not supported yet, and return `501 NotImplemented`.

## Configuration

Each setting is a flag with a `PAIL_*` environment fallback. A flag overrides its variable. An empty variable counts as unset.

| Flag | Variable | Default | Description |
| --- | --- | --- | --- |
| `--addr` | `PAIL_ADDR` | `127.0.0.1:9000` | Listen address. |
| `--data` | `PAIL_DATA` | `./data` | Data directory. pail creates it at startup. One pail process owns it. |
| `--access-key` | `PAIL_ACCESS_KEY_ID` | none, required | Access key ID. |
| `--secret-key` | `PAIL_SECRET_ACCESS_KEY` | none, required | Secret access key. |
| `--region` | `PAIL_REGION` | `us-east-1` | Region. |
| `--domain` | `PAIL_DOMAIN` | empty | Base domain for virtual-hosted-style requests. Empty turns them off. |
| `--log-level` | `PAIL_LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`. Case-insensitive. |
| `--version` | none | none | Print the build version and exit. |

pail exits with an error that names the missing setting when a key is empty.

Set `PAIL_SECRET_ACCESS_KEY` instead of passing `--secret-key`. Flags show in process lists. Environment variables are less exposed.
