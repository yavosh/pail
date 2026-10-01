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

Supported operations: `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, and `GetBucketLocation`. Bucket names follow the AWS general-purpose naming rules. pail serves one region, `--region`. A `CreateBucket` with another region's `LocationConstraint` fails, and creating a bucket you already own answers `409 BucketAlreadyOwnedByYou`.

## Authentication

Every S3 request must carry an AWS Signature Version 4 `Authorization` header, signed with the configured access key pair. pail accepts any region in the signature. A body with a signed SHA-256 payload hash is checked as it is read. Every `x-amz-*` header must be signed.

A captured signed request can be replayed for up to 15 minutes, as on AWS. Keep pail on `127.0.0.1`, or behind TLS, when the network is not trusted.

Presigned URLs and streaming uploads (`aws-chunked`) are not supported yet, and return `501 NotImplemented`. The AWS CLI and some SDK upload paths send streaming uploads by default.

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
