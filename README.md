# pail

pail is a small S3-compatible server written in pure Go. It stores objects on local disk. It targets local testing and personal projects. The goal is S3 API compatibility, not scale. pail is a work in progress.

pail is not a production object store. It has no clustering, replication, or multi-tenant support. It serves one access key pair and one region. It has no versioning, ACLs, bucket policies, or lifecycle rules. An operation it does not support fails with `501 NotImplemented`.

## Install

Pick one of these.

- Go. This needs Go 1.27 or later.

  ```bash
  go install github.com/yavosh/pail/cmd/pail@latest
  ```

- Docker. The image listens on port 9000 and keeps objects in the `/data` volume.

  ```bash
  docker run -p 9000:9000 \
    -e PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000 \
    -e PAIL_SECRET_ACCESS_KEY=example-secret-key \
    -v pail-data:/data \
    ghcr.io/yavosh/pail
  ```

- Docker Compose. Set both keys in your shell or in a `.env` file next to `compose.yaml`, then start the service.

  ```bash
  export PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
  export PAIL_SECRET_ACCESS_KEY=example-secret-key
  docker compose up -d
  ```

- Release binaries. Each [GitHub release](https://github.com/yavosh/pail/releases) has `tar.gz` files for Linux and macOS on amd64 and arm64, and a `sha256sums.txt` file.

- Source. `make build` writes `./pail`. `make docker` builds the image as `pail:dev`.

The keys in these examples are for local use only. Choose your own.

## Quickstart

This quickstart needs the [AWS CLI](https://aws.amazon.com/cli/).

1. Start pail. It listens on `127.0.0.1:9000` and writes objects to `./data`.

   ```bash
   export PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
   export PAIL_SECRET_ACCESS_KEY=example-secret-key
   pail
   ```

   pail stops cleanly on SIGINT or SIGTERM.

2. In a second terminal, give the AWS CLI the same keys. These variables override any profile you have set up. The CLI sends the requests to pail, so they never reach AWS.

   ```bash
   export AWS_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
   export AWS_SECRET_ACCESS_KEY=example-secret-key
   export AWS_DEFAULT_REGION=us-east-1
   ```

3. Create a bucket, upload a file, and list the bucket. Always pass `--endpoint-url`.

   ```bash
   echo "hello" > file.txt
   aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
   aws --endpoint-url http://127.0.0.1:9000 s3 cp file.txt s3://demo/
   aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://demo
   ```

   The last command prints a line for `file.txt`.

## Client setup

Every client needs the endpoint, the access key pair, and path-style addressing. pail accepts any region in the signature.

### AWS CLI

Pass `--endpoint-url` on each command, or set `AWS_ENDPOINT_URL` once.

```bash
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
aws s3 ls
```

### aws-sdk-go-v2

Set `BaseEndpoint` and `UsePathStyle` on the S3 client options.

```go
cfg, err := config.LoadDefaultConfig(ctx,
	config.WithRegion("us-east-1"),
	config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider("AKIAEXAMPLEKEY000000", "example-secret-key", "")),
)
if err != nil {
	return err
}
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
	o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
	o.UsePathStyle = true
})
```

### boto3

Set `endpoint_url`.

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="AKIAEXAMPLEKEY000000",
    aws_secret_access_key="example-secret-key",
    region_name="us-east-1",
)
```

boto3 signs presigned URLs for a custom endpoint with Signature Version 2 by default. pail does not support Signature Version 2. To presign, create the client with `Config`:

```python
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="AKIAEXAMPLEKEY000000",
    aws_secret_access_key="example-secret-key",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)
```

### Virtual-hosted style

Clients use path-style requests with the settings above. To serve `http://<bucket>.localhost:9000/<key>`, start pail with `--domain localhost`. Your client must then resolve `<bucket>.localhost` to pail.

## Requests

- Path-style requests always work: `http://127.0.0.1:9000/<bucket>/<key>`.
- Virtual-hosted-style requests work when `--domain` is set. For example, `--domain localhost` serves `http://<bucket>.localhost:9000/<key>`.
- An operation that pail does not support returns `501 NotImplemented` with an S3 XML error.
- `GET /_pail/health` returns 200 and needs no credentials. See [Health check](#health-check).

Supported operations:

- Buckets: `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, and `GetBucketLocation`. Bucket names follow the AWS general-purpose naming rules.
- Listing: `ListObjectsV2` and `ListObjects`, with `prefix`, `delimiter`, pagination, `fetch-owner`, and `encoding-type=url`. aws-sdk-go-v2 leaves `encoding-type=url` keys encoded; decode them with `url.QueryUnescape`. Use `encoding-type=url` for keys with control characters: XML cannot carry them, so a plain listing replaces them with U+FFFD.
- Objects: `PutObject`, `GetObject`, `HeadObject`, and `DeleteObject`. They support system and `x-amz-meta-*` metadata, `Content-MD5`, a single `Range`, the `If-*` read conditions, `If-None-Match: *` and `If-Match` on writes, and the `response-*` overrides. A `PutObject` is limited to 5 GiB, keys to 1024 bytes, and user metadata to 2 KB, as on AWS.
- Copy: `CopyObject` copies within a bucket or across buckets. `x-amz-copy-source` is a URL-encoded `bucket/key`. `x-amz-metadata-directive` is `COPY` (the default) or `REPLACE`. A copy onto itself needs `REPLACE`, as on AWS. The `x-amz-copy-source-if-match`, `-if-none-match`, `-if-modified-since`, and `-if-unmodified-since` conditions fail with `412 PreconditionFailed`. A source over 5 GiB fails. pail has no versioning, so only `versionId=null` is accepted.
- Batch delete: `DeleteObjects` takes 1 to 1000 keys and supports `Quiet`. It returns a `Deleted` or `Error` entry per key. The request needs a checksum: `Content-MD5` or an `x-amz-checksum-*` header. The checksum may also arrive in an `aws-chunked` trailer. The SDKs send `x-amz-checksum-crc32` by default.
- Multipart uploads: `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, and `ListMultipartUploads`. Part numbers run from 1 to 10,000, and a repeated number replaces the part. Every part except the last must be at least 5 MiB, and an object is at most 5 TiB. `CompleteMultipartUpload` needs the parts in ascending order with their ETags, and supports `If-None-Match: *` and `If-Match`. An upload stays until it is completed or aborted. Aborting an upload that already ended succeeds, as on AWS. Also as on AWS, deleting a bucket discards its pending uploads. `ListParts` and `ListMultipartUploads` return at most 1000 entries per page. `ListMultipartUploads` does not support `encoding-type`.
- Multipart checksums: `x-amz-checksum-algorithm` and `x-amz-checksum-type` on `CreateMultipartUpload` choose the checksum. CRC64NVME is always `FULL_OBJECT`. CRC32 and CRC32C are `COMPOSITE` by default, or `FULL_OBJECT` on request. SHA-1 and SHA-256 are `COMPOSITE` only. Any other combination fails with `InvalidRequest`. pail checks each part's checksum on upload. A `COMPOSITE` object gets the checksum of its part checksums, as `<value>-<parts>`. A `FULL_OBJECT` object gets the checksum of all its bytes, and `CompleteMultipartUpload` checks it against an `x-amz-checksum-*` header. An upload with no algorithm gets a CRC64NVME `FULL_OBJECT` checksum. The object ETag is the MD5 of the part MD5s, plus `-<parts>`.
- Checksums: CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256, sent as an `x-amz-checksum-*` header or an `aws-chunked` trailer. pail verifies and stores one per object, computes CRC64NVME when a client sends none, as AWS does, and returns it on reads with `x-amz-checksum-mode: ENABLED` and in listings.

pail serves one region, `--region`. A `CreateBucket` with another region's `LocationConstraint` fails. Re-creating a bucket you already own succeeds in `us-east-1`, as AWS's legacy behavior there, and answers `409 BucketAlreadyOwnedByYou` in other regions.

As on AWS, a request path with a literal `..` segment gets a bare `400 Bad Request`. Percent-encoded dots, such as `%2E%2E`, are not checked. An object with a `..` key that an older pail stored is now reachable only through the encoded form.

## Authentication

Every S3 request must carry an AWS Signature Version 4 `Authorization` header, signed with the configured access key pair. pail accepts any region in the signature. A body with a signed SHA-256 payload hash is checked as it is read. Every `x-amz-*` header must be signed.

A captured signed request can be replayed for up to 15 minutes, as on AWS. Keep pail on `127.0.0.1`, or behind TLS, when the network is not trusted.

Streaming uploads (`aws-chunked`) work in the three SigV4 modes: signed chunks, unsigned chunks with a trailer, and signed chunks with a signed trailer. The AWS CLI and the SDKs send them by default over HTTPS. pail checks each chunk signature and the decoded length as it reads the body. It stores the object under its `x-amz-decoded-content-length`. As on AWS, a signed chunk other than the last must hold at least 8 KiB. It removes `aws-chunked` from the stored `Content-Encoding`.

Presigned URLs (query-string SigV4) work for any operation. `X-Amz-Expires` must be 0 to 604800 seconds. The payload is not signed. As on AWS, an expired URL, or one dated more than 15 minutes ahead, gets `403 AccessDenied`.

## Health check

`GET /_pail/health` returns 200 and needs no credentials.

`pail --healthcheck` calls that endpoint on the `--addr` address and exits 0 when it answers 200. It exits 1 with an error otherwise. It needs no keys. The Docker image and `compose.yaml` use it as the container healthcheck, because the image has no `curl`.

```bash
PAIL_ADDR=127.0.0.1:9000 pail --healthcheck
```

A wildcard address, such as `0.0.0.0:9000`, is checked on loopback.

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
| `--healthcheck` | none | off | Check that a pail at `--addr` answers `/_pail/health`, then exit. Needs no keys. |
| `--version` | none | none | Print the build version and exit. |

pail exits with an error that names the missing setting when a key is empty.

The Docker image sets `PAIL_ADDR=0.0.0.0:9000` and `PAIL_DATA=/data`, and runs as a non-root user.

Set `PAIL_SECRET_ACCESS_KEY` instead of passing `--secret-key`. Flags show in process lists. Environment variables are less exposed.

## Test

`go test ./...` runs the unit tests and the aws-sdk-go-v2 tests. `make smoke` starts a real pail and runs the AWS CLI and boto3 against it. It needs the AWS CLI and a Python with boto3 installed. CI runs it as the `smoke` job. The `docker` job builds the image, starts it, waits for its healthcheck, and runs the AWS CLI against it.
