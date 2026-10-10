# pail

pail is a small S3-compatible, SQS-compatible, and SNS-compatible server written in pure Go. It stores objects on local disk and serves Amazon SQS queues and SNS topics from the same port. Use it for local testing and personal projects. pail aims for API compatibility, not scale. It is a work in progress.

## Limitations

pail isn't a production object store:

- It has no clustering, replication, or multi-tenant support.
- It serves one access key pair and one region.
- It has no versioning, bucket policies, or storage tiers. It stores and returns encryption, storage class, and website redirect settings, but it doesn't encrypt, archive, or lock anything.
- Unsupported S3 operations fail with `501 NotImplemented`. So do requests with `x-amz-tagging` or `x-amz-tagging-directive`.
- SQS messages live in memory and are lost when pail restarts. Queue definitions persist.
- SQS FIFO queues don't model high-throughput quotas, and `ReceiveRequestAttemptId` is ignored. Dead-letter queues move a message on the next receive, not in the background.
- SQS uses the AWS JSON protocol only, which current SDKs and the AWS CLI send. It doesn't support the legacy query protocol.
- SNS delivers to SQS queues and to HTTP and HTTPS endpoints. It supports subscription filter policies on message attributes and on the message body. It has no FIFO topics, SMS, email, Lambda, or mobile push.
- SNS signs notifications with pail's own certificate, which pail serves at `/_pail/sns/signing-cert.pem`. A verifier that requires an `amazonaws.com` host rejects it.

## Install

Install pail in one of these ways:

- **Go.** Requires Go 1.27 or later.

  ```bash
  go install github.com/yavosh/pail/cmd/pail@latest
  ```

- **Docker.** Release images for Linux amd64 and arm64 are on [GitHub Container Registry](https://github.com/yavosh/pail/pkgs/container/pail). The image listens on port 9000 and stores objects in the `/data` volume.

  ```bash
  docker run -p 9000:9000 \
    -e PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000 \
    -e PAIL_SECRET_ACCESS_KEY=example-secret-key \
    -v pail-data:/data \
    ghcr.io/yavosh/pail:latest
  ```

  `latest` tracks the latest stable release. To pin a release, use its version without the `v` prefix, such as `:1.2.3`. For container settings, see [Docker](#docker).

- **Docker Compose.** Run it from a source checkout, because `compose.yaml` builds the image. Set both keys in your shell or in a `.env` file next to `compose.yaml`, and then start the service:

  ```bash
  export PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
  export PAIL_SECRET_ACCESS_KEY=example-secret-key
  docker compose up -d
  ```

- **Release binaries.** Each [GitHub release](https://github.com/yavosh/pail/releases) has `tar.gz` files for Linux and macOS on amd64 and arm64, and a `sha256sums.txt` file.

- **Source.** `make build` writes `./pail`. `make docker` builds the `pail:dev` image.

The keys in these examples are for local use only. Choose your own.

## Quickstart

This quickstart uses the [AWS CLI](https://aws.amazon.com/cli/).

1. Start pail with the example keys:

   ```bash
   export PAIL_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
   export PAIL_SECRET_ACCESS_KEY=example-secret-key
   pail
   ```

   pail listens on `127.0.0.1:9000` and stores objects in `./data`. If you built from source, run `./pail`. pail stops cleanly on SIGINT or SIGTERM.

2. In a second terminal, give the AWS CLI the same keys and pail's region:

   ```bash
   export AWS_ACCESS_KEY_ID=AKIAEXAMPLEKEY000000
   export AWS_SECRET_ACCESS_KEY=example-secret-key
   export AWS_REGION=us-east-1
   ```

   These variables override your AWS profile. In the next step, `--endpoint-url` sends every request to pail, so no request reaches AWS.

3. Create a bucket, upload an object, and list the bucket:

   ```bash
   aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
   echo "hello" | aws --endpoint-url http://127.0.0.1:9000 s3 cp - s3://demo/hello.txt
   aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://demo
   ```

   The last command prints a line for `hello.txt`.

## Configure clients

Give each client pail's endpoint, access key pair, and region. pail accepts any region in a signature. However, `CreateBucket` rejects a location constraint for any region other than pail's, which is `us-east-1` by default.

The examples below use the AWS CLI. An AWS SDK takes the same endpoint, keys, and region. For S3, also turn on path-style addressing, or see [Virtual-hosted style](#virtual-hosted-style).

### AWS CLI

To skip `--endpoint-url` on each command, set `AWS_ENDPOINT_URL` once:

```bash
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
aws s3 ls
```

Presigned URLs work with pail:

```bash
aws s3 presign s3://demo/hello.txt
```

### SQS

Point the AWS CLI at the same endpoint as S3. SQS requests use the signing scope `sqs`, and pail routes them by that scope. Pass `--endpoint-url`, or set `AWS_ENDPOINT_URL_SQS`:

```bash
aws --endpoint-url http://127.0.0.1:9000 sqs create-queue --queue-name demo
```

pail supports FIFO queues (names that end in `.fifo`, with `FifoQueue=true`) and dead-letter queues (`RedrivePolicy` and `RedriveAllowPolicy`). See [SQS and SNS compatibility](docs/sqs-sns-compatibility.md).

Queue URLs use account `000000000000` and the host of the request that returned them, for example `http://127.0.0.1:9000/000000000000/demo`. A queue URL works with any host name that reaches pail.

### SNS

Point the AWS CLI at the same endpoint as S3 and SQS. SNS requests use the signing scope `sns`. Pass `--endpoint-url`, or set `AWS_ENDPOINT_URL_SNS`:

```bash
aws --endpoint-url http://127.0.0.1:9000 sns create-topic --name demo
```

A topic ARN is `arn:aws:sns:<region>:000000000000:<name>`. To receive messages, subscribe one of these:

- A queue, with protocol `sqs` and the queue's ARN. pail confirms the subscription at once and doesn't check the queue policy.
- A URL, with protocol `http` or `https`. pail posts a `SubscriptionConfirmation` to the URL, and the endpoint confirms through its `SubscribeURL`. pail then posts notifications in the background, with retries.

Filter policies limit what a subscription receives. See [SQS and SNS compatibility](docs/sqs-sns-compatibility.md).

### Browser form uploads

To let a browser upload directly to pail, follow these steps:

1. Set bucket CORS rules that allow your application's origin and the `POST` method:

   ```bash
   aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-cors --bucket demo --cors-configuration \
     '{"CORSRules":[{"AllowedOrigins":["https://app.example.com"],"AllowedMethods":["POST"],"AllowedHeaders":["*"]}]}'
   ```

2. On your backend, create a presigned `POST` form with an AWS SDK. Sign it with Signature Version 4. Form policies require it.
3. Return the form's URL and fields to the browser. In the browser, add each field to a `FormData` object, add the file last, and send the form to the URL.

### Virtual-hosted style

The settings above send path-style requests, such as `http://127.0.0.1:9000/<bucket>/<key>`. To also accept virtual-hosted-style requests, start pail with `--domain localhost`. pail then serves `http://<bucket>.localhost:9000/<key>`. Your client must resolve `<bucket>.localhost` to pail.

## Supported operations

| Area | Operations |
| --- | --- |
| Buckets | `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, `GetBucketLocation` |
| Objects | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject` |
| Listing | `ListObjectsV2`, `ListObjects` |
| Multipart uploads | `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` |
| CORS | `PutBucketCors`, `GetBucketCors`, `DeleteBucketCors` |
| Lifecycle | `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, `DeleteBucketLifecycle` |
| ACLs | `GetBucketAcl`, `PutBucketAcl`, `GetObjectAcl`, `PutObjectAcl` |
| Browser forms | `POST Object` |
| SQS | `CreateQueue`, `GetQueueUrl`, `DeleteQueue`, `PurgeQueue`, `ListQueues`, `ListDeadLetterSourceQueues`, `GetQueueAttributes`, `SetQueueAttributes`, `TagQueue`, `UntagQueue`, `ListQueueTags`, `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` |
| SNS | `CreateTopic`, `DeleteTopic`, `ListTopics`, `GetTopicAttributes`, `SetTopicAttributes`, `TagResource`, `UntagResource`, `ListTagsForResource`, `Subscribe`, `ConfirmSubscription`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`, `SetSubscriptionAttributes`, `Publish`, `PublishBatch` |

pail also verifies checksums, honors conditional headers, and accepts `aws-chunked` streaming uploads. For limits and exact behavior, see [S3 compatibility](docs/s3-compatibility.md) and [SQS and SNS compatibility](docs/sqs-sns-compatibility.md).

## Authentication

- S3, SQS, and SNS requests use AWS Signature Version 4 (SigV4) with the configured access key pair.
- Presigned URLs accept SigV4 and Signature Version 2 (SigV2). Form policies require SigV4. SigV2 `Authorization` headers aren't supported.
- CORS preflight requests, and operations that a public ACL grant allows, need no signature.
- The SNS `SubscribeURL` and `UnsubscribeURL` links need no signature, as on AWS. A link confirms a subscription with its token, or removes a subscription that was confirmed through its link.

Like AWS, pail accepts a signed request for up to 15 minutes, so anyone who captures one can replay it in that window. If the network isn't trusted, keep pail on `127.0.0.1` or put it behind TLS.

Anyone with the keys can subscribe any URL to an SNS topic, and pail then sends HTTP requests to that URL. That includes addresses on your local network, which AWS refuses.

## Configuration

Each setting is a flag with a `PAIL_*` environment variable fallback. A flag overrides its variable. An empty variable counts as unset.

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

If a key is empty, pail exits with an error that names the missing setting. To keep the secret key out of process lists, set `PAIL_SECRET_ACCESS_KEY` instead of passing `--secret-key`.

### Docker

- The image sets `PAIL_ADDR=0.0.0.0:9000` and `PAIL_DATA=/data`.
- To change the listen address, set `PAIL_ADDR` with `-e`. Don't pass `--addr` after the image name. The container healthcheck reads `PAIL_ADDR`, so an `--addr` argument leaves the container unhealthy.
- The container runs as a non-root user, UID 65532. A named volume works as is. A bind mount must be owned by that user, for example, `chown 65532 ./data`.

### Health check

`GET /_pail/health` returns `200` and needs no credentials.

`pail --healthcheck` calls that endpoint at the `--addr` address. It exits 0 when the endpoint answers `200`, and exits 1 with an error otherwise. It needs no keys. A wildcard address, such as `0.0.0.0:9000`, is checked on loopback.

```bash
PAIL_ADDR=127.0.0.1:9000 pail --healthcheck
```

The Docker image and `compose.yaml` use `pail --healthcheck` as the container healthcheck, because the image has no `curl`.

## Development

For project guidance, see [AGENTS.md](AGENTS.md). For Go style references, see [Go code style](docs/code-style.md).

- `go test ./...` runs the unit tests and the aws-sdk-go-v2 tests.
- `make smoke` builds pail, starts it, and runs the AWS CLI and boto3 against it. It needs the AWS CLI and a Python that can import boto3. CI runs it as the `smoke` job.
- The CI `docker` job builds the image, starts it, waits for its healthcheck, and runs the AWS CLI against it.

## License

pail is released under the MIT License. See [`LICENSE`](LICENSE).
