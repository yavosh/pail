# Storage layout

Paths are relative to the data directory. `internal/store` accesses them through `vfs.FS`.

## S3 files

| Path | Contents |
| --- | --- |
| `buckets/<bucket>/bucket.json` | Bucket metadata. |
| `buckets/<bucket>/objects/<sha256(key)>.json` | Current metadata. Renaming this file into place commits a write. |
| `buckets/<bucket>/blobs/<id>` | Object bytes, immutable after commit. |
| `buckets/<bucket>/versions/<sha256(key)>/<versionID>.json` | A noncurrent version or delete marker. `null.json` holds the null version. |
| `buckets/<bucket>/uploads/<id>/upload.json` | Multipart upload metadata, removed after completion or abort. |
| `buckets/<bucket>/uploads/<id>/part-<n>.json` | Part metadata that names its data file. |
| `buckets/<bucket>/uploads/<id>/part-<n>-<id>` | Part bytes. |
| `buckets/<bucket>/ended-uploads/<id>` | An empty tombstone for a completed or aborted upload. |

## Version transitions

The current version or delete marker always lives in `objects/`.
A bucket that was never versioned uses no `versions/` path.
A key with any version has an `objects/` file.
Each key has at most one null version.

A versioned write saves the previous current record under `versions/` before replacing the record in `objects/`.
A permanent delete writes the promoted record to `objects/` before removing the old files.
`store.Open` repairs duplicates and orphans left by interrupted transitions.

See [S3 versioning compatibility](s3-compatibility.md#versioning) for API behavior.

## SQS and SNS definitions

| Path | Contents |
| --- | --- |
| `sqs/queues/<sha256(name)>.json` | Queue attributes and tags. |
| `sns/topics/<sha256(name)>.json` | Topic attributes and tags. |
| `sns/subscriptions/<uuid>.json` | Subscription metadata. The UUID comes from the subscription ARN. |

Queue messages and pending HTTP deliveries live in memory.
See [SQS and SNS compatibility](sqs-sns-compatibility.md) for persistence behavior.
