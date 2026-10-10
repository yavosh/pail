# S3 compatibility

This page describes how pail implements the S3 operations it supports, and where it differs from AWS. For setup and a summary, see the [README](../README.md).

## Requests

- Path-style requests always work: `http://127.0.0.1:9000/<bucket>/<key>`.
- Virtual-hosted-style requests work when `--domain` is set. For example, with `--domain localhost`, pail serves `http://<bucket>.localhost:9000/<key>`.
- An operation that pail doesn't support returns `501 NotImplemented` with an S3 XML error.
- pail checks `x-amz-expected-bucket-owner` on every bucket and object request. See [Expected bucket owner](#expected-bucket-owner). The next section lists the object write options that pail checks.
- A browser form field for an unsupported `x-amz-*` option returns `501 NotImplemented`.
- Like AWS, pail answers a request path with a literal `..` segment with an empty `400 Bad Request`. For GET and DELETE requests, that response includes request IDs. AWS front ends vary in sending them.
- pail doesn't check percent-encoded dots, such as `%2E%2E`. An object with a `..` key that an older pail stored is reachable only through the encoded form.

## Object options

These headers apply to `PutObject`, `CopyObject`, `CreateMultipartUpload`, and the matching browser form fields. pail stores and returns the values. It doesn't encrypt, archive, or lock anything.

- Server-side encryption: `x-amz-server-side-encryption` accepts `AES256`, `aws:kms`, and `aws:kms:dsse`. Any other value returns `400 InvalidArgument`.
  - `PutObject`, `GetObject`, `HeadObject`, `CopyObject`, `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload` return the stored value. If the request set none, they return `AES256`, as AWS does with default encryption.
  - pail returns no KMS key ID header and ignores the KMS key ID, context, and bucket key headers.
  - An SSE-KMS `ETag` on AWS isn't the MD5 digest of the body. pail returns the MD5 digest.
  - `CopyObject` takes the method from its own request, not from the source.
- SSE-C: any `x-amz-server-side-encryption-customer-*` header on `PutObject` returns `403 AccessDenied`, as AWS does on new buckets. `CopyObject`, `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, and browser forms give the same answer, and so do `x-amz-copy-source-server-side-encryption-customer-*` headers. Those cases are unverified.
- Object Lock: `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and `x-amz-object-lock-legal-hold` return `400 InvalidRequest`, as on an AWS bucket without Object Lock. `CreateBucket` with `x-amz-bucket-object-lock-enabled: true` returns `501 NotImplemented`. AWS creates the bucket, so this difference is unverified.
- Storage class: `x-amz-storage-class` accepts `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, and `GLACIER_IR`. Any other value returns `400 InvalidStorageClass`.
  - `PutObject`, `HeadObject`, `GetObject`, and `UploadPart` return `x-amz-storage-class` for a class other than `STANDARD`. `GetObject` is unverified.
  - `ListObjectsV2`, `ListObjects`, `ListMultipartUploads`, and `ListParts` show the class. `ListParts` is unverified.
  - A completed multipart upload keeps the class of the upload.
  - `CopyObject` sets the class from its request. Without the header, the copy is `STANDARD`. A copy onto the same key is valid when the request changes the metadata, storage class, website redirect, or encryption. The website redirect case is unverified.
  - `GetObject` of a `GLACIER` or `DEEP_ARCHIVE` object returns `403 InvalidObjectState`. `HeadObject` works. `CopyObject` from such an object returns the same error, which is unverified. pail has no transitions and no restore.
- Website redirect: `x-amz-website-redirect-location` must start with `/`, `http://`, or `https://`. Otherwise the request returns `400 InvalidRedirectLocation`.
  - `HeadObject` and `GetObject` return the value (`GetObject` is unverified). `PutObject` doesn't echo it.
  - `CopyObject` never copies the source's redirect, whatever the metadata directive. It stores the redirect that the request sets.
  - pail doesn't serve website hosting, so it never follows the redirect.

## Buckets

pail supports `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, and `GetBucketLocation`.

- Bucket names follow the AWS general-purpose naming rules.
- pail serves one region, which `--region` sets. A `CreateBucket` request with another region's `LocationConstraint` fails.
- Re-creating a bucket that you already own succeeds in `us-east-1`, which matches the AWS legacy behavior there. In other regions, it returns `409 BucketAlreadyOwnedByYou`.
- Like AWS, deleting a bucket discards its pending multipart uploads.

## Objects

`PutObject`, `GetObject`, `HeadObject`, and `DeleteObject` support the following:

- System metadata and `x-amz-meta-*` user metadata. Responses return user metadata names in lowercase.
- `Content-MD5`.
- A single `Range`.
- The `If-*` read conditions.
- `If-None-Match: *` and `If-Match` on writes.
- The `response-*` overrides.
- `partNumber` on `GetObject` and `HeadObject`. See [Part reads](#part-reads).

Like AWS, pail limits a `PutObject` body to 5 GiB, a key to 1,024 bytes, and user metadata to 2 KB.

### Copy

- `CopyObject` copies within a bucket or across buckets.
- `x-amz-copy-source` is a URL-encoded `bucket/key`.
- `x-amz-metadata-directive` is `COPY`, the default, or `REPLACE`. Like AWS, a copy onto itself needs `REPLACE`.
- If an `x-amz-copy-source-if-match`, `x-amz-copy-source-if-none-match`, `x-amz-copy-source-if-modified-since`, or `x-amz-copy-source-if-unmodified-since` condition isn't met, the copy fails with `412 PreconditionFailed`.
- `CopyObject` also accepts `If-Match` and `If-None-Match` for the destination. pail evaluates them atomically with the write, as `PutObject` does, and a failed condition writes nothing.
  - `If-None-Match: *` returns `412 PreconditionFailed` when the destination exists.
  - `If-Match` returns `412 PreconditionFailed` when the destination's ETag differs, and `404 NoSuchKey` when the destination doesn't exist.
  - `If-None-Match` with an ETag returns `501 NotImplemented`, as `PutObject` does.
- A copy from a source larger than 5 GiB fails.
- `x-amz-copy-source` accepts `versionId`, including `null`. See [Versioning](#versioning). A source that is a delete marker named by its version ID returns `400 InvalidRequest`, and a source whose latest version is a delete marker returns `404 NoSuchKey`.

### Conditional deletes

- `DeleteObject` accepts `If-Match`. `DeleteObjects` accepts an `ETag` for each key.
- A matching ETag, quoted or unquoted, permits the deletion. `*` requires an existing object.
- A mismatch returns `PreconditionFailed`. A conditional delete of a missing key returns `NoSuchKey`.
- pail checks the condition and deletes the object under the same object lock.
- An unconditional delete of a missing key still succeeds.

### Batch delete

- `DeleteObjects` takes 1 to 1,000 keys and supports `Quiet`. Each `Object` can carry a `VersionId`. See [Versioning](#versioning).
- The response has a `Deleted` or `Error` entry for each key. `Quiet` suppresses successes, but not failed preconditions.
- The request needs a checksum: `Content-MD5` or an `x-amz-checksum-*` header. The checksum can also arrive in an `aws-chunked` trailer. The SDKs send `x-amz-checksum-crc32` by default.

## Listing

- `ListObjectsV2` and `ListObjects` support `prefix`, `delimiter`, pagination, `fetch-owner`, and `encoding-type=url`.
- aws-sdk-go-v2 leaves `encoding-type=url` keys encoded. To decode them, use `url.QueryUnescape`.
- XML can't carry control characters, so a plain listing replaces them with U+FFFD. For keys with control characters, use `encoding-type=url`.

## Part reads

`GetObject` and `HeadObject` accept `partNumber` to read one part.

- A part read returns `206 Partial Content` with the part's bytes, `Content-Range`, and a `Content-Length` of the part size. The ETag is the whole object's ETag.
- A multipart object also returns `x-amz-mp-parts-count`. A simple object has one part, so `partNumber=1` returns the whole object without that header.
- A part number past the last part returns `416 InvalidPartNumber`. `HeadObject` returns the status only.
- `partNumber=0`, a value above 10,000, or a value that isn't a positive decimal integer, returns `400 InvalidArgument`. `partNumber` with a `Range` header returns `400 InvalidRequest`.
- Conditional headers, `response-*` overrides, and the archived-class check work as for a read without `partNumber`. With `x-amz-checksum-mode: ENABLED`, a part read of a multipart object returns that part's checksum and the object's `x-amz-checksum-type`.
- pail stores the part numbers and sizes of a completed multipart object. A multipart object that was completed before pail kept them is one part. This is pail's choice, not AWS behavior.
- An empty object returns `206` for `partNumber=1` without `Content-Range`. This is unverified.

## GetObjectAttributes

`GET /bucket/key?attributes` returns the attributes that `x-amz-object-attributes` names. The header is a comma-separated list of `ETag`, `Checksum`, `ObjectParts`, `StorageClass`, and `ObjectSize`.

- The response holds only the requested elements, in this order: `ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`. The `ETag` has no quotes. The `Last-Modified` header is set.
- `Checksum` holds the object's checksum and `ChecksumType`. A composite value has no `-<parts>` suffix.
- `ObjectParts` appears only for a multipart object. It holds `PartsCount` and a page of `Part` entries with `PartNumber`, `Size`, and the part checksum. `x-amz-max-parts` (default and maximum 1,000) and `x-amz-part-number-marker` select the page.
- A missing or empty `x-amz-object-attributes` header returns `400 InvalidRequest`. An unknown name returns `400 InvalidArgument`. A missing key returns `404 NoSuchKey`.
- The response has no `Content-Type` header, as in the AWS recording.
- pail ignores the `If-*` conditional headers on this operation. This is unverified.
- `versionId` selects a version. See [Versioning](#versioning). The response has `x-amz-version-id` in a bucket that has versioning.
- An anonymous request is denied, even if the object ACL is public.

## Versioning

pail supports `PutBucketVersioning`, `GetBucketVersioning`, and `ListObjectVersions`. The AWS recording `versioning` covers the cases below unless this page marks them unverified.

### Bucket state

- `GetBucketVersioning` returns an empty `VersioningConfiguration` for a bucket that never had versioning. `PutBucketVersioning` sets `Enabled` or `Suspended`. A bucket can't return to the never-versioned state.
- Any other `Status` returns `400 MalformedXML`. `MfaDelete` set to `Enabled` returns `403 AccessDenied`, because pail has no MFA devices. `Content-MD5` is optional on `PutBucketVersioning`. This is unverified.
- Version IDs have 32 lowercase hex characters, and the oldest sorts first. A request with any other `versionId` except `null` returns `400 InvalidArgument`, even when the key doesn't exist. A well-formed ID that doesn't exist returns `404 NoSuchVersion`. This is unverified.

### Writes

- With versioning enabled, `PutObject`, `CopyObject`, `CompleteMultipartUpload`, and browser forms create a version and return `x-amz-version-id`. The earlier current version becomes noncurrent.
- With versioning suspended, a write creates the `null` version and replaces any earlier null version of the key. The response has no `x-amz-version-id`. Earlier versions with an ID stay.
- An object that was written before versioning is the `null` version.
- `If-Match` and `If-None-Match` on a write apply to the current version. A delete marker counts as a missing key. This is unverified.
- ACLs, tags, storage class, and the other object options belong to one version. `PutObjectAcl`, `GetObjectAcl`, `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging`, and `GetObjectAttributes` take `versionId`. Writes to a version echo `x-amz-version-id`.
- The ACL and tagging operations answer a delete marker as `GetObject` and `HeadObject` do: `404 NoSuchKey` with the marker headers for the current marker, and `405 MethodNotAllowed` for a marker named by its version ID. This is unverified against AWS.

### Reads

- `GetObject`, `HeadObject`, and `GetObjectAttributes` return `x-amz-version-id` for the version they serve. The null version is `null`. A bucket that never had versioning sends no header.
- `versionId=null` names the null version. In a bucket that never had versioning, it names the current object.
- `ListObjects` and `ListObjectsV2` hide a key whose current version is a delete marker.

### Delete markers

- `DeleteObject` without `versionId` adds a delete marker as the current version and returns `204` with `x-amz-delete-marker: true` and the marker's `x-amz-version-id`. While versioning is suspended, the marker is the null version and replaces the null version. A delete of a missing key adds a marker too.
- `GetObject` and `HeadObject` of a key whose latest version is a delete marker return `404 NoSuchKey` with `x-amz-delete-marker: true` and `x-amz-version-id`.
- `GetObject` or `HeadObject` with the marker's `versionId` returns `405 MethodNotAllowed` with the same two headers and `Last-Modified`.
- `DeleteObject` with `versionId` removes that version for good. The response has `x-amz-version-id`, and `x-amz-delete-marker: true` when the version was a marker. Removing the current version makes the newest remaining version current. Removing a marker restores the object. Removing a version that doesn't exist succeeds. This is unverified.
- `If-Match` on a delete with a `versionId` applies to that version. This is unverified.
- `DeleteObjects` takes a `VersionId` in each `Object`. A `Deleted` entry has `VersionId` when the request named one. When a marker was created or removed, it has `DeleteMarker` and `DeleteMarkerVersionId`. A malformed `VersionId` returns an `Error` entry with `InvalidArgument`.
- `DeleteBucket` returns `409 BucketNotEmpty` while the bucket holds any version or delete marker.

### Copy

- `x-amz-copy-source` takes `?versionId=<id>`. The response has `x-amz-copy-source-version-id` when the source bucket has versioning, and `x-amz-version-id` for the new version. `UploadPartCopy` takes the same source.

### ListObjectVersions

- The response lists `Version` and `DeleteMarker` entries in key order and, within a key, newest first. `IsLatest` marks the current one.
- `prefix`, `key-marker`, `version-id-marker`, and `max-keys` page the result. A truncated response has `NextKeyMarker` and `NextVersionIdMarker`. A `version-id-marker` without `key-marker` returns `400 InvalidArgument`.
- `delimiter` with `CommonPrefixes` and `encoding-type=url` follow `ListObjects`. These are unverified.
- A bucket that never had versioning lists every object as a `null` version. This is unverified.

### Differences from AWS

- pail has no MFA Delete.
- Lifecycle rules can't act on noncurrent versions. They return `501 NotImplemented`.
- A `versionId` on an operation that doesn't take one returns `501 NotImplemented`.

## Multipart uploads

pail supports `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, and `ListMultipartUploads`.

- Part numbers run from 1 to 10,000. A repeated part number replaces the part.
- Every part except the last must be at least 5 MiB. An object is at most 53,687,091,200,000 bytes (48.8 TiB), which is 10,000 parts of 5 GiB, as on AWS.
- `CompleteMultipartUpload` needs the parts in ascending order with their ETags. It supports `If-None-Match: *` and `If-Match`.
- `CompleteMultipartUpload` checks `x-amz-mp-object-size`. The value must be a non-negative decimal integer that equals the size of the completed object. Otherwise the request fails with `400 InvalidRequest`, commits nothing, and leaves the upload open for a retry.
- An upload stays until it's completed, aborted, or removed by a lifecycle rule. Like AWS, aborting an upload that already ended succeeds.
- `ListParts` and `ListMultipartUploads` return at most 1,000 entries per page.
- `ListMultipartUploads` supports `encoding-type=url`. It encodes `Key`, `KeyMarker`, `NextKeyMarker`, `Prefix`, `Delimiter`, and `CommonPrefixes` as `ListObjects` does, and returns `EncodingType`. Any other value fails with `400 InvalidArgument`.
- The object ETag is the MD5 of the part MD5s, followed by `-<parts>`.

### UploadPartCopy

`UploadPartCopy` copies a source object, or a byte range of it, into a part of an upload.

- `x-amz-copy-source` works as in `CopyObject`, including `versionId=null`. `x-amz-source-expected-bucket-owner` applies to the source bucket.
- `x-amz-copy-source-range` must have the form `bytes=first-last`. A range that is malformed, or that extends past the last byte of the source, returns `400 InvalidArgument`. Without the header, pail copies the whole source.
- `x-amz-copy-source-if-match`, `-if-none-match`, `-if-modified-since`, and `-if-unmodified-since` act as on `CopyObject`. A failed condition returns `412 PreconditionFailed`.
- A missing source returns `404 NoSuchKey`. An unknown upload returns `404 NoSuchUpload`. A source in the `GLACIER` or `DEEP_ARCHIVE` class returns `403 InvalidObjectState`, which is unverified.
- The response is a `CopyPartResult` with `LastModified` and the `ETag` of the copied bytes. If the upload has a checksum algorithm, the result also carries the part checksum, computed from the copied bytes. This is unverified. The response also returns `x-amz-server-side-encryption`, as `UploadPart` does, which is unverified.
- A copied part follows the `UploadPart` size rules: at most 5 GiB, and the 5 MiB minimum applies at `CompleteMultipartUpload`. The maximum is unverified.
- An empty source without a range copies an empty part. Any range on an empty source returns `400 InvalidArgument`. Both are unverified.

### Multipart checksums

`x-amz-checksum-algorithm` and `x-amz-checksum-type` on `CreateMultipartUpload` choose the checksum:

| Algorithm | Checksum type |
| --- | --- |
| CRC64NVME | `FULL_OBJECT` |
| CRC32 or CRC32C | `COMPOSITE` by default, or `FULL_OBJECT` on request |
| SHA-1 or SHA-256 | `COMPOSITE` |
| SHA-512, MD5, or XXHASH64 | `COMPOSITE` (unverified) |
| None | CRC64NVME `FULL_OBJECT` |

Any other combination fails with `InvalidRequest`. For SHA-512, MD5, and XXHASH64, pail follows the SHA-1 and SHA-256 rules, because no AWS recording covers multipart uploads with them.

- pail checks each part's checksum on upload.
- A `COMPOSITE` object gets the checksum of its part checksums, as `<value>-<parts>`. To complete a composite upload, send each part checksum, with consecutive part numbers starting at 1.
- A `FULL_OBJECT` object gets the checksum of all its bytes. `CompleteMultipartUpload` checks it against an `x-amz-checksum-*` header.
- A checksum type sent with `CompleteMultipartUpload` must match the upload.

## Checksums

- pail supports CRC32, CRC32C, CRC64NVME, SHA-1, SHA-256, SHA-512, MD5, and XXHASH64, sent as an `x-amz-checksum-*` header or an `aws-chunked` trailer.
  - `x-amz-checksum-md5` is a flexible checksum. It's separate from `Content-MD5`.
  - The `XXHASH64` value is the 8-byte big-endian digest of XXH64 with seed 0, in base64.
- `x-amz-checksum-xxhash3` and `x-amz-checksum-xxhash128`, and `x-amz-sdk-checksum-algorithm` and `x-amz-checksum-algorithm` values that name them, return `501 NotImplemented`. AWS supports both algorithms.
- Any other `x-amz-checksum-<name>` header returns `400 InvalidRequest`. pail never ignores a checksum that it can't verify. The exceptions are the real headers `x-amz-checksum-type`, `x-amz-checksum-mode`, and `x-amz-checksum-algorithm`.
- pail verifies and stores one checksum per object. Like AWS, it computes CRC64NVME when a client sends none.
- Reads with `x-amz-checksum-mode: ENABLED` and listings return the checksum.

## Tagging

pail supports `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging`, `PutBucketTagging`, `GetBucketTagging`, and `DeleteBucketTagging`. AWS recordings verify the behavior below, except where noted.

- `PutObjectTagging` replaces the tag set and returns `200`. `Content-MD5` is optional. It doesn't change the object's ETag or `Last-Modified`.
- `GetObjectTagging` returns the tags in the order you stored them. An object with no tags returns an empty `TagSet`. A missing key returns `404 NoSuchKey`.
- `DeleteObjectTagging` returns `204`.
- `PutBucketTagging` and `DeleteBucketTagging` return `204`. `GetBucketTagging` returns `404 NoSuchTagSet` when the bucket has no tags, including after a `PutBucketTagging` with an empty tag set.
- Limits: 10 tags for an object. A bucket allows 50 tags, which comes from the AWS documentation and isn't recorded. A key has 1 to 128 characters and a value has up to 256. A value can be empty.
- A tag set with too many tags returns `400 BadRequest`. A duplicate key, an empty key, a key that starts with `aws:`, or a key or value over its limit returns `400 InvalidTag`. A malformed document returns `400 MalformedXML`. pail doesn't restrict the characters in a tag.
- `x-amz-tagging` sets tags on `PutObject`, `CopyObject`, and `CreateMultipartUpload`. It uses URL query encoding, such as `a=1&b=2`. A key without `=` has an empty value. A multipart upload keeps its tags through `CompleteMultipartUpload`.
- `CopyObject` takes `x-amz-tagging-directive`. `COPY`, the default, copies the source's tags. `REPLACE` takes the tags from `x-amz-tagging`. Any other value returns `400 InvalidArgument`.
- A browser form sets tags with a `tagging` field that holds a `Tagging` XML document. This isn't recorded against AWS.
- `GetObject` and `HeadObject` return `x-amz-tagging-count` when the object has tags and the request is signed. An anonymous read, allowed by an ACL, gets no count, because AWS returns it only to a caller allowed to read the tags. This is unverified.
- `PutObject` and `CopyObject` replace the whole object, so an overwrite keeps no earlier tags.

## CORS

pail supports `PutBucketCors`, `GetBucketCors`, and `DeleteBucketCors`.

- Preflight requests need no authentication. Responses include cross-origin headers.
- Rules support origin and header wildcards.

## Lifecycle

pail supports `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, and `DeleteBucketLifecycle`.

- Enabled rules expire objects by age or date, and abort incomplete multipart uploads.
- Filters support prefixes, tags, and exclusive size bounds, including `And`. An object matches only when it has every listed tag with the same value.
- Cleanup runs at startup and every minute. Age rules round up to midnight UTC.
- Reads and writes report `x-amz-expiration`.
- Tag filters aren't recorded against AWS. pail follows the AWS documentation.
- A rule with a tag filter can't also abort multipart uploads. It returns `400 InvalidArgument`.
- Transitions and noncurrent-version actions return `501 NotImplemented`.
- In a bucket with versioning, expiration of the current version adds a delete marker, as on AWS. This is unverified. With versioning enabled, the data stays as a noncurrent version. When versioning is suspended, the marker is a null marker. It replaces a null current version, and pail then removes that version's data, as on AWS.

## Event notifications

pail supports `PutBucketNotificationConfiguration` and `GetBucketNotificationConfiguration`. It delivers object events to SQS queues and SNS topics in the same pail.

- A configuration is a list of `QueueConfiguration` and `TopicConfiguration` elements. Each has an optional `Id`, a destination ARN, one or more `Event` values, and an optional `Filter` with `prefix` and `suffix` rules. pail generates a missing `Id`.
- `GetBucketNotificationConfiguration` returns an empty `NotificationConfiguration` when the bucket has none. It returns the topic configurations first, then the queue configurations. Filter rule names come back as `Prefix` and `Suffix`, as on AWS.
- An empty `NotificationConfiguration` clears the configuration.
- pail raises these events: `s3:ObjectCreated:*`, `Put`, `Post`, `Copy`, `CompleteMultipartUpload`, and `s3:ObjectRemoved:*`, `Delete`, `DeleteMarkerCreated`. `PutObject`, `POST Object`, `CopyObject`, `CompleteMultipartUpload`, `DeleteObject`, and each key that `DeleteObjects` removes raise an event after the write commits. `DeleteMarkerCreated` needs versioning.
- Validation errors:
  - An unknown event, a bad filter rule name, a repeated filter rule, or a repeated `Id` returns `400 InvalidArgument`.
  - A destination that doesn't exist in this pail, a FIFO queue, or a destination with the wrong service returns `400 InvalidArgument`. The ARN's region and account must be pail's.
  - Two configurations that share an event type and have overlapping prefixes and suffixes return `400 InvalidArgument`.
  - Malformed XML, or a queue configuration with a `Topic` element, returns `400 MalformedXML`.
- `CloudFunctionConfiguration`, `LambdaFunctionConfiguration`, and `EventBridgeConfiguration` return `501 NotImplemented`. So do the AWS event types pail never raises: object restore, replication, lifecycle, intelligent tiering, tagging, and ACL events.
- A `PutBucketNotificationConfiguration` that adds a destination sends it a test event: `{"Service":"Amazon S3","Event":"s3:TestEvent","Time":...,"Bucket":...,"RequestId":...,"HostId":...}`. pail sends it only to a destination that the old configuration lacked. Whether AWS also sends one for a changed filter or event list is unverified.
- An event is a `Records` array with one record. It carries the AWS fields `eventVersion` `2.6`, `eventSource`, `awsRegion`, `eventTime`, `eventName`, `userIdentity`, `requestParameters.sourceIPAddress`, `responseElements`, and `s3` with `configurationId`, `bucket`, and `object`. The object key is URL-encoded, with `/` kept. A removal has no `size` or `eTag`. A copy adds `hasObjectAnnotation: false`. The AWS recording verifies this shape.
- A queue destination gets the event as the message body. A topic destination gets it as the message, with the subject `Amazon S3 Notification`. The subject is from the AWS documentation and is unverified.
- The `sequencer` is a hex string that grows with every event in a pail process. Its format is unverified.
- Difference from AWS: pail delivers before it answers the S3 request. AWS delivers in the background, usually within seconds. A failed delivery is logged and never fails the request. A queue or topic that disappears after the configuration is stored drops the events.
- Difference from AWS: pail doesn't check queue or topic policies. AWS needs a policy that lets `s3.amazonaws.com` send. `PutBucketNotificationConfiguration` also doesn't fail when the test event can't be delivered.
- A delete of a key that doesn't exist raises `ObjectRemoved:Delete` in a bucket that never had versioning. This is unverified.
- In a bucket with versioning enabled or suspended, an event for a write carries `s3.object.versionId`, the version the write created. A write to a suspended bucket creates the null version, which has no ID, so its event has none. This is from the AWS documentation and is unverified.
- In a bucket with versioning enabled or suspended, a `DeleteObject` or `DeleteObjects` entry without a `versionId` raises `ObjectRemoved:DeleteMarkerCreated` with the marker's `versionId` (`null` when suspended). A delete with a `versionId` raises `ObjectRemoved:Delete` with that `versionId`. This is from the AWS documentation and is unverified. A bucket that never had versioning keeps the event shape above.
- The configuration is stored with the bucket and removed with it.

## ACLs

pail supports `GetBucketAcl`, `PutBucketAcl`, `GetObjectAcl`, and `PutObjectAcl`.

- pail enables ACLs on a bucket that has no ownership controls. New AWS buckets disable ACLs by default. See [Ownership controls](#ownership-controls). To compare ACL behavior with AWS, use an ACL-enabled AWS bucket.
- Canned ACLs, XML grants, and `x-amz-grant-*` headers persist. Canonical grants round-trip.
- Object writes, copies, forms, and multipart creation accept ACLs.
- Public grants permit anonymous reads, listings, ACL access, and new object uploads. Anonymous uploads can't overwrite objects that the configured account owns.
- pail authenticates only its configured account. A grant to another account doesn't let that account authenticate.
- Email grantees return `501 NotImplemented`.

## Expected bucket owner

pail serves one account, `000000000000`. It checks `x-amz-expected-bucket-owner` after authentication and before the operation runs, so a failed check changes nothing.

- A value that isn't exactly 12 digits returns `400 InvalidBucketOwnerAWSAccountID`.
- A 12-digit value other than the account returns `403 AccessDenied`.
- The account's own ID, `000000000000`, passes.
- `CopyObject` applies the same rules to `x-amz-source-expected-bucket-owner` for the source bucket.
- The check covers bucket and object requests, signed or anonymous. It doesn't cover `CreateBucket`, because the bucket doesn't exist yet, or `ListBuckets`. Browser POST forms ignore the header. The AWS recording covers the common operations, not these three.

## Ownership controls

pail supports `PutBucketOwnershipControls`, `GetBucketOwnershipControls`, and `DeleteBucketOwnershipControls`. `CreateBucket` accepts `x-amz-object-ownership`.

- Valid values are `BucketOwnerEnforced`, `BucketOwnerPreferred`, and `ObjectWriter`. Other values return `400 InvalidArgument` on `CreateBucket` and `400 MalformedXML` on `PutBucketOwnershipControls`.
- `BucketOwnerEnforced` disables ACLs. `PutObjectAcl` and `PutBucketAcl` return `400 AccessControlListNotSupported`. Writes that set an ACL other than `private` or `bucket-owner-full-control`, or any `x-amz-grant-*` header, return the same error. This covers `CreateBucket`, including a re-create of an existing bucket in `us-east-1`, `PutObject`, `CopyObject`, `CreateMultipartUpload`, and browser forms. The re-create case is unverified.
- `GetObjectAcl` and `GetBucketAcl` still work under `BucketOwnerEnforced`. They report the bucket owner with `FULL_CONTROL`. Stored grants allow nothing, so anonymous requests that a public ACL once allowed return `403`.
- `BucketOwnerPreferred` and `ObjectWriter` keep ACLs enabled. pail doesn't change object ownership for either value.
- Difference from AWS: a bucket created without `x-amz-object-ownership` has no ownership controls. ACLs stay enabled, and `GetBucketOwnershipControls` returns `404 OwnershipControlsNotFoundError`. AWS creates such a bucket with `BucketOwnerEnforced`. `DeleteBucketOwnershipControls` returns a bucket to this state.
- Behavior for `CopyObject`, `CreateMultipartUpload`, and browser forms isn't recorded against AWS. The AWS documentation describes it.

## Browser forms

- `POST Object` accepts SigV4 policies from boto3 and aws-sdk-go-v2.
- Policies enforce expiration, exact matches, prefixes, required fields, and file size bounds.
- Forms support `${filename}`, metadata, ACLs, the object options above, tags, MD5 and flexible checksums, redirects, and the success statuses `200`, `201`, and `204`.
- The file field must come last.

## Authentication

S3 requests use AWS Signature Version 4 (SigV4) with the configured access key pair.

- pail accepts any region in the signature.
- Every `x-amz-*` header must be signed.
- pail checks a body with a signed SHA-256 payload hash as it reads the body.
- Like AWS, a captured signed request can be replayed for up to 15 minutes.
- CORS preflight requests, and operations that a public ACL grant allows, need no signature.
- Presigned URLs and form policies delegate access without sharing the secret key.

### Streaming uploads

- `aws-chunked` uploads work in three SigV4 modes: signed chunks, unsigned chunks with a trailer, and signed chunks with a signed trailer.
- The AWS CLI and the SDKs send them by default over HTTPS.
- pail checks each chunk signature and the decoded length as it reads the body. It stores the object under its `x-amz-decoded-content-length`.
- Like AWS, each signed chunk except the last must hold at least 8 KiB.
- pail removes `aws-chunked` from the stored `Content-Encoding`.

### Presigned URLs with SigV4

- Query-string SigV4 works for any operation.
- `X-Amz-Expires` must be 0 to 604,800 seconds.
- The payload isn't signed.
- Like AWS, an expired URL, or one dated more than 15 minutes ahead, gets `403 AccessDenied`.

### Presigned URLs with SigV2

- SigV2 URLs use `AWSAccessKeyId`, `Signature`, and `Expires`. `Expires` is a Unix timestamp, without the SigV4 seven-day limit.
- The signature covers the method, MD5, content type, `x-amz-*` headers, escaped resource path, and S3 subresources. Like S3 SigV2, other query parameters stay unsigned.
- Expired URLs, and URLs without `Expires`, get `403 AccessDenied`.
- Upload clients must send the headers used when presigning.
- SigV2 `Authorization` headers aren't supported.
