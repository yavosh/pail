# S3 compatibility

This page describes how pail implements the S3 operations it supports, and where it differs from AWS. For setup and a summary, see the [README](../README.md).

## Requests

- Path-style requests always work: `http://127.0.0.1:9000/<bucket>/<key>`.
- Virtual-hosted-style requests work when `--domain` is set. For example, with `--domain localhost`, pail serves `http://<bucket>.localhost:9000/<key>`.
- An operation that pail doesn't support returns `501 NotImplemented` with an S3 XML error. Requests with `x-amz-tagging` or `x-amz-tagging-directive` also return `501 NotImplemented`.
- pail ignores options that it doesn't implement on object writes. Examples are server-side encryption, Object Lock, storage class, website redirect, and `x-amz-expected-bucket-owner` (checked only on CORS and lifecycle requests). It also ignores destination preconditions on `CopyObject`, and a standalone checksum header for an algorithm it doesn't support, such as `x-amz-checksum-sha512`. A browser form field for an unsupported `x-amz-*` option returns `501 NotImplemented`.
- Like AWS, pail answers a request path with a literal `..` segment with an empty `400 Bad Request`. For GET and DELETE requests, that response includes request IDs. AWS front ends vary in sending them.
- pail doesn't check percent-encoded dots, such as `%2E%2E`. An object with a `..` key that an older pail stored is reachable only through the encoded form.

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

Like AWS, pail limits a `PutObject` body to 5 GiB, a key to 1,024 bytes, and user metadata to 2 KB.

### Copy

- `CopyObject` copies within a bucket or across buckets.
- `x-amz-copy-source` is a URL-encoded `bucket/key`.
- `x-amz-metadata-directive` is `COPY`, the default, or `REPLACE`. Like AWS, a copy onto itself needs `REPLACE`.
- If an `x-amz-copy-source-if-match`, `x-amz-copy-source-if-none-match`, `x-amz-copy-source-if-modified-since`, or `x-amz-copy-source-if-unmodified-since` condition isn't met, the copy fails with `412 PreconditionFailed`.
- A copy from a source larger than 5 GiB fails.
- pail has no versioning, so it accepts only `versionId=null`.

### Conditional deletes

- `DeleteObject` accepts `If-Match`. `DeleteObjects` accepts an `ETag` for each key.
- A matching ETag, quoted or unquoted, permits the deletion. `*` requires an existing object.
- A mismatch returns `PreconditionFailed`. A conditional delete of a missing key returns `NoSuchKey`.
- pail checks the condition and deletes the object under the same object lock.
- An unconditional delete of a missing key still succeeds.

### Batch delete

- `DeleteObjects` takes 1 to 1,000 keys and supports `Quiet`.
- The response has a `Deleted` or `Error` entry for each key. `Quiet` suppresses successes, but not failed preconditions.
- The request needs a checksum: `Content-MD5` or an `x-amz-checksum-*` header. The checksum can also arrive in an `aws-chunked` trailer. The SDKs send `x-amz-checksum-crc32` by default.

## Listing

- `ListObjectsV2` and `ListObjects` support `prefix`, `delimiter`, pagination, `fetch-owner`, and `encoding-type=url`.
- aws-sdk-go-v2 leaves `encoding-type=url` keys encoded. To decode them, use `url.QueryUnescape`.
- XML can't carry control characters, so a plain listing replaces them with U+FFFD. For keys with control characters, use `encoding-type=url`.

## Multipart uploads

pail supports `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, and `ListMultipartUploads`.

- Part numbers run from 1 to 10,000. A repeated part number replaces the part.
- Every part except the last must be at least 5 MiB. An object is at most 5 TiB.
- `CompleteMultipartUpload` needs the parts in ascending order with their ETags. It supports `If-None-Match: *` and `If-Match`.
- An upload stays until it's completed, aborted, or removed by a lifecycle rule. Like AWS, aborting an upload that already ended succeeds.
- `ListParts` and `ListMultipartUploads` return at most 1,000 entries per page. `ListMultipartUploads` doesn't support `encoding-type`.
- The object ETag is the MD5 of the part MD5s, followed by `-<parts>`.

### Multipart checksums

`x-amz-checksum-algorithm` and `x-amz-checksum-type` on `CreateMultipartUpload` choose the checksum:

| Algorithm | Checksum type |
| --- | --- |
| CRC64NVME | `FULL_OBJECT` |
| CRC32 or CRC32C | `COMPOSITE` by default, or `FULL_OBJECT` on request |
| SHA-1 or SHA-256 | `COMPOSITE` |
| None | CRC64NVME `FULL_OBJECT` |

Any other combination fails with `InvalidRequest`.

- pail checks each part's checksum on upload.
- A `COMPOSITE` object gets the checksum of its part checksums, as `<value>-<parts>`. To complete a composite upload, send each part checksum, with consecutive part numbers starting at 1.
- A `FULL_OBJECT` object gets the checksum of all its bytes. `CompleteMultipartUpload` checks it against an `x-amz-checksum-*` header.
- A checksum type sent with `CompleteMultipartUpload` must match the upload.

## Checksums

- pail supports CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256, sent as an `x-amz-checksum-*` header or an `aws-chunked` trailer.
- pail verifies and stores one checksum per object. Like AWS, it computes CRC64NVME when a client sends none.
- Reads with `x-amz-checksum-mode: ENABLED` and listings return the checksum.

## CORS

pail supports `PutBucketCors`, `GetBucketCors`, and `DeleteBucketCors`.

- Preflight requests need no authentication. Responses include cross-origin headers.
- Rules support origin and header wildcards.

## Lifecycle

pail supports `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, and `DeleteBucketLifecycle`.

- Enabled rules expire objects by age or date, and abort incomplete multipart uploads.
- Filters support prefixes and exclusive size bounds, including `And`.
- Cleanup runs at startup and every minute. Age rules round up to midnight UTC.
- Reads and writes report `x-amz-expiration`.
- Tag filters, transitions, and version actions return `501 NotImplemented`.

## ACLs

pail supports `GetBucketAcl`, `PutBucketAcl`, `GetObjectAcl`, and `PutObjectAcl`.

- pail enables ACLs on every bucket. New AWS buckets disable ACLs through Object Ownership by default. To compare ACL behavior with AWS, use an ACL-enabled AWS bucket.
- Canned ACLs, XML grants, and `x-amz-grant-*` headers persist. Canonical grants round-trip.
- Object writes, copies, forms, and multipart creation accept ACLs.
- Public grants permit anonymous reads, listings, ACL access, and new object uploads. Anonymous uploads can't overwrite objects that the configured account owns.
- pail authenticates only its configured account. A grant to another account doesn't let that account authenticate.
- Email grantees return `501 NotImplemented`.

## Browser forms

- `POST Object` accepts SigV4 policies from boto3 and aws-sdk-go-v2.
- Policies enforce expiration, exact matches, prefixes, required fields, and file size bounds.
- Forms support `${filename}`, metadata, ACLs, MD5 and flexible checksums, redirects, and the success statuses `200`, `201`, and `204`.
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
