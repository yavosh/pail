# AWS S3 parity review

Reviewed `main` at `5d1a82e` on 2026-10-07. This is a snapshot of the findings before fixes.

Current status: the `Status:` lines below were checked against `main` at `722508e` on 2026-10-10. Only finding A has a fix on `main`. The other findings and the gaps in sections 2 and 3 remain open.

## Summary

pail covers everyday S3 object-storage workflows, but not the full AWS S3 API. The highest-priority gaps are options that silently succeed without being enforced, rather than APIs that explicitly return `501 NotImplemented`.

The review combined route, handler, storage, and test inspection; current AWS documentation and the pinned boto3 service model; and targeted boto3 requests against disposable local pail instances. `go test -race ./...` and `golangci-lint run ./...` passed. No live AWS requests were made: local behavior was compared with AWS's documented contract.

## 1. Highest-priority compatibility gaps

### A. Conditional deletes silently delete objects

**Priority: High — data-loss risk**

Remediation: fixed in commit `0af542f`, merged in PR #49 (`7dec374`); the AWS recording is in `d8f85a2`. Both APIs now evaluate current ETags under the storage key lock; mismatch, wildcard, missing-key, stale-ETag, concurrency, and Quiet behavior have regression tests. The maintainer-recorded AWS differential fixture now replays all 30 exchanges successfully, confirming matching, mismatching, wildcard, missing-key, stale-ETag, mixed-batch, and Quiet behavior.

Confirmed locally before the fix:
- `DeleteObject(IfMatch='"wrong"')` returned 204 and deleted the object.
- `DeleteObjects` with a mismatched per-object `ETag` returned a successful `Deleted` entry and deleted the object.

AWS supports ETag-conditional deletes for general-purpose buckets. A mismatch must prevent deletion.

Code: `internal/s3api/objects.go:380`, `internal/s3api/delete.go:22`.

Recommendation: Implement both forms, checking the condition under the same storage lock as deletion.

AWS references: [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html), [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html), [conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html).

### B. CopyObject ignores destination preconditions

**Priority: High — unintended overwrite risk**

Source conditions are supported, but destination conditions are not. Copying over an existing destination with `IfNoneMatch="*"` returned 200 and overwrote it. A mismatched destination `IfMatch` also returned 200.

Code: `internal/s3api/copy.go:65`.

Status: fixed on branch feat/s3-copy-conditions-owner. `CopyObject` passes `If-Match` and `If-None-Match` to the store's conditional write, as `PutObject` does.

Recommendation: Pass destination conditions through to the existing conditional-write storage implementation.

AWS reference: [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html).

### C. Encryption and Object Lock requests succeed without protection

**Priority: High — misleading security behavior**

`PutObject(ServerSideEncryption="AES256")` returned 200, but the object's bytes were present unencrypted in pail's blob file and HEAD returned no encryption metadata. `PutObject(ObjectLockLegalHoldStatus="ON")` returned 200, followed by a successful ordinary delete.

There is no implemented SSE or Object Lock subsystem. AWS applies the requested protection or rejects requests whose prerequisites are absent.

Code: `internal/s3api/objects.go:42`, `internal/s3api/objects.go:125`.

Status: fixed on branch feat/s3-object-options. pail validates and stores the encryption method, answers SSE-C and Object Lock headers with an error, and rejects `CreateBucket` with Object Lock. `docs/s3-compatibility.md` lists the rules.

Recommendation: Reject unsupported encryption, retention, and legal-hold options before implementing the full feature. Validate PUT, copy, multipart initiation, and bucket creation consistently.

### D. Unsupported checksum headers can bypass integrity checking

**Priority: High — integrity contract violated**

pail supports CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256. Current AWS documentation also includes SHA-512, MD5 as a flexible checksum (distinct from `Content-MD5`), XXHASH64, XXHASH3, and XXHASH128.

`ChecksumAlgorithm="SHA512"` returned 400, but an incorrect standalone `ChecksumSHA512` returned 200, stored the object, and substituted a CRC64NVME checksum. The parser only examines known checksum headers, so an unsupported checksum is treated as absent.

Code: `internal/s3api/objects.go:162`, `internal/checksum/checksum.go:39`.

Status: fixed on branch feat/s3-object-options. pail adds SHA-512, MD5, and XXHASH64, answers XXHASH3 and XXHASH128 with `501 NotImplemented`, and rejects any other checksum header with `400 InvalidRequest`.

Recommendation: Reject unsupported checksum headers rather than ignoring them; add algorithms according to client demand.

AWS reference: [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).

### E. Expected-owner checks are largely missing

**Priority: High — caller safeguards not enforced**

`GetObject(ExpectedBucketOwner="000000000000")` returned 200. Only the CORS/lifecycle configuration handler checks `x-amz-expected-bucket-owner`. Object, bucket, multipart, and copy source/destination operations lack equivalent enforcement.

The configuration check compares against pail's derived canonical owner ID, whereas AWS's expected-owner parameter uses an account ID.

Code: `internal/s3api/configuration.go:29`, `internal/s3api/buckets.go:73`.

Status: fixed on branch feat/s3-copy-conditions-owner. `expectedOwner` in `internal/s3api/handler.go` checks the header for every bucket and object request, and `CopyObject` also checks `x-amz-source-expected-bucket-owner`.

Recommendation: Define an account-ID model separately from canonical ACL owner IDs and enforce checks consistently.

### F. Bucket ownership controls are silently ignored

**Priority: High for authorization-testing fidelity**

`CreateBucket(ObjectOwnership="BucketOwnerEnforced")` returned 200; setting a public bucket ACL afterward also returned 200. AWS's `BucketOwnerEnforced` disables ACLs. New AWS buckets also default to ACLs disabled and Block Public Access enabled. pail defaults to an ACL-enabled model and has no ownership-controls or public-access-block APIs.

Code: `internal/s3api/buckets.go:124`, `internal/s3api/acl.go`.

Status: fixed on branch feat/s3-ownership-controls. No code read `ObjectOwnership` before. ACL support (`e53a83e`) kept ACLs enabled on every bucket, and `docs/s3-compatibility.md` documents that.

Recommendation: Reject unsupported ownership-control requests. Decide explicitly whether AWS's current defaults or a documented legacy-style mode should be pail's default.

AWS reference: [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html).

## 2. Gaps within supported workflows

| Gap | Current pail behavior | Impact |
| --- | --- | --- |
| UploadPartCopy | Confirmed 501 | SDK-managed multipart copies, particularly copies over 5 GiB, cannot work. |
| GET/HEAD with partNumber | Routes reject them; GET confirmed 501 | Completed multipart parts cannot be retrieved individually. |
| GetObjectAttributes | Confirmed 501 | Missing combined size, ETag, checksum, and multipart-part inspection API. |
| Explicit versionId=null | GET confirmed 501; routes also exclude versioned HEAD/DELETE | Unversioned-object workflows using explicit null versions fail. Copy and batch delete already handle null versions. |
| Multipart expected size | Completing a 3-byte upload with MpuObjectSize=999 returned 200 | AWS requires 400 InvalidRequest for a size mismatch. |
| Multipart listing URL encoding | EncodingType=url is ignored; no encoding declaration returned | Special-character keys cannot reliably round-trip, especially characters XML cannot represent. |
| Storage-class options | STANDARD_IA returned 200 but no storage-class metadata was retained (fixed on branch feat/s3-object-options) | Applications may believe a requested class was applied. |
| Website redirect metadata | PUT accepted WebsiteRedirectLocation; HEAD did not return it (fixed on branch feat/s3-object-options) | Metadata is silently lost independently of website hosting support. |
| Maximum multipart object size | Hard-coded to 5 TiB | Current AWS documentation specifies 48.8 TiB; README's “as on AWS” claim is outdated. |

Status: the storage-class and website-redirect gaps are fixed on branch feat/s3-object-options. The explicit `versionId=null`, multipart expected size, multipart listing URL encoding, and maximum multipart object size gaps are fixed on branch feat/s3-multipart-extras. The other three are open. `resolve` in `internal/s3api/route.go` has no route for `UploadPartCopy`, `GetObjectAttributes`, or `partNumber` reads.

Relevant code: `internal/s3api/route.go`, `internal/s3api/multipart.go`, `internal/s3api/objects.go`, `internal/store/uploads.go:38`.

AWS references: [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html), [GetObjectAttributes](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html), [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), [multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).

## 3. Missing feature families

These are mostly intentional scope limitations rather than implementation bugs.

| Feature family | Missing capabilities |
| --- | --- |
| Versioning | Enable/suspend versioning, version IDs, version listing, delete markers, version-specific reads/deletes, MFA Delete. |
| Tagging | Bucket/object tagging APIs, upload/copy tags, tag counts, lifecycle tag filters. |
| Policy-based authorization | Bucket policies, policy status, IAM-style principals and permissions, multi-account behavior. |
| Public-access and ownership controls | Block Public Access APIs, ownership-controls APIs and enforcement. |
| Encryption | Bucket default encryption, SSE-S3, SSE-KMS/DSSE-KMS, SSE-C, bucket keys. |
| Object Lock | Configuration, retention, legal holds, governance bypass. |
| Events | Bucket notifications and delivery to queues, topics, functions, or EventBridge. |
| Storage management | Storage tiers, archive restore, lifecycle transitions/noncurrent-version actions, replication. |
| Bucket services | Website hosting, access logging, Requester Pays, acceleration. |
| Reporting and advanced services | Inventory, analytics, metrics, S3 metadata configurations, SelectObjectContent. |
| Specialized S3 products | Directory buckets/S3 Express sessions, append/rename semantics, access points, Object Lambda, multi-region access points, Outposts. |

Status: no family has been added since the review. ACLs, CORS, and lifecycle expiration were already on `main` at the review. Tag filters, transitions, and version actions in lifecycle rules still return `501 NotImplemented`.

For pail's local-development purpose, implementing everything would be excessive. Tagging, versioning, notifications, and selected policy controls are the most useful additions for broader application testing.

## 4. Existing strengths

pail supports bucket CRUD and pagination; object CRUD, metadata, ranges, and common read/write conditions; single-request copy and batch deletion; ordinary multipart uploads and checksum modes; SigV4 header/query authentication and streaming; SigV2 presigned URLs; CORS, ACLs, browser POST policies; lifecycle expiration and multipart cleanup; and both addressing styles. Integration tests cover AWS CLI, boto3, and aws-sdk-go-v2.

There are 30 named operations including browser POST. Operation count is not a meaningful parity percentage: an operation can exist while important parameters remain unsupported.

## 5. Test coverage

The differential suite has 15 golden fixtures and 266 exchanges, with no entries in `known-diffs.txt` or `pending.txt`. This is evidence for the tested subset, not complete parity:

- Confirmed gaps above are outside the recorded scenarios.
- Comparisons normalize volatile values and inspect selected headers.
- Scenarios without golden files are skipped during AWS-golden replay.
- Some tests encode assumptions without an AWS recording; combined conditional-read precedence deserves a targeted differential check.

## Recommended implementation order

1. Stop silent success: reject unsupported security, checksum, storage, and ownership options.
   Status: partly fixed on branch feat/s3-object-options. Encryption, Object Lock, checksum, and storage options are fixed. Ownership options remain.
2. Fix destructive-operation conditions: single/batch delete and destination-conditional copy.
   Status: fixed. Single and batch delete are fixed (PR #49). Destination-conditional copy is fixed on branch feat/s3-copy-conditions-owner.
3. Complete smaller gaps: expected-owner checks, multipart size validation, null versions, multipart URL encoding, redirect metadata.
   Status: partly fixed. Redirect metadata is fixed on branch feat/s3-object-options. Expected-owner checks are fixed on branch feat/s3-copy-conditions-owner. The other gaps are open.
4. Add high-value APIs: UploadPartCopy, GetObjectAttributes, part-number reads.
   Status: open.
5. Expand scope deliberately: tagging first; versioning and notifications according to actual users.
   Status: open.
6. Add AWS-recorded scenarios for each fix, especially negative cases where requests must not mutate data.
   Status: partly done. The `conditional-deletes` scenario covers the delete fix. No scenario covers the open findings.
