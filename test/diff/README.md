# Differential suite

This suite checks pail against AWS S3 as a black box. It talks to both over HTTP only.

Each scenario in `scenarios_test.go` is a list of raw S3 requests. One SigV4 signer signs them, so AWS and pail receive the same requests. A streaming step sends its body `aws-chunked` and signs each chunk with the suite's own code, so a recording checks that code against AWS. A presigned step signs the query string with the same signer and an unsigned payload. The suite does not use an SDK, because SDK retries and normalization would hide differences. The clients do not follow redirects or decompress responses, for the same reason.

The signer writes a bare subresource such as `?location` as `?location=`, as the AWS SDKs do. Golden files show the scenario form.

## Replay

Replay is the default mode, and CI runs it:

```bash
go test ./test/diff
```

Replay starts pail in-process, sends every scenario that has a golden file, and compares the results with `testdata/golden/<scenario>.json`. A scenario without a golden file is skipped.

## Record

Record mode sends the scenarios to AWS S3 in `us-east-1` and writes the golden files. A maintainer runs it by hand and commits the result.

```bash
eval "$(aws configure export-credentials --format env)"
go test ./test/diff -record -run TestDiff -v
```

- Record mode reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. The `export-credentials` command sets them from your profile or SSO login.
- It creates only buckets named `pail-diff-<random>`, and deletes them afterward, also when a step fails.
- It never calls `ListBuckets`, so the golden files never contain your other buckets.
- Review the golden file diff before you commit it.

Record again after you add or change a scenario. Each golden step stores a fingerprint of its method, key, query, headers, body, and signing mode, so replay fails when a step changed since it was recorded.

Check these steps by hand in a new recording:

- `bucket-lifecycle/head-after-delete`: AWS deletes buckets with eventual consistency, so this step can flap. Record it again if it does.
- Object writes and listings: AWS adds a default CRC64NVME checksum to every object. It shows as `x-amz-checksum-*` headers on writes and `ChecksumAlgorithm` and `ChecksumType` in listings. Those steps are in `pending.txt` until #10.

The `cors-configuration`, `lifecycle-configuration`, `acl-grants`, and `post-policy` scenarios have AWS recordings checked during replay.
The ACL scenario creates an ACL-enabled AWS bucket with `ObjectWriter` ownership.
Form steps build signed multipart policies with the suite's signing code.
They validate successful uploads and rejected signatures, expired policies, keys, fields, and sizes.

The `presigned-v2` scenario compares SigV2 uploads, downloads, response overrides, ACLs, and rejected URLs.
Record it with `go test ./test/diff -record -run '^TestDiff/presigned-v2$' -v`.
The AWS recording includes `403 AccessDenied` for missing `Expires`.

The `conditional-deletes` scenario covers matching, mismatching, wildcard, missing-key, and stale-ETag deletes, plus mixed and quiet batches.
Its maintainer-recorded AWS fixture replays all 30 exchanges successfully, including `412 PreconditionFailed` for mismatched ETags and `404 NoSuchKey` for conditional missing keys.
Re-record it with `go test ./test/diff -record -run '^TestDiff/conditional-deletes$' -v`; never edit the golden file manually.

## What is compared

- The status code.
- The S3 error `Code`. The error `Message` and diagnostic fields are not compared.
- A fixed list of headers, in `normalize_test.go`. `Last-Modified`, `x-amz-request-id`, and `x-amz-id-2` are compared for presence only.
- Request IDs on empty `400` responses for literal `..` path segments are ignored; AWS front ends vary in sending them.
- `Content-Length` only for object data. XML formatting and error messages differ between servers.
- The body. Successful object reads are compared byte for byte, including XML content. A body that is not valid UTF-8 is stored as base64. Protocol XML is compared element by element. Values that change on every run, such as dates, owner IDs, upload IDs, continuation tokens, and the `Location` URL of a completed upload, are compared for presence only. Bucket names in protocol responses become `{bucket}`.

## Known differences

`testdata/known-diffs.txt` lists accepted differences, one per line:

```text
<scenario>/<step> <field> <reason, with an issue link>
```

`<field>` is `status`, `body`, or `header:<Name>`, as the failure message prints it. An unlisted difference fails the test. A listed difference that no longer occurs also fails, so the list stays current.

## Pending steps

`testdata/pending.txt` lists steps recorded before pail implements them, one per line:

```text
<scenario>/<step> <reason, with an issue link>
```

Replay tolerates differences in a pending step, and logs them when you run `go test -v`. A difference in any other step still fails, so the steps that already work stay checked. Once a pending step has no unlisted difference, the test fails until you remove its line. A line that names no step also fails.

Use `pending.txt` for behavior pail will implement, with the issue that implements it. Use `known-diffs.txt` for accepted, permanent differences.

## Add a scenario

1. Add the scenario to `scenarios()`. Use a new bucket per scenario, and delete what you create. A step can write `{uploadId}` in its query, headers, or body. It stands for the ID that the latest `CreateMultipartUpload` step returned.
2. Record it against AWS.
3. Run replay. For each step that differs, fix pail, add the step to `pending.txt` with the issue that will implement it, or list the accepted difference in `known-diffs.txt`.
4. Commit the golden file together with its `pending.txt` and `known-diffs.txt` lines, so CI stays green.
