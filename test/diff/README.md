# Differential suite

This suite checks pail against AWS S3 as a black box. It talks to both over HTTP only.

Each scenario in `scenarios_test.go` is a list of raw S3 requests. One SigV4 signer signs them, so AWS and pail receive the same requests. The suite does not use an SDK, because SDK retries and normalization would hide differences.

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

Record again after you add or change a scenario. Replay fails when a golden file does not match its scenario's steps.

## What is compared

- The status code.
- The S3 error `Code`. The error `Message` and diagnostic fields are not compared.
- A fixed list of headers, in `normalize_test.go`. `Last-Modified`, `x-amz-request-id`, and `x-amz-id-2` are compared for presence only.
- The body. An XML body is compared element by element. Values that change on every run, such as dates, owner IDs, upload IDs, and continuation tokens, are compared for presence only. Bucket names become `{bucket}`.

## Known differences

`testdata/known-diffs.txt` lists accepted differences, one per line:

```text
<scenario>/<step> <field> <reason, with an issue link>
```

`<field>` is `status`, `body`, or `header:<Name>`, as the failure message prints it. An unlisted difference fails the test. A listed difference that no longer occurs also fails, so the list stays current.

## Add a scenario

1. Add the scenario to `scenarios()`. Use a new bucket per scenario, and delete what you create.
2. Record it against AWS, and commit the golden file.
3. Run replay. Fix pail, or list each remaining difference with a reason.
