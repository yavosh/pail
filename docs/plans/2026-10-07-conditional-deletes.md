# Conditional deletes

Implement finding A in `findings.md`: failed delete preconditions must not remove objects.

## Scope

- Add optional ETag conditions to the storage delete contract.
- Read current metadata and evaluate the condition under the key lock held through deletion.
- Accept a strong quoted or unquoted ETag, and `*` for any existing object.
- Preserve unconditional missing-key success; conditional missing keys return `NoSuchKey`.
- Pass `If-Match` from DeleteObject and each XML `ETag` from DeleteObjects.
- Preserve per-key errors and Quiet semantics in batch deletes.
- Cover mismatch, match, wildcard, absent keys, empty conditions, stale ETags, and lock coverage.
- Add SDK tests in both addressing styles and a separate differential scenario without modifying existing golden files.

## References

- [AWS conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)
- [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
- [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)

## Validation

Targeted tests, full race tests, lint, modernizers, and a pure Go build pass. Repeated targeted race tests pass, and pinned boto3 probes now return 412 for a single mismatched ETag and a per-key PreconditionFailed error in batch responses, preserving both objects.

The new differential scenario executes in the local determinism test, but AWS-golden replay skips it until a maintainer records `go test ./test/diff -record -run '^TestDiff/conditional-deletes$' -v`. No golden files were edited or manufactured from pail.
