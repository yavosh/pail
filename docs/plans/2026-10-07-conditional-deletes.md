# Conditional deletes

Status: done in #49, merged on 2026-10-08.

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

The maintainer-recorded AWS fixture for `conditional-deletes` replays all 30 exchanges successfully. Matching and wildcard deletes succeed; mismatched/stale ETags preserve objects; conditional missing keys return NoSuchKey; mixed and Quiet batches retain per-key errors. A fresh full race run and lint pass with the fixture present. Existing golden files are unchanged, and no fixture was manufactured from pail.
