# CORS, lifecycle, ACLs, and browser uploads

Status: done in #47, merged on 2026-10-07.

Implement bucket CORS, lifecycle expiration, ACL grants, and SigV4 POST policies.
Keep the existing single-account, unversioned storage model.

## Scope

- Persist and validate bucket CORS and lifecycle configurations.
- Apply CORS to preflight and actual requests.
- Execute prefix and size filtered expiration and incomplete multipart cleanup.
- Report object expiration dates.
- Persist bucket and object ACLs, including write and multipart ACLs.
- Enforce public grants without accepting invalid signatures.
- Verify browser POST policies and size limits before committing objects.
- Add SDK tests in both addressing styles and CLI/boto3 smoke checks.
- Add differential scenarios without editing AWS golden files.

## Validation

Formatting, modernizers, lint, race tests, and the pure Go build pass.
AWS CLI and pinned boto3 smoke tests pass against disposable local storage.
Simulated time covers expiration boundaries and the cleanup worker.
Existing AWS golden files and the four new AWS recordings replay successfully.

## Limits

Storage tiers, object versions, tags, email grantees, and additional accounts remain unsupported.
AWS comparisons require ACL-enabled buckets because pail enables ACLs by default.
