# SigV2 presigned URLs

Implement [issue #43](https://github.com/yavosh/pail/issues/43) for boto3's default presigned GET and PUT URLs.

## Scope

- Verify base64 HMAC-SHA1 signatures and Unix expiration timestamps.
- Preserve escaped paths and include virtual buckets in canonical resources.
- Canonicalize signed headers and S3 subresources, including response overrides.
- Reject modified, expired, incomplete, duplicate, and mixed-version query authentication.
- Preserve configured account ownership on SigV2 writes.
- Keep SigV2 Authorization headers and POST policies unsupported.
- Add SDK round trips in both addressing styles and default boto3 smoke checks.
- Add the `presigned-v2` differential scenario without editing AWS golden files.

## References

- [AWS SigV2 authentication rules](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTAuthentication.html)
- [botocore 1.42.97 signing implementation](https://github.com/boto/botocore/blob/1.42.97/botocore/auth.py)

## Validation

Signature tests, SDK tests, existing AWS differential replay, race tests, lint, modernizers, and the pure Go build pass.
AWS CLI and pinned boto3 smoke tests pass against disposable local storage. The vulnerability scan reports no vulnerabilities.
The maintainer-recorded `presigned-v2` AWS fixture and all existing AWS fixtures replay successfully.
