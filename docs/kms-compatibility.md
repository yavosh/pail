# KMS compatibility

pail serves KMS on the same listener as S3, SQS, and SNS.
Requests use SigV4 scope `kms`, `POST /`, AWS JSON 1.1, and `X-Amz-Target: TrentService.<operation>`.
An unsigned request with a KMS target reaches the KMS authentication error handler.
All services share the configured credentials, region, and account `000000000000`.

## Supported operations

| Area | Operations |
| --- | --- |
| Keys | `CreateKey`, `DescribeKey`, `ListKeys`, `UpdateKeyDescription` |
| States | `EnableKey`, `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion` |
| Aliases | `CreateAlias`, `UpdateAlias`, `DeleteAlias`, `ListAliases` |
| Tags | `TagResource`, `UntagResource`, `ListResourceTags` |
| Policies | `GetKeyPolicy`, `PutKeyPolicy`, `ListKeyPolicies` |
| Cryptography | `Encrypt`, `Decrypt`, `ReEncrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext`, `GenerateRandom` |

## Keys and identifiers

- `CreateKey` supports `KeySpec=SYMMETRIC_DEFAULT`, `KeyUsage=ENCRYPT_DECRYPT`, and `Origin=AWS_KMS`.
  Omitting these parameters selects those values. Each call creates a new enabled key.
- Key IDs are UUIDs. Key ARNs use `arn:aws:kms:<region>:000000000000:key/<id>`.
- `DescribeKey` and cryptographic operations accept a key ID, key ARN, alias name, or alias ARN.
  Other key operations require a key ID or key ARN.
- Aliases start with `alias/`, contain letters, digits, slashes, underscores, or hyphens, and have at most 256 characters.
  The `alias/aws/` prefix is reserved. Alias creation is not idempotent.
- Descriptions have at most 8192 characters. A key has at most 50 tags.
- List operations return stable sorted pages. Keys and policies default to 100 items and accept limits from 1 to 1000.
  Aliases default to 50 and accept 1 to 100. Tags default to 50 and accept 1 to 50.
  `NextMarker` is opaque and specific to the operation and filter. `Truncated` indicates another page.

## Cryptographic behavior

- `Encrypt` accepts 1 to 4096 plaintext bytes and uses AES-256-GCM with a random nonce.
  Wire binary fields use base64. SDK clients receive byte slices.
- Encryption context is authenticated as additional data. Decryption requires the same case-sensitive map, independent of entry order.
  Omitted and empty maps are equivalent. Context keys must be nonempty, and the combined context limit is 8192 bytes.
- The ciphertext identifies its key. `Decrypt` can omit `KeyId`.
  A conflicting explicit key returns `IncorrectKeyException`.
  Corruption or a different context returns `InvalidCiphertextException`.
- `ReEncrypt` changes the key, encryption context, or both. It returns the source and destination key ARNs.
- Data key operations require exactly one of `KeySpec` or `NumberOfBytes`.
  `AES_128` produces 16 bytes; `AES_256` produces 32 bytes. Explicit lengths accept 1 to 1024 bytes.
  `GenerateDataKeyWithoutPlaintext` returns only the encrypted copy and key ARN.
- `GenerateRandom` requires `NumberOfBytes`, from 1 to 1024.
- Supported cryptographic requests with `DryRun=true` validate the request and return `DryRunOperationException` without returning cryptographic output.

## Key states and persistence

Disabled keys reject cryptographic operations with `DisabledException`.
Pending-deletion keys reject cryptographic operations with `KMSInvalidStateException`.
Alias creation, tag changes, and description changes also reject pending-deletion keys.
Policy operations, tag listing, alias deletion, and moving an alias to another usable key remain available.

`ScheduleKeyDeletion` accepts 7 to 30 days and defaults to 30 days.
The key immediately enters `PendingDeletion`. The deadline is measured from the request time.
`CancelKeyDeletion` changes the state to `Disabled`; clients must enable the key separately.
The next engine operation removes expired keys and their aliases.
Deletion does not need a background worker.

Keys, aliases, tags, policies, and deletion schedules survive restarts.
See [Storage layout](storage.md#kms-keys).

## Policies and unsupported features

Policies are stored and returned without enforcing IAM authorization or policy lockout checks.
The only policy name is `default`. Omitting the name selects it.
Omitting the create policy installs an account-root policy.
Basic JSON validation requires a policy object with a `Statement` field.

Rotation, asymmetric and HMAC keys, key agreement, key encapsulation, imported material, grants, custom stores, multi-region keys, and attestation are unsupported.
Recognized unsupported operations return `400 UnsupportedOperationException`; unknown operation names return `400 UnknownOperationException`.
Grant tokens and recipient attestation parameters are rejected explicitly.
Request bodies are capped at 128 KiB.

KMS key material is stored without encryption at rest, in an owner-only file.
This local service does not provide AWS's hardware security boundary.
Ciphertext is specific to pail and cannot be exchanged with AWS KMS.
S3, SQS, and SNS encryption settings remain metadata-only.

## Verification

AWS Go SDK tests exercise the API through the real server.
AWS CLI and boto3 smoke tests cover encryption and data key workflows with default client settings.
Engine tests cover persistence, tampering, encryption context, cancellation, concurrent updates, storage failures, and deletion expiry.
The `kms-symmetric` differential scenario verifies payload and key relationships before masking random output.
Its recorded AWS fixture replays all 25 steps against pail.
Other operation details have local SDK coverage but remain unverified against AWS recordings.
