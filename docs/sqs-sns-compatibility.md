# SQS and SNS compatibility

This page describes how pail implements Amazon SQS, and where it differs from AWS. SNS support is not available yet. For setup and a summary, see the [README](../README.md).

## SQS

pail serves SQS on the same listener and port as S3. It routes a request to SQS by the `sqs` service in the SigV4 credential scope. It uses the S3 access key pair.

### Protocol

- pail speaks the AWS JSON 1.0 protocol: `POST /` with `X-Amz-Target: AmazonSQS.<Operation>` and `Content-Type: application/x-amz-json-1.0`.
- It does not support the legacy query protocol. Current AWS SDKs and the AWS CLI use JSON.
- Every response carries `x-amzn-RequestId`. Errors also carry `x-amzn-query-error: <Code>;Sender`.
- A request body is limited to 4 MiB.

### Operations

pail supports these operations:

- Queues: `CreateQueue`, `GetQueueUrl`, `DeleteQueue`, `PurgeQueue`, `ListQueues`.
- Attributes and tags: `GetQueueAttributes`, `SetQueueAttributes`, `TagQueue`, `UntagQueue`, `ListQueueTags`.
- Messages: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`.

Every other operation returns `UnsupportedOperation`. That includes `AddPermission`, `RemovePermission`, `StartMessageMoveTask`, `CancelMessageMoveTask`, `ListMessageMoveTasks`, and `ListDeadLetterSourceQueues`.

### Queue URLs and ARNs

- A queue URL is `<scheme>://<host>/000000000000/<name>`. The scheme and host come from the request that returned the URL.
- pail ignores the host when it reads a queue URL. A URL from `localhost` works through `127.0.0.1` or a container host name.
- A queue URL with another account or a malformed path returns `QueueDoesNotExist`. An empty `QueueUrl` returns `MissingParameter`.
- A queue ARN is `arn:aws:sqs:<region>:000000000000:<name>`. The region is the `--region` setting.
- A queue name is 1 to 80 letters, digits, hyphens, or underscores. `CreateQueue` with a name that ends in `.fifo` returns `UnsupportedOperation`. `GetQueueUrl` for such a name returns `QueueDoesNotExist`.

### Defaults and limits

| Setting | Default | Range |
| --- | --- | --- |
| `VisibilityTimeout` | 30 seconds | 0 to 43,200 |
| `DelaySeconds` | 0 | 0 to 900 |
| `MaximumMessageSize` | 1,048,576 bytes | 1,024 to 1,048,576 |
| `MessageRetentionPeriod` | 345,600 seconds | 60 to 1,209,600 |
| `ReceiveMessageWaitTimeSeconds` | 0 | 0 to 20 |
| `SqsManagedSseEnabled` | `true` | `true` or `false` |

- pail also accepts `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, and `Policy`. It stores them and enforces none of them.
- A queue has at most 50 tags.
- A message has at most 10 message attributes. A message body uses the SQS character set, and it can't be empty.
- `ReceiveMessage` returns 1 to 10 messages, and waits up to 20 seconds.
- `ListQueues` returns up to 1,000 queues. `MaxResults` is 1 to 1,000. `NextToken` is an opaque value from the previous page.
- `PurgeQueue` allows one purge for each queue every 60 seconds. A second purge returns `PurgeQueueInProgress`.
- A visibility timeout can't extend a message past 12 hours from its receive.
- `GetQueueAttributes` with no `AttributeNames` returns no attributes.
- Retention, delay, and visibility are evaluated on each call. pail runs no timer for them.

### Persistence

Queue definitions, which are attributes and tags, persist in the data directory. Messages live in memory. A restart keeps the queues and loses their messages.

### Long polling and shutdown

- `ReceiveMessage` with `WaitTimeSeconds` waits until a message is visible, the wait ends, or the client disconnects.
- A send, a visibility change to a shorter timeout, or the time that a delayed message becomes visible wakes a waiting receive.
- When pail shuts down, it ends every long poll with an empty response. Shutdown does not wait for the full wait time.

### Receipt handles

- Each receive of a message issues a new receipt handle. Only the newest handle changes the message.
- Deleting with a stale handle succeeds and deletes nothing. Deleting an already deleted message with its last handle also succeeds.
- A handle that pail never issued returns `ReceiptHandleIsInvalid` with HTTP status 404.
- A restart invalidates all handles.
- `ChangeMessageVisibility` with the latest handle applies even when the message is visible again. It hides the message for the new timeout.
- `ChangeMessageVisibility` with a stale handle, or for a deleted message, returns `InvalidParameterValue`. `DeleteMessage` succeeds in both cases.

### Message attributes and checksums

- Attribute types are `String`, `Number`, and `Binary`, with an optional custom label such as `String.json`. A `Number` is a decimal number.
- `StringListValues` and `BinaryListValues` return `InvalidParameterValue`.
- `MD5OfMessageBody` is the MD5 of the body. `MD5OfMessageAttributes` is the SQS attribute checksum.
- `ReceiveMessage` returns the attributes that `MessageAttributeNames` selects. The names are `All` or `.*` for every attribute, an exact name, or `prefix.*` for names that start with `prefix`. The match uses the text before `.*` without the dot, so `col.*` also matches `color`.
- The attribute checksum on a received message covers only the returned attributes. A receive that returns no attributes omits it.
- `ReceiveMessage` returns system attributes that `AttributeNames` or `MessageSystemAttributeNames` request: `SenderId`, `SentTimestamp`, `ApproximateReceiveCount`, `ApproximateFirstReceiveTimestamp`, and `AWSTraceHeader`. `All` returns every one that applies.
- `SendMessage` accepts the `AWSTraceHeader` system attribute.

### Batches

- A batch has 1 to 10 entries. Each `Id` is 1 to 80 letters, digits, hyphens, or underscores, and is unique in the batch.
- A batch body, with its message attributes, is at most 1,048,576 bytes.
- A request that breaks these rules fails as a whole with `EmptyBatchRequest`, `TooManyEntriesInBatchRequest`, `InvalidBatchEntryId`, `BatchEntryIdsNotDistinct`, or `BatchRequestTooLong`.
- An invalid entry fails alone. The response lists it in `Failed` with `SenderFault`, a `Code`, and a `Message`. The other entries still run. An entry fails alone for an invalid message, list attribute values, or a missing `VisibilityTimeout`; the last two use `InvalidParameterValue` (unverified).
- `SendMessage` and `SendMessageBatch` accept `MessageGroupId` on a standard queue and ignore it. AWS fair queues use it for tenant fairness, which pail doesn't model. `MessageDeduplicationId` on a standard queue returns `InvalidParameterValue` (unverified).

### Errors

AWS recordings in `test/diff` verify every row except those marked unverified. A failed batch entry's `Message` text differs from AWS and is not compared.

| Condition | Status | `__type` | Query code |
| --- | --- | --- | --- |
| Missing queue | 400 | `com.amazonaws.sqs#QueueDoesNotExist` | `AWS.SimpleQueueService.NonExistentQueue` |
| Missing signature, unknown key, or bad signature | 403 | `com.amazon.coral.service#...` | `AccessDenied`, `InvalidClientTokenId`, or `SignatureDoesNotMatch` |
| Operation that is not an SQS operation, or no `X-Amz-Target` | 400 | `com.amazon.coral.service#UnknownOperationException` | `InvalidAction` |
| SQS operation that pail doesn't implement (unverified) | 400 | `com.amazonaws.sqs#UnsupportedOperation` | `AWS.SimpleQueueService.UnsupportedOperation` |
| Queue exists with other attributes | 400 | `com.amazonaws.sqs#QueueNameExists` | `QueueAlreadyExists` |
| Bad attribute name or value | 400 | `com.amazonaws.sqs#InvalidAttributeName` or `#InvalidAttributeValue` | same name |
| Bad parameter, queue name, missing `QueueName`, or oversized message | 400 | `com.amazon.coral.service#InvalidParameterValueException` | `InvalidParameterValue` |
| Empty `MessageBody` | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Missing `QueueUrl` or `ReceiptHandle` (unverified) | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Invalid message characters | 400 | `com.amazonaws.sqs#InvalidMessageContents` | `InvalidMessageContents` |
| Bad receipt handle | 404 | `com.amazonaws.sqs#ReceiptHandleIsInvalid` | `ReceiptHandleIsInvalid` |
| Purge too soon | 403 | `com.amazonaws.sqs#PurgeQueueInProgress` | `AWS.SimpleQueueService.PurgeQueueInProgress` |
| Batch rule broken | 400 | `com.amazonaws.sqs#<Batch error name>` | `AWS.SimpleQueueService.<Batch error name>` |
| Malformed request body | 400 | `com.amazon.coral.service#SerializationException` | `MalformedInput` |

A failed batch entry uses the code `InvalidParameterValue`, `InvalidMessageContents`, `ReceiptHandleIsInvalid`, or `InternalError`. `InvalidMessageContents` is verified. The others are unverified.

### Differences from AWS

- pail has no `QueueDeletedRecently` delay. You can re-create a queue right after you delete it.
- pail does not enforce IAM, KMS, or queue policies. It stores `Policy` and returns it.
- pail serves one account. `SenderId` is the access key ID, where AWS returns the caller's IAM unique ID.
- `QueueOwnerAWSAccountId` in `GetQueueUrl` is ignored.
- `ReceiveRequestAttemptId` is ignored.
- A message that is sent to a standard queue is delivered in send order. AWS gives no ordering guarantee.
- pail doesn't support the SQS query protocol.
- FIFO queues, dead-letter queues, and redrive are not supported yet. `MessageDeduplicationId` returns `InvalidParameterValue`.
