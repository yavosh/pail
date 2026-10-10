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

- Queues: `CreateQueue`, `GetQueueUrl`, `DeleteQueue`, `PurgeQueue`, `ListQueues`, `ListDeadLetterSourceQueues`.
- Attributes and tags: `GetQueueAttributes`, `SetQueueAttributes`, `TagQueue`, `UntagQueue`, `ListQueueTags`.
- Messages: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`.

Every other operation returns `UnsupportedOperation`. That includes `AddPermission`, `RemovePermission`, `StartMessageMoveTask`, `CancelMessageMoveTask`, and `ListMessageMoveTasks`.

### Queue URLs and ARNs

- A queue URL is `<scheme>://<host>/000000000000/<name>`. The scheme and host come from the request that returned the URL.
- pail ignores the host when it reads a queue URL. A URL from `localhost` works through `127.0.0.1` or a container host name.
- A queue URL with another account or a malformed path returns `QueueDoesNotExist`. An empty `QueueUrl` returns `MissingParameter`.
- A queue ARN is `arn:aws:sqs:<region>:000000000000:<name>`. The region is the `--region` setting.
- A queue name is 1 to 80 letters, digits, hyphens, or underscores. A FIFO queue name also ends in `.fifo`, and the whole name, including the suffix, is at most 80 characters.

### Defaults and limits

| Setting | Default | Range |
| --- | --- | --- |
| `VisibilityTimeout` | 30 seconds | 0 to 43,200 |
| `DelaySeconds` | 0 | 0 to 900 |
| `MaximumMessageSize` | 1,048,576 bytes | 1,024 to 1,048,576 |
| `MessageRetentionPeriod` | 345,600 seconds | 60 to 1,209,600 |
| `ReceiveMessageWaitTimeSeconds` | 0 | 0 to 20 |
| `SqsManagedSseEnabled` | `true` | `true` or `false` |

- FIFO queues add `ContentBasedDeduplication`, `DeduplicationScope`, and `FifoThroughputLimit`. See [FIFO queues](#fifo-queues). `RedrivePolicy` and `RedriveAllowPolicy` are described in [Dead-letter queues](#dead-letter-queues).
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
- `ChangeMessageVisibility` with a stale handle, or for a deleted message, returns `InvalidParameterValue` (unverified). `DeleteMessage` succeeds in both cases.

### Message attributes and checksums

- Attribute types are `String`, `Number`, and `Binary`, with an optional custom label such as `String.json`. A `Number` is a decimal number.
- `StringListValues` and `BinaryListValues` return `InvalidParameterValue`.
- `MD5OfMessageBody` is the MD5 of the body. `MD5OfMessageAttributes` is the SQS attribute checksum.
- `ReceiveMessage` returns the attributes that `MessageAttributeNames` selects. The names are `All` or `.*` for every attribute, an exact name, or `prefix.*` for names that start with `prefix`. The match uses the text before `.*` without the dot, so `col.*` also matches `color`.
- The attribute checksum on a received message covers only the returned attributes. A receive that returns no attributes omits it.
- `ReceiveMessage` returns system attributes that `AttributeNames` or `MessageSystemAttributeNames` request: `SenderId`, `SentTimestamp`, `ApproximateReceiveCount`, `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader`, `SequenceNumber`, `MessageGroupId`, `MessageDeduplicationId`, and `DeadLetterQueueSourceArn`. `All` returns every one that applies. A message has the last four only when the queue type or a redrive gives it one.
- `SendMessage` accepts the `AWSTraceHeader` system attribute.

### Batches

- A batch has 1 to 10 entries. Each `Id` is 1 to 80 letters, digits, hyphens, or underscores, and is unique in the batch.
- A batch body, with its message attributes, is at most 1,048,576 bytes.
- A request that breaks these rules fails as a whole with `EmptyBatchRequest`, `TooManyEntriesInBatchRequest`, `InvalidBatchEntryId`, `BatchEntryIdsNotDistinct`, or `BatchRequestTooLong`.
- An invalid entry fails alone. The response lists it in `Failed` with `SenderFault`, a `Code`, and a `Message`. The other entries still run. An entry fails alone for an invalid message, list attribute values, or a missing `VisibilityTimeout`; the last two use `InvalidParameterValue` (unverified).
- `SendMessage` and `SendMessageBatch` accept `MessageGroupId` on a standard queue. See [Fair queues](#fair-queues). `MessageDeduplicationId` on a standard queue returns `InvalidParameterValue`.

### FIFO queues

AWS recordings in `sqs-fifo` verify these rules, except those marked unverified.

- A name that ends in `.fifo` is a FIFO queue. `CreateQueue` for such a name needs the `FifoQueue` attribute set to `true`, and `FifoQueue` `true` needs a `.fifo` name. Otherwise it returns `InvalidParameterValue`. `FifoQueue` `false` on a standard queue is accepted and not stored (unverified).
- `FifoQueue` can't change after creation. `SetQueueAttributes` returns `InvalidAttributeValue`.
- A FIFO queue has these attributes. They are not valid on a standard queue, which returns `InvalidAttributeName`.

  | Attribute | Default | Values |
  | --- | --- | --- |
  | `ContentBasedDeduplication` | `false` | `true` or `false` |
  | `DeduplicationScope` | `queue` | `queue` or `messageGroup` |
  | `FifoThroughputLimit` | `perQueue` | `perQueue` or `perMessageGroupId` |

  `GetQueueAttributes` with `All` also returns `FifoQueue`. A standard queue rejects these attributes, as recorded in `sqs-fair-queue`. A standard queue never returns these attributes.
- A send needs `MessageGroupId`, 1 to 128 printable ASCII characters (the character rule is unverified). A missing group returns `MissingParameter`.
- A send needs `MessageDeduplicationId` unless the queue has `ContentBasedDeduplication` set to `true`. Without either, it returns `InvalidParameterValue`. The ID has the same character rules as the group. An explicit ID takes priority over the content hash.
- A per-message `DelaySeconds` returns `InvalidParameterValue`. The queue's `DelaySeconds` applies.
- Deduplication: a send with the same ID within 5 minutes of the first send is not enqueued again. It returns the first message's `MessageId`, checksums, and `SequenceNumber`. The window starts at the first send, and a duplicate does not extend it. The exact 5-minute boundary is unverified. `PurgeQueue` does not reset deduplication (unverified). With `DeduplicationScope` `messageGroup`, the same ID in another group is not a duplicate (unverified). The content ID is the SHA-256 hash of the body, in hexadecimal.
- `SequenceNumber` is a per-queue counter, written in 20 digits. It increases with each enqueued message. `SendMessage` and `SendMessageBatch` return it. A restart resets it, because messages do not persist.
- Group locking: while a message of a group is in flight, `ReceiveMessage` returns no other message of that group. A message that is delayed or hidden also blocks the later messages of its group. Messages that one call receives can include several of the same group, as on AWS. When a visibility timeout ends, the first message of the group comes back first.
- `ReceiveRequestAttemptId` is accepted and ignored. A retry with the same ID does not return the same messages again.
- `MessageGroupId`, `MessageDeduplicationId`, and `SequenceNumber` are returned as system attributes when requested.

### Dead-letter queues

AWS recordings in `sqs-dead-letter` verify these rules, except those marked unverified.

- `RedrivePolicy` is a JSON object with `deadLetterTargetArn` and `maxReceiveCount`. `maxReceiveCount` is 1 to 1,000, as a JSON number or a decimal string. pail stores the canonical form `{"deadLetterTargetArn":"<arn>","maxReceiveCount":<n>}`, and returns it from `GetQueueAttributes`. Other fields are dropped.
- `CreateQueue` and `SetQueueAttributes` check the policy. A bad shape returns `InvalidAttributeValue` (unverified). These cases return `InvalidParameterValue`: a `maxReceiveCount` outside 1 to 1000, a target that is not an existing queue of this account and region, a queue that targets itself (unverified), a target of the other type (FIFO or standard), and a target whose `RedriveAllowPolicy` does not permit the source. An empty value removes the policy.
- `RedriveAllowPolicy` is a JSON object with `redrivePermission`: `allowAll`, `denyAll`, or `byQueue`. `byQueue` needs `sourceQueueArns` with 1 to 10 ARNs. The other values must not have it. pail enforces the policy only when a source sets its `RedrivePolicy`. A later change does not affect existing sources. `denyAll` and `allowAll` are verified. `byQueue` is unverified.
- When a receive finds a visible message whose receive count is at least `maxReceiveCount`, it moves the message to the dead-letter queue instead of returning it. The move happens on that receive, not in the background. The message keeps its `MessageId`, body, attributes, send time, and receive count. A FIFO target gives it a new sequence number.
- The moved message is visible at once in the target. A long poll on the target wakes. `ReceiveMessage` on the target returns `DeadLetterQueueSourceArn` as a system attribute. Its `ApproximateReceiveCount` continues from the source count.
- If the target queue is deleted, the message is delivered from the source as usual (unverified).
- `ListDeadLetterSourceQueues` returns the URLs of the queues whose `RedrivePolicy` targets the queue, as `queueUrls`. `MaxResults` and `NextToken` work as in `ListQueues` (unverified). An unknown queue returns `QueueDoesNotExist`. With no sources, the response is an empty object. AWS lists sources with eventual consistency, so a source created seconds earlier can be missing; pail lists it at once, and `test/diff` lists this as a known difference.
- A cycle of redrive policies (a to b to a) moves a message back and forth, so neither queue delivers it, as on AWS. Only a queue that targets itself is rejected (unverified).
- Message move tasks (`StartMessageMoveTask` and related operations) are not supported.

### Fair queues

A standard queue accepts `MessageGroupId`, as AWS fair queues do. pail stores it and returns it as the `MessageGroupId` system attribute. Recordings verify that AWS accepts the field and returns it on receive. pail does not model tenant fairness.

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
| Bad parameter, queue name, missing `QueueName` (verified for `GetQueueUrl`; unverified for `CreateQueue`), or oversized message | 400 | `com.amazon.coral.service#InvalidParameterValueException` | `InvalidParameterValue` |
| Empty `MessageBody`, or a missing `MessageGroupId` on a FIFO queue | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Missing `QueueUrl` or `ReceiptHandle` (unverified) | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Invalid message characters | 400 | `com.amazonaws.sqs#InvalidMessageContents` | `InvalidMessageContents` |
| Bad receipt handle | 404 | `com.amazonaws.sqs#ReceiptHandleIsInvalid` | `ReceiptHandleIsInvalid` |
| Purge too soon | 403 | `com.amazonaws.sqs#PurgeQueueInProgress` | `AWS.SimpleQueueService.PurgeQueueInProgress` |
| Batch rule broken | 400 | `com.amazonaws.sqs#<Batch error name>` | `AWS.SimpleQueueService.<Batch error name>` |
| Malformed request body | 400 | `com.amazon.coral.service#SerializationException` | `MalformedInput` |

A failed batch entry uses the code `InvalidParameterValue`, `MissingParameter`, `InvalidMessageContents`, `ReceiptHandleIsInvalid`, or `InternalError`. `InvalidMessageContents` and `ReceiptHandleIsInvalid` are verified. The others are unverified.

### Differences from AWS

- pail has no `QueueDeletedRecently` delay. You can re-create a queue right after you delete it.
- pail does not enforce IAM, KMS, or queue policies. It stores `Policy` and returns it.
- pail serves one account. `SenderId` is the access key ID, where AWS returns the caller's IAM unique ID.
- `QueueOwnerAWSAccountId` in `GetQueueUrl` is ignored.
- `ReceiveRequestAttemptId` is ignored.
- A message that is sent to a standard queue is delivered in send order. AWS gives no ordering guarantee.
- pail doesn't support the SQS query protocol.
- Dead-letter queues move a message when a receive finds it past `maxReceiveCount`. AWS moves it in the background.
- FIFO queues have no high-throughput quotas.
