# SQS and SNS compatibility

This page describes how pail implements Amazon SQS and Amazon SNS, and where it differs from AWS. For setup and a summary, see the [README](../README.md).

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
- A queue URL with another account or a malformed path returns `QueueDoesNotExist`. A missing or empty `QueueUrl` returns `QueueDoesNotExist` (verified for `SendMessage`).
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
- `StringListValues` and `BinaryListValues` return `UnsupportedOperation`, as AWS does. A message body or batch over 1,048,576 bytes returns `InvalidParameterValue`.
- `MD5OfMessageBody` is the MD5 of the body. `MD5OfMessageAttributes` is the SQS attribute checksum.
- `ReceiveMessage` returns the attributes that `MessageAttributeNames` selects. The names are `All` or `.*` for every attribute, an exact name, or `prefix.*` for names that start with `prefix`. The match uses the text before `.*` without the dot, so `col.*` also matches `color`.
- The attribute checksum on a received message covers only the returned attributes. A receive that returns no attributes omits it.
- `ReceiveMessage` returns system attributes that `AttributeNames` or `MessageSystemAttributeNames` request: `SenderId`, `SentTimestamp`, `ApproximateReceiveCount`, `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader`, `SequenceNumber`, `MessageGroupId`, `MessageDeduplicationId`, and `DeadLetterQueueSourceArn`. `All` returns every one that applies. A message has the last four only when the queue type or a redrive gives it one.
- `SendMessage` accepts the `AWSTraceHeader` system attribute.

### Batches

- A batch has 1 to 10 entries. Each `Id` is 1 to 80 letters, digits, hyphens, or underscores, and is unique in the batch.
- A batch body, with its message attributes, is at most 1,048,576 bytes.
- A request that breaks these rules fails as a whole with `EmptyBatchRequest`, `TooManyEntriesInBatchRequest`, `InvalidBatchEntryId`, `BatchEntryIdsNotDistinct`, or `BatchRequestTooLong`.
- An invalid entry fails alone. The response lists it in `Failed` with `SenderFault`, a `Code`, and a `Message`. The other entries still run. An entry fails alone for an invalid message, list attribute values (`UnsupportedOperation`, unverified for an entry), or a missing `VisibilityTimeout` (`InvalidParameterValue`, unverified).
- `SendMessage` and `SendMessageBatch` accept `MessageGroupId` on a standard queue. See [Fair queues](#fair-queues). `MessageDeduplicationId` on a standard queue returns `InvalidParameterValue` (verified).

### FIFO queues

AWS recordings in `sqs-fifo` verify these rules, except those marked unverified.

- A name that ends in `.fifo` is a FIFO queue. `CreateQueue` for such a name needs the `FifoQueue` attribute set to `true`, and `FifoQueue` `true` needs a `.fifo` name. Otherwise it returns `InvalidParameterValue`. `FifoQueue` on a standard queue, even `false`, returns `InvalidAttributeName`, in `CreateQueue` and in `SetQueueAttributes`.
- `FifoQueue` can't change after creation. `SetQueueAttributes` on a FIFO queue returns `InvalidAttributeValue` (unverified).
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
- `SequenceNumber` is a per-queue counter, written in 20 digits (the format is unverified). It increases with each enqueued message. `SendMessage` and `SendMessageBatch` return it. A restart resets it, because messages do not persist.
- Group locking: while a message of a group is in flight, `ReceiveMessage` returns no other message of that group. A message that is delayed or hidden also blocks the later messages of its group. Messages that one call receives can include several of the same group, as on AWS. When a visibility timeout ends, the first message of the group comes back first.
- `ReceiveRequestAttemptId` is accepted and ignored. A retry with the same ID does not return the same messages again.
- `MessageGroupId`, `MessageDeduplicationId`, and `SequenceNumber` are returned as system attributes when requested.

### Dead-letter queues

AWS recordings in `sqs-dead-letter` verify these rules, except those marked unverified.

- `RedrivePolicy` is a JSON object with `deadLetterTargetArn` and `maxReceiveCount`. `maxReceiveCount` is 1 to 1,000, as a JSON number or a decimal string. pail stores the canonical form `{"deadLetterTargetArn":"<arn>","maxReceiveCount":<n>}`, and returns it from `GetQueueAttributes`. Other fields are dropped.
- `CreateQueue` and `SetQueueAttributes` check the policy. A bad shape returns `InvalidAttributeValue` (unverified). These cases return `InvalidParameterValue`: a `maxReceiveCount` outside 1 to 1000, a target that is not an existing queue of this account and region, a target of the other type (FIFO or standard), and a target whose `RedriveAllowPolicy` does not permit the source. An empty value removes the policy. A queue can name itself as its dead-letter target: AWS accepts it, and so does pail.
- `RedriveAllowPolicy` is a JSON object with `redrivePermission`: `allowAll`, `denyAll`, or `byQueue`. `byQueue` needs `sourceQueueArns` with 1 to 10 ARNs. The other values must not have it. `byQueue` without ARNs and `allowAll` with ARNs return `InvalidParameterValue`, not `InvalidAttributeValue` (verified; `denyAll` with ARNs and a `byQueue` list of 11 or more ARNs are unverified). pail enforces the policy only when a source sets its `RedrivePolicy`. A later change does not affect existing sources. `allowAll`, `denyAll`, and `byQueue` are verified.
- When a receive finds a visible message whose receive count is at least `maxReceiveCount`, it moves the message to the dead-letter queue instead of returning it. The move happens on that receive, not in the background. The message keeps its `MessageId`, body, attributes, send time, and receive count. A FIFO target gives it a new sequence number.
- The moved message is visible at once in the target. A long poll on the target wakes. `ReceiveMessage` on the target returns `DeadLetterQueueSourceArn` as a system attribute. Its `ApproximateReceiveCount` continues from the source count.
- If the target queue is deleted, the message is delivered from the source as usual (unverified).
- `ListDeadLetterSourceQueues` returns the URLs of the queues whose `RedrivePolicy` targets the queue, as `queueUrls` (the non-empty shape is unverified, because AWS listed none in the recording). `MaxResults` and `NextToken` work as in `ListQueues` (unverified). An unknown queue returns `QueueDoesNotExist`. With no sources, the response is an empty object. AWS lists sources with eventual consistency, so a source created seconds earlier can be missing; pail lists it at once, and `test/diff` lists this as a known difference.
- A cycle of redrive policies (a to b to a) moves a message back and forth, so neither queue delivers it, as on AWS. pail does not guard against a queue that targets itself; a message there moves back to the same queue on each receive (unverified on AWS).
- Message move tasks (`StartMessageMoveTask` and related operations) are not supported.

### Fair queues

A standard queue accepts `MessageGroupId`, as AWS fair queues do. pail stores it and returns it as the `MessageGroupId` system attribute. Recordings verify that AWS accepts the field and returns it on receive. pail does not model tenant fairness.

### Errors

AWS recordings in `test/diff` verify every row except those marked unverified. A failed batch entry's `Message` text differs from AWS and is not compared.

| Condition | Status | `__type` | Query code |
| --- | --- | --- | --- |
| Missing queue, or a missing `QueueUrl` (verified for `SendMessage`; unverified for the other operations) | 400 | `com.amazonaws.sqs#QueueDoesNotExist` | `AWS.SimpleQueueService.NonExistentQueue` |
| Missing signature, unknown key, or bad signature | 403 | `com.amazon.coral.service#...` | `AccessDenied`, `InvalidClientTokenId`, or `SignatureDoesNotMatch` |
| Malformed `Authorization` header | 400 | `com.amazon.coral.service#IncompleteSignatureException` | `IncompleteSignature` |
| Operation that is not an SQS operation, or no `X-Amz-Target` | 400 | `com.amazon.coral.service#UnknownOperationException` | `InvalidAction` |
| `StringListValues` or `BinaryListValues`; an SQS operation that pail doesn't implement (unverified) | 400 | `com.amazonaws.sqs#UnsupportedOperation` | `AWS.SimpleQueueService.UnsupportedOperation` |
| Queue exists with other attributes | 400 | `com.amazonaws.sqs#QueueNameExists` | `QueueAlreadyExists` |
| Bad attribute name or value, including `FifoQueue` on a standard queue | 400 | `com.amazonaws.sqs#InvalidAttributeName` or `#InvalidAttributeValue` | same name |
| Bad parameter, queue name, missing `QueueName` (verified for `GetQueueUrl`; unverified for `CreateQueue`), or oversized message | 400 | `com.amazon.coral.service#InvalidParameterValueException` | `InvalidParameterValue` |
| Empty `MessageBody`, or a missing `MessageGroupId` on a FIFO queue | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Missing `ReceiptHandle`, or missing `VisibilityTimeout` in `ChangeMessageVisibility` | 400 | `com.amazon.coral.service#MissingRequiredParameterException` | `MissingParameter` |
| Invalid message characters | 400 | `com.amazonaws.sqs#InvalidMessageContents` | `InvalidMessageContents` |
| Bad receipt handle | 404 | `com.amazonaws.sqs#ReceiptHandleIsInvalid` | `ReceiptHandleIsInvalid` |
| Purge too soon | 403 | `com.amazonaws.sqs#PurgeQueueInProgress` | `AWS.SimpleQueueService.PurgeQueueInProgress` |
| Batch rule broken | 400 | `com.amazonaws.sqs#<Batch error name>` | `AWS.SimpleQueueService.<Batch error name>` |
| Malformed request body, including JSON that is not an object | 400 | `com.amazon.coral.service#SerializationException` | `MalformedInput` |

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

## SNS

pail serves SNS on the same listener and port as S3 and SQS. It routes a request to SNS by the `sns` service in the SigV4 credential scope, and routes the two unsigned link actions there too. See [Unsigned links](#unsigned-links). It uses the S3 access key pair. AWS recordings in `test/diff` verify the behavior on this page, except what it marks as unverified.

### Protocol

- pail speaks the AWS query protocol: a form-encoded `POST /` with `Action=<Operation>&Version=2010-03-31`. A `GET` with the same fields in the query also works.
- Responses are XML with `Content-Type: text/xml`. Every response carries `x-amzn-RequestId`.
- A success is `<{Action}Response><{Action}Result>...</{Action}Result><ResponseMetadata>...`. `DeleteTopic`, `SetTopicAttributes`, `Unsubscribe`, and `SetSubscriptionAttributes` have no `Result` element, as in the service model. `TagResource` and `UntagResource` return an empty `Result` element.
- A request body is limited to 4 MiB.

### Operations

pail supports these operations:

- Topics: `CreateTopic`, `DeleteTopic`, `ListTopics`, `GetTopicAttributes`, `SetTopicAttributes`.
- Tags: `TagResource`, `UntagResource`, `ListTagsForResource`.
- Subscriptions: `Subscribe`, `ConfirmSubscription`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`, `SetSubscriptionAttributes`.
- Messages: `Publish`, `PublishBatch`.

Every other action returns `InvalidAction` with the message `<Action> is not supported` (unverified for model actions such as `CreatePlatformApplication`). A request without `Action` returns `InvalidAction`.

### Topics and ARNs

- A topic ARN is `arn:aws:sns:<region>:000000000000:<name>`. The region is the `--region` setting.
- A topic name is 1 to 256 letters, digits, hyphens, or underscores. A name that ends in `.fifo` returns `InvalidParameter`, because pail has no FIFO topics.
- `CreateTopic` is idempotent. For a topic that exists, it returns the ARN. If a given attribute differs from the stored value, it returns `InvalidParameter`.
- `DeleteTopic` removes the topic and its subscriptions. For a topic that does not exist, it succeeds.
- A topic has at most 50 tags. More return `TagLimitExceeded`. The tag operations answer `ResourceNotFound` for an unknown topic. `ListTagsForResource` writes `Value` before `Key` in each member.
- `ListTopics` and the subscription lists return 100 entries a page. `NextToken` is an opaque value from the previous page.
- `CreateTopic` with `DataProtectionPolicy` returns `InvalidParameter`.

### Topic attributes

`GetTopicAttributes` returns these entries in this order. The recordings `sns-topic-basics`, `sns-topic-attributes`, `sns-delivery-policy`, and `sns-edge-cases` verify the order and the default values. `DisplayName` set at creation and by `SetTopicAttributes` shows in the next read.

| Attribute | Value |
| --- | --- |
| `Policy` | AWS's default policy for the topic, or the policy that a client set. |
| `SignatureVersion` | Only when a client set it. |
| `Owner` | `000000000000`. |
| `SubscriptionsPending` | The number of pending subscriptions. (unverified: AWS updates the counts with a delay). |
| `KmsMasterKeyId` | Only when a client set it. |
| `TopicArn` | The topic ARN. |
| `TracingConfig` | Only when a client set it. |
| `EffectiveDeliveryPolicy` | AWS's default delivery policy, or the stored `DeliveryPolicy` when one is set (recorded in `sns-delivery-policy` for a full policy). A partial policy overlays the defaults, so `{}` reads as the default policy (unverified). |
| `SubscriptionsConfirmed` | The number of confirmed subscriptions (unverified: AWS updates the counts with a delay). |
| `DisplayName` | Empty by default. |
| `DeliveryPolicy` | Only when a client set it. See [Delivery policies](#delivery-policies). |
| `SubscriptionsDeleted` | `0`. |

`CreateTopic` accepts `FifoTopic` and `ContentBasedDeduplication` with the value `false` and ignores them. The value `true` returns `InvalidParameter`. `DisplayName` has at most 100 characters and no control characters (unverified). A tag key has 1 to 128 characters and a tag value has 0 to 256 characters (unverified).

A client can set `DisplayName`, `Policy` (a JSON object), `DeliveryPolicy` (a JSON object that pail validates), `KmsMasterKeyId`, `SignatureVersion` (`1` or `2`), and `TracingConfig` (`PassThrough` or `Active`). Any other name returns `InvalidParameter`. An empty value unsets an attribute (unverified). pail stores `Policy` and `KmsMasterKeyId` and enforces neither. pail applies `DeliveryPolicy` to HTTP and HTTPS deliveries.

### Subscriptions

- pail supports the protocols `sqs`, `http`, and `https`. Any other protocol returns `InvalidParameter` with the message `protocol is not supported`.
- An `sqs` endpoint must be the ARN of an SQS queue in the same region and account, `arn:aws:sqs:<region>:000000000000:<name>`. The queue need not exist when you subscribe. A delivery to a queue that does not exist is logged and dropped.
- An endpoint that names a FIFO queue returns `InvalidParameter`, because pail has no FIFO topics (verified for a FIFO queue endpoint on a standard topic).
- An `http` or `https` endpoint must be a URL whose scheme equals the protocol, with a host and no user info. A scheme that differs from the protocol and an endpoint that is not a URL return `InvalidParameter`. AWS also refuses an endpoint on an internal address, such as `127.0.0.1`, with `AuthorizationError`. pail accepts it, because local testing needs it.
- pail confirms an SQS subscription at once. A subscription ARN is the topic ARN, a colon, and a lowercase UUID.
- An HTTP or HTTPS subscription is pending until its endpoint confirms it. See [Delivery to HTTP and HTTPS](#delivery-to-http-and-https). While it is pending, `Subscribe` returns the `SubscriptionArn` `pending confirmation`, unless `ReturnSubscriptionArn` is `true`. Then it returns the real ARN.
- `ListSubscriptions` and `ListSubscriptionsByTopic` show the `SubscriptionArn` of a pending subscription as `PendingConfirmation`.
- `ConfirmSubscription` takes `TopicArn` and `Token`, and returns the `SubscriptionArn`. A missing `Token` returns `ValidationError`. A missing topic returns `NotFound`. A token that no subscription of the topic holds returns `InvalidParameter`, signed or unsigned. Confirming a confirmed subscription again returns its ARN and changes nothing (unverified). `AuthenticateOnUnsubscribe` set to `true` on a signed call records the confirmation as authenticated.
- `Unsubscribe` of a pending subscription returns `InvalidParameter`, signed or unsigned, and the subscription stays. It goes away with its topic.
- An unsigned `Unsubscribe` removes only a subscription confirmed without `AuthenticateOnUnsubscribe` (unverified). An unsigned `Unsubscribe` of an SQS subscription returns `AuthorizationError`.
- `Subscribe` is idempotent. The same topic, protocol, and endpoint return the existing ARN. Different attributes return `InvalidParameter` (unverified). A repeat `Subscribe` of a pending subscription sends the confirmation again with the same token (unverified).
- A confirmation token and its pending subscription expire 3 days after the subscription was created (the AWS documentation; unverified). An expired token returns `InvalidParameter`. An expired subscription disappears from listings, attributes (`NotFound`), and counts. pail removes it, and its file, when `Subscribe`, `ConfirmSubscription`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`, `SetSubscriptionAttributes`, or `GetTopicAttributes` runs. A confirmed subscription never expires.
- `Subscribe` and `SetSubscriptionAttributes` accept `RawMessageDelivery` (`true` or `false`), `FilterPolicy`, `FilterPolicyScope`, and, for HTTP and HTTPS subscriptions, `DeliveryPolicy`. `RedrivePolicy` and `SubscriptionRoleArn` return `InvalidParameter`, and so does `DeliveryPolicy` on an SQS subscription (recorded for `SetSubscriptionAttributes`; unverified for `Subscribe`). An empty value for `FilterPolicy` or `FilterPolicyScope` unsets it. An empty policy object, `{}`, removes the policy too. pail checks a policy against its scope whenever either one changes, so a call that leaves them inconsistent returns `InvalidParameter`. See [Filter policies](#filter-policies).
- `GetSubscriptionAttributes` returns `SubscriptionPrincipal`, `Owner`, `RawMessageDelivery`, `FilterPolicy`, `TopicArn`, `Endpoint`, `FilterPolicyScope`, `Protocol`, `PendingConfirmation`, `ConfirmationWasAuthenticated`, and `SubscriptionArn`, in this order. `PendingConfirmation` and `ConfirmationWasAuthenticated` follow the confirmation state. An HTTP or HTTPS subscription adds `EffectiveDeliveryPolicy` after `Endpoint`, and `DeliveryPolicy` right after it when one is set (recorded in `sns-delivery-policy`). Their position relative to `FilterPolicyScope` is unverified. See [Delivery policies](#delivery-policies). `FilterPolicy` (the text you set) and `FilterPolicyScope` (`MessageAttributes` by default) appear only when a policy is set, even when you set the scope. AWS returns the caller's ARN in `SubscriptionPrincipal`. pail returns `arn:aws:iam::000000000000:root`. The recording masks that value, so its shape is unverified.
- `Unsubscribe` of a well-formed ARN of a subscription that does not exist succeeds with an empty result. A value that is not an ARN returns `InvalidParameter` (unverified).
- pail does not check the queue policy. AWS delivers only when the policy allows the topic.

### Delivery policies

A delivery policy sets how pail posts to HTTP and HTTPS endpoints. A topic holds a default for its subscriptions. A subscription can override it.

Verified in the `sns-delivery-policy` recording:

- `SetTopicAttributes` with `DeliveryPolicy` validates the JSON and stores it again in a fixed key order: `http`, then `defaultHealthyRetryPolicy` (`minDelayTarget`, `maxDelayTarget`, `numRetries`, `numMaxDelayRetries`, `numNoDelayRetries`, `numMinDelayRetries`, `backoffFunction`), `disableSubscriptionOverrides`, `defaultThrottlePolicy` (`maxReceivesPerSecond`), and `defaultRequestPolicy` (`headerContentType`). A section that the input leaves out stays out. An empty value unsets the policy, and `EffectiveDeliveryPolicy` returns to the default.
- `InvalidParameter` for `numRetries` above 100, a `backoffFunction` other than `linear`, `arithmetic`, `geometric`, or `exponential`, a `minDelayTarget` above `maxDelayTarget`, phase counts (`numNoDelayRetries`, `numMinDelayRetries`, `numMaxDelayRetries`) that add up to more than `numRetries`, a `headerContentType` of `text/bogus`, an unknown key, and text that is not JSON.
- An HTTP or HTTPS subscription derives its `EffectiveDeliveryPolicy` from the topic: `{"healthyRetryPolicy": <defaultHealthyRetryPolicy>, "sicklyRetryPolicy": null, "throttlePolicy": <defaultThrottlePolicy>, "requestPolicy": <defaultRequestPolicy>, "guaranteed": false}`. The recording shows `sicklyRetryPolicy: null` only. A `throttlePolicy` or `requestPolicy` that is absent is `null` in the default policy, and `null` is also stored for an absent section of a subscription policy (both unverified).
- `SetSubscriptionAttributes` with `DeliveryPolicy` works on a pending subscription. It stores `healthyRetryPolicy`, `sicklyRetryPolicy` (`null` when absent), `throttlePolicy`, `requestPolicy`, and `guaranteed` in that order, and the effective policy equals it. `numRetries` above 100 and any `DeliveryPolicy` on an SQS subscription return `InvalidParameter`.

Unverified, and pail's choice:

- A key that the input leaves out inside a retry policy takes AWS's default (`minDelayTarget` 20, `maxDelayTarget` 20, `numRetries` 3, the phase counts 0, `linear`). A missing `maxReceivesPerSecond` is invalid.
- Delays are 1 to 3,600 seconds (the AWS documentation), and `maxReceivesPerSecond` is at least 1.
- `headerContentType` accepts `text/plain`, `text/csv`, `application/json`, and `application/xml`, each with an optional `; charset=UTF-8`. Only `application/json`, `text/plain`, and the rejection of `text/bogus` are recorded.
- A subscription policy fills only the sections it names. The topic supplies the rest. With `disableSubscriptionOverrides` set to `true`, pail ignores the subscription policy.
- `Subscribe` accepts `DeliveryPolicy` for HTTP and HTTPS with the same rules.
- When pail starts, it normalizes a stored `DeliveryPolicy` that is valid. It drops one that is not, with a warning that names the topic or subscription, so files from earlier versions still load.
- `sicklyRetryPolicy` and `guaranteed` are stored and reported. pail does not use them.

How pail applies the effective policy, all unverified:

- **Retries.** `numRetries` is the number of retries after the first attempt. The phases follow the AWS documentation, in order: `numNoDelayRetries` retries at once, `numMinDelayRetries` retries after `minDelayTarget` seconds, a backoff phase, then `numMaxDelayRetries` retries after `maxDelayTarget` seconds. The backoff phase has `numRetries` minus the other three counts, called n. Retry i of n waits `minDelayTarget` (min) plus a share of the distance to `maxDelayTarget` (max), rounded to whole seconds. The share is i/(n+1) for `linear`, i(i+1) / ((n+1)(n+2)) for `arithmetic`, and (2^i - 1) / (2^(n+1) - 1) for `exponential`. `geometric` waits min × (max/min)^(i/(n+1)). The default policy gives attempts at 0, 20, 40, and 60 seconds, as before.
- **Throttle.** `maxReceivesPerSecond` limits posts per second for each subscription. Retries count. pail spaces posts evenly, so a burst waits in a worker.
- **Content type.** `headerContentType` sets the `Content-Type` of notifications. Confirmations always use `text/plain; charset=UTF-8`.

### Filter policies

A subscription without a filter policy receives every message. With a policy, it receives only the messages that match. pail checks the policy when you set it, and it checks the scope together with the policy.

The `sns-filter-policies` and `sns-filter-edge-cases` recordings verify the rules below, except the operators that pail rejects.

- `FilterPolicyScope` is `MessageAttributes` (the default) or `MessageBody`. Any other value returns `InvalidParameter`.
- `FilterPolicy` is a JSON object. Each key maps to an array of conditions. A message matches when every key matches (AND). A key matches when any condition in its array matches (OR). An empty object `{}` removes the policy.
- With `MessageBody`, a key can map to a nested object, and pail matches the path from the root of the message. In `MessageAttributes` scope, a nested object returns `InvalidParameter`.
- A condition is one of:
  - A string or a number, which matches an equal value. A number compares by value, so `15` matches the `Number` attribute `15.0`. A string never matches a number, and the reverse.
  - `{"prefix": "text"}`.
  - `{"anything-but": ...}` with a string, a number, a non-empty array of strings and numbers, or `{"prefix": "text"}`.
  - `{"numeric": ["=", n]}`, or one or two pairs of an operator (`<`, `<=`, `>`, `>=`) and a number. With two pairs, one is a lower bound and the other is an upper bound, in either order.
  - `{"exists": true}` or `{"exists": false}`.
- An empty array, a boolean, a null, an array inside the array, and an object with more than one key return `InvalidParameter`. So do `$or` and the operators that pail does not implement, such as `suffix`, `equals-ignore-case`, `cidr`, and `wildcard`.
- A policy has at most 5 keys that map to an array, counting nested ones, and the product of the lengths of those arrays is at most 150. Intermediate keys of a nested policy do not count. So 6 top-level keys, 6 nested keys under one top-level key, and 156 combinations return `InvalidParameter`, but 5 nested keys under 2 top-level keys do not.
- In `MessageAttributes` scope, the key names a message attribute:
  - `String` and `Number` (with or without a custom label) give one value. A `Number` value compares as a number. A `String` value never matches a number or `numeric`.
  - `String.Array` gives its elements. The key matches when any element matches.
  - `Binary` exists but has no value that a condition other than `exists` can match.
  - An attribute that is not on the message does not match any condition except `{"exists": false}`. That includes `anything-but`.
  - A value of another type satisfies `anything-but`. For example, `{"anything-but": ["prod"]}` matches the `Number` attribute `5`.
- In `MessageBody` scope, the message must be a JSON object. Any other message does not match. An array on the path fans out into its elements, and the key matches when any of them matches. An object at the end of the path is not a value. A missing path, or one whose parent object is missing, does not match, except `{"exists": false}`.
- With `MessageStructure` set to `json`, pail chooses the text per protocol, and a body policy matches the text that its own subscription receives. For an SQS subscription, that is the `sqs` value, or `default` when there is no `sqs` value (verified). A policy that matches only the `default` value does not match when an `sqs` value exists. An `http` subscription uses the `http` value and an `https` subscription uses the `https` value, each with the same fallback (unverified, from the AWS documentation).
- pail filters before it signs. A publish that matches no subscription sends nothing.
- Filter policies apply to HTTP and HTTPS subscriptions too. See the `MessageStructure` rule above.

### Publish

- `Publish` needs `TopicArn` and a non-empty `Message`. `TargetArn` and `PhoneNumber` return `InvalidParameter`.
- A message and its attributes total at most 262,144 bytes. A larger one returns `InvalidParameter`.
- `Subject` is at most 100 printable ASCII characters, with no line breaks.
- `MessageStructure` is empty or `json`. With `json`, the message is a JSON object of string values with a `default` key. pail delivers the value under the protocol name of the subscription (`sqs`, `http`, or `https`) when it exists, and `default` otherwise. The `sqs` key is verified. The `http` and `https` keys are unverified.
- A message has at most 10 attributes. Names follow the SQS rules. The types are `String`, `String.Array` (the value is a JSON array), `Number`, and `Binary`, with an optional custom label such as `String.json`.
- A standard topic accepts `MessageGroupId`. pail forwards it as the `MessageGroupId` of the SQS message. `MessageDeduplicationId` returns `InvalidParameter`. Both are verified.
- A missing `Message` parameter returns `ValidationError`. A long `Subject` and a bad `MessageStructure` return `InvalidParameter`. An empty `Message` returns `InvalidParameter` too: verified for a `PublishBatch` entry, unverified for `Publish`. A `MessageGroupId` must be 1 to 128 printable ASCII characters (unverified). The other rules above return `InvalidParameter` too. A message attribute with a data type other than `String`, `String.Array`, `Number`, or `Binary` returns `ParameterValueInvalid` (verified). The other attribute errors (name, value, and count) return `InvalidParameter`; AWS may use `ParameterValueInvalid` for some of them (unverified).
- `Publish` returns after pail sends to every subscribed queue, so a receive right after it sees the message. A missing queue or a failed send is logged and does not fail the publish.
- `PublishBatch` takes 1 to 10 entries. Each `Id` is 1 to 80 letters, digits, hyphens, or underscores, and is unique. The entries total at most 262,144 bytes. A request that breaks these rules fails as a whole with `ValidationError` (no entries), `TooManyEntriesInBatchRequest`, `InvalidBatchEntryId`, `BatchEntryIdsNotDistinct`, or `BatchRequestTooLong`. An invalid entry fails alone and appears in `Failed` with `Code`, `Message`, `SenderFault`, and `Id`, in this order (the `Message` position is unverified). The result always has `Failed` first, empty when nothing failed, then `Successful`. A `Successful` member is `MessageId` then `Id`. pail writes an empty `Successful` when every entry failed. A failed entry's `Message` text is not compared with AWS.

### Delivery to SQS

By default, the queue receives a JSON envelope as the message body. The keys are in this order:

1. `Type`: `Notification`.
2. `MessageId`: the ID that `Publish` returned.
3. `TopicArn`.
4. `Subject`, only when set.
5. `Message`.
6. `Timestamp`: UTC, for example `2026-10-10T06:39:33.308Z`.
7. `SignatureVersion`: the topic's `SignatureVersion`, `1` by default.
8. `Signature`: base64.
9. `SigningCertURL`: `<scheme>://<host>/_pail/sns/signing-cert.pem`, from the request that published.
10. `UnsubscribeURL`: `<scheme>://<host>/?Action=Unsubscribe&SubscriptionArn=<ARN>`. See [Unsigned links](#unsigned-links).
11. `MessageAttributes`, only when set: `{"<name>":{"Type":"String","Value":"..."}}`. A `Binary` value is base64.

An envelope delivery sends no SQS message attributes.

With `RawMessageDelivery` set to `true`, the queue receives the message text as the body, with no envelope. The message attributes become SQS message attributes of the same name and type, including `String.Array`, whose value stays the JSON array text.

### Delivery to HTTP and HTTPS

pail posts to an HTTP or HTTPS endpoint in the background. `Publish` does not wait for the endpoint and does not fail when a delivery fails. Everything on this page about HTTP delivery is unverified, because the recording cannot reach a public endpoint. The `sns-http-subscriptions` scenario records the subscription rules only. Its public endpoint differs by target, so pail never posts to it in tests.

- `Subscribe` makes the subscription pending and sends a `SubscriptionConfirmation` to the endpoint. A pending subscription receives no notifications.
- The confirmation body is a JSON object with these keys in this order: `Type` (`SubscriptionConfirmation`), `MessageId`, `Token`, `TopicArn`, `Message`, `SubscribeURL`, `Timestamp`, `SignatureVersion`, `Signature`, `SigningCertURL`.
- `SubscribeURL` is `<scheme>://<host>/?Action=ConfirmSubscription&TopicArn=<topic ARN>&Token=<token>`. The ARN is not escaped, as in the AWS documentation. The token is 64 hex characters.
- The string to sign is the lines `Message`, `MessageId`, `SubscribeURL`, `Timestamp`, `Token`, `TopicArn`, and `Type`, each as a name line and a value line, in that order. The topic's `SignatureVersion` picks the hash, as for notifications.
- The endpoint confirms with an unsigned `GET` of the `SubscribeURL`, or with a signed `ConfirmSubscription` call.
- After confirmation, `Publish` posts the [envelope](#delivery-to-sqs) to the endpoint. With `RawMessageDelivery` set to `true`, it posts the message text only, with no message attributes.
- Every request is a `POST` with these headers: `Content-Type: text/plain; charset=UTF-8`, `User-Agent: Amazon Simple Notification Service Agent`, `x-amz-sns-message-type` (`Notification` or `SubscriptionConfirmation`), `x-amz-sns-message-id`, and `x-amz-sns-topic-arn`. A notification also sends `x-amz-sns-subscription-arn`. A raw delivery also sends `x-amz-sns-rawdelivery: true`.
- A `2xx` answer is a success. A network error, a `5xx`, and a `429` are retried. Any other status ends the delivery without a retry.
- By default pail makes at most 4 attempts, 20 seconds apart. This is the default `healthyRetryPolicy` of AWS. A `DeliveryPolicy` changes the schedule, the rate, and the content type. See [Delivery policies](#delivery-policies).
- Each attempt times out after 15 seconds. pail does not follow redirects.
- pail verifies the TLS certificate of an HTTPS endpoint against the system roots. An endpoint with a self-signed certificate fails. To accept any certificate, start pail with `--sns-tls-skip-verify` or `PAIL_SNS_TLS_SKIP_VERIFY=true`. This turns off server authentication for every HTTPS subscription, so use it only for local testing.
- A fixed pool of 100 workers (pail's choice) posts at most 100 requests at once. A queue of 1,000 deliveries waits for a free worker. pail drops a delivery when the queue is full, and logs a warning. A worker holds a delivery through its retries, so a slow or unreachable endpoint can delay deliveries to other endpoints.
- When an unsigned `Unsubscribe` removes an HTTP or HTTPS subscription, pail posts an `UnsubscribeConfirmation` to the endpoint. AWS sends a final message when the requester is not the owner (the AWS documentation; unverified). A signed `Unsubscribe` sends nothing.
  - The body keys are in this order: `Type` (`UnsubscribeConfirmation`), `MessageId`, `Token`, `TopicArn`, `Message`, `SubscribeURL`, `Timestamp`, `SignatureVersion`, `Signature`, `SigningCertURL`. The `x-amz-sns-message-type` header is `UnsubscribeConfirmation`.
  - `Message` is `You have chosen to deactivate subscription <subscription ARN>.` and `To cancel this operation and restore the subscription, visit the SubscribeURL included in this message.`, on two lines. The string to sign is the same as for a `SubscriptionConfirmation`, with the new `Type`.
  - A visit to the `SubscribeURL` (`ConfirmSubscription` with the token) within 3 days restores the subscription with its ARN and attributes. The restore data is in memory, so a restart loses it. If the endpoint subscribed again meanwhile, the token returns that subscription's ARN and restores nothing. Deleting the topic drops its restore data, so a token from before then returns `InvalidParameter`. Both choices are unverified.
- A delivery is in memory. A restart loses the deliveries that wait, and a pending subscription stays pending.
- Any client with credentials can make pail send a `POST` to any URL that pail can reach. Run pail only where that is acceptable.

### Unsigned links

An SNS message links to pail with an unsigned `GET`. pail routes a request to SNS when all of these hold:

- It has no SigV4 signature.
- Its method is `GET` and its path is `/`.
- Its query has `Action` set to `ConfirmSubscription` or `Unsubscribe`.

pail ignores the host when it routes, so an unsigned `GET` of a virtual-hosted bucket root with one of those two actions goes to SNS too. Every other unsigned request goes to S3, including a `POST` with `Action` in the query and a `GET` of a bucket path. SNS accepts these two actions without a signature. Any other unsigned action returns `MissingAuthenticationToken`, and a request with a bad signature still fails.

### Signatures

- pail creates an RSA-2048 key and a self-signed certificate (common name `pail SNS`, valid for 10 years) the first time it needs one. It stores them under `sns/` in the data directory and reuses them after a restart.
- `GET /_pail/sns/signing-cert.pem` returns the certificate as `application/x-pem-file`. It needs no credentials.
- The string to sign is the lines `Message`, `MessageId`, `Subject` (only when set), `Timestamp`, `TopicArn`, and `Type`, each as a name line and a value line, in that order, with a newline after each line.
- `SignatureVersion` `1` signs with SHA-1 and `2` with SHA-256, both with RSA PKCS #1 v1.5.
- pail signs with its own certificate, so a verifier that requires an `amazonaws.com` host rejects it.

### Errors

The recordings verify every row except `AuthorizationError` and `InternalError`, which come from the service model and are unverified. Every error is an `ErrorResponse` with `Type`, `Code`, and `Message`.

| Condition | Status | `Type` | `Code` |
| --- | --- | --- | --- |
| Missing signature, unknown key, or bad signature | 403 | `Sender` | `MissingAuthenticationToken`, `InvalidClientTokenId`, or `SignatureDoesNotMatch` |
| Missing topic or subscription | 404 | `Sender` | `NotFound` |
| Unsigned `Unsubscribe` of an authenticated subscription | 403 | `Sender` | `AuthorizationError` |
| Tag operation on a missing topic | 404 | `Sender` | `ResourceNotFound` |
| Bad parameter, name, attribute, protocol, or endpoint | 400 | `Sender` | `InvalidParameter` |
| Message attribute with an unknown data type | 400 | `Sender` | `ParameterValueInvalid` |
| More than 50 tags | 400 | `Sender` | `TagLimitExceeded` |
| Action that pail does not implement, or no `Action` | 400 | `Sender` | `InvalidAction` |
| Missing `Message`, or an empty batch | 400 | `Sender` | `ValidationError` |
| Batch rule broken | 400 | `Sender` | `TooManyEntriesInBatchRequest`, `BatchEntryIdsNotDistinct`, `InvalidBatchEntryId`, or `BatchRequestTooLong` |
| Server fault | 500 | `Receiver` | `InternalError` |

### Differences from AWS

- pail delivers to SQS queues and to HTTP and HTTPS endpoints. Filter policies support the operators in [Filter policies](#filter-policies) only.
- pail has no FIFO topics, SMS, email, Lambda, or mobile push.
- pail does not enforce topic policies, queue policies, IAM, or KMS.
- pail serves one account.
- An SQS delivery is synchronous and has no retries. AWS delivers in the background with retries. HTTP and HTTPS deliveries run in the background, with the retries above.
- The retry delays of the backoff functions, the `headerContentType` list, and the effect of `disableSubscriptionOverrides` are unverified. HTTP delivery as a whole is unverified.
- pail accepts a `DeliveryPolicy` for HTTP and HTTPS only. It has no sickly-state logic, so `sicklyRetryPolicy` and `guaranteed` have no effect.
- pail accepts endpoints on internal addresses, which AWS refuses with `AuthorizationError`.
- An unsigned form `POST` with `Action` in the query still goes to S3. Only the two unsigned `GET` links above reach SNS.
- The signing certificate belongs to pail, not to AWS.
