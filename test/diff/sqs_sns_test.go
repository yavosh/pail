package diff

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Scenarios never call SNS ListTopics or ListSubscriptions, and call SQS ListQueues
// only with QueueNamePrefix: golden files must not expose other resources.

func sqsStep(name, op, body string) step {
	return step{name: name, method: http.MethodPost, service: "sqs", target: "AmazonSQS." + op, body: body}
}

func snsStep(name, form string) step {
	return step{name: name, method: http.MethodPost, service: "sns", body: form}
}

func sqsAuthErrorsScenario() scenario {
	const list = `{"QueueNamePrefix":"{name}"}`
	none := sqsStep("no-credentials", "ListQueues", list)
	none.auth = authNone
	unknown := sqsStep("unknown-key", "ListQueues", list)
	unknown.auth = authUnknownKey
	bad := sqsStep("bad-signature", "ListQueues", list)
	bad.auth = authBadSignature
	skewed := sqsStep("clock-skew", "ListQueues", list)
	skewed.auth = authSkewed
	return scenario{name: "sqs-auth-errors", steps: []step{none, unknown, bad, skewed}}
}

func sqsQueueBasicsScenario() scenario {
	return scenario{name: "sqs-queue-basics", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-url", "GetQueueUrl", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-attributes", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["All"]}`),
		sqsStep("send-message", "SendMessage", `{"QueueUrl":"{queueUrl}","MessageBody":"hello"}`),
		sqsStep("receive-message", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","MaxNumberOfMessages":1,"WaitTimeSeconds":5}`),
		sqsStep("delete-message", "DeleteMessage", `{"QueueUrl":"{queueUrl}","ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("get-missing-queue-url", "GetQueueUrl", `{"QueueName":"{name}-missing"}`),
		sqsStep("list-queues", "ListQueues", `{"QueueNamePrefix":"{name}"}`),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}

func sqsMessageAttributesScenario() scenario {
	const recv = `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":5,"VisibilityTimeout":0`
	return scenario{name: "sqs-message-attributes", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("send-with-attributes", "SendMessage", `{"QueueUrl":"{queueUrl}","MessageBody":"hello","MessageAttributes":{`+
			`"color":{"DataType":"String","StringValue":"blue"},`+
			`"count":{"DataType":"Number","StringValue":"3"},`+
			`"blob":{"DataType":"Binary","BinaryValue":"AQID"}}}`),
		sqsStep("receive-all", "ReceiveMessage", recv+`,"MaxNumberOfMessages":1,"MessageAttributeNames":["All"],"MessageSystemAttributeNames":["All"]}`),
		sqsStep("receive-filtered", "ReceiveMessage", recv+`,"MessageAttributeNames":["color"]}`),
		sqsStep("receive-prefix", "ReceiveMessage", recv+`,"MessageAttributeNames":["col.*"]}`),
		sqsStep("receive-none", "ReceiveMessage", recv+`}`),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}

func sqsVisibilityScenario() scenario {
	const handle = `"QueueUrl":"{queueUrl}","ReceiptHandle":"{receiptHandle}"`
	return scenario{name: "sqs-visibility", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("send-message", "SendMessage", `{"QueueUrl":"{queueUrl}","MessageBody":"hello"}`),
		sqsStep("receive-first", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","VisibilityTimeout":30,"WaitTimeSeconds":5}`),
		sqsStep("receive-while-invisible", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":0}`),
		sqsStep("change-visibility-zero", "ChangeMessageVisibility", `{`+handle+`,"VisibilityTimeout":0}`),
		sqsStep("change-visibility-not-inflight", "ChangeMessageVisibility", `{`+handle+`,"VisibilityTimeout":10}`),
		sqsStep("receive-second", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":5,"AttributeNames":["ApproximateReceiveCount"]}`),
		sqsStep("get-attributes", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"]}`),
		sqsStep("delete-message", "DeleteMessage", `{`+handle+`}`),
		sqsStep("delete-message-again", "DeleteMessage", `{`+handle+`}`),
		sqsStep("delete-garbage-handle", "DeleteMessage", `{"QueueUrl":"{queueUrl}","ReceiptHandle":"garbage"}`),
		sqsStep("purge-queue", "PurgeQueue", `{"QueueUrl":"{queueUrl}"}`),
		sqsStep("purge-again", "PurgeQueue", `{"QueueUrl":"{queueUrl}"}`),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}

func sqsBatchesScenario() scenario {
	// Standard queues do not order messages, so every body is the same.
	entry := func(id string) string { return `{"Id":"` + id + `","MessageBody":"fine"}` }
	var eleven []string
	for i := range 11 {
		eleven = append(eleven, entry("e"+strings.Repeat("x", i)))
	}
	send := func(entries ...string) string {
		return `{"QueueUrl":"{queueUrl}","Entries":[` + strings.Join(entries, ",") + `]}`
	}
	return scenario{name: "sqs-batches", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("send-batch", "SendMessageBatch", send(entry("a"), entry("b"))),
		sqsStep("send-batch-partial", "SendMessageBatch", send(entry("ok"), `{"Id":"bad","MessageBody":"\u0000"}`)),
		sqsStep("send-batch-empty", "SendMessageBatch", send()),
		sqsStep("send-batch-too-many", "SendMessageBatch", send(eleven...)),
		sqsStep("send-batch-duplicate-ids", "SendMessageBatch", send(entry("a"), entry("a"))),
		sqsStep("send-batch-invalid-id", "SendMessageBatch", send(entry("bad id"))),
		sqsStep("send-batch-fifo-field", "SendMessageBatch", send(entry("ok"), `{"Id":"fifo","MessageBody":"fine","MessageGroupId":"g"}`)),
		sqsStep("receive-one", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","MaxNumberOfMessages":1,"WaitTimeSeconds":5}`),
		sqsStep("change-visibility-batch", "ChangeMessageVisibilityBatch", `{"QueueUrl":"{queueUrl}","Entries":[`+
			`{"Id":"x","ReceiptHandle":"{receiptHandle}","VisibilityTimeout":0},`+
			`{"Id":"y","ReceiptHandle":"garbage","VisibilityTimeout":0}]}`),
		sqsStep("receive-again", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","MaxNumberOfMessages":1,"WaitTimeSeconds":5}`),
		sqsStep("delete-batch", "DeleteMessageBatch", `{"QueueUrl":"{queueUrl}","Entries":[`+
			`{"Id":"x","ReceiptHandle":"{receiptHandle}"},`+
			`{"Id":"y","ReceiptHandle":"garbage"}]}`),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}

func sqsErrorsScenario() scenario {
	const url = `"QueueUrl":"{queueUrl}"`
	return scenario{name: "sqs-errors", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}","Attributes":{"VisibilityTimeout":"30"}}`),
		sqsStep("create-queue-same", "CreateQueue", `{"QueueName":"{name}","Attributes":{"VisibilityTimeout":"30"}}`),
		sqsStep("create-queue-conflict", "CreateQueue", `{"QueueName":"{name}","Attributes":{"VisibilityTimeout":"60"}}`),
		sqsStep("create-invalid-name", "CreateQueue", `{"QueueName":"{name} bad"}`),
		sqsStep("get-attributes-invalid-name", "GetQueueAttributes", `{`+url+`,"AttributeNames":["Bogus"]}`),
		sqsStep("set-attributes-invalid-value", "SetQueueAttributes", `{`+url+`,"Attributes":{"VisibilityTimeout":"99999"}}`),
		sqsStep("set-attributes-unknown", "SetQueueAttributes", `{`+url+`,"Attributes":{"Bogus":"1"}}`),
		sqsStep("set-max-size", "SetQueueAttributes", `{`+url+`,"Attributes":{"MaximumMessageSize":"1024"}}`),
		sqsStep("send-too-long", "SendMessage", `{`+url+`,"MessageBody":"`+strings.Repeat("x", 1025)+`"}`),
		sqsStep("send-invalid-chars", "SendMessage", `{`+url+`,"MessageBody":"\u0000"}`),
		sqsStep("send-empty-body", "SendMessage", `{`+url+`,"MessageBody":""}`),
		sqsStep("send-fifo-field-standard-queue", "SendMessage", `{`+url+`,"MessageBody":"x","MessageGroupId":"g"}`),
		sqsStep("receive-max-11", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":11}`),
		sqsStep("receive-wait-21", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":21}`),
		sqsStep("get-queue-url-missing-name", "GetQueueUrl", `{}`),
		sqsStep("unknown-operation", "Bogus", `{}`),
		sqsStep("malformed-json", "GetQueueUrl", `{`),
		sqsStep("tag-queue", "TagQueue", `{`+url+`,"Tags":{"env":"test"}}`),
		sqsStep("list-queue-tags", "ListQueueTags", `{`+url+`}`),
		sqsStep("untag-queue", "UntagQueue", `{`+url+`,"TagKeys":["env"]}`),
		sqsStep("list-queue-tags-empty", "ListQueueTags", `{`+url+`}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+url+`}`),
	}}
}

func sqsFifoScenario() scenario {
	const url = `"QueueUrl":"{queueUrl}"`
	send := func(fields string) string { return `{` + url + `,"MessageBody":` + fields + `}` }
	return scenario{name: "sqs-fifo", queues: []string{"{name}.fifo", "{name}-cbd.fifo", "{name}-noattr.fifo", "{name}-std", "{name}-ht.fifo"}, steps: []step{
		sqsStep("create-fifo-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("get-fifo-attributes", "GetQueueAttributes", `{`+url+`,"AttributeNames":["All"]}`),
		sqsStep("set-fifo-queue-attribute", "SetQueueAttributes", `{`+url+`,"Attributes":{"FifoQueue":"false"}}`),
		sqsStep("create-fifo-attr-on-standard-name", "CreateQueue", `{"QueueName":"{name}-std","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("create-high-throughput", "CreateQueue", `{"QueueName":"{name}-ht.fifo","Attributes":{"FifoQueue":"true","DeduplicationScope":"messageGroup","FifoThroughputLimit":"perMessageGroupId"}}`),
		sqsStep("get-high-throughput-attributes", "GetQueueAttributes", `{`+url+`,"AttributeNames":["All"]}`),
		sqsStep("get-main-queue-url", "GetQueueUrl", `{"QueueName":"{name}.fifo"}`),
		sqsStep("create-fifo-without-attribute", "CreateQueue", `{"QueueName":"{name}-noattr.fifo"}`),
		sqsStep("send-missing-group", "SendMessage", send(`"x","MessageDeduplicationId":"d0"`)),
		sqsStep("send-missing-dedup", "SendMessage", send(`"x","MessageGroupId":"a"`)),
		sqsStep("send-per-message-delay", "SendMessage", send(`"x","MessageGroupId":"a","MessageDeduplicationId":"d0","DelaySeconds":5`)),
		sqsStep("send-a1", "SendMessage", send(`"a1","MessageGroupId":"a","MessageDeduplicationId":"1"`)),
		sqsStep("send-a1-duplicate", "SendMessage", send(`"a1","MessageGroupId":"a","MessageDeduplicationId":"1"`)),
		sqsStep("send-a2", "SendMessage", send(`"a2","MessageGroupId":"a","MessageDeduplicationId":"2"`)),
		sqsStep("receive-first", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":1,"WaitTimeSeconds":5,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("receive-locked", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":1,"WaitTimeSeconds":0}`),
		sqsStep("delete-first", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("receive-next", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":1,"WaitTimeSeconds":5}`),
		sqsStep("delete-next", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("create-cbd-queue", "CreateQueue", `{"QueueName":"{name}-cbd.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`),
		sqsStep("send-cbd-1", "SendMessage", send(`"same","MessageGroupId":"g"`)),
		sqsStep("send-cbd-2", "SendMessage", send(`"same","MessageGroupId":"g"`)),
		// The second receive runs while the first message is in flight, so it is empty either way.
		sqsStep("receive-cbd", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":10,"WaitTimeSeconds":5,"MessageSystemAttributeNames":["MessageDeduplicationId"]}`),
		sqsStep("receive-cbd-again", "ReceiveMessage", `{`+url+`,"MaxNumberOfMessages":10,"WaitTimeSeconds":0}`),
		sqsStep("delete-cbd-queue", "DeleteQueue", `{`+url+`}`),
		sqsStep("get-high-throughput-url", "GetQueueUrl", `{"QueueName":"{name}-ht.fifo"}`),
		sqsStep("delete-high-throughput-queue", "DeleteQueue", `{`+url+`}`),
		sqsStep("get-fifo-url", "GetQueueUrl", `{"QueueName":"{name}.fifo"}`),
		sqsStep("delete-fifo-queue", "DeleteQueue", `{`+url+`}`),
	}}
}

func sqsDeadLetterScenario() scenario {
	const url = `"QueueUrl":"{queueUrl}"`
	// The policy is a JSON string inside the JSON body, so its quotes are escaped.
	policy := func(queue, arn, count string) string {
		return `{"QueueName":"` + queue + `","Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"` + arn + `\",\"maxReceiveCount\":` + count + `}"}}`
	}
	return scenario{name: "sqs-dead-letter", queues: []string{"{name}", "{name}-dlq", "{name}-dlq.fifo", "{name}-mm", "{name}-denied"}, steps: []step{
		sqsStep("create-dlq", "CreateQueue", `{"QueueName":"{name}-dlq"}`),
		sqsStep("get-dlq-attributes", "GetQueueAttributes", `{`+url+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("list-dlq-sources-empty", "ListDeadLetterSourceQueues", `{`+url+`}`),
		sqsStep("create-fifo-dlq", "CreateQueue", `{"QueueName":"{name}-dlq.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("get-fifo-dlq-attributes", "GetQueueAttributes", `{`+url+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("create-source-type-mismatch", "CreateQueue", policy("{name}-mm", `{queueArn}`, `1`)),
		sqsStep("delete-fifo-dlq", "DeleteQueue", `{`+url+`}`),
		sqsStep("get-dlq-url-before-policy", "GetQueueUrl", `{"QueueName":"{name}-dlq"}`),
		sqsStep("get-dlq-arn-again", "GetQueueAttributes", `{`+url+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("set-allow-deny-all", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"denyAll\"}"}}`),
		sqsStep("create-source-denied", "CreateQueue", policy("{name}-denied", `{queueArn}`, `1`)),
		sqsStep("get-dlq-allow-policy", "GetQueueAttributes", `{`+url+`,"AttributeNames":["RedriveAllowPolicy"]}`),
		sqsStep("set-allow-all", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"allowAll\"}"}}`),
		sqsStep("create-source-bad-target", "CreateQueue", policy("{name}", `{queueArn}-missing`, `1`)),
		sqsStep("create-source-bad-count", "CreateQueue", policy("{name}", `{queueArn}`, `0`)),
		sqsStep("create-source", "CreateQueue", policy("{name}", `{queueArn}`, `\"1\"`)),
		sqsStep("get-source-attributes", "GetQueueAttributes", `{`+url+`,"AttributeNames":["RedrivePolicy"]}`),
		sqsStep("send-message", "SendMessage", `{`+url+`,"MessageBody":"poison"}`),
		sqsStep("receive-once", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":5,"VisibilityTimeout":0,"AttributeNames":["ApproximateReceiveCount"]}`),
		sqsStep("receive-moved", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":5}`),
		sqsStep("get-dlq-url", "GetQueueUrl", `{"QueueName":"{name}-dlq"}`),
		sqsStep("list-dlq-sources", "ListDeadLetterSourceQueues", `{`+url+`}`),
		sqsStep("receive-from-dlq", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":5,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("delete-from-dlq", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("delete-dlq", "DeleteQueue", `{`+url+`}`),
		sqsStep("get-source-url", "GetQueueUrl", `{"QueueName":"{name}"}`),
		sqsStep("delete-source", "DeleteQueue", `{`+url+`}`),
	}}
}

func sqsFairQueueScenario() scenario {
	const url = `"QueueUrl":"{queueUrl}"`
	// The normalizer masks MessageId and SequenceNumber, so sqs-fifo settles
	// deduplication through receive-next returning a2, not through the send results.
	return scenario{name: "sqs-fair-queue", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("set-fifo-attr-on-standard", "SetQueueAttributes", `{`+url+`,"Attributes":{"ContentBasedDeduplication":"true"}}`),
		sqsStep("send-dedup-id-standard", "SendMessage", `{`+url+`,"MessageBody":"x","MessageDeduplicationId":"d1"}`),
		sqsStep("send-with-group", "SendMessage", `{`+url+`,"MessageBody":"tenant","MessageGroupId":"tenant-a"}`),
		sqsStep("receive-with-attributes", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":5,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("delete-message", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+url+`}`),
	}}
}

func snsAuthErrorsScenario() scenario {
	// A harmless read, in case auth unexpectedly succeeds.
	const form = "Action=GetTopicAttributes&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3A{name}&Version=2010-03-31"
	none := snsStep("no-credentials", form)
	none.auth = authNone
	unknown := snsStep("unknown-key", form)
	unknown.auth = authUnknownKey
	bad := snsStep("bad-signature", form)
	bad.auth = authBadSignature
	skewed := snsStep("clock-skew", form)
	skewed.auth = authSkewed
	return scenario{name: "sns-auth-errors", steps: []step{none, unknown, bad, skewed}}
}

func snsTopicBasicsScenario() scenario {
	const attributes = "Action=GetTopicAttributes&TopicArn={topicArn}&Version=2010-03-31"
	return scenario{name: "sns-topic-basics", topics: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}&Version=2010-03-31"),
		snsStep("get-topic-attributes", attributes),
		snsStep("publish", "Action=Publish&TopicArn={topicArn}&Message=hello&Version=2010-03-31"),
		snsStep("delete-topic", "Action=DeleteTopic&TopicArn={topicArn}&Version=2010-03-31"),
		snsStep("get-deleted-topic-attributes", attributes),
	}}
}

// snsVersion ends every SNS form, as the SDKs send it.
const snsVersion = "&Version=2010-03-31"

func snsAttributesScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	return scenario{name: "sns-topic-attributes", topics: []string{"{name}", "{name}.fifo"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=pail"+snsVersion),
		snsStep("get-topic-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("set-display-name", "Action=SetTopicAttributes"+topic+"&AttributeName=DisplayName&AttributeValue=renamed"+snsVersion),
		snsStep("get-after-set", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("create-topic-again", "Action=CreateTopic&Name={name}"+snsVersion),
		snsStep("create-topic-conflict", "Action=CreateTopic&Name={name}&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=other"+snsVersion),
		snsStep("create-invalid-name", "Action=CreateTopic&Name={name}+bad"+snsVersion),
		snsStep("create-fifo-name", "Action=CreateTopic&Name={name}.fifo"+snsVersion),
		snsStep("tag-resource", "Action=TagResource&ResourceArn={topicArn}&Tags.member.1.Key=env&Tags.member.1.Value=test"+snsVersion),
		snsStep("list-tags", "Action=ListTagsForResource&ResourceArn={topicArn}"+snsVersion),
		snsStep("untag-resource", "Action=UntagResource&ResourceArn={topicArn}&TagKeys.member.1=env"+snsVersion),
		snsStep("list-tags-empty", "Action=ListTagsForResource&ResourceArn={topicArn}"+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		snsStep("delete-topic-again", "Action=DeleteTopic"+topic+snsVersion),
	}}
}

// snsQueuePolicy is a JSON string inside a JSON body, so its quotes are escaped. AWS
// delivers to an SQS queue only when the queue policy allows the topic.
const snsQueuePolicy = `{"QueueUrl":"{queueUrl}","Attributes":{"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",` +
	`\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"{queueArn}\",` +
	`\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"{topicArn}\"}}}]}"}}`

func snsSQSDeliveryScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	const url = `"QueueUrl":"{queueUrl}"`
	const attr = "&MessageAttributes.entry.1.Name=color&MessageAttributes.entry.1.Value.DataType=String&MessageAttributes.entry.1.Value.StringValue=blue"
	return scenario{name: "sns-sqs-delivery", topics: []string{"{name}"}, queues: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{`+url+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("set-queue-policy", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}"+snsVersion),
		snsStep("get-subscription-attributes", "Action=GetSubscriptionAttributes&SubscriptionArn={subscriptionArn}"+snsVersion),
		snsStep("list-subscriptions-by-topic", "Action=ListSubscriptionsByTopic"+topic+snsVersion),
		snsStep("publish", "Action=Publish"+topic+"&Message=hello&Subject=greeting"+attr+snsVersion),
		sqsStep("receive-envelope", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":10,"MaxNumberOfMessages":1}`),
		sqsStep("delete-envelope", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		snsStep("set-raw-delivery", "Action=SetSubscriptionAttributes&SubscriptionArn={subscriptionArn}&AttributeName=RawMessageDelivery&AttributeValue=true"+snsVersion),
		snsStep("publish-raw", "Action=Publish"+topic+"&Message=raw"+attr+
			"&MessageAttributes.entry.2.Name=tags&MessageAttributes.entry.2.Value.DataType=String.Array&MessageAttributes.entry.2.Value.StringValue=%5B%22a%22%2C%22b%22%5D"+snsVersion),
		sqsStep("receive-raw", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":10,"MaxNumberOfMessages":1,"MessageAttributeNames":["All"]}`),
		sqsStep("delete-raw", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		snsStep("publish-structured", "Action=Publish"+topic+"&MessageStructure=json&Message=%7B%22default%22%3A%22d%22%2C%22sqs%22%3A%22s%22%7D"+snsVersion),
		sqsStep("receive-structured", "ReceiveMessage", `{`+url+`,"WaitTimeSeconds":10,"MaxNumberOfMessages":1}`),
		sqsStep("delete-structured", "DeleteMessage", `{`+url+`,"ReceiptHandle":"{receiptHandle}"}`),
		snsStep("unsubscribe", "Action=Unsubscribe&SubscriptionArn={subscriptionArn}"+snsVersion),
		snsStep("list-subscriptions-after", "Action=ListSubscriptionsByTopic"+topic+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{`+url+`}`),
	}}
}

func snsPublishBatchScenario() scenario {
	const topic = "Action=PublishBatch&TopicArn={topicArn}"
	const member = "&PublishBatchRequestEntries.member."
	entry := func(n int, id, message string) string {
		return fmt.Sprintf("%s%d.Id=%s%s%d.Message=%s", member, n, id, member, n, message)
	}
	var eleven string
	for i := 1; i <= 11; i++ {
		eleven += entry(i, fmt.Sprint("e", i), "fine")
	}
	return scenario{name: "sns-publish-batch", topics: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		snsStep("publish-batch", topic+entry(1, "a", "one")+entry(2, "b", "two")+snsVersion),
		snsStep("publish-batch-partial", topic+entry(1, "ok", "fine")+entry(2, "empty", "")+snsVersion),
		snsStep("publish-batch-empty", topic+snsVersion),
		snsStep("publish-batch-too-many", topic+eleven+snsVersion),
		snsStep("publish-batch-duplicate-ids", topic+entry(1, "a", "fine")+entry(2, "a", "fine")+snsVersion),
		snsStep("publish-batch-invalid-id", topic+entry(1, "bad+id", "fine")+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic&TopicArn={topicArn}"+snsVersion),
	}}
}

func snsErrorsScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	return scenario{name: "sns-errors", topics: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		snsStep("publish-no-message", "Action=Publish"+topic+snsVersion),
		snsStep("publish-long-subject", "Action=Publish"+topic+"&Message=x&Subject="+strings.Repeat("s", 101)+snsVersion),
		snsStep("publish-bad-structure", "Action=Publish"+topic+"&Message=%7B%22sqs%22%3A%22x%22%7D&MessageStructure=json"+snsVersion),
		snsStep("publish-group-id-standard", "Action=Publish"+topic+"&Message=x&MessageGroupId=g"+snsVersion),
		snsStep("subscribe-bad-protocol", "Action=Subscribe"+topic+"&Protocol=smoke&Endpoint=x"+snsVersion),
		snsStep("subscribe-bad-endpoint", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint=not-an-arn"+snsVersion),
		snsStep("get-missing-subscription-attributes", "Action=GetSubscriptionAttributes&SubscriptionArn={topicArn}:00000000-0000-0000-0000-000000000000"+snsVersion),
		snsStep("unknown-action", "Action=Bogus"+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		snsStep("publish-to-deleted", "Action=Publish"+topic+"&Message=x"+snsVersion),
	}}
}

func snsFilterPoliciesScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	const queueURL = `"QueueUrl":"{queueUrl}"`
	const raw = "&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"
	const receive = `{` + queueURL + `,"WaitTimeSeconds":10,"MaxNumberOfMessages":10,"MessageAttributeNames":["All"]}`
	del := `{` + queueURL + `,"ReceiptHandle":"{receiptHandle}"}`
	subscribe := func(policy string) string {
		return "Action=Subscribe" + topic + "&Protocol=sqs&Endpoint={queueArn}" + raw +
			"&Attributes.entry.2.key=FilterPolicy&Attributes.entry.2.value=" + url.QueryEscape(policy) + snsVersion
	}
	set := func(name, value string) string {
		return "Action=SetSubscriptionAttributes&SubscriptionArn={subscriptionArn}&AttributeName=" + name +
			"&AttributeValue=" + url.QueryEscape(value) + snsVersion
	}
	// publish builds a Publish form; attrs holds name, data type, and value triples.
	publish := func(message string, attrs ...string) string {
		form := "Action=Publish" + topic + "&Message=" + url.QueryEscape(message)
		for i := 0; i < len(attrs); i += 3 {
			n := i/3 + 1
			form += fmt.Sprintf("&MessageAttributes.entry.%d.Name=%s&MessageAttributes.entry.%d.Value.DataType=%s&MessageAttributes.entry.%d.Value.StringValue=%s",
				n, attrs[i], n, attrs[i+1], n, url.QueryEscape(attrs[i+2]))
		}
		return form + snsVersion
	}
	words := func(prefix string, n int) string {
		var list []string
		for i := range n {
			list = append(list, fmt.Sprintf("%q", fmt.Sprint(prefix, i)))
		}
		return "[" + strings.Join(list, ",") + "]"
	}
	return scenario{name: "sns-filter-policies", topics: []string{"{name}"}, queues: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{`+queueURL+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("set-queue-policy", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe-bad-filter", subscribe(`{"color":[{"bogus":"x"}]}`)),
		snsStep("subscribe", subscribe(`{"color":["blue","green"],"size":[{"numeric":[">",10,"<=",20]}]}`)),
		snsStep("get-subscription-attributes", "Action=GetSubscriptionAttributes&SubscriptionArn={subscriptionArn}"+snsVersion),
		snsStep("publish-wrong-color", publish("wrong-color", "color", "String", "red", "size", "Number", "15")),
		snsStep("publish-size-as-string", publish("size-as-string", "color", "String", "blue", "size", "String", "15")),
		snsStep("publish-match", publish("match", "color", "String", "blue", "size", "Number", "15")),
		sqsStep("receive-match", "ReceiveMessage", receive),
		sqsStep("delete-match", "DeleteMessage", del),
		snsStep("set-array-policy", set("FilterPolicy", `{"tags":["b"],"region":[{"prefix":"eu-"}],"env":[{"anything-but":["prod","stage"]}],"trace":[{"exists":false}]}`)),
		snsStep("publish-anything-but-miss", publish("anything-but-miss", "tags", "String.Array", `["a","b"]`, "region", "String", "eu-west-1", "env", "String", "prod")),
		snsStep("publish-exists-miss", publish("exists-miss", "tags", "String.Array", `["b"]`, "region", "String", "eu-west-1", "env", "String", "dev", "trace", "String", "x")),
		snsStep("publish-array-match", publish("array", "tags", "String.Array", `["a","b"]`, "region", "String", "eu-west-1", "env", "String", "dev")),
		sqsStep("receive-array", "ReceiveMessage", receive),
		sqsStep("delete-array", "DeleteMessage", del),
		snsStep("set-body-scope", set("FilterPolicyScope", "MessageBody")),
		snsStep("set-body-policy", set("FilterPolicy", `{"order":{"total":[{"numeric":[">=",100]}],"kind":["book"]}}`)),
		snsStep("publish-body-miss", publish(`{"order":{"total":5,"kind":"book"}}`)),
		snsStep("publish-body-not-json", publish("plain")),
		snsStep("publish-body-match", publish(`{"order":{"total":150,"kind":["book","pen"]}}`)),
		sqsStep("receive-body", "ReceiveMessage", receive),
		sqsStep("delete-body", "DeleteMessage", del),
		snsStep("set-scope-attributes-nested", set("FilterPolicyScope", "MessageAttributes")),
		snsStep("set-not-json", set("FilterPolicy", "{")),
		snsStep("set-too-many-keys", set("FilterPolicy", `{"a":["x"],"b":["x"],"c":["x"],"d":["x"],"e":["x"],"f":["x"]}`)),
		snsStep("set-too-complex", set("FilterPolicy", `{"a":`+words("a", 12)+`,"b":`+words("b", 13)+`}`)),
		snsStep("set-empty-policy", set("FilterPolicy", "{}")),
		snsStep("get-attributes-after-empty", "Action=GetSubscriptionAttributes&SubscriptionArn={subscriptionArn}"+snsVersion),
		snsStep("unsubscribe", "Action=Unsubscribe&SubscriptionArn={subscriptionArn}"+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{`+queueURL+`}`),
	}}
}

func TestSQSStepBodiesAreJSON(t *testing.T) {
	vars := map[string]string{"name": "pail-diff-1"}
	for _, v := range variables {
		vars[v] = "arn:aws:sqs:us-east-1:000000000000:v"
	}
	for _, sc := range scenarios() {
		for _, st := range sc.steps {
			if st.service != "sqs" {
				continue
			}
			expanded, _ := st.withVars(vars)
			if !json.Valid([]byte(expanded.body)) && st.name != "malformed-json" {
				t.Errorf("%s/%s body = %s, want valid JSON", sc.name, st.name, expanded.body)
			}
		}
	}
}

func TestRedrivePolicyStepBody(t *testing.T) {
	var found step
	for _, st := range sqsDeadLetterScenario().steps {
		if st.name == "create-source" {
			found = st
		}
	}
	st, _ := found.withVars(map[string]string{"name": "n", "queueArn": "arn:q"})
	var got struct{ Attributes map[string]string }
	if err := json.Unmarshal([]byte(st.body), &got); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", st.body, err)
	}
	if want := `{"deadLetterTargetArn":"arn:q","maxReceiveCount":"1"}`; got.Attributes["RedrivePolicy"] != want {
		t.Errorf("RedrivePolicy = %q, want %q", got.Attributes["RedrivePolicy"], want)
	}
}
