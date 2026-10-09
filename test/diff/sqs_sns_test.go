package diff

import (
	"net/http"
	"strings"
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
