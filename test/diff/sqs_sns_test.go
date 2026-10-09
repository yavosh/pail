package diff

import "net/http"

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
