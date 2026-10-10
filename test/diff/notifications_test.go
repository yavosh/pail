package diff

import (
	"net/http"
	"net/url"
)

// notificationsScenario records S3 event notifications to an SQS queue: the
// configuration API and its errors, the test event, prefix filters, and
// created, copied, and removed events. The bucket and the queue share {name}.
func notificationsScenario() scenario {
	// The policy is a JSON string inside the JSON body, so its quotes are escaped.
	policy := `{"QueueUrl":"{queueUrl}","Attributes":{"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",` +
		`\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"{queueArn}\",` +
		`\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::{name}\"}}}]}"}}`
	queueConfig := func(id, arn, event, prefix string) string {
		return `<QueueConfiguration><Id>` + id + `</Id><Queue>` + arn + `</Queue><Event>` + event + `</Event>` +
			`<Filter><S3Key><FilterRule><Name>prefix</Name><Value>` + prefix + `</Value></FilterRule></S3Key></Filter></QueueConfiguration>`
	}
	config := func(rules ...string) string {
		body := `<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
		for _, r := range rules {
			body += r
		}
		return body + `</NotificationConfiguration>`
	}
	putConfig := func(name, body string) step {
		return step{name: name, method: http.MethodPut, query: "notification", body: body}
	}
	const receive = `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":10,"MaxNumberOfMessages":10}`
	const del = `{"QueueUrl":"{queueUrl}","ReceiptHandle":"{receiptHandle}"}`
	valid := config(
		`<QueueConfiguration><Id>created</Id><Queue>{queueArn}</Queue><Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event>` +
			`<Filter><S3Key><FilterRule><Name>prefix</Name><Value>in/</Value></FilterRule></S3Key></Filter></QueueConfiguration>`,
	)
	return scenario{name: "bucket-notifications", queues: []string{"{name}"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		sqsStep("set-queue-policy", "SetQueueAttributes", policy),
		createBucket(),
		{name: "get-config-new", method: http.MethodGet, query: "notification"},
		putConfig("put-config-bad-event", config(queueConfig("bad", "{queueArn}", "s3:Bogus", "in/"))),
		putConfig("put-config-missing-queue", config(queueConfig("missing", "{queueArn}-missing", "s3:ObjectCreated:*", "in/"))),
		putConfig("put-config-overlap", config(
			queueConfig("one", "{queueArn}", "s3:ObjectCreated:*", "in/"),
			queueConfig("two", "{queueArn}", "s3:ObjectCreated:Put", "in/"))),
		putConfig("put-config-malformed", "<NotificationConfiguration>"),
		putConfig("put-config", valid),
		sqsStep("receive-test-event", "ReceiveMessage", receive),
		sqsStep("delete-test-event", "DeleteMessage", del),
		{name: "get-config", method: http.MethodGet, query: "notification"},
		{name: "put-in", method: http.MethodPut, key: "in/a.txt", body: "hello"},
		{name: "put-out", method: http.MethodPut, key: "out/b.txt", body: "skip"},
		sqsStep("receive-created", "ReceiveMessage", receive),
		sqsStep("delete-created", "DeleteMessage", del),
		{name: "copy-in", method: http.MethodPut, key: "in/c.txt", header: map[string]string{"x-amz-copy-source": "{bucket}/in/a.txt"}},
		sqsStep("receive-copied", "ReceiveMessage", receive),
		sqsStep("delete-copied", "DeleteMessage", del),
		{name: "delete-in", method: http.MethodDelete, key: "in/a.txt"},
		sqsStep("receive-removed", "ReceiveMessage", receive),
		sqsStep("delete-removed", "DeleteMessage", del),
		putConfig("put-config-empty", config()),
		{name: "get-config-empty", method: http.MethodGet, query: "notification"},
		// AWS applies a cleared configuration with a delay, so no step checks for silence.
		{name: "delete-objects", method: http.MethodPost, query: "delete", body: deleteBody("in/c.txt", "out/b.txt"),
			header: map[string]string{"Content-MD5": md5Base64(deleteBody("in/c.txt", "out/b.txt"))}},
		deleteBucket(),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}

// notificationsSNSScenario records S3 event notifications to an SNS topic. A
// queue subscribed with raw delivery receives each event as its body.
func notificationsSNSScenario() scenario {
	// Only the JSON around {topicArn} is escaped, so the placeholder stays intact.
	topicPolicy := url.QueryEscape(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},"Action":"SNS:Publish","Resource":"`) +
		"{topicArn}" + url.QueryEscape(`","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:s3:::`) + "{name}" + url.QueryEscape(`"}}}]}`)
	config := `<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TopicConfiguration><Id>topic</Id><Topic>{topicArn}</Topic>` +
		`<Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></TopicConfiguration></NotificationConfiguration>`
	const receive = `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":10,"MaxNumberOfMessages":10}`
	const del = `{"QueueUrl":"{queueUrl}","ReceiptHandle":"{receiptHandle}"}`
	return scenario{name: "bucket-notifications-sns", topics: []string{"{name}"}, queues: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		sqsStep("set-queue-policy", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe-raw", "Action=Subscribe&TopicArn={topicArn}&Protocol=sqs&Endpoint={queueArn}"+
			"&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"+snsVersion),
		snsStep("set-topic-policy", "Action=SetTopicAttributes&TopicArn={topicArn}&AttributeName=Policy&AttributeValue="+topicPolicy+snsVersion),
		createBucket(),
		{name: "put-config", method: http.MethodPut, query: "notification", body: config},
		sqsStep("receive-test-event", "ReceiveMessage", receive),
		sqsStep("delete-test-event", "DeleteMessage", del),
		{name: "get-config", method: http.MethodGet, query: "notification"},
		{name: "put-skipped", method: http.MethodPut, key: "image.png", body: "png"},
		{name: "put-matched", method: http.MethodPut, key: "notes.txt", body: "notes"},
		sqsStep("receive-created", "ReceiveMessage", receive),
		sqsStep("delete-created", "DeleteMessage", del),
		{name: "delete-objects", method: http.MethodPost, query: "delete", body: deleteBody("image.png", "notes.txt"),
			header: map[string]string{"Content-MD5": md5Base64(deleteBody("image.png", "notes.txt"))}},
		deleteBucket(),
		snsStep("delete-topic", "Action=DeleteTopic&TopicArn={topicArn}"+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}
