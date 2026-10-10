package diff

import (
	"fmt"
	"net/url"
	"strings"
)

// sqsEdgeCasesScenario records SQS behavior that docs/sqs-sns-compatibility.md
// listed as unverified.
func sqsEdgeCasesScenario() scenario {
	const url = `"QueueUrl":"{queueUrl}"`
	malformed := sqsStep("incomplete-signature", "ListQueues", `{"QueueNamePrefix":"{name}"}`)
	malformed.auth = authNone
	malformed.header = map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=bogus"}
	return scenario{name: "sqs-edge-cases", queues: []string{"{name}", "{name}-std"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{`+url+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("send-missing-queue-url", "SendMessage", `{"MessageBody":"x"}`),
		sqsStep("delete-missing-receipt-handle", "DeleteMessage", `{`+url+`}`),
		sqsStep("change-visibility-missing-timeout", "ChangeMessageVisibility", `{`+url+`,"ReceiptHandle":"garbage"}`),
		sqsStep("send-dedup-id-standard", "SendMessage", `{`+url+`,"MessageBody":"x","MessageDeduplicationId":"d"}`),
		sqsStep("send-string-list-values", "SendMessage", `{`+url+`,"MessageBody":"x","MessageAttributes":{"a":{"DataType":"String","StringListValues":["v"]}}}`),
		sqsStep("send-binary-list-values", "SendMessage", `{`+url+`,"MessageBody":"x","MessageAttributes":{"a":{"DataType":"Binary","BinaryListValues":["AQID"]}}}`),
		sqsStep("body-array", "GetQueueUrl", `[]`),
		sqsStep("body-string", "GetQueueUrl", `"x"`),
		sqsStep("set-fifo-false-standard", "SetQueueAttributes", `{`+url+`,"Attributes":{"FifoQueue":"false"}}`),
		sqsStep("create-fifo-false-standard", "CreateQueue", `{"QueueName":"{name}-std","Attributes":{"FifoQueue":"false"}}`),
		sqsStep("get-std-url", "GetQueueUrl", `{"QueueName":"{name}"}`),
		sqsStep("redrive-to-self", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"{queueArn}\",\"maxReceiveCount\":1}"}}`),
		sqsStep("allow-by-queue", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"byQueue\",\"sourceQueueArns\":[\"{queueArn}\"]}"}}`),
		sqsStep("get-allow-by-queue", "GetQueueAttributes", `{`+url+`,"AttributeNames":["RedriveAllowPolicy"]}`),
		sqsStep("allow-by-queue-no-arns", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"byQueue\"}"}}`),
		sqsStep("allow-all-with-arns", "SetQueueAttributes", `{`+url+`,"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"allowAll\",\"sourceQueueArns\":[\"{queueArn}\"]}"}}`),
		malformed,
		sqsStep("send-over-1mib", "SendMessage", `{`+url+`,"MessageBody":"`+strings.Repeat("x", 1<<20+1)+`"}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+url+`}`),
	}}
}

// snsEdgeCasesScenario records SNS behavior that docs/sqs-sns-compatibility.md
// listed as unverified.
func snsEdgeCasesScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	var tags strings.Builder
	for i := 1; i <= 51; i++ {
		fmt.Fprintf(&tags, "&Tags.member.%d.Key=k%d&Tags.member.%d.Value=v", i, i, i)
	}
	const member = "&PublishBatchRequestEntries.member."
	entry := func(n int, id, message string) string {
		return fmt.Sprintf("%s%d.Id=%s%s%d.Message=%s", member, n, id, member, n, url.QueryEscape(message))
	}
	big := strings.Repeat("b", 200<<10)
	attrs := "&Attributes.entry.1.key=KmsMasterKeyId&Attributes.entry.1.value=alias%2Faws%2Fsns" +
		"&Attributes.entry.2.key=SignatureVersion&Attributes.entry.2.value=2" +
		"&Attributes.entry.3.key=TracingConfig&Attributes.entry.3.value=Active"
	return scenario{name: "sns-edge-cases", topics: []string{"{name}"}, queues: []string{"{name}", "{name}.fifo"}, steps: []step{
		snsStep("create-topic-with-attributes", "Action=CreateTopic&Name={name}"+attrs+snsVersion),
		snsStep("get-topic-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("tag-too-many", "Action=TagResource&ResourceArn={topicArn}"+tags.String()+snsVersion),
		snsStep("tag-missing-topic", "Action=TagResource&ResourceArn={topicArn}-missing&Tags.member.1.Key=k&Tags.member.1.Value=v"+snsVersion),
		snsStep("list-tags-missing-topic", "Action=ListTagsForResource&ResourceArn={topicArn}-missing"+snsVersion),
		snsStep("publish-dedup-id-standard", "Action=Publish"+topic+"&Message=x&MessageDeduplicationId=d"+snsVersion),
		snsStep("publish-bad-attribute-type", "Action=Publish"+topic+"&Message=x&MessageAttributes.entry.1.Name=a&MessageAttributes.entry.1.Value.DataType=Bogus&MessageAttributes.entry.1.Value.StringValue=v"+snsVersion),
		snsStep("publish-batch-all-fail", "Action=PublishBatch"+topic+entry(1, "a", "")+entry(2, "b", "")+snsVersion),
		snsStep("publish-batch-too-long", "Action=PublishBatch"+topic+entry(1, "a", big)+entry(2, "b", big)+snsVersion),
		snsStep("unsubscribe-missing", "Action=Unsubscribe&SubscriptionArn={topicArn}:00000000-0000-0000-0000-000000000000"+snsVersion),
		sqsStep("create-fifo-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("get-fifo-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		snsStep("subscribe-fifo-queue", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}"+snsVersion),
		sqsStep("delete-fifo-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		sqsStep("set-queue-policy", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe-raw", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"+snsVersion),
		snsStep("publish-group-id", "Action=Publish"+topic+"&Message=grouped&MessageGroupId=g1"+snsVersion),
		sqsStep("receive-group-id", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":10,"MaxNumberOfMessages":10,"MessageSystemAttributeNames":["MessageGroupId"]}`),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}
