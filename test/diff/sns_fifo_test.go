package diff

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
)

func snsFIFOAttributesScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	const fifo = "&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true"
	set := func(name, value string) string {
		return "Action=SetTopicAttributes" + topic + "&AttributeName=" + name + "&AttributeValue=" + value + snsVersion
	}
	return scenario{name: "sns-fifo-attributes", topics: []string{"{name}.fifo", "{name}-invalid", "{name}-missing.fifo"}, steps: []step{
		snsStep("create-missing-fifo-attribute", "Action=CreateTopic&Name={name}-missing.fifo"+snsVersion),
		snsStep("create-fifo-on-standard-name", "Action=CreateTopic&Name={name}-invalid"+fifo+snsVersion),
		snsStep("create-topic", "Action=CreateTopic&Name={name}.fifo"+fifo+snsVersion),
		snsStep("get-default-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("create-again", "Action=CreateTopic&Name={name}.fifo"+fifo+snsVersion),
		snsStep("create-conflicting-dedup", "Action=CreateTopic&Name={name}.fifo"+fifo+"&Attributes.entry.2.key=ContentBasedDeduplication&Attributes.entry.2.value=true"+snsVersion),
		snsStep("change-type-false", set("FifoTopic", "false")),
		snsStep("change-type-true", set("FifoTopic", "true")),
		snsStep("bad-dedup", set("ContentBasedDeduplication", "yes")),
		snsStep("empty-dedup", set("ContentBasedDeduplication", "")),
		snsStep("enable-dedup", set("ContentBasedDeduplication", "true")),
		snsStep("bad-scope", set("FifoThroughputScope", "messageGroup")),
		snsStep("enable-group-scope", set("FifoThroughputScope", "MessageGroup")),
		snsStep("revert-scope", set("FifoThroughputScope", "Topic")),
		snsStep("set-tracing", set("TracingConfig", "Active")),
		snsStep("get-updated-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("subscribe-https", "Action=Subscribe"+topic+"&Protocol=https&Endpoint={httpEndpoint}"+snsVersion),
		snsStep("publish-missing-group", "Action=Publish"+topic+"&Message=body"+snsVersion),
		snsStep("publish-bad-group", "Action=Publish"+topic+"&Message=body&MessageGroupId=bad+group"+snsVersion),
		snsStep("publish-bad-dedup", "Action=Publish"+topic+"&Message=body&MessageGroupId=g&MessageDeduplicationId=bad+id"+snsVersion),
		snsStep("disable-dedup", set("ContentBasedDeduplication", "false")),
		snsStep("publish-missing-dedup", "Action=Publish"+topic+"&Message=body&MessageGroupId=g"+snsVersion),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
	}}
}

func snsFIFODeliveryScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	const queue = `"QueueUrl":"{queueUrl}"`
	publish := func(message, group, dedup string) string {
		return "Action=Publish" + topic + "&Message=" + url.QueryEscape(message) + "&MessageGroupId=" + group + "&MessageDeduplicationId=" + dedup + snsVersion
	}
	receive := func(name string) step {
		return sqsStep(name, "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":10,"MaxNumberOfMessages":1,"MessageSystemAttributeNames":["All"],"MessageAttributeNames":["All"]}`)
	}
	remove := func(name string) step {
		return sqsStep(name, "DeleteMessage", `{`+queue+`,"ReceiptHandle":"{receiptHandle}"}`)
	}
	const batch = "Action=PublishBatch" + topic + "&PublishBatchRequestEntries.member."
	return scenario{name: "sns-fifo-delivery", check: checkFIFODelivery, topics: []string{"{name}.fifo"}, queues: []string{"{name}.fifo", "{name}-raw.fifo"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}.fifo&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true"+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("allow-topic", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}"+snsVersion),
		snsStep("publish-first", publish("one", "g", "one")),
		snsStep("publish-duplicate", publish("ignored", "g", "one")),
		snsStep("publish-second", publish("two", "g", "two")),
		receive("receive-first"),
		sqsStep("receive-blocked-group", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":1,"MaxNumberOfMessages":1}`),
		remove("delete-first"),
		receive("receive-second"),
		remove("delete-second"),
		sqsStep("create-raw-queue", "CreateQueue", `{"QueueName":"{name}-raw.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("get-raw-queue-arn", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("allow-raw-topic", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe-raw", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"+snsVersion),
		snsStep("publish-raw", "Action=Publish"+topic+"&Message=raw&MessageGroupId=g&MessageDeduplicationId=raw&MessageAttributes.entry.1.Name=color&MessageAttributes.entry.1.Value.DataType=String&MessageAttributes.entry.1.Value.StringValue=blue"+snsVersion),
		receive("receive-raw"),
		remove("delete-raw"),
		snsStep("publish-batch-missing-group", batch+"1.Id=a&PublishBatchRequestEntries.member.1.Message=batch-one&PublishBatchRequestEntries.member.1.MessageGroupId=g&PublishBatchRequestEntries.member.1.MessageDeduplicationId=batch-one"+
			"&PublishBatchRequestEntries.member.2.Id=bad&PublishBatchRequestEntries.member.2.Message=missing-group&PublishBatchRequestEntries.member.2.MessageDeduplicationId=bad"+
			"&PublishBatchRequestEntries.member.3.Id=b&PublishBatchRequestEntries.member.3.Message=batch-two&PublishBatchRequestEntries.member.3.MessageGroupId=g&PublishBatchRequestEntries.member.3.MessageDeduplicationId=batch-two"+snsVersion),
		sqsStep("receive-after-invalid-batch", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":1,"MaxNumberOfMessages":1}`),
		snsStep("publish-batch-empty-message", batch+"1.Id=a&PublishBatchRequestEntries.member.1.Message=batch-one&PublishBatchRequestEntries.member.1.MessageGroupId=g&PublishBatchRequestEntries.member.1.MessageDeduplicationId=batch-one"+
			"&PublishBatchRequestEntries.member.2.Id=bad&PublishBatchRequestEntries.member.2.Message=&PublishBatchRequestEntries.member.2.MessageGroupId=g&PublishBatchRequestEntries.member.2.MessageDeduplicationId=bad"+
			"&PublishBatchRequestEntries.member.3.Id=b&PublishBatchRequestEntries.member.3.Message=batch-two&PublishBatchRequestEntries.member.3.MessageGroupId=g&PublishBatchRequestEntries.member.3.MessageDeduplicationId=batch-two"+snsVersion),
		snsStep("publish-batch-missing-dedup", batch+"1.Id=a&PublishBatchRequestEntries.member.1.Message=body&PublishBatchRequestEntries.member.1.MessageGroupId=g"+snsVersion),
		snsStep("publish-batch-invalid-dedup", batch+"1.Id=a&PublishBatchRequestEntries.member.1.Message=body&PublishBatchRequestEntries.member.1.MessageGroupId=g&PublishBatchRequestEntries.member.1.MessageDeduplicationId=bad+id"+snsVersion),
		sqsStep("receive-after-empty-batch", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":1,"MaxNumberOfMessages":1}`),
		snsStep("publish-batch", batch+"1.Id=a&PublishBatchRequestEntries.member.1.Message=batch-one&PublishBatchRequestEntries.member.1.MessageGroupId=g&PublishBatchRequestEntries.member.1.MessageDeduplicationId=batch-one"+
			"&PublishBatchRequestEntries.member.2.Id=b&PublishBatchRequestEntries.member.2.Message=batch-two&PublishBatchRequestEntries.member.2.MessageGroupId=g&PublishBatchRequestEntries.member.2.MessageDeduplicationId=batch-two"+snsVersion),
		receive("receive-batch-first"),
		remove("delete-batch-first"),
		receive("receive-batch-second"),
		remove("delete-batch-second"),
		sqsStep("receive-empty", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":1,"MaxNumberOfMessages":1}`),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-raw-queue", "DeleteQueue", `{`+queue+`}`),
		sqsStep("get-envelope-queue", "GetQueueUrl", `{"QueueName":"{name}.fifo"}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+queue+`}`),
	}}
}

// A standard queue exposes SNS deduplication without SQS FIFO deduplication masking it.
func snsFIFODedupScenario(scope string) scenario {
	const topic = "&TopicArn={topicArn}"
	const queue = `"QueueUrl":"{queueUrl}"`
	publish := func(name, message, group, dedup string) step {
		body := "Action=Publish" + topic + "&Message=" + url.QueryEscape(message) + "&MessageGroupId=" + group
		if dedup != "" {
			body += "&MessageDeduplicationId=" + dedup
		}
		return snsStep(name, body+snsVersion)
	}
	receive := func(name string) step {
		return sqsStep(name, "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":10,"MaxNumberOfMessages":1,"MessageSystemAttributeNames":["All"]}`)
	}
	remove := func(name string) step {
		return sqsStep(name, "DeleteMessage", `{`+queue+`,"ReceiptHandle":"{receiptHandle}"}`)
	}
	sc := scenario{name: "sns-fifo-dedup-" + scope, topics: []string{"{name}.fifo"}, queues: []string{"{name}"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}.fifo&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true&Attributes.entry.2.key=ContentBasedDeduplication&Attributes.entry.2.value=true&Attributes.entry.3.key=FifoThroughputScope&Attributes.entry.3.value="+scope+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("allow-topic", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"+snsVersion),
		publish("publish-first", "body", "a", ""),
		receive("receive-first"), remove("delete-first"),
		publish("publish-content-duplicate", "body", "a", ""),
		publish("publish-other-group", "body", "b", ""),
	}}
	if scope == "MessageGroup" {
		sc.steps = append(sc.steps, receive("receive-other-group"), remove("delete-other-group"))
	}
	sc.steps = append(sc.steps,
		publish("publish-override", "body", "a", "override"),
		receive("receive-override"), remove("delete-override"),
		sqsStep("receive-empty", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":1,"MaxNumberOfMessages":1}`),
		snsStep("delete-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{`+queue+`}`),
	)
	return sc
}

// checkFIFODelivery checks identifiers before normalization masks their values.
func checkFIFODelivery(steps []step, responses []response) error {
	byName := make(map[string]response, len(steps))
	for i, st := range steps {
		byName[st.name] = responses[i]
	}
	type identifiers struct {
		MessageID      string `xml:"MessageId" json:"MessageId"`
		SequenceNumber string
	}
	published := map[string]identifiers{}
	for _, name := range []string{"publish-first", "publish-duplicate", "publish-second", "publish-raw"} {
		var out struct {
			Result identifiers `xml:"PublishResult"`
		}
		r := byName[name]
		if r.status != http.StatusOK {
			return fmt.Errorf("%s status = %d, want 200", name, r.status)
		}
		if err := xml.Unmarshal(r.body, &out); err != nil {
			return fmt.Errorf("decode %s: %w", name, err)
		}
		if out.Result.MessageID == "" {
			return fmt.Errorf("%s returned no MessageId", name)
		}
		published[name] = out.Result
	}
	if published["publish-duplicate"] != published["publish-first"] {
		return fmt.Errorf("duplicate publish did not return the original MessageId and SequenceNumber")
	}
	var batch struct {
		Successful []identifiers `xml:"PublishBatchResult>Successful>member"`
		Failed     []struct{}    `xml:"PublishBatchResult>Failed>member"`
	}
	if r := byName["publish-batch"]; r.status != http.StatusOK {
		return fmt.Errorf("publish-batch status = %d, want 200", r.status)
	} else if err := xml.Unmarshal(r.body, &batch); err != nil {
		return fmt.Errorf("decode publish-batch: %w", err)
	}
	if len(batch.Successful) != 2 || len(batch.Failed) != 0 {
		return fmt.Errorf("publish-batch returned %d successes and %d failures, want 2 and 0", len(batch.Successful), len(batch.Failed))
	}
	ordered := []identifiers{published["publish-first"], published["publish-second"], published["publish-raw"]}
	ordered = append(ordered, batch.Successful...)
	previous := new(big.Int)
	seen := map[string]bool{}
	for _, out := range ordered {
		seq, ok := new(big.Int).SetString(out.SequenceNumber, 10)
		if !ok || seq.BitLen() > 128 || seq.Cmp(previous) <= 0 {
			return fmt.Errorf("publish sequence numbers are not increasing 128-bit integers")
		}
		if out.MessageID == "" || seen[out.MessageID] {
			return fmt.Errorf("unique publishes returned missing or repeated MessageId values")
		}
		previous, seen[out.MessageID] = seq, true
	}
	for _, pair := range [][2]string{{"receive-first", "publish-first"}, {"receive-second", "publish-second"}} {
		var received struct{ Messages []struct{ Body string } }
		if err := json.Unmarshal(byName[pair[0]].body, &received); err != nil {
			return fmt.Errorf("decode %s: %w", pair[0], err)
		}
		if len(received.Messages) != 1 {
			return fmt.Errorf("%s returned %d messages, want 1", pair[0], len(received.Messages))
		}
		var env identifiers
		if err := json.Unmarshal([]byte(received.Messages[0].Body), &env); err != nil {
			return fmt.Errorf("decode %s envelope: %w", pair[0], err)
		}
		if env != published[pair[1]] {
			return fmt.Errorf("%s envelope identifiers differ from the publish response", pair[0])
		}
	}
	return nil
}
