package diff

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
)

func snsRedrivePolicyScenario() scenario {
	const sub = "&SubscriptionArn={subscriptionArn}"
	const policy = `{"deadLetterTargetArn":"{queueArn}"}`
	set := func(name, value string) step {
		return snsStep(name, "Action=SetSubscriptionAttributes"+sub+"&AttributeName=RedrivePolicy&AttributeValue="+value+snsVersion)
	}
	get := func(name string) step { return snsStep(name, "Action=GetSubscriptionAttributes"+sub+snsVersion) }
	sc := scenario{name: "sns-redrive-policy", topics: []string{"{name}", "{name}.fifo"}, queues: []string{"{name}", "{name}.fifo"}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		snsStep("subscribe", "Action=Subscribe&TopicArn={topicArn}&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RedrivePolicy&Attributes.entry.1.value="+`{"deadLetterTargetArn":"{queueArn}-missing"}`+snsVersion),
		get("get-initial"),
	}}
	for _, tt := range []struct{ name, value string }{
		{"empty", ""}, {"object", "{}"}, {"null", "null"}, {"array", "[]"}, {"malformed", "x"},
		{"number", `{"deadLetterTargetArn":1}`}, {"unknown", `{"other":"x"}`},
		{"extra", `{"deadLetterTargetArn":"{queueArn}","other":true}`},
		{"null-target", `{"deadLetterTargetArn":null}`}, {"empty-target", `{"deadLetterTargetArn":""}`},
		{"bad-arn", `{"deadLetterTargetArn":"bad"}`}, {"wrong-service", `{"deadLetterTargetArn":"{topicArn}"}`},
		{"wrong-account", `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:111111111111:queue"}`},
		{"wrong-region", `{"deadLetterTargetArn":"arn:aws:sqs:us-west-2:000000000000:queue"}`},
	} {
		sc.steps = append(sc.steps, set("invalid-"+tt.name, tt.value))
	}
	sc.steps = append(sc.steps,
		set("set-policy", policy), get("get-policy"),
		set("set-missing-target", `{"deadLetterTargetArn":"{queueArn}-missing"}`), get("get-missing-target"),
		set("set-fifo-on-standard", `{"deadLetterTargetArn":"{queueArn}.fifo"}`),
		snsStep("clear-policy", "Action=SetSubscriptionAttributes"+sub+"&AttributeName=RedrivePolicy"+snsVersion), get("get-cleared"),
		snsStep("create-fifo-topic", "Action=CreateTopic&Name={name}.fifo&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true"+snsVersion),
		snsStep("subscribe-standard-to-fifo", "Action=Subscribe&TopicArn={topicArn}&Protocol=sqs&Endpoint={queueArn}"+snsVersion),
		set("fifo-standard-endpoint-standard-dlq", policy),
		set("fifo-standard-endpoint-fifo-dlq", `{"deadLetterTargetArn":"{queueArn}.fifo"}`),
		sqsStep("create-fifo-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true"}}`),
		sqsStep("fifo-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		snsStep("subscribe-fifo-to-fifo", "Action=Subscribe&TopicArn={topicArn}&Protocol=sqs&Endpoint={queueArn}"+snsVersion),
		set("fifo-fifo-endpoint-fifo-dlq", policy),
		sqsStep("get-standard-queue", "GetQueueUrl", `{"QueueName":"{name}"}`),
		sqsStep("standard-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		set("fifo-fifo-endpoint-standard-dlq", policy),
	)
	return sc
}

func snsRedriveDeliveryScenario(fifo, raw bool) scenario {
	suffix, topicAttrs, queueAttrs, publishAttrs := "", "", "", ""
	name := "sns-redrive-delivery"
	if fifo {
		suffix = ".fifo"
		name += "-fifo"
		topicAttrs = "&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true"
		queueAttrs = `,"Attributes":{"FifoQueue":"true"}`
		publishAttrs = "&MessageGroupId=group&MessageDeduplicationId=dedup"
	}
	rawAttrs := ""
	if raw {
		name += "-raw"
		rawAttrs = "&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"
	}
	const queue = `"QueueUrl":"{queueUrl}"`
	return scenario{name: name, topics: []string{"{name}" + suffix}, queues: []string{"{name}-source" + suffix, "{name}-dead" + suffix}, check: func(steps []step, responses []response) error {
		return checkRedriveDelivery(steps, responses, fifo, raw)
	}, steps: []step{
		snsStep("create-topic", "Action=CreateTopic&Name={name}"+suffix+topicAttrs+snsVersion),
		sqsStep("create-source", "CreateQueue", `{"QueueName":"{name}-source`+suffix+`"`+queueAttrs+`}`),
		sqsStep("source-arn", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["QueueArn"]}`),
		snsStep("subscribe", "Action=Subscribe&TopicArn={topicArn}&Protocol=sqs&Endpoint={queueArn}"+rawAttrs+snsVersion),
		sqsStep("create-dead", "CreateQueue", `{"QueueName":"{name}-dead`+suffix+`"`+queueAttrs+`}`),
		sqsStep("dead-arn", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["QueueArn"]}`),
		sqsStep("allow-topic", "SetQueueAttributes", snsQueuePolicy),
		snsStep("set-redrive", "Action=SetSubscriptionAttributes&SubscriptionArn={subscriptionArn}&AttributeName=RedrivePolicy&AttributeValue={"+`"deadLetterTargetArn":"{queueArn}"}`+snsVersion),
		sqsStep("get-source", "GetQueueUrl", `{"QueueName":"{name}-source`+suffix+`"}`),
		sqsStep("delete-source", "DeleteQueue", `{`+queue+`}`),
		snsStep("publish", "Action=Publish&TopicArn={topicArn}&Message=dead-letter-body&Subject=dead-letter-subject"+publishAttrs+
			"&MessageAttributes.entry.1.Name=color&MessageAttributes.entry.1.Value.DataType=String&MessageAttributes.entry.1.Value.StringValue=blue"+
			"&MessageAttributes.entry.2.Name=number&MessageAttributes.entry.2.Value.DataType=Number&MessageAttributes.entry.2.Value.StringValue=3.5"+
			"&MessageAttributes.entry.3.Name=binary&MessageAttributes.entry.3.Value.DataType=Binary&MessageAttributes.entry.3.Value.BinaryValue="+url.QueryEscape("YWJj")+snsVersion),
		sqsStep("get-dead", "GetQueueUrl", `{"QueueName":"{name}-dead`+suffix+`"}`),
		sqsStep("receive-dead", "ReceiveMessage", `{`+queue+`,"WaitTimeSeconds":20,"MessageAttributeNames":["All"],"MessageSystemAttributeNames":["All"]}`),
		sqsStep("delete-message", "DeleteMessage", `{`+queue+`,"ReceiptHandle":"{receiptHandle}"}`),
	}}
}

func checkRedriveDelivery(steps []step, responses []response, fifo, raw bool) error {
	var pub struct {
		Result struct {
			MessageID string `xml:"MessageId"`
			Sequence  string `xml:"SequenceNumber"`
		} `xml:"PublishResult"`
	}
	for i, st := range steps {
		if st.name == "publish" {
			if err := xml.Unmarshal(responses[i].body, &pub); err != nil {
				return err
			}
		}
		if st.name != "receive-dead" {
			continue
		}
		var out struct {
			Messages []struct {
				Body              string
				Attributes        map[string]string
				MessageAttributes map[string]struct{ DataType, StringValue, BinaryValue string }
			}
		}
		if responses[i].status != http.StatusOK {
			return fmt.Errorf("receive-dead status = %d, want 200", responses[i].status)
		}
		if err := json.Unmarshal(responses[i].body, &out); err != nil {
			return err
		}
		if len(out.Messages) != 1 {
			return fmt.Errorf("dead-letter messages = %d, want 1", len(out.Messages))
		}
		m := out.Messages[0]
		if fifo && (m.Attributes["MessageGroupId"] != "group" || m.Attributes["MessageDeduplicationId"] != "dedup") {
			return fmt.Errorf("dead-letter FIFO identifiers changed")
		}
		if raw {
			if m.Body != "dead-letter-body" || m.MessageAttributes["color"].StringValue != "blue" || m.MessageAttributes["number"].StringValue != "3.5" || m.MessageAttributes["binary"].BinaryValue != "YWJj" {
				return fmt.Errorf("raw dead-letter body or attributes changed")
			}
			continue
		}
		var env struct {
			MessageID                        string `json:"MessageId"`
			SequenceNumber, Message, Subject string
		}
		if err := json.Unmarshal([]byte(m.Body), &env); err != nil {
			return err
		}
		if pub.Result.MessageID == "" || env.MessageID != pub.Result.MessageID || env.Message != "dead-letter-body" || env.Subject != "dead-letter-subject" || (fifo && env.SequenceNumber != pub.Result.Sequence) {
			return fmt.Errorf("dead-letter envelope changed publication identity or content")
		}
	}
	return nil
}
