package diff

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func sqsReceiveRetriesScenario() scenario {
	const queue = `"QueueUrl":"{queueUrl}"`
	const receive = `{` + queue + `,"ReceiveRequestAttemptId":"attempt","VisibilityTimeout":30,"MaxNumberOfMessages":2,"MessageSystemAttributeNames":["All"]`
	return scenario{name: "sqs-receive-retries", queues: []string{"{name}.fifo"}, check: checkSQSReceiveRetries, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`),
		sqsStep("send-one", "SendMessage", `{`+queue+`,"MessageBody":"one","MessageGroupId":"g"}`),
		sqsStep("send-two", "SendMessage", `{`+queue+`,"MessageBody":"two","MessageGroupId":"g"}`),
		sqsStep("receive-first", "ReceiveMessage", receive+`,"WaitTimeSeconds":5}`),
		sqsStep("receive-retry", "ReceiveMessage", receive+`,"WaitTimeSeconds":5}`),
		sqsStep("receive-changed-wait", "ReceiveMessage", receive+`,"WaitTimeSeconds":0}`),
		sqsStep("receive-changed-max", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"attempt","VisibilityTimeout":30,"MaxNumberOfMessages":1}`),
		sqsStep("receive-changed-visibility", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"attempt","VisibilityTimeout":60,"MaxNumberOfMessages":2}`),
		sqsStep("receive-distinct-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"distinct","WaitTimeSeconds":0}`),
		sqsStep("delete-one", "DeleteMessage", `{`+queue+`,"ReceiptHandle":"{receiptHandle}"}`),
		sqsStep("receive-after-delete", "ReceiveMessage", receive+`}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+queue+`}`),
	}}
}

func sqsReceiveRetryEmptyScenario() scenario {
	const queue = `"QueueUrl":"{queueUrl}"`
	const receive = `{` + queue + `,"ReceiveRequestAttemptId":"empty","WaitTimeSeconds":0}`
	return scenario{name: "sqs-receive-retry-empty", queues: []string{"{name}.fifo"}, steps: []step{
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`),
		sqsStep("receive-empty", "ReceiveMessage", receive),
		sqsStep("send-one", "SendMessage", `{`+queue+`,"MessageBody":"one","MessageGroupId":"g"}`),
		sqsStep("receive-empty-retry", "ReceiveMessage", receive),
		sqsStep("receive-distinct-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"new","WaitTimeSeconds":5,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("receive-explicit-defaults", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"new","MaxNumberOfMessages":1,"VisibilityTimeout":30,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("receive-in-flight", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"in-flight","WaitTimeSeconds":1}`),
		sqsStep("change-visibility", "ChangeMessageVisibility", `{`+queue+`,"ReceiptHandle":"{receiptHandle}","VisibilityTimeout":0}`),
		sqsStep("receive-after-change", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"new","WaitTimeSeconds":0}`),
		sqsStep("receive-after-change-new-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"after-change","WaitTimeSeconds":5,"MessageSystemAttributeNames":["All"]}`),
		sqsStep("purge", "PurgeQueue", `{`+queue+`}`),
		sqsStep("receive-after-purge", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"after-change","WaitTimeSeconds":0}`),
		sqsStep("receive-empty-zero-visibility", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"zero","VisibilityTimeout":0,"WaitTimeSeconds":0}`),
		sqsStep("receive-empty-zero-visibility-retry", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"zero","VisibilityTimeout":0,"WaitTimeSeconds":0}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+queue+`}`),
	}}
}

func sqsReceiveRetryValidationScenario() scenario {
	const queue = `"QueueUrl":"{queueUrl}"`
	steps := []step{
		sqsStep("create-standard", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("standard-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"attempt"}`),
		sqsStep("standard-invalid-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":"not valid"}`),
		sqsStep("standard-empty-token", "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":""}`),
		sqsStep("create-fifo", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true"}}`),
	}
	for _, tt := range []struct{ name, id string }{
		{"empty", ""}, {"space", "not valid"}, {"unicode", "é"},
		{"too-long", strings.Repeat("a", 129)}, {"max-length", strings.Repeat("a", 128)},
		{"punctuation", "!~"},
	} {
		id, _ := json.Marshal(tt.id)
		steps = append(steps, sqsStep("fifo-token-"+tt.name, "ReceiveMessage", `{`+queue+`,"ReceiveRequestAttemptId":`+string(id)+`}`))
	}
	return scenario{name: "sqs-receive-retry-validation", queues: []string{"{name}", "{name}.fifo"}, steps: steps}
}

func checkSQSReceiveRetries(steps []step, responses []response) error {
	type message struct {
		MessageID  string `json:"MessageId"`
		Body       string
		Attributes map[string]string
	}
	var first []message
	for i, st := range steps {
		if st.name != "receive-first" && st.name != "receive-retry" && st.name != "receive-changed-wait" {
			continue
		}
		var out struct{ Messages []message }
		if responses[i].status != http.StatusOK {
			return fmt.Errorf("%s status = %d, want 200", st.name, responses[i].status)
		}
		if err := json.Unmarshal(responses[i].body, &out); err != nil {
			return fmt.Errorf("decode %s: %w", st.name, err)
		}
		if len(out.Messages) != 2 {
			return fmt.Errorf("%s returned %d messages, want 2", st.name, len(out.Messages))
		}
		if first == nil {
			first = out.Messages
		}
		for j, m := range out.Messages {
			if m.MessageID == "" || m.MessageID != first[j].MessageID || m.Body != first[j].Body || m.Attributes["ApproximateReceiveCount"] != "1" {
				return fmt.Errorf("%s message %d changed identity, body, or receive count", st.name, j)
			}
		}
	}
	return nil
}
