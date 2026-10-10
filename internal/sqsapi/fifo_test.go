package sqsapi

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
)

const (
	invalidParam = "com.amazon.coral.service#InvalidParameterValueException"
	missingParam = "com.amazon.coral.service#MissingRequiredParameterException"
)

// createFifo creates a FIFO queue with content-based deduplication off.
func (e *env) createFifo(name string, extra string) string {
	e.t.Helper()
	var out struct {
		QueueURL string `json:"QueueUrl"`
	}
	e.ok("CreateQueue", fmt.Sprintf(`{"QueueName":%q,"Attributes":{"FifoQueue":"true"%s}}`, name, extra), &out)
	return out.QueueURL
}

type sentBody struct {
	MessageID      string `json:"MessageId"`
	SequenceNumber string
}

func TestFifoSendReceive(t *testing.T) {
	e := newEnv(t)
	u := e.createFifo("orders.fifo", "")

	var first, dup, second sentBody
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"a1","MessageGroupId":"a","MessageDeduplicationId":"1"}`, u), &first)
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"a1","MessageGroupId":"a","MessageDeduplicationId":"1"}`, u), &dup)
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"a2","MessageGroupId":"a","MessageDeduplicationId":"2"}`, u), &second)
	if first.SequenceNumber == "" || dup != first || second.MessageID == first.MessageID || second.SequenceNumber <= first.SequenceNumber {
		t.Errorf("sends = %+v, %+v, %+v; want a sequence number, the same result for a duplicate, and a later number for the next message", first, dup, second)
	}

	var got receivedBody
	recv := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"MessageSystemAttributeNames":["All"]}`, u)
	e.ok("ReceiveMessage", recv, &got)
	if len(got.Messages) != 1 || got.Messages[0].Body != "a1" {
		t.Fatalf("ReceiveMessage = %+v, want a1", got)
	}
	attrs := got.Messages[0].Attributes
	if attrs["SequenceNumber"] != first.SequenceNumber || attrs["MessageGroupId"] != "a" || attrs["MessageDeduplicationId"] != "1" {
		t.Errorf("system attributes = %v, want SequenceNumber %s, MessageGroupId a, MessageDeduplicationId 1", attrs, first.SequenceNumber)
	}
	if _, ok := attrs["DeadLetterQueueSourceArn"]; ok {
		t.Errorf("system attributes = %v, want no DeadLetterQueueSourceArn", attrs)
	}
	var locked receivedBody
	e.ok("ReceiveMessage", recv, &locked)
	if len(locked.Messages) != 0 {
		t.Errorf("ReceiveMessage while a1 is in flight = %+v, want none", locked)
	}
	e.okEmpty("DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, u, got.Messages[0].ReceiptHandle))
	var next receivedBody
	e.ok("ReceiveMessage", recv, &next)
	if len(next.Messages) != 1 || next.Messages[0].Body != "a2" {
		t.Errorf("ReceiveMessage after delete = %+v, want a2", next)
	}

	// Named requests return only the named attributes.
	e.createFifo("named.fifo", "")
	nu := strings.Replace(u, "orders.fifo", "named.fifo", 1)
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x","MessageGroupId":"g","MessageDeduplicationId":"d"}`, nu), nil)
	var named receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["MessageGroupId"]}`, nu), &named)
	if len(named.Messages) != 1 || len(named.Messages[0].Attributes) != 1 || named.Messages[0].Attributes["MessageGroupId"] != "g" {
		t.Errorf("named ReceiveMessage = %+v, want only MessageGroupId", named)
	}
}

func TestFifoErrors(t *testing.T) {
	e := newEnv(t)
	u := e.createFifo("orders.fifo", "")
	send := func(fields string) string {
		return fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x"%s}`, u, fields)
	}
	e.fail("SendMessage", send(`,"MessageDeduplicationId":"d"`), 400, missingParam, "MissingParameter")
	e.fail("SendMessage", send(`,"MessageGroupId":"g"`), 400, invalidParam, "InvalidParameterValue")
	e.fail("SendMessage", send(`,"MessageGroupId":"g","MessageDeduplicationId":"d","DelaySeconds":5`), 400, invalidParam, "InvalidParameterValue")
	e.fail("SendMessage", send(`,"MessageGroupId":"a b","MessageDeduplicationId":"d"`), 400, invalidParam, "InvalidParameterValue")
	e.fail("CreateQueue", `{"QueueName":"q.fifo"}`, 400, invalidParam, "InvalidParameterValue")
	e.fail("CreateQueue", `{"QueueName":"q","Attributes":{"FifoQueue":"true"}}`, 400, invalidParam, "InvalidParameterValue")
	e.fail("CreateQueue", `{"QueueName":"q","Attributes":{"ContentBasedDeduplication":"true"}}`, 400, "com.amazonaws.sqs#InvalidAttributeName", "InvalidAttributeName")
	e.fail("SetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"Attributes":{"FifoQueue":"true"}}`, u), 400, "com.amazonaws.sqs#InvalidAttributeValue", "InvalidAttributeValue")

	var out batchBody
	e.ok("SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"ok","MessageBody":"x","MessageGroupId":"g","MessageDeduplicationId":"1"},{"Id":"nogroup","MessageBody":"x","MessageDeduplicationId":"2"},{"Id":"nodedup","MessageBody":"x","MessageGroupId":"g"}]}`, u), &out)
	if len(out.Successful) != 1 || out.Successful[0].ID != "ok" || len(out.Failed) != 2 ||
		out.Failed[0].ID != "nogroup" || out.Failed[0].Code != "MissingParameter" || out.Failed[1].ID != "nodedup" || out.Failed[1].Code != "InvalidParameterValue" {
		t.Errorf("SendMessageBatch = %+v, %+v; want ok sent, nogroup MissingParameter, nodedup InvalidParameterValue", out.Successful, out.Failed)
	}
}

func TestFifoBatchSequenceNumbers(t *testing.T) {
	e := newEnv(t)
	u := e.createFifo("orders.fifo", `,"ContentBasedDeduplication":"true"`)
	var out struct {
		Successful []struct {
			ID             string `json:"Id"`
			SequenceNumber string
		}
	}
	e.ok("SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"a","MessageBody":"x","MessageGroupId":"g"},{"Id":"b","MessageBody":"y","MessageGroupId":"g"}]}`, u), &out)
	if len(out.Successful) != 2 || out.Successful[0].SequenceNumber == "" || out.Successful[1].SequenceNumber <= out.Successful[0].SequenceNumber {
		t.Errorf("Successful = %+v, want increasing sequence numbers", out.Successful)
	}
}

func TestDeadLetterQueue(t *testing.T) {
	e := newEnv(t)
	dlq := e.createQueue("dlq")
	var attrs struct{ Attributes map[string]string }
	e.ok("GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["QueueArn"]}`, dlq), &attrs)
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "1"})
	req, _ := json.Marshal(map[string]any{"QueueName": "src", "Attributes": map[string]string{"RedrivePolicy": string(policy)}})
	var created struct {
		QueueURL string `json:"QueueUrl"`
	}
	e.ok("CreateQueue", string(req), &created)
	src := created.QueueURL

	e.ok("GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["RedrivePolicy"]}`, src), &attrs)
	if want := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":1}`; attrs.Attributes["RedrivePolicy"] != want {
		t.Errorf("RedrivePolicy = %q, want %q", attrs.Attributes["RedrivePolicy"], want)
	}
	badArn := `{"QueueName":"bad","Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:none\",\"maxReceiveCount\":1}"}}`
	e.fail("CreateQueue", badArn, 400, invalidParam, "InvalidParameterValue")
	badCount := `{"QueueName":"bad","Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:dlq\",\"maxReceiveCount\":0}"}}`
	e.fail("CreateQueue", badCount, 400, "com.amazonaws.sqs#InvalidAttributeValue", "InvalidAttributeValue")

	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"poison"}`, src), nil)
	var got receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"VisibilityTimeout":0}`, src), &got)
	if len(got.Messages) != 1 {
		t.Fatalf("first ReceiveMessage = %+v, want 1 message", got)
	}
	var moved, dead receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, src), &moved)
	if len(moved.Messages) != 0 {
		t.Errorf("second ReceiveMessage = %+v, want none: the message moved", moved)
	}
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageSystemAttributeNames":["All"]}`, dlq), &dead)
	if len(dead.Messages) != 1 || dead.Messages[0].Body != "poison" ||
		dead.Messages[0].Attributes["DeadLetterQueueSourceArn"] != "arn:aws:sqs:us-east-1:000000000000:src" || dead.Messages[0].Attributes["ApproximateReceiveCount"] != "2" {
		t.Errorf("DLQ ReceiveMessage = %+v, want poison with DeadLetterQueueSourceArn", dead)
	}
}

func TestListDeadLetterSourceQueues(t *testing.T) {
	e := newEnv(t)
	dlq := e.createQueue("dlq")
	policy := `{"QueueName":%q,"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:dlq\",\"maxReceiveCount\":1}"}}`
	for _, n := range []string{"b", "a", "c"} {
		e.ok("CreateQueue", fmt.Sprintf(policy, n), nil)
	}
	type list struct {
		QueueURLs []string `json:"queueUrls"`
		NextToken string
	}
	var page list
	e.ok("ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q}`, dlq), &page)
	want := []string{"http://" + testHost + "/000000000000/a", "http://" + testHost + "/000000000000/b", "http://" + testHost + "/000000000000/c"}
	if !slices.Equal(page.QueueURLs, want) || page.NextToken != "" {
		t.Errorf("ListDeadLetterSourceQueues = %+v, want %v and no token", page, want)
	}
	e.ok("ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q,"MaxResults":2}`, dlq), &page)
	if !slices.Equal(page.QueueURLs, want[:2]) || page.NextToken == "" {
		t.Fatalf("first page = %+v, want %v and a token", page, want[:2])
	}
	var rest list
	e.ok("ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q,"MaxResults":2,"NextToken":%q}`, dlq, page.NextToken), &rest)
	if !slices.Equal(rest.QueueURLs, want[2:]) || rest.NextToken != "" {
		t.Errorf("second page = %+v, want %v and no token", rest, want[2:])
	}
	w := e.send(testHost, testSecret, "ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q}`, e.createQueue("lonely")))
	if w.Body.String() != `{"queueUrls":[]}` {
		t.Errorf("empty ListDeadLetterSourceQueues = %s, want {\"queueUrls\":[]}", w.Body)
	}
	e.fail("ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q,"MaxResults":0}`, dlq), 400, invalidParam, "InvalidParameterValue")
	e.fail("ListDeadLetterSourceQueues", fmt.Sprintf(`{"QueueUrl":%q,"NextToken":"!"}`, dlq), 400, invalidParam, "InvalidParameterValue")
	e.fail("ListDeadLetterSourceQueues", `{"QueueUrl":"http://x/000000000000/gone"}`, 400, "com.amazonaws.sqs#QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue")
	e.fail("ListDeadLetterSourceQueues", `{}`, 400, missingParam, "MissingParameter")
}

func TestStandardQueueReturnsGroupID(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x","MessageGroupId":"tenant"}`, u), nil)
	var got receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageSystemAttributeNames":["All"]}`, u), &got)
	if len(got.Messages) != 1 || got.Messages[0].Attributes["MessageGroupId"] != "tenant" || got.Messages[0].Attributes["SequenceNumber"] != "" {
		t.Errorf("ReceiveMessage = %+v, want MessageGroupId tenant and no SequenceNumber", got)
	}
}
