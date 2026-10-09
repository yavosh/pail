package sqsapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	testKey    = "AKIAPAILTEST00000000"
	testSecret = "pail-test-secret"
	testHost   = "pail.test:9000"
	jsonType   = "application/x-amz-json-1.0"
)

// env is a handler over a real engine. Its logs hold the handler's access lines.
type env struct {
	t    *testing.T
	h    http.Handler
	logs *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	engine, err := queue.Open(t.Context(), fsys, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &env{t, New(Options{AccessKeyID: testKey, SecretAccessKey: testSecret, Queues: engine}), logs}
}

// send posts a request signed with secret; an empty secret leaves it unsigned.
func (e *env) send(host, secret, op, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(body))
	r.Host = host
	r.Header.Set("Content-Type", jsonType)
	r.Header.Set("X-Amz-Target", "AmazonSQS."+op)
	if secret != "" {
		sum := sha256.Sum256([]byte(body))
		creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: secret}
		if err := v4.NewSigner().SignHTTP(r.Context(), creds, r, hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
			e.t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	if got := w.Header().Get("Content-Type"); got != jsonType {
		e.t.Errorf("%s Content-Type = %q, want %q", op, got, jsonType)
	}
	if w.Header().Get("x-amzn-RequestId") == "" {
		e.t.Errorf("%s x-amzn-RequestId is empty", op)
	}
	return w
}

// ok calls op, requires 200, and decodes the body into out when out is set.
// Use okEmpty for operations whose body must be empty.
func (e *env) ok(op, body string, out any) {
	e.t.Helper()
	w := e.send(testHost, testSecret, op, body)
	if w.Code != http.StatusOK {
		e.t.Fatalf("%s %s: status %d, body %s, want 200", op, body, w.Code, w.Body)
	}
	if out == nil {
		return
	}
	if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
		e.t.Fatalf("%s %s: body %q: %v", op, body, w.Body, err)
	}
}

// okEmpty calls op and requires 200 with an empty body.
func (e *env) okEmpty(op, body string) {
	e.t.Helper()
	if w := e.send(testHost, testSecret, op, body); w.Code != http.StatusOK || w.Body.Len() != 0 {
		e.t.Fatalf("%s %s: status %d, body %q, want 200 and an empty body", op, body, w.Code, w.Body)
	}
}

// fail calls op and requires the given status, __type, and query error code.
func (e *env) fail(op, body string, status int, typ, query string) {
	e.t.Helper()
	w := e.send(testHost, testSecret, op, body)
	var got struct {
		Type string `json:"__type"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != status || got.Type != typ || w.Header().Get("x-amzn-query-error") != query+";Sender" {
		e.t.Errorf("%s %s: status %d, __type %q, query error %q; want %d, %q, %q; body %s",
			op, body, w.Code, got.Type, w.Header().Get("x-amzn-query-error"), status, typ, query+";Sender", w.Body)
	}
}

// createQueue creates a queue and returns its URL.
func (e *env) createQueue(name string) string {
	e.t.Helper()
	var out struct {
		QueueURL string `json:"QueueUrl"`
	}
	e.ok("CreateQueue", fmt.Sprintf(`{"QueueName":%q}`, name), &out)
	return out.QueueURL
}

func TestAuth(t *testing.T) {
	tests := []struct {
		name      string
		sign      string // secret to sign with; "" leaves the request unsigned
		body      string
		wantCode  int
		wantType  string
		wantQuery string
	}{
		{"signed", testSecret, `{}`, 200, "", ""},
		{"unsigned", "", `{}`, 403, "com.amazon.coral.service#AccessDeniedException", "AccessDenied;Sender"},
		{"wrong secret", "wrong", `{}`, 403, "com.amazon.coral.service#InvalidSignatureException", "SignatureDoesNotMatch;Sender"},
		{"over the cap", testSecret, strings.Repeat("x", maxRequestBytes+1), 413, "com.amazon.coral.service#RequestEntityTooLargeException", "RequestEntityTooLarge;Sender"},
	}
	e := newEnv(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := e.send(testHost, tt.sign, "ListQueues", tt.body)
			var body struct {
				Type string `json:"__type"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &body)
			if w.Code != tt.wantCode || body.Type != tt.wantType || w.Header().Get("x-amzn-query-error") != tt.wantQuery {
				t.Errorf("status %d, __type %q, query error %q; want %d, %q, %q", w.Code, body.Type, w.Header().Get("x-amzn-query-error"), tt.wantCode, tt.wantType, tt.wantQuery)
			}
		})
	}
}

func TestUnsupportedAndMalformed(t *testing.T) {
	e := newEnv(t)
	const unsupported = "com.amazonaws.sqs#UnsupportedOperation"
	e.fail("Bogus", `{}`, 400, "com.amazon.coral.service#UnknownOperationException", "InvalidAction")
	for _, op := range []string{"AddPermission", "RemovePermission", "StartMessageMoveTask", "CancelMessageMoveTask", "ListMessageMoveTasks", "ListDeadLetterSourceQueues"} {
		e.fail(op, `{}`, 400, unsupported, "AWS.SimpleQueueService.UnsupportedOperation")
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", jsonType)
	sum := sha256.Sum256([]byte(`{}`))
	if err := v4.NewSigner().SignHTTP(r.Context(), aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}, r, hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "UnknownOperationException") {
		t.Errorf("no target: status %d, body %s, want 400 UnknownOperationException", w.Code, w.Body)
	}
	const serial = "com.amazon.coral.service#SerializationException"
	for _, body := range []string{`{`, ``, `[]`, `{"QueueName":5}`} {
		e.fail("CreateQueue", body, 400, serial, "MalformedInput")
	}
}

func TestQueueLifecycle(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	if want := "http://" + testHost + "/000000000000/alpha"; u != want {
		t.Errorf("CreateQueue URL = %q, want %q", u, want)
	}
	e.createQueue("alpha") // same attributes: no change
	e.ok("CreateQueue", `{"QueueName":"beta","tags":{"env":"test"}}`, nil)
	e.createQueue("gamma")

	var got struct {
		QueueURL string            `json:"QueueUrl"`
		Attrs    map[string]string `json:"Attributes"`
	}
	e.ok("GetQueueUrl", `{"QueueName":"alpha"}`, &got)
	if got.QueueURL != u {
		t.Errorf("GetQueueUrl = %q, want %q", got.QueueURL, u)
	}

	var list struct {
		QueueUrls []string
		NextToken string
	}
	e.ok("ListQueues", `{"QueueNamePrefix":"al"}`, &list)
	if len(list.QueueUrls) != 1 || list.QueueUrls[0] != u {
		t.Errorf("ListQueues prefix al = %v, want [%s]", list.QueueUrls, u)
	}
	w := e.send(testHost, testSecret, "ListQueues", `{"QueueNamePrefix":"nomatch"}`)
	if w.Body.String() != `{}` {
		t.Errorf("ListQueues with no match = %s, want {}", w.Body)
	}
	var seen []string
	token := ""
	for range 5 {
		list.QueueUrls, list.NextToken = nil, ""
		e.ok("ListQueues", fmt.Sprintf(`{"MaxResults":2,"NextToken":%q}`, token), &list)
		seen = append(seen, list.QueueUrls...)
		if token = list.NextToken; token == "" {
			break
		}
	}
	if len(seen) != 3 || !strings.HasSuffix(seen[2], "/gamma") {
		t.Errorf("paged ListQueues = %v, want 3 URLs ending in gamma", seen)
	}
	const invalid = "com.amazon.coral.service#InvalidParameterValueException"
	e.fail("ListQueues", `{"MaxResults":0}`, 400, invalid, "InvalidParameterValue")
	e.fail("ListQueues", `{"MaxResults":1001}`, 400, invalid, "InvalidParameterValue")
	e.fail("ListQueues", `{"NextToken":"!!"}`, 400, invalid, "InvalidParameterValue")

	e.ok("GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["All"]}`, u), &got)
	if got.Attrs["VisibilityTimeout"] != "30" || got.Attrs["QueueArn"] != "arn:aws:sqs:us-east-1:000000000000:alpha" {
		t.Errorf("GetQueueAttributes All = %v", got.Attrs)
	}
	w = e.send(testHost, testSecret, "GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q}`, u))
	if w.Body.String() != `{}` {
		t.Errorf("GetQueueAttributes with no names = %s, want {}", w.Body)
	}
	e.okEmpty("SetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"Attributes":{"VisibilityTimeout":"60"}}`, u))
	e.ok("GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["VisibilityTimeout"]}`, u), &got)
	if got.Attrs["VisibilityTimeout"] != "60" {
		t.Errorf("VisibilityTimeout after SetQueueAttributes = %q, want 60", got.Attrs["VisibilityTimeout"])
	}

	var tags struct{ Tags map[string]string }
	body := fmt.Sprintf(`{"QueueUrl":%q}`, u)
	w = e.send(testHost, testSecret, "ListQueueTags", body)
	if w.Body.String() != `{}` {
		t.Errorf("ListQueueTags on an untagged queue = %s, want {}", w.Body)
	}
	e.okEmpty("TagQueue", fmt.Sprintf(`{"QueueUrl":%q,"Tags":{"a":"1","b":"2"}}`, u))
	e.okEmpty("UntagQueue", fmt.Sprintf(`{"QueueUrl":%q,"TagKeys":["a"]}`, u))
	e.ok("ListQueueTags", body, &tags)
	if len(tags.Tags) != 1 || tags.Tags["b"] != "2" {
		t.Errorf("ListQueueTags = %v, want map[b:2]", tags.Tags)
	}

	e.okEmpty("PurgeQueue", body)
	e.fail("PurgeQueue", body, 403, "com.amazonaws.sqs#PurgeQueueInProgress", "AWS.SimpleQueueService.PurgeQueueInProgress")
	e.fail("CreateQueue", `{"QueueName":"alpha","Attributes":{"VisibilityTimeout":"61"}}`, 400, "com.amazonaws.sqs#QueueNameExists", "QueueAlreadyExists")
	e.fail("CreateQueue", `{"QueueName":"bad name"}`, 400, invalid, "InvalidParameterValue")
	e.fail("CreateQueue", `{}`, 400, invalid, "InvalidParameterValue")
	e.fail("CreateQueue", `{"QueueName":"q","Attributes":{"Bogus":"1"}}`, 400, "com.amazonaws.sqs#InvalidAttributeName", "InvalidAttributeName")
	e.fail("CreateQueue", `{"QueueName":"q","Attributes":{"VisibilityTimeout":"x"}}`, 400, "com.amazonaws.sqs#InvalidAttributeValue", "InvalidAttributeValue")
	e.fail("CreateQueue", `{"QueueName":"q.fifo"}`, 400, "com.amazonaws.sqs#UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation")

	e.okEmpty("DeleteQueue", body)
	const missing = "com.amazonaws.sqs#QueueDoesNotExist"
	e.fail("GetQueueUrl", `{"QueueName":"alpha"}`, 400, missing, "AWS.SimpleQueueService.NonExistentQueue")
	e.fail("GetQueueUrl", `{}`, 400, invalid, "InvalidParameterValue")
	e.fail("DeleteQueue", body, 400, missing, "AWS.SimpleQueueService.NonExistentQueue")
}

func TestQueueURL(t *testing.T) {
	e := newEnv(t)
	e.createQueue("alpha")
	const missing = "com.amazonaws.sqs#QueueDoesNotExist"
	tests := []struct {
		name, url string
		ok        bool
		wantType  string
		wantQuery string
	}{
		{"same host", "http://" + testHost + "/000000000000/alpha", true, "", ""},
		{"other host", "https://other.example:1234/000000000000/alpha", true, "", ""},
		{"wrong account", "http://" + testHost + "/111111111111/alpha", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"no account", "http://" + testHost + "/alpha", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"extra segment", "http://" + testHost + "/000000000000/alpha/x", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"no name", "http://" + testHost + "/000000000000/", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"not a URL", "%zz", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"unknown queue", "http://" + testHost + "/000000000000/nope", false, missing, "AWS.SimpleQueueService.NonExistentQueue"},
		{"empty", "", false, "com.amazon.coral.service#MissingRequiredParameterException", "MissingParameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"QueueUrl":%q}`, tt.url)
			if tt.ok {
				e.ok("ListQueueTags", body, nil)
				return
			}
			e.fail("ListQueueTags", body, 400, tt.wantType, tt.wantQuery)
		})
	}
	// A queue URL reflects the host and scheme of the request that asked for it.
	w := e.send("127.0.0.1:9000", testSecret, "GetQueueUrl", `{"QueueName":"alpha"}`)
	if want := `{"QueueUrl":"http://127.0.0.1:9000/000000000000/alpha"}`; w.Body.String() != want {
		t.Errorf("GetQueueUrl via 127.0.0.1 = %s, want %s", w.Body, want)
	}
}

type receivedBody struct {
	Messages []struct {
		MessageID              string `json:"MessageId"`
		ReceiptHandle          string
		MD5OfBody              string
		Body                   string
		Attributes             map[string]string
		MD5OfMessageAttributes string
		MessageAttributes      map[string]struct {
			DataType    string
			StringValue string
			BinaryValue string
		}
	}
}

func TestSendReceiveDelete(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")

	var sent struct {
		MD5OfMessageBody, MD5OfMessageAttributes string
		MessageID                                string `json:"MessageId"`
	}
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"hello"}`, u), &sent)
	if sent.MD5OfMessageBody != queue.MD5OfBody("hello") || sent.MessageID == "" || sent.MD5OfMessageAttributes != "" {
		t.Errorf("SendMessage = %+v, want the body MD5 and an ID only", sent)
	}

	var got receivedBody
	recv := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"WaitTimeSeconds":1,"VisibilityTimeout":30}`, u)
	e.ok("ReceiveMessage", recv, &got)
	if len(got.Messages) != 1 || got.Messages[0].Body != "hello" || got.Messages[0].MessageID != sent.MessageID || got.Messages[0].Attributes != nil {
		t.Fatalf("ReceiveMessage = %+v, want the sent message without attributes", got)
	}
	handle := got.Messages[0].ReceiptHandle

	w := e.send(testHost, testSecret, "ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, u))
	if w.Body.String() != `{}` {
		t.Errorf("empty ReceiveMessage = %s, want {}", w.Body)
	}
	e.okEmpty("DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, u, handle))
	e.okEmpty("DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, u, handle)) // a deleted message's handle still succeeds
	e.fail("DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":"garbage"}`, u), 404, "com.amazonaws.sqs#ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid")
	e.fail("DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q}`, u), 400, "com.amazon.coral.service#MissingRequiredParameterException", "MissingParameter")
	e.fail("ChangeMessageVisibility", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, u, handle), 400, "com.amazon.coral.service#MissingRequiredParameterException", "MissingParameter")

	const invalid = "com.amazon.coral.service#InvalidParameterValueException"
	e.fail("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":""}`, u), 400, "com.amazon.coral.service#MissingRequiredParameterException", "MissingParameter")
	e.fail("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"\u0000"}`, u), 400, "com.amazonaws.sqs#InvalidMessageContents", "InvalidMessageContents")
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x","MessageGroupId":"g"}`, u), nil) // fair queues accept it
	e.fail("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x","MessageDeduplicationId":"d"}`, u), 400, invalid, "InvalidParameterValue")
	e.fail("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"x","MessageAttributes":{"a":{"DataType":"String","StringListValues":["b"]}}}`, u), 400, invalid, "InvalidParameterValue")
	e.fail("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":11}`, u), 400, invalid, "InvalidParameterValue")
	e.fail("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":21}`, u), 400, invalid, "InvalidParameterValue")
}

func TestSystemAttributes(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	tests := []struct {
		name, fields string
		want         []string
	}{
		{"none", ``, nil},
		{"all", `"AttributeNames":["All"]`, []string{"ApproximateFirstReceiveTimestamp", "ApproximateReceiveCount", "SenderId", "SentTimestamp"}},
		{"named", `"MessageSystemAttributeNames":["SenderId","SentTimestamp"]`, []string{"SenderId", "SentTimestamp"}},
		{"both lists", `"AttributeNames":["ApproximateReceiveCount"],"MessageSystemAttributeNames":["SenderId"]`, []string{"ApproximateReceiveCount", "SenderId"}},
		{"unknown name", `"AttributeNames":["Bogus"]`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"m"}`, u), nil)
			req := fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":1,"VisibilityTimeout":0`, u)
			if tt.fields != "" {
				req += "," + tt.fields
			}
			var got receivedBody
			e.ok("ReceiveMessage", req+"}", &got)
			if len(got.Messages) != 1 {
				t.Fatalf("got %d messages, want 1", len(got.Messages))
			}
			attrs := got.Messages[0].Attributes
			if len(attrs) != len(tt.want) {
				t.Errorf("attributes = %v, want names %v", attrs, tt.want)
			}
			for _, n := range tt.want {
				if attrs[n] == "" {
					t.Errorf("attribute %s is missing in %v", n, attrs)
				}
			}
			if attrs["SenderId"] != "" && attrs["SenderId"] != testKey {
				t.Errorf("SenderId = %q, want the access key ID", attrs["SenderId"])
			}
			// A fresh queue per case drops the leftover message.
			e.okEmpty("DeleteQueue", fmt.Sprintf(`{"QueueUrl":%q}`, u))
			e.createQueue("alpha")
		})
	}
}

func TestMessageAttributeFilter(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	const attrs = `{"color":{"DataType":"String","StringValue":"blue"},"col.x":{"DataType":"Number","StringValue":"3"},"blob":{"DataType":"Binary","BinaryValue":"AQID"}}`
	var sent struct{ MD5OfMessageAttributes string }
	e.ok("SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"m","MessageAttributes":%s}`, u, attrs), &sent)
	all := map[string]queue.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
		"col.x": {DataType: "Number", StringValue: "3"},
		"blob":  {DataType: "Binary", BinaryValue: []byte{1, 2, 3}},
	}
	if want := queue.MD5OfAttributes(all); sent.MD5OfMessageAttributes != want {
		t.Errorf("SendMessage MD5OfMessageAttributes = %q, want %q", sent.MD5OfMessageAttributes, want)
	}
	tests := []struct {
		name  string
		names string
		want  []string
	}{
		{"none", ``, nil},
		{"All", `"All"`, []string{"blob", "col.x", "color"}},
		{"dot star", `".*"`, []string{"blob", "col.x", "color"}},
		{"exact", `"color"`, []string{"color"}},
		{"prefix", `"col.*"`, []string{"col.x", "color"}},
		{"two names", `"color","blob"`, []string{"blob", "color"}},
		{"no match", `"nope"`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":1,"VisibilityTimeout":0`, u)
			if tt.names != "" {
				req += `,"MessageAttributeNames":[` + tt.names + `]`
			}
			var got receivedBody
			e.ok("ReceiveMessage", req+"}", &got)
			if len(got.Messages) != 1 {
				t.Fatalf("got %d messages, want 1", len(got.Messages))
			}
			m := got.Messages[0]
			wantSet := map[string]queue.MessageAttribute{}
			for _, n := range tt.want {
				wantSet[n] = all[n]
				if m.MessageAttributes[n].DataType != all[n].DataType {
					t.Errorf("attribute %s = %+v, want data type %s", n, m.MessageAttributes[n], all[n].DataType)
				}
			}
			if len(m.MessageAttributes) != len(tt.want) {
				t.Errorf("returned %d attributes, want %v", len(m.MessageAttributes), tt.want)
			}
			if want := queue.MD5OfAttributes(wantSet); m.MD5OfMessageAttributes != want {
				t.Errorf("MD5OfMessageAttributes = %q, want %q (over the returned attributes)", m.MD5OfMessageAttributes, want)
			}
		})
	}
	var got receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"VisibilityTimeout":0,"MessageAttributeNames":["blob"]}`, u), &got)
	if blob := got.Messages[0].MessageAttributes["blob"]; blob.BinaryValue != "AQID" || blob.StringValue != "" {
		t.Errorf("binary attribute = %+v, want base64 AQID", blob)
	}
}

type batchBody struct {
	Successful []struct {
		ID               string `json:"Id"`
		MD5OfMessageBody string
	}
	Failed []struct {
		ID          string `json:"Id"`
		SenderFault bool
		Code        string
		Message     string
	}
}

func TestBatchValidation(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	entries := func(n int, mk func(i int) string) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = mk(i)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	send := func(i int) string { return fmt.Sprintf(`{"Id":"e%d","MessageBody":"b"}`, i) }
	del := func(i int) string { return fmt.Sprintf(`{"Id":"e%d","ReceiptHandle":"h"}`, i) }
	vis := func(i int) string { return fmt.Sprintf(`{"Id":"e%d","ReceiptHandle":"h","VisibilityTimeout":0}`, i) }
	big := strings.Repeat("x", 600000)
	tests := []struct {
		name, entries string
		typ, query    string
	}{
		{"empty", `[]`, "EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest"},
		{"missing", ``, "EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest"},
		{"eleven", entries(11, send), "TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"},
		{"duplicate", `[{"Id":"a","MessageBody":"b"},{"Id":"a","MessageBody":"b"}]`, "BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"},
		{"bad id", `[{"Id":"bad id","MessageBody":"b"}]`, "InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId"},
		{"empty id", `[{"Id":"","MessageBody":"b"}]`, "InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId"},
		{"long id", `[{"Id":"` + strings.Repeat("a", 81) + `","MessageBody":"b"}]`, "InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId"},
		{"too long", `[{"Id":"a","MessageBody":"` + big + `"},{"Id":"b","MessageBody":"` + big + `"}]`, "BatchRequestTooLong", "AWS.SimpleQueueService.BatchRequestTooLong"},
	}
	for _, tt := range tests {
		t.Run("send "+tt.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"QueueUrl":%q`, u)
			if tt.entries != "" {
				body += `,"Entries":` + tt.entries
			}
			e.fail("SendMessageBatch", body+"}", 400, "com.amazonaws.sqs#"+tt.typ, tt.query)
		})
	}
	for _, op := range []struct {
		name string
		mk   func(int) string
	}{{"DeleteMessageBatch", del}, {"ChangeMessageVisibilityBatch", vis}} {
		for _, tt := range tests[:6] {
			if tt.name == "duplicate" || tt.name == "eleven" {
				continue
			}
			t.Run(op.name+" "+tt.name, func(t *testing.T) {
				body := fmt.Sprintf(`{"QueueUrl":%q`, u)
				if tt.entries != "" {
					body += `,"Entries":` + strings.ReplaceAll(strings.ReplaceAll(tt.entries, `,"MessageBody":"b"`, `,"ReceiptHandle":"h","VisibilityTimeout":0`), `"MessageBody":"b"`, `"ReceiptHandle":"h"`)
				}
				e.fail(op.name, body+"}", 400, "com.amazonaws.sqs#"+tt.typ, tt.query)
			})
		}
		t.Run(op.name+" eleven", func(t *testing.T) {
			e.fail(op.name, fmt.Sprintf(`{"QueueUrl":%q,"Entries":%s}`, u, entries(11, op.mk)), 400, "com.amazonaws.sqs#TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
		})
		t.Run(op.name+" duplicate", func(t *testing.T) {
			e.fail(op.name, fmt.Sprintf(`{"QueueUrl":%q,"Entries":%s}`, u, entries(2, func(int) string { return op.mk(0) })), 400, "com.amazonaws.sqs#BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
		})
	}
}

func TestBatchPartialFailure(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	var sent batchBody
	e.ok("SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"ok","MessageBody":"fine"},{"Id":"bad","MessageBody":"\u0000"}]}`, u), &sent)
	if len(sent.Successful) != 1 || sent.Successful[0].ID != "ok" || sent.Successful[0].MD5OfMessageBody != queue.MD5OfBody("fine") {
		t.Errorf("Successful = %+v, want entry ok", sent.Successful)
	}
	if len(sent.Failed) != 1 || sent.Failed[0].ID != "bad" || !sent.Failed[0].SenderFault || sent.Failed[0].Code != "InvalidMessageContents" || sent.Failed[0].Message == "" {
		t.Errorf("Failed = %+v, want entry bad with InvalidMessageContents", sent.Failed)
	}

	// Entries that cannot convert fail alone and the others are sent.
	var mixed batchBody
	e.ok("SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"ok","MessageBody":"fine"},{"Id":"fifo","MessageBody":"fine","MessageGroupId":"g"},{"Id":"list","MessageBody":"fine","MessageAttributes":{"a":{"DataType":"String","StringListValues":["x"]}}}]}`, u), &mixed)
	if len(mixed.Successful) != 2 || len(mixed.Failed) != 1 || mixed.Failed[0].ID != "list" || mixed.Failed[0].Code != "InvalidParameterValue" {
		t.Errorf("mixed SendMessageBatch = %+v, %+v; want ok and fifo sent, list failed with InvalidParameterValue", mixed.Successful, mixed.Failed)
	}

	var got receivedBody
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":1}`, u), &got)
	handle := got.Messages[0].ReceiptHandle
	var vis batchBody
	e.ok("ChangeMessageVisibilityBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"x","ReceiptHandle":%q,"VisibilityTimeout":0},{"Id":"y","ReceiptHandle":"garbage","VisibilityTimeout":0}]}`, u, handle), &vis)
	if len(vis.Successful) != 1 || vis.Successful[0].ID != "x" || len(vis.Failed) != 1 || vis.Failed[0].ID != "y" || vis.Failed[0].Code != "ReceiptHandleIsInvalid" {
		t.Errorf("ChangeMessageVisibilityBatch = %+v, want x ok and y ReceiptHandleIsInvalid", vis)
	}
	e.ok("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":1}`, u), &got)
	handle = got.Messages[0].ReceiptHandle
	var noTimeout batchBody
	e.ok("ChangeMessageVisibilityBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"n","ReceiptHandle":%q}]}`, u, handle), &noTimeout)
	if len(noTimeout.Successful) != 0 || len(noTimeout.Failed) != 1 || noTimeout.Failed[0].Code != "InvalidParameterValue" {
		t.Errorf("entry without VisibilityTimeout = %+v, %+v; want failed with InvalidParameterValue", noTimeout.Successful, noTimeout.Failed)
	}
	var del batchBody
	e.ok("DeleteMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"x","ReceiptHandle":%q},{"Id":"y","ReceiptHandle":"garbage"}]}`, u, handle), &del)
	if len(del.Successful) != 1 || len(del.Failed) != 1 || del.Failed[0].Code != "ReceiptHandleIsInvalid" {
		t.Errorf("DeleteMessageBatch = %+v, want one success and one ReceiptHandleIsInvalid", del)
	}
}

func TestAccessLog(t *testing.T) {
	e := newEnv(t)
	u := e.createQueue("alpha")
	e.send(testHost, testSecret, "GetQueueUrl", `{"QueueName":"nope"}`)
	e.send(testHost, "wrong", "ListQueues", `{}`)
	e.send(testHost, testSecret, "DeleteQueue", fmt.Sprintf(`{"QueueUrl":%q}`, u))
	log := e.logs.String()
	for _, want := range []string{
		"op=CreateQueue status=200", "op=GetQueueUrl status=400", "op=ListQueues status=403", "op=DeleteQueue status=200", "component=sqsapi",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("access log lacks %q:\n%s", want, log)
		}
	}
}
