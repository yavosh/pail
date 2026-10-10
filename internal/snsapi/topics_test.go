package snsapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/topic"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

// rig is a handler over a real topic engine and a real queue engine.
type rig struct {
	h      http.Handler
	queues *queue.Engine
	topics *topic.Engine
}

func newRig(t *testing.T) *rig {
	t.Helper()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	queues, err := queue.Open(t.Context(), fsys, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	topics, err := topic.Open(t.Context(), fsys, "us-east-1", queues)
	if err != nil {
		t.Fatal(err)
	}
	return &rig{h: New(Options{AccessKeyID: testKey, SecretAccessKey: testSecret, Topics: topics}), queues: queues, topics: topics}
}

var requestIDRE = regexp.MustCompile(`<RequestId>[0-9a-f-]{36}</RequestId>`)

// do sends a signed form and returns the status and the body with the request ID masked.
func (g *rig) do(t *testing.T, form string) (int, string) {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://pail.test:9000/", strings.NewReader(form+"&Version=2010-03-31"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	sum := sha256.Sum256([]byte(form + "&Version=2010-03-31"))
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := v4.NewSigner().SignHTTP(r.Context(), creds, r, hex.EncodeToString(sum[:]), "sns", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	g.h.ServeHTTP(w, r)
	if w.Header().Get("Content-Type") != "text/xml" || w.Header().Get("x-amzn-RequestId") == "" {
		t.Errorf("%s: Content-Type %q, x-amzn-RequestId %q, want text/xml and a request ID", form, w.Header().Get("Content-Type"), w.Header().Get("x-amzn-RequestId"))
	}
	return w.Code, requestIDRE.ReplaceAllString(w.Body.String(), "<RequestId>ID</RequestId>")
}

// ok fails the test unless the call succeeds.
func (g *rig) ok(t *testing.T, form string) string {
	t.Helper()
	status, body := g.do(t, form)
	if status != http.StatusOK {
		t.Fatalf("%s: status %d, body %s", form, status, body)
	}
	return body
}

// tag returns the text of the first element called name.
func tag(t *testing.T, body, name string) string {
	t.Helper()
	m := regexp.MustCompile(`<` + name + `>([^<]*)</` + name + `>`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no <%s> in %s", name, body)
	}
	return m[1]
}

func esc(s string) string { return url.QueryEscape(s) }

const ns = ` xmlns="http://sns.amazonaws.com/doc/2010-03-31/"`

func TestTopicShapes(t *testing.T) {
	g := newRig(t)
	body := g.ok(t, "Action=CreateTopic&Name=orders")
	arn := "arn:aws:sns:us-east-1:000000000000:orders"
	want := `<CreateTopicResponse` + ns + `><CreateTopicResult><TopicArn>` + arn + `</TopicArn></CreateTopicResult><ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata></CreateTopicResponse>`
	if body != want {
		t.Errorf("CreateTopic body = %s\nwant %s", body, want)
	}
	if again := g.ok(t, "Action=CreateTopic&Name=orders"); again != want {
		t.Errorf("CreateTopic again = %s, want the same ARN", again)
	}

	body = g.ok(t, "Action=GetTopicAttributes&TopicArn="+esc(arn))
	var got struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"GetTopicAttributesResult>Attributes>entry"`
	}
	if err := xml.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range got.Entries {
		keys = append(keys, e.Key)
	}
	if want := "Policy Owner SubscriptionsPending TopicArn EffectiveDeliveryPolicy SubscriptionsConfirmed DisplayName SubscriptionsDeleted"; strings.Join(keys, " ") != want {
		t.Errorf("attribute keys = %v, want %s", keys, want)
	}
	if !strings.Contains(body, "<key>DisplayName</key><value></value>") {
		t.Errorf("body %s lacks the empty DisplayName entry", body)
	}

	body = g.ok(t, "Action=CreateTopic&Name=t2&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=pail&Attributes.entry.2.key=SignatureVersion&Attributes.entry.2.value=2&Tags.member.1.Key=env&Tags.member.1.Value=test")
	arn2 := tag(t, body, "TopicArn")
	body = g.ok(t, "Action=GetTopicAttributes&TopicArn="+esc(arn2))
	for _, want := range []string{"<key>DisplayName</key><value>pail</value>", "<key>SignatureVersion</key><value>2</value>"} {
		if !strings.Contains(body, want) {
			t.Errorf("attributes %s lack %s", body, want)
		}
	}
	if body := g.ok(t, "Action=ListTagsForResource&ResourceArn="+esc(arn2)); !strings.Contains(body, `<Tags><member><Key>env</Key><Value>test</Value></member></Tags>`) {
		t.Errorf("ListTagsForResource = %s, want the env tag", body)
	}

	if body := g.ok(t, "Action=SetTopicAttributes&TopicArn="+esc(arn)+"&AttributeName=DisplayName&AttributeValue=renamed"); body != `<SetTopicAttributesResponse`+ns+`><ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata></SetTopicAttributesResponse>` {
		t.Errorf("SetTopicAttributes body = %s, want no result element", body)
	}
	if body := g.ok(t, "Action=GetTopicAttributes&TopicArn="+esc(arn)); !strings.Contains(body, "<key>DisplayName</key><value>renamed</value>") {
		t.Errorf("DisplayName not renamed in %s", body)
	}

	if body := g.ok(t, "Action=TagResource&ResourceArn="+esc(arn)+"&Tags.member.1.Key=a&Tags.member.1.Value=1&Tags.member.2.Key=b&Tags.member.2.Value=2"); body != `<TagResourceResponse`+ns+`><TagResourceResult/><ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata></TagResourceResponse>` {
		t.Errorf("TagResource body = %s", body)
	}
	if body := g.ok(t, "Action=ListTagsForResource&ResourceArn="+esc(arn)); !strings.Contains(body, `<Tags><member><Key>a</Key><Value>1</Value></member><member><Key>b</Key><Value>2</Value></member></Tags>`) {
		t.Errorf("ListTagsForResource = %s, want a and b sorted", body)
	}
	g.ok(t, "Action=UntagResource&ResourceArn="+esc(arn)+"&TagKeys.member.1=a&TagKeys.member.2=b")
	if body := g.ok(t, "Action=ListTagsForResource&ResourceArn="+esc(arn)); !strings.Contains(body, `<ListTagsForResourceResult><Tags/></ListTagsForResourceResult>`) {
		t.Errorf("ListTagsForResource after untag = %s, want an empty Tags", body)
	}

	if body := g.ok(t, "Action=DeleteTopic&TopicArn="+esc(arn)); body != `<DeleteTopicResponse`+ns+`><ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata></DeleteTopicResponse>` {
		t.Errorf("DeleteTopic body = %s", body)
	}
	status, body := g.do(t, "Action=GetTopicAttributes&TopicArn="+esc(arn))
	if status != 404 || tag(t, body, "Code") != "NotFound" || tag(t, body, "Type") != "Sender" {
		t.Errorf("GetTopicAttributes after delete = %d %s, want 404 NotFound Sender", status, body)
	}
}

func TestListTopicsPaging(t *testing.T) {
	g := newRig(t)
	if body := g.ok(t, "Action=ListTopics"); !strings.Contains(body, "<ListTopicsResult><Topics/></ListTopicsResult>") {
		t.Errorf("ListTopics on an empty server = %s, want an empty Topics", body)
	}
	for i := range 101 {
		g.ok(t, fmt.Sprintf("Action=CreateTopic&Name=t%03d", i))
	}
	first := g.ok(t, "Action=ListTopics")
	if n := strings.Count(first, "<TopicArn>"); n != 100 {
		t.Fatalf("first page has %d topics, want 100", n)
	}
	token := tag(t, first, "NextToken")
	second := g.ok(t, "Action=ListTopics&NextToken="+esc(token))
	if n := strings.Count(second, "<TopicArn>"); n != 1 || strings.Contains(second, "NextToken") || !strings.Contains(second, "t100") {
		t.Errorf("second page = %s, want only t100 and no token", second)
	}
	if status, body := g.do(t, "Action=ListTopics&NextToken=%21%21"); status != 400 || tag(t, body, "Code") != "InvalidParameter" {
		t.Errorf("ListTopics with a bad token = %d %s, want 400 InvalidParameter", status, body)
	}
}

func TestSubscriptionShapes(t *testing.T) {
	g := newRig(t)
	arn := tag(t, g.ok(t, "Action=CreateTopic&Name=subs"), "TopicArn")
	qarn := "arn:aws:sqs:us-east-1:000000000000:q1"
	body := g.ok(t, "Action=Subscribe&TopicArn="+esc(arn)+"&Protocol=sqs&Endpoint="+esc(qarn)+"&ReturnSubscriptionArn=true")
	sub := tag(t, body, "SubscriptionArn")
	if !strings.HasPrefix(sub, arn+":") {
		t.Errorf("SubscriptionArn = %q, want prefix %q", sub, arn+":")
	}
	if again := tag(t, g.ok(t, "Action=Subscribe&TopicArn="+esc(arn)+"&Protocol=sqs&Endpoint="+esc(qarn)), "SubscriptionArn"); again != sub {
		t.Errorf("Subscribe again = %q, want %q", again, sub)
	}

	want := `<ListSubscriptionsByTopicResponse` + ns + `><ListSubscriptionsByTopicResult><Subscriptions><member><SubscriptionArn>` + sub + `</SubscriptionArn><Owner>000000000000</Owner><Protocol>sqs</Protocol><Endpoint>` + qarn + `</Endpoint><TopicArn>` + arn + `</TopicArn></member></Subscriptions></ListSubscriptionsByTopicResult><ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata></ListSubscriptionsByTopicResponse>`
	if got := g.ok(t, "Action=ListSubscriptionsByTopic&TopicArn="+esc(arn)); got != want {
		t.Errorf("ListSubscriptionsByTopic = %s\nwant %s", got, want)
	}
	if got := g.ok(t, "Action=ListSubscriptions"); !strings.Contains(got, "<ListSubscriptionsResult><Subscriptions><member><SubscriptionArn>"+sub) {
		t.Errorf("ListSubscriptions = %s, want the subscription", got)
	}

	attrs := g.ok(t, "Action=GetSubscriptionAttributes&SubscriptionArn="+esc(sub))
	for _, want := range []string{"<key>SubscriptionArn</key><value>" + sub + "</value>", "<key>RawMessageDelivery</key><value>false</value>", "<key>Endpoint</key><value>" + qarn + "</value>"} {
		if !strings.Contains(attrs, want) {
			t.Errorf("subscription attributes %s lack %s", attrs, want)
		}
	}
	g.ok(t, "Action=SetSubscriptionAttributes&SubscriptionArn="+esc(sub)+"&AttributeName=RawMessageDelivery&AttributeValue=true")
	if attrs := g.ok(t, "Action=GetSubscriptionAttributes&SubscriptionArn="+esc(sub)); !strings.Contains(attrs, "<key>RawMessageDelivery</key><value>true</value>") {
		t.Errorf("RawMessageDelivery not set in %s", attrs)
	}

	g.ok(t, "Action=Unsubscribe&SubscriptionArn="+esc(sub))
	if got := g.ok(t, "Action=ListSubscriptionsByTopic&TopicArn="+esc(arn)); !strings.Contains(got, "<Subscriptions/>") {
		t.Errorf("ListSubscriptionsByTopic after Unsubscribe = %s, want empty", got)
	}
}

func TestErrors(t *testing.T) {
	g := newRig(t)
	arn := tag(t, g.ok(t, "Action=CreateTopic&Name=errs"), "TopicArn")
	missing := arn + "x"
	tests := []struct {
		name, form string
		status     int
		code       string
	}{
		{"create without a name", "Action=CreateTopic", 400, "InvalidParameter"},
		{"create with a bad name", "Action=CreateTopic&Name=bad+name", 400, "InvalidParameter"},
		{"create a FIFO topic", "Action=CreateTopic&Name=t.fifo", 400, "InvalidParameter"},
		{"create with data protection", "Action=CreateTopic&Name=x&DataProtectionPolicy=%7B%7D", 400, "InvalidParameter"},
		{"create with another attribute value", "Action=CreateTopic&Name=errs&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=other", 400, "InvalidParameter"},
		{"get without an ARN", "Action=GetTopicAttributes", 400, "InvalidParameter"},
		{"get a missing topic", "Action=GetTopicAttributes&TopicArn=" + esc(missing), 404, "NotFound"},
		{"set an unknown attribute", "Action=SetTopicAttributes&TopicArn=" + esc(arn) + "&AttributeName=Bogus&AttributeValue=x", 400, "InvalidParameter"},
		{"subscribe to a missing topic", "Action=Subscribe&Protocol=sqs&Endpoint=x&TopicArn=" + esc(missing), 404, "NotFound"},
		{"subscribe with a bad protocol", "Action=Subscribe&Protocol=smoke&Endpoint=x&TopicArn=" + esc(arn), 400, "InvalidParameter"},
		{"subscribe with a bad endpoint", "Action=Subscribe&Protocol=sqs&Endpoint=not-an-arn&TopicArn=" + esc(arn), 400, "InvalidParameter"},
		{"get a missing subscription", "Action=GetSubscriptionAttributes&SubscriptionArn=" + esc(arn+":00000000-0000-0000-0000-000000000000"), 404, "NotFound"},
		{"unsubscribe a missing subscription", "Action=Unsubscribe&SubscriptionArn=" + esc(arn+":00000000-0000-0000-0000-000000000000"), 404, "NotFound"},
		{"list subscriptions of a missing topic", "Action=ListSubscriptionsByTopic&TopicArn=" + esc(missing), 404, "NotFound"},
		{"publish without a message", "Action=Publish&TopicArn=" + esc(arn), 400, "InvalidParameter"},
		{"publish without a topic", "Action=Publish&Message=x", 400, "InvalidParameter"},
		{"publish to a target", "Action=Publish&Message=x&TargetArn=" + esc(arn), 400, "InvalidParameter"},
		{"publish to a phone number", "Action=Publish&Message=x&PhoneNumber=%2B15555550100", 400, "InvalidParameter"},
		{"publish to a missing topic", "Action=Publish&Message=x&TopicArn=" + esc(missing), 404, "NotFound"},
		{"publish with a bad attribute", "Action=Publish&Message=x&TopicArn=" + esc(arn) + "&MessageAttributes.entry.1.Name=a&MessageAttributes.entry.1.Value.DataType=Blob", 400, "InvalidParameter"},
		{"publish with a bad binary value", "Action=Publish&Message=x&TopicArn=" + esc(arn) + "&MessageAttributes.entry.1.Name=a&MessageAttributes.entry.1.Value.DataType=Binary&MessageAttributes.entry.1.Value.BinaryValue=%21", 400, "InvalidParameter"},
		{"tag a missing topic", "Action=TagResource&Tags.member.1.Key=a&Tags.member.1.Value=1&ResourceArn=" + esc(missing), 404, "ResourceNotFound"},
		{"untag a missing topic", "Action=UntagResource&TagKeys.member.1=a&ResourceArn=" + esc(missing), 404, "ResourceNotFound"},
		{"list tags of a missing topic", "Action=ListTagsForResource&ResourceArn=" + esc(missing), 404, "ResourceNotFound"},
		{"tag without tags", "Action=TagResource&ResourceArn=" + esc(arn), 400, "InvalidParameter"},
		{"untag without keys", "Action=UntagResource&ResourceArn=" + esc(arn), 400, "InvalidParameter"},
		{"unknown action", "Action=Bogus", 400, "InvalidAction"},
		{"unimplemented action", "Action=AddPermission", 400, "InvalidAction"},
		{"no action", "Foo=bar", 400, "InvalidAction"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := g.do(t, tt.form)
			if status != tt.status || tag(t, body, "Code") != tt.code || tag(t, body, "Type") != "Sender" {
				t.Errorf("%s = %d %s, want %d %s Sender", tt.form, status, body, tt.status, tt.code)
			}
			if !strings.Contains(body, "<ErrorResponse"+ns+">") || !strings.Contains(body, "<RequestId>ID</RequestId>") {
				t.Errorf("error body %s lacks the ErrorResponse wrapper or request ID", body)
			}
		})
	}
}

// receive returns the bodies waiting in a queue.
func (g *rig) receive(t *testing.T, name string) []queue.Message {
	t.Helper()
	msgs, err := g.queues.Receive(t.Context(), name, queue.ReceiveInput{Max: 10})
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

func TestPublishDelivery(t *testing.T) {
	g := newRig(t)
	arn := tag(t, g.ok(t, "Action=CreateTopic&Name=news"), "TopicArn")
	for _, name := range []string{"env", "raw"} {
		if err := g.queues.CreateQueue(t.Context(), name, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	g.ok(t, "Action=Subscribe&Protocol=sqs&TopicArn="+esc(arn)+"&Endpoint="+esc("arn:aws:sqs:us-east-1:000000000000:env"))
	g.ok(t, "Action=Subscribe&Protocol=sqs&TopicArn="+esc(arn)+"&Endpoint="+esc("arn:aws:sqs:us-east-1:000000000000:raw")+
		"&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true")

	body := g.ok(t, "Action=Publish&TopicArn="+esc(arn)+"&Message=hello+%26+bye&Subject=greeting"+
		"&MessageAttributes.entry.1.Name=color&MessageAttributes.entry.1.Value.DataType=String&MessageAttributes.entry.1.Value.StringValue=blue"+
		"&MessageAttributes.entry.2.Name=blob&MessageAttributes.entry.2.Value.DataType=Binary&MessageAttributes.entry.2.Value.BinaryValue="+esc(base64.StdEncoding.EncodeToString([]byte{1, 2, 3}))+
		"&MessageAttributes.entry.3.Name=tags&MessageAttributes.entry.3.Value.DataType=String.Array&MessageAttributes.entry.3.Value.StringValue="+esc(`["a","b"]`))
	id := tag(t, body, "MessageId")

	envMsgs := g.receive(t, "env")
	if len(envMsgs) != 1 {
		t.Fatalf("env queue has %d messages, want 1", len(envMsgs))
	}
	var env struct {
		Type, TopicArn, Subject, Message string
		MessageID                        string `json:"MessageId"`
		MessageAttributes                map[string]struct{ Type, Value string }
	}
	if err := json.Unmarshal([]byte(envMsgs[0].Body), &env); err != nil {
		t.Fatal(err)
	}
	if env.Type != "Notification" || env.MessageID != id || env.TopicArn != arn || env.Subject != "greeting" || env.Message != "hello & bye" {
		t.Errorf("envelope = %+v, want Notification %s on %s", env, id, arn)
	}
	if got := env.MessageAttributes["blob"]; got.Type != "Binary" || got.Value != "AQID" {
		t.Errorf("blob attribute = %+v, want Binary AQID", got)
	}
	if !strings.Contains(envMsgs[0].Body, `"SigningCertURL":"http://pail.test:9000/_pail/sns/signing-cert.pem"`) {
		t.Errorf("envelope %s lacks the SigningCertURL for the request host", envMsgs[0].Body)
	}

	rawMsgs := g.receive(t, "raw")
	if len(rawMsgs) != 1 || rawMsgs[0].Body != "hello & bye" {
		t.Fatalf("raw queue = %+v, want one message with the bare text", rawMsgs)
	}
	attrs := rawMsgs[0].Attributes
	if attrs["color"].StringValue != "blue" || attrs["tags"].DataType != "String.Array" || attrs["tags"].StringValue != `["a","b"]` || !bytes.Equal(attrs["blob"].BinaryValue, []byte{1, 2, 3}) {
		t.Errorf("raw attributes = %+v, want color, tags, and blob", attrs)
	}
}

func TestPublishBatch(t *testing.T) {
	g := newRig(t)
	arn := tag(t, g.ok(t, "Action=CreateTopic&Name=batch"), "TopicArn")
	if err := g.queues.CreateQueue(t.Context(), "q", nil, nil); err != nil {
		t.Fatal(err)
	}
	g.ok(t, "Action=Subscribe&Protocol=sqs&TopicArn="+esc(arn)+"&Endpoint="+esc("arn:aws:sqs:us-east-1:000000000000:q")+
		"&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true")
	const p = "&PublishBatchRequestEntries.member."
	body := g.ok(t, "Action=PublishBatch&TopicArn="+esc(arn)+
		p+"1.Id=a"+p+"1.Message=one"+p+"1.MessageAttributes.entry.1.Name=color"+p+"1.MessageAttributes.entry.1.Value.DataType=String"+p+"1.MessageAttributes.entry.1.Value.StringValue=blue"+
		p+"2.Id=b"+p+"2.Message="+p+"2.Subject=ignored"+
		p+"3.Id=c"+p+"3.Message=three"+p+"3.Subject=s")
	var got struct {
		OK []struct {
			ID        string `xml:"Id"`
			MessageID string `xml:"MessageId"`
		} `xml:"PublishBatchResult>Successful>member"`
		Failed []struct {
			ID          string `xml:"Id"`
			Code        string
			SenderFault string
		} `xml:"PublishBatchResult>Failed>member"`
	}
	if err := xml.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.OK) != 2 || got.OK[0].ID != "a" || got.OK[1].ID != "c" || len(got.Failed) != 1 || got.Failed[0].ID != "b" || got.Failed[0].Code != "InvalidParameter" || got.Failed[0].SenderFault != "true" {
		t.Errorf("PublishBatch = %+v, want a and c delivered and b failed", got)
	}
	if strings.Index(body, "<Successful>") > strings.Index(body, "<Failed>") {
		t.Errorf("body %s lists Failed before Successful", body)
	}
	msgs := g.receive(t, "q")
	if len(msgs) != 2 || msgs[0].Body != "one" || msgs[0].Attributes["color"].StringValue != "blue" {
		t.Errorf("queue = %+v, want one (with its attribute) and three", msgs)
	}
	if body := g.ok(t, "Action=PublishBatch&TopicArn="+esc(arn)+p+"1.Id=a"+p+"1.Message=x"); strings.Contains(body, "<Failed>") {
		t.Errorf("all-success batch = %s, want no Failed element", body)
	}
}

func TestPublishBatchValidation(t *testing.T) {
	g := newRig(t)
	arn := tag(t, g.ok(t, "Action=CreateTopic&Name=rules"), "TopicArn")
	const p = "&PublishBatchRequestEntries.member."
	entries := func(n int) string {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, "%s%d.Id=e%d%s%d.Message=m", p, i, i, p, i)
		}
		return b.String()
	}
	tests := []struct {
		name, form, code string
	}{
		{"no entries", "", "EmptyBatchRequest"},
		{"11 entries", entries(11), "TooManyEntriesInBatchRequest"},
		{"duplicate ids", p + "1.Id=a" + p + "1.Message=m" + p + "2.Id=a" + p + "2.Message=m", "BatchEntryIdsNotDistinct"},
		{"id with a space", p + "1.Id=bad+id" + p + "1.Message=m", "InvalidBatchEntryId"},
		{"empty id", p + "1.Message=m", "InvalidBatchEntryId"},
		{"id of 81 characters", p + "1.Id=" + strings.Repeat("a", 81) + p + "1.Message=m", "InvalidBatchEntryId"},
		{"batch too long", p + "1.Id=a" + p + "1.Message=" + strings.Repeat("x", 150000) + p + "2.Id=b" + p + "2.Message=" + strings.Repeat("x", 150000), "BatchRequestTooLong"},
		{"missing topic", "&TopicArn=x", ""},
	}
	for _, tt := range tests {
		if tt.code == "" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			status, body := g.do(t, "Action=PublishBatch&TopicArn="+esc(arn)+tt.form)
			if status != 400 || tag(t, body, "Code") != tt.code {
				t.Errorf("PublishBatch %s = %d %s, want 400 %s", tt.name, status, body[:min(len(body), 300)], tt.code)
			}
		})
	}
	if status, body := g.do(t, "Action=PublishBatch&TopicArn="+esc(arn+"x")+p+"1.Id=a"+p+"1.Message=m"); status != 404 || tag(t, body, "Code") != "NotFound" {
		t.Errorf("PublishBatch to a missing topic = %d %s, want 404 NotFound", status, body)
	}
	if status, _ := g.do(t, "Action=PublishBatch"+entries(1)); status != 400 {
		t.Errorf("PublishBatch without TopicArn status = %d, want 400", status)
	}
}

func TestDecode(t *testing.T) {
	p := params(url.Values{
		"Attributes.entry.1.key": {"a"}, "Attributes.entry.1.value": {"1"},
		"Attributes.entry.2.key": {"b"},
		"Attributes.entry.4.key": {"after-gap"},
		"Tags.member.1.Key":      {"k"}, "Tags.member.1.Value": {"v"},
		"TagKeys.member.1": {"x"}, "TagKeys.member.2": {"y"}, "TagKeys.member.4": {"gap"},
	})
	tests := []struct {
		name, want string
		got        func() any
	}{
		{"attributes stop at the first gap", "map[a:1 b:]", func() any { return p.attributes("Attributes") }},
		{"tags", "map[k:v]", func() any { return p.tags("Tags") }},
		{"list stops at the first gap", "[x y]", func() any { return p.list("TagKeys") }},
		{"absent attributes", "map[]", func() any { return p.attributes("None") }},
		{"absent tags", "map[]", func() any { return p.tags("None") }},
		{"absent list", "[]", func() any { return p.list("None") }},
	}
	for _, tt := range tests {
		if got := fmt.Sprint(tt.got()); got != tt.want {
			t.Errorf("%s: decoded from %v, got %s, want %s", tt.name, url.Values(p), got, tt.want)
		}
	}
	if got := p.attributes("None"); got != nil {
		t.Errorf("attributes(None) = %v, want nil", got)
	}
}

func TestAccessLog(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	g := newRig(t)
	buf.Reset()
	g.ok(t, "Action=CreateTopic&Name=logged")
	g.do(t, "Action=Bogus")
	log := buf.String()
	for _, want := range []string{"component=snsapi", "op=CreateTopic", "status=200", "op=Bogus", "status=400", "method=POST", "duration="} {
		if !strings.Contains(log, want) {
			t.Errorf("access log %q lacks %q", log, want)
		}
	}
}
