package topic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const testRegion = "us-east-1"

// fakeQueues records every send. A queue named in missing fails like a deleted queue.
type fakeQueues struct {
	mu      sync.Mutex
	sent    []sentMessage
	missing map[string]bool
}

type sentMessage struct {
	Queue string
	In    queue.SendInput
}

func (f *fakeQueues) Send(_ context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missing[name] {
		return nil, queue.ErrQueueDoesNotExist
	}
	for _, m := range in {
		f.sent = append(f.sent, sentMessage{name, m})
	}
	return make([]queue.SendResult, len(in)), nil
}

func openEngine(t *testing.T, dir string, q Queues) *Engine {
	t.Helper()
	fsys, err := localdisk.Open(dir)
	if err != nil {
		t.Fatalf("localdisk.Open(%q) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	e, err := Open(t.Context(), fsys, testRegion, q)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", dir, err)
	}
	return e
}

func newEngine(t *testing.T) (*Engine, *fakeQueues) {
	t.Helper()
	q := &fakeQueues{missing: map[string]bool{}}
	return openEngine(t, t.TempDir(), q), q
}

func mustTopic(t *testing.T, e *Engine, name string) string {
	t.Helper()
	arn, err := e.CreateTopic(t.Context(), name, nil, nil)
	if err != nil {
		t.Fatalf("CreateTopic(%q) error = %v", name, err)
	}
	return arn
}

func queueARN(name string) string { return "arn:aws:sqs:us-east-1:000000000000:" + name }

func mustSubscribe(t *testing.T, e *Engine, topicARN, queueName string, attrs map[string]string) string {
	t.Helper()
	arn, err := e.Subscribe(t.Context(), topicARN, "sqs", queueARN(queueName), attrs)
	if err != nil {
		t.Fatalf("Subscribe(%q, %q, %v) error = %v", topicARN, queueName, attrs, err)
	}
	return arn
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	attrs := map[string]string{"DisplayName": "orders", "SignatureVersion": "2"}
	arn, err := e.CreateTopic(t.Context(), "Orders", attrs, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("CreateTopic(Orders) error = %v", err)
	}
	subARN := mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": "true"})
	wantAttrs, _ := e.TopicAttributes(t.Context(), arn)
	wantSubAttrs, _ := e.SubscriptionAttributes(t.Context(), subARN)
	cert, err := e.CertPEM(t.Context())
	if err != nil {
		t.Fatalf("CertPEM() error = %v", err)
	}

	again := openEngine(t, dir, &fakeQueues{})
	gotAttrs, err := again.TopicAttributes(t.Context(), arn)
	if err != nil || !reflect.DeepEqual(gotAttrs, wantAttrs) {
		t.Errorf("TopicAttributes after reopen = %v, %v; want %v", gotAttrs, err, wantAttrs)
	}
	gotSub, err := again.SubscriptionAttributes(t.Context(), subARN)
	if err != nil || !reflect.DeepEqual(gotSub, wantSubAttrs) {
		t.Errorf("SubscriptionAttributes after reopen = %v, %v; want %v", gotSub, err, wantSubAttrs)
	}
	tags, err := again.ListTagsForResource(t.Context(), arn)
	if err != nil || !reflect.DeepEqual(tags, map[string]string{"env": "test"}) {
		t.Errorf("ListTagsForResource after reopen = %v, %v", tags, err)
	}
	gotCert, err := again.CertPEM(t.Context())
	if err != nil || string(gotCert) != string(cert) {
		t.Errorf("CertPEM after reopen differs from before (err %v)", err)
	}
	if err := again.DeleteTopic(t.Context(), arn); err != nil {
		t.Fatal(err)
	}
	third := openEngine(t, dir, &fakeQueues{})
	if arns, _, _ := third.ListTopics(t.Context(), ""); len(arns) != 0 {
		t.Errorf("ListTopics after delete and reopen = %v, want none", arns)
	}
	if subs, _, _ := third.ListSubscriptions(t.Context(), ""); len(subs) != 0 {
		t.Errorf("ListSubscriptions after delete and reopen = %v, want none", subs)
	}
}

func TestOpenRejectsBadFiles(t *testing.T) {
	arn := "arn:aws:sns:us-east-1:000000000000:t"
	sub := arn + ":11111111-1111-4111-8111-111111111111"
	tests := []struct {
		name  string
		dir   string
		file  func(topicFile string) string // returns the name to write
		body  string
		reuse bool // create topic "t" first
	}{
		{name: "not JSON", dir: topicsDir, body: `{`},
		{name: "bad name", dir: topicsDir, body: `{"name":"bad name"}`},
		{name: "unknown attribute", dir: topicsDir, body: `{"name":"t","attributes":{"Bogus":"1"}}`},
		{name: "name does not match file", dir: topicsDir, file: func(string) string { return "wrong.json" }, body: `{"name":"t"}`},
		{name: "subscription without topic", dir: subsDir, file: func(string) string { return "11111111-1111-4111-8111-111111111111.json" },
			body: `{"arn":"` + sub + `","topicArn":"` + arn + `","protocol":"sqs","endpoint":"` + queueARN("q") + `"}`},
		{name: "subscription with bad protocol", reuse: true, dir: subsDir, file: func(string) string { return "11111111-1111-4111-8111-111111111111.json" },
			body: `{"arn":"` + sub + `","topicArn":"` + arn + `","protocol":"email","endpoint":"x"}`},
		{name: "subscription file name differs", reuse: true, dir: subsDir, file: func(string) string { return "other.json" },
			body: `{"arn":"` + sub + `","topicArn":"` + arn + `","protocol":"sqs","endpoint":"` + queueARN("q") + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.reuse {
				mustTopic(t, openEngine(t, dir, &fakeQueues{}), "t")
			} else {
				openEngine(t, dir, &fakeQueues{}) // creates the directories
			}
			name := "x.json"
			if tt.file != nil {
				name = tt.file("")
			}
			if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(tt.dir), name), []byte(tt.body), 0o644); err != nil {
				t.Fatal(err)
			}
			fsys, err := localdisk.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer fsys.Close()
			if _, err := Open(t.Context(), fsys, testRegion, &fakeQueues{}); err == nil {
				t.Errorf("Open with %s = nil error, want a failure", tt.body)
			}
		})
	}
}

func TestCreateTopic(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "orders")
	if want := "arn:aws:sns:us-east-1:000000000000:orders"; arn != want {
		t.Errorf("CreateTopic ARN = %q, want %q", arn, want)
	}
	tests := []struct {
		name    string
		topic   string
		attrs   map[string]string
		want    error
		wantARN string
	}{
		{"same name, no attributes", "orders", nil, nil, arn},
		{"same name, default attribute", "orders", map[string]string{"DisplayName": ""}, nil, arn},
		{"same name, different attribute", "orders", map[string]string{"DisplayName": "other"}, ErrInvalidParameter, ""},
		{"fifo suffix", "orders.fifo", nil, ErrInvalidParameter, ""},
		{"empty name", "", nil, ErrInvalidParameter, ""},
		{"257 characters", strings.Repeat("a", 257), nil, ErrInvalidParameter, ""},
		{"256 characters", strings.Repeat("a", 256), nil, nil, "arn:aws:sns:us-east-1:000000000000:" + strings.Repeat("a", 256)},
		{"bad character", "bad name", nil, ErrInvalidParameter, ""},
		{"dot", "a.b", nil, ErrInvalidParameter, ""},
		{"unknown attribute", "new1", map[string]string{"Bogus": "1"}, ErrInvalidParameter, ""},
		{"bad signature version", "new2", map[string]string{"SignatureVersion": "3"}, ErrInvalidParameter, ""},
		{"bad tracing config", "new3", map[string]string{"TracingConfig": "Maybe"}, ErrInvalidParameter, ""},
		{"policy not an object", "new4", map[string]string{"Policy": "[]"}, ErrInvalidParameter, ""},
		{"fifo attribute", "new5", map[string]string{"FifoTopic": "true"}, ErrInvalidParameter, ""},
		{"FifoTopic false", "new6", map[string]string{"FifoTopic": "false"}, nil, "arn:aws:sns:us-east-1:000000000000:new6"},
		{"ContentBasedDeduplication false", "new7", map[string]string{"ContentBasedDeduplication": "false"}, nil, "arn:aws:sns:us-east-1:000000000000:new7"},
		{"ContentBasedDeduplication true", "new8", map[string]string{"ContentBasedDeduplication": "true"}, ErrInvalidParameter, ""},
		{"display name of 100", "new9", map[string]string{"DisplayName": strings.Repeat("d", 100)}, nil, "arn:aws:sns:us-east-1:000000000000:new9"},
		{"display name of 101", "new10", map[string]string{"DisplayName": strings.Repeat("d", 101)}, ErrInvalidParameter, ""},
		{"display name with a control character", "new11", map[string]string{"DisplayName": "a\nb"}, ErrInvalidParameter, ""},
		{"same name, FifoTopic false", "orders", map[string]string{"FifoTopic": "false"}, nil, arn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := e.CreateTopic(t.Context(), tt.topic, tt.attrs, nil)
			if !errors.Is(err, tt.want) || (tt.want == nil && err != nil) {
				t.Fatalf("CreateTopic(%q, %v) error = %v, want %v", tt.topic, tt.attrs, err, tt.want)
			}
			if got != tt.wantARN {
				t.Errorf("CreateTopic(%q, %v) = %q, want %q", tt.topic, tt.attrs, got, tt.wantARN)
			}
		})
	}
	for name, tags := range map[string]map[string]string{
		"empty key":  {"": "v"},
		"long key":   {strings.Repeat("k", 129): "v"},
		"long value": {"k": strings.Repeat("v", 257)},
	} {
		if _, err := e.CreateTopic(t.Context(), "badtags", nil, tags); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("CreateTopic with a %s tag error = %v, want %v", name, err, ErrInvalidParameter)
		}
	}
	if _, err := e.CreateTopic(t.Context(), "goodtags", nil, map[string]string{strings.Repeat("k", 128): "", "k": strings.Repeat("v", 256)}); err != nil {
		t.Errorf("CreateTopic with tags at the limits error = %v, want success", err)
	}
	tags := map[string]string{}
	for i := range 51 {
		tags[fmt.Sprint("k", i)] = "v"
	}
	if _, err := e.CreateTopic(t.Context(), "tagged", nil, tags); !errors.Is(err, ErrTagLimitExceeded) {
		t.Errorf("CreateTopic with 51 tags error = %v, want %v", err, ErrTagLimitExceeded)
	}
}

// Golden values come from test/diff/testdata/golden/sns-topic-basics.json.
const (
	goldenPolicy   = `{"Version":"2008-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},"Action":["SNS:GetTopicAttributes","SNS:SetTopicAttributes","SNS:AddPermission","SNS:RemovePermission","SNS:DeleteTopic","SNS:Subscribe","SNS:ListSubscriptionsByTopic","SNS:Publish"],"Resource":"{arn}","Condition":{"StringEquals":{"AWS:SourceOwner":"000000000000"}}}]}`
	goldenDelivery = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}`
)

func TestTopicAttributes(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "attrs")
	want := []Attribute{
		{"Policy", strings.Replace(goldenPolicy, "{arn}", arn, 1)},
		{"Owner", "000000000000"},
		{"SubscriptionsPending", "0"},
		{"TopicArn", arn},
		{"EffectiveDeliveryPolicy", goldenDelivery},
		{"SubscriptionsConfirmed", "0"},
		{"DisplayName", ""},
		{"SubscriptionsDeleted", "0"},
	}
	got, err := e.TopicAttributes(t.Context(), arn)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("TopicAttributes(new topic) = %v, %v\nwant %v", got, err, want)
	}

	mustSubscribe(t, e, arn, "q1", nil)
	got, _ = e.TopicAttributes(t.Context(), arn)
	if got[5] != (Attribute{"SubscriptionsConfirmed", "1"}) {
		t.Errorf("TopicAttributes after Subscribe entry 5 = %v, want SubscriptionsConfirmed 1", got[5])
	}

	for _, set := range []struct{ name, value string }{
		{"DisplayName", "renamed"}, {"DeliveryPolicy", `{"http":{}}`}, {"KmsMasterKeyId", "alias/x"},
		{"SignatureVersion", "2"}, {"TracingConfig", "Active"}, {"Policy", `{"Version":"1"}`},
	} {
		if err := e.SetTopicAttribute(t.Context(), arn, set.name, set.value); err != nil {
			t.Fatalf("SetTopicAttribute(%s, %q) error = %v", set.name, set.value, err)
		}
	}
	got, _ = e.TopicAttributes(t.Context(), arn)
	wantKeys := []string{"Policy", "Owner", "SubscriptionsPending", "TopicArn", "EffectiveDeliveryPolicy", "SubscriptionsConfirmed", "DisplayName", "SubscriptionsDeleted", "DeliveryPolicy", "KmsMasterKeyId", "SignatureVersion", "TracingConfig"}
	var keys []string
	for _, a := range got {
		keys = append(keys, a.Key)
	}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("TopicAttributes keys after set = %v, want %v", keys, wantKeys)
	}
	if got[0].Value != `{"Version":"1"}` || got[6].Value != "renamed" || got[4].Value != goldenDelivery {
		t.Errorf("TopicAttributes after set: Policy %q, DisplayName %q, EffectiveDeliveryPolicy %q", got[0].Value, got[6].Value, got[4].Value)
	}

	if err := e.SetTopicAttribute(t.Context(), arn, "DisplayName", ""); err != nil {
		t.Fatal(err)
	}
	if got, _ = e.TopicAttributes(t.Context(), arn); got[6].Value != "" {
		t.Errorf("DisplayName after unset = %q, want empty", got[6].Value)
	}

	tests := []struct {
		name, arn, attr, value string
		want                   error
	}{
		{"unknown attribute", arn, "Bogus", "x", ErrInvalidParameter},
		{"read-only attribute", arn, "Owner", "x", ErrInvalidParameter},
		{"bad delivery policy", arn, "DeliveryPolicy", "x", ErrInvalidParameter},
		{"long display name", arn, "DisplayName", strings.Repeat("d", 101), ErrInvalidParameter},
		{"missing topic", arn + "x", "DisplayName", "x", ErrNotFound},
		{"not an ARN", "x", "DisplayName", "x", ErrInvalidParameter},
	}
	for _, tt := range tests {
		if err := e.SetTopicAttribute(t.Context(), tt.arn, tt.attr, tt.value); !errors.Is(err, tt.want) {
			t.Errorf("%s: SetTopicAttribute(%q, %q, %q) error = %v, want %v", tt.name, tt.arn, tt.attr, tt.value, err, tt.want)
		}
	}
	if _, err := e.TopicAttributes(t.Context(), arn+"x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("TopicAttributes(missing) error = %v, want %v", err, ErrNotFound)
	}
}

func TestDeleteTopic(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "doomed")
	subARN := mustSubscribe(t, e, arn, "q", nil)
	if err := e.DeleteTopic(t.Context(), arn); err != nil {
		t.Fatalf("DeleteTopic error = %v", err)
	}
	if _, err := e.TopicAttributes(t.Context(), arn); !errors.Is(err, ErrNotFound) {
		t.Errorf("TopicAttributes after delete error = %v, want %v", err, ErrNotFound)
	}
	if _, err := e.SubscriptionAttributes(t.Context(), subARN); !errors.Is(err, ErrNotFound) {
		t.Errorf("SubscriptionAttributes after delete error = %v, want %v", err, ErrNotFound)
	}
	if err := e.DeleteTopic(t.Context(), arn); err != nil {
		t.Errorf("DeleteTopic again error = %v, want nil", err)
	}
	if err := e.DeleteTopic(t.Context(), "x"); !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("DeleteTopic(x) error = %v, want %v", err, ErrInvalidParameter)
	}
	if mustTopic(t, e, "doomed") != arn {
		t.Error("re-created topic has another ARN")
	}
	if got, _, _ := e.ListSubscriptionsByTopic(t.Context(), arn, ""); len(got) != 0 {
		t.Errorf("re-created topic has subscriptions %v", got)
	}
}

func TestSubscribe(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "subs")
	ep := queueARN("q1")
	first := mustSubscribe(t, e, arn, "q1", nil)
	if !strings.HasPrefix(first, arn+":") || !uuidRE.MatchString(strings.TrimPrefix(first, arn+":")) {
		t.Errorf("Subscribe ARN = %q, want %q plus a lowercase UUID", first, arn+":")
	}
	if again := mustSubscribe(t, e, arn, "q1", nil); again != first {
		t.Errorf("Subscribe again = %q, want %q", again, first)
	}
	if again := mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": "false"}); again != first {
		t.Errorf("Subscribe with default attribute = %q, want %q", again, first)
	}
	tests := []struct {
		name                      string
		topic, protocol, endpoint string
		attrs                     map[string]string
		want                      error
	}{
		{"missing topic", arn + "x", "sqs", ep, nil, ErrNotFound},
		{"not an ARN", "x", "sqs", ep, nil, ErrInvalidParameter},
		{"http protocol", arn, "http", "http://example.com", nil, ErrInvalidParameter},
		{"unknown protocol", arn, "smoke", "x", nil, ErrInvalidParameter},
		{"endpoint not an ARN", arn, "sqs", "not-an-arn", nil, ErrInvalidParameter},
		{"endpoint in another region", arn, "sqs", "arn:aws:sqs:eu-west-1:000000000000:q", nil, ErrInvalidParameter},
		{"endpoint in another account", arn, "sqs", "arn:aws:sqs:us-east-1:111111111111:q", nil, ErrInvalidParameter},
		{"endpoint without queue", arn, "sqs", "arn:aws:sqs:us-east-1:000000000000:", nil, ErrInvalidParameter},
		{"fifo queue", arn, "sqs", queueARN("q.fifo"), nil, ErrInvalidParameter},
		{"filter policy", arn, "sqs", queueARN("q2"), map[string]string{"FilterPolicy": "{}"}, ErrInvalidParameter},
		{"redrive policy", arn, "sqs", queueARN("q2"), map[string]string{"RedrivePolicy": "{}"}, ErrInvalidParameter},
		{"bad raw value", arn, "sqs", queueARN("q2"), map[string]string{"RawMessageDelivery": "yes"}, ErrInvalidParameter},
		{"different attributes", arn, "sqs", ep, map[string]string{"RawMessageDelivery": "true"}, ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := e.Subscribe(t.Context(), tt.topic, tt.protocol, tt.endpoint, tt.attrs); !errors.Is(err, tt.want) {
				t.Errorf("Subscribe(%q, %q, %q, %v) = %q, %v; want error %v", tt.topic, tt.protocol, tt.endpoint, tt.attrs, got, err, tt.want)
			}
		})
	}

	want := []Attribute{
		{"SubscriptionArn", first}, {"TopicArn", arn}, {"Owner", "000000000000"}, {"Protocol", "sqs"}, {"Endpoint", ep},
		{"RawMessageDelivery", "false"}, {"ConfirmationWasAuthenticated", "true"}, {"PendingConfirmation", "false"},
		{"SubscriptionPrincipal", "arn:aws:iam::000000000000:root"},
	}
	if got, err := e.SubscriptionAttributes(t.Context(), first); err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("SubscriptionAttributes = %v, %v\nwant %v", got, err, want)
	}
	if err := e.SetSubscriptionAttribute(t.Context(), first, "RawMessageDelivery", "true"); err != nil {
		t.Fatalf("SetSubscriptionAttribute error = %v", err)
	}
	if got, _ := e.SubscriptionAttributes(t.Context(), first); got[5] != (Attribute{"RawMessageDelivery", "true"}) {
		t.Errorf("RawMessageDelivery after set = %v, want true", got[5])
	}
	// A set clones the stored map, so an earlier snapshot keeps its value.
	before := e.subs[first].Attributes
	if err := e.SetSubscriptionAttribute(t.Context(), first, "RawMessageDelivery", "false"); err != nil {
		t.Fatal(err)
	}
	if before["RawMessageDelivery"] != "true" || e.subs[first].Attributes["RawMessageDelivery"] != "false" {
		t.Errorf("attributes after second set: old map %v, new map %v; want the old map unchanged", before, e.subs[first].Attributes)
	}
	for _, name := range []string{"FilterPolicy", "Bogus"} {
		if err := e.SetSubscriptionAttribute(t.Context(), first, name, "{}"); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("SetSubscriptionAttribute(%s) error = %v, want %v", name, err, ErrInvalidParameter)
		}
	}
	if err := e.SetSubscriptionAttribute(t.Context(), arn+":x", "RawMessageDelivery", "true"); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetSubscriptionAttribute(missing) error = %v, want %v", err, ErrNotFound)
	}

	// A file that is already gone does not fail the unsubscribe.
	if err := e.fs.Remove(subFile(first)); err != nil {
		t.Fatal(err)
	}
	if err := e.Unsubscribe(t.Context(), first); err != nil {
		t.Fatalf("Unsubscribe error = %v", err)
	}
	if err := e.Unsubscribe(t.Context(), first); !errors.Is(err, ErrNotFound) {
		t.Errorf("Unsubscribe again error = %v, want %v", err, ErrNotFound)
	}
	if err := e.Unsubscribe(t.Context(), "x"); !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("Unsubscribe(x) error = %v, want %v", err, ErrInvalidParameter)
	}
}

func TestListing(t *testing.T) {
	e, _ := newEngine(t)
	var want []string
	for i := range 101 {
		want = append(want, mustTopic(t, e, fmt.Sprintf("t%03d", i)))
	}
	first, next, err := e.ListTopics(t.Context(), "")
	if err != nil || len(first) != 100 || next == "" || first[0] != want[0] || first[99] != want[99] {
		t.Fatalf("ListTopics first page = %d topics, next %q, error %v; want 100 topics and a token", len(first), next, err)
	}
	second, next, err := e.ListTopics(t.Context(), next)
	if err != nil || !reflect.DeepEqual(second, want[100:]) || next != "" {
		t.Errorf("ListTopics second page = %v, next %q, error %v; want %v and no token", second, next, err, want[100:])
	}

	a, b := want[0], want[1]
	s1 := mustSubscribe(t, e, b, "q1", nil)
	s2 := mustSubscribe(t, e, a, "q2", nil)
	s3 := mustSubscribe(t, e, a, "q3", nil)
	all, _, err := e.ListSubscriptions(t.Context(), "")
	if err != nil || len(all) != 3 || all[0].TopicARN != a || all[2].ARN != s1 {
		t.Errorf("ListSubscriptions = %v, %v; want 3 sorted by ARN", all, err)
	}
	byTopic, _, err := e.ListSubscriptionsByTopic(t.Context(), a, "")
	if err != nil || len(byTopic) != 2 {
		t.Fatalf("ListSubscriptionsByTopic = %v, %v; want 2", byTopic, err)
	}
	got := Subscription{ARN: byTopic[0].ARN, Owner: "000000000000", Protocol: "sqs", Endpoint: byTopic[0].Endpoint, TopicARN: a}
	if byTopic[0] != got || (byTopic[0].ARN != s2 && byTopic[0].ARN != s3) {
		t.Errorf("ListSubscriptionsByTopic[0] = %+v, want fields filled for a subscription of %s", byTopic[0], a)
	}
	if _, _, err := e.ListSubscriptionsByTopic(t.Context(), a+"x", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("ListSubscriptionsByTopic(missing) error = %v, want %v", err, ErrNotFound)
	}
}

func TestTags(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "tagged")
	if err := e.TagResource(t.Context(), arn, map[string]string{"b": "2", "a": "1"}); err != nil {
		t.Fatalf("TagResource error = %v", err)
	}
	if err := e.TagResource(t.Context(), arn, map[string]string{"a": "3"}); err != nil {
		t.Fatal(err)
	}
	got, err := e.ListTagsForResource(t.Context(), arn)
	if want := map[string]string{"a": "3", "b": "2"}; err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ListTagsForResource = %v, %v; want %v", got, err, want)
	}
	if err := e.UntagResource(t.Context(), arn, []string{"a", "missing"}); err != nil {
		t.Fatalf("UntagResource error = %v", err)
	}
	if got, _ = e.ListTagsForResource(t.Context(), arn); !reflect.DeepEqual(got, map[string]string{"b": "2"}) {
		t.Errorf("tags after untag = %v, want b only", got)
	}
	many := map[string]string{}
	for i := range 50 {
		many[fmt.Sprint("k", i)] = "v"
	}
	if err := e.TagResource(t.Context(), arn, many); !errors.Is(err, ErrTagLimitExceeded) {
		t.Errorf("TagResource past 50 tags error = %v, want %v", err, ErrTagLimitExceeded)
	}
	for name, tags := range map[string]map[string]string{"empty key": {"": "v"}, "long value": {"k": strings.Repeat("v", 257)}} {
		if err := e.TagResource(t.Context(), arn, tags); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("TagResource with a %s tag error = %v, want %v", name, err, ErrInvalidParameter)
		}
	}
	missing := arn + "x"
	for name, err := range map[string]error{
		"TagResource":         e.TagResource(t.Context(), missing, map[string]string{"a": "1"}),
		"UntagResource":       e.UntagResource(t.Context(), missing, []string{"a"}),
		"ListTagsForResource": func() error { _, err := e.ListTagsForResource(t.Context(), missing); return err }(),
	} {
		if !errors.Is(err, ErrResourceNotFound) {
			t.Errorf("%s(missing) error = %v, want %v", name, err, ErrResourceNotFound)
		}
	}
}
