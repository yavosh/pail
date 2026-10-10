package s3api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	testQueueARN = "arn:aws:sqs:us-east-1:000000000000:events"
	testTopicARN = "arn:aws:sns:us-east-1:000000000000:events"
)

type delivery struct{ arn, message, baseURL string }

// fakeNotifier knows two destinations and records what it delivers.
type fakeNotifier struct {
	mu   sync.Mutex
	sent []delivery
	fail error
}

func (f *fakeNotifier) Exists(_ context.Context, arn string) bool {
	return arn == testQueueARN || arn == testTopicARN
}

func (f *fakeNotifier) Deliver(_ context.Context, arn, message, baseURL string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, delivery{arn, message, baseURL})
	return f.fail
}

// take returns the deliveries so far and forgets them.
func (f *fakeNotifier) take() []delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.sent
	f.sent = nil
	return out
}

func notifyServer(t *testing.T, n Notifier) *httptest.Server {
	t.Helper()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	st, err := store.Open(context.Background(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions("")
	opts.Region, opts.Store, opts.Notifier = "us-east-1", st, n
	srv := httptest.NewServer(New(opts))
	t.Cleanup(srv.Close)
	return srv
}

func queueRule(id, arn, prefix string, events ...string) string {
	s := "<QueueConfiguration><Id>" + id + "</Id><Queue>" + arn + "</Queue>"
	for _, e := range events {
		s += "<Event>" + e + "</Event>"
	}
	if prefix != "" {
		s += "<Filter><S3Key><FilterRule><Name>prefix</Name><Value>" + prefix + "</Value></FilterRule></S3Key></Filter>"
	}
	return s + "</QueueConfiguration>"
}

func notificationDoc(rules ...string) string {
	return "<NotificationConfiguration>" + strings.Join(rules, "") + "</NotificationConfiguration>"
}

func TestPutBucketNotificationValidation(t *testing.T) {
	srv := notifyServer(t, &fakeNotifier{})
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	topicRule := func(id, arn string) string {
		return "<TopicConfiguration><Id>" + id + "</Id><Topic>" + arn + "</Topic><Event>s3:ObjectCreated:*</Event></TopicConfiguration>"
	}
	created := "s3:ObjectCreated:*"
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"queue", notificationDoc(queueRule("a", testQueueARN, "", created)), 200, ""},
		{"topic", notificationDoc(topicRule("a", testTopicARN)), 200, ""},
		{"empty clears", notificationDoc(), 200, ""},
		{"no id", notificationDoc(queueRule("", testQueueARN, "", created)), 200, ""},
		{"all event names", notificationDoc(queueRule("a", testQueueARN, "", "s3:ObjectCreated:Put", "s3:ObjectCreated:Post",
			"s3:ObjectCreated:Copy", "s3:ObjectCreated:CompleteMultipartUpload", "s3:ObjectRemoved:*")), 200, ""},
		{"disjoint prefixes", notificationDoc(queueRule("a", testQueueARN, "a/", created), queueRule("b", testQueueARN, "b/", created)), 200, ""},
		{"disjoint families", notificationDoc(queueRule("a", testQueueARN, "", created), queueRule("b", testQueueARN, "", "s3:ObjectRemoved:*")), 200, ""},
		{"unknown event", notificationDoc(queueRule("a", testQueueARN, "", "s3:Bogus")), 400, "InvalidArgument"},
		{"restore event", notificationDoc(queueRule("a", testQueueARN, "", "s3:ObjectRestore:Post")), 501, "NotImplemented"},
		{"missing queue", notificationDoc(queueRule("a", testQueueARN+"-missing", "", created)), 400, "InvalidArgument"},
		{"topic ARN in queue slot", notificationDoc(queueRule("a", testTopicARN, "", created)), 400, "InvalidArgument"},
		{"not an ARN", notificationDoc(queueRule("a", "events", "", created)), 400, "InvalidArgument"},
		{"overlap", notificationDoc(queueRule("a", testQueueARN, "in/", created), queueRule("b", testQueueARN, "in/", "s3:ObjectCreated:Put")), 400, "InvalidArgument"},
		{"overlap by shorter prefix", notificationDoc(queueRule("a", testQueueARN, "", created), queueRule("b", testQueueARN, "in/", created)), 400, "InvalidArgument"},
		{"duplicate id", notificationDoc(queueRule("a", testQueueARN, "a/", created), queueRule("a", testQueueARN, "b/", created)), 400, "InvalidArgument"},
		{"no event", notificationDoc(queueRule("a", testQueueARN, "")), 400, "MalformedXML"},
		{"queue with topic element", notificationDoc("<QueueConfiguration><Topic>" + testTopicARN + "</Topic><Event>" + created + "</Event></QueueConfiguration>"), 400, "MalformedXML"},
		{"bad filter name", notificationDoc("<QueueConfiguration><Queue>" + testQueueARN + "</Queue><Event>" + created +
			"</Event><Filter><S3Key><FilterRule><Name>middle</Name><Value>x</Value></FilterRule></S3Key></Filter></QueueConfiguration>"), 400, "InvalidArgument"},
		{"two prefixes", notificationDoc("<QueueConfiguration><Queue>" + testQueueARN + "</Queue><Event>" + created +
			"</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>a</Value></FilterRule><FilterRule><Name>Prefix</Name><Value>b</Value></FilterRule></S3Key></Filter></QueueConfiguration>"), 400, "InvalidArgument"},
		{"lambda", notificationDoc("<CloudFunctionConfiguration><Event>" + created + "</Event></CloudFunctionConfiguration>"), 501, "NotImplemented"},
		{"event bridge", notificationDoc("<EventBridgeConfiguration/>"), 501, "NotImplemented"},
		{"unknown element", notificationDoc("<Other/>"), 400, "MalformedXML"},
		{"malformed", "<NotificationConfiguration>", 400, "MalformedXML"},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/bkt?notification", tt.body, nil)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: PutBucketNotificationConfiguration = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
		}
	}
	if r := call(t, srv, http.MethodPut, "/missing?notification", notificationDoc(), nil); r.status != 404 || r.code != "NoSuchBucket" {
		t.Errorf("PutBucketNotificationConfiguration on a missing bucket = %d %q, want 404 NoSuchBucket", r.status, r.code)
	}
}

func TestBucketNotificationRoundTrip(t *testing.T) {
	n := &fakeNotifier{}
	srv := notifyServer(t, n)
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	empty := `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<NotificationConfiguration xmlns="` + s3Namespace + `"></NotificationConfiguration>`
	if r := call(t, srv, http.MethodGet, "/bkt?notification", "", nil); r.status != 200 || r.body != empty {
		t.Errorf("GetBucketNotificationConfiguration on a new bucket = %d %q, want 200 %q", r.status, r.body, empty)
	}
	rule := queueRule("one", testQueueARN, "in/", "s3:ObjectCreated:*")
	if r := call(t, srv, http.MethodPut, "/bkt?notification", notificationDoc(rule), nil); r.status != 200 {
		t.Fatalf("PutBucketNotificationConfiguration = %d, want 200", r.status)
	}
	want := `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<NotificationConfiguration xmlns="` + s3Namespace + `"><QueueConfiguration><Id>one</Id><Queue>` + testQueueARN +
		`</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>Prefix</Name><Value>in/</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>`
	r := call(t, srv, http.MethodGet, "/bkt?notification", "", nil)
	if r.status != 200 || r.body != want {
		t.Errorf("GetBucketNotificationConfiguration = %d %q, want 200 %q", r.status, r.body, want)
	}
	if _, set := r.header["Content-Type"]; set {
		t.Errorf("GetBucketNotificationConfiguration Content-Type = %q, want none", r.header.Get("Content-Type"))
	}
	_ = call(t, srv, http.MethodPut, "/bkt?notification", notificationDoc(), nil)
	if r := call(t, srv, http.MethodGet, "/bkt?notification", "", nil); r.body != empty {
		t.Errorf("GetBucketNotificationConfiguration after clearing = %q, want %q", r.body, empty)
	}
}

func TestNotificationNeedsNotifier(t *testing.T) {
	srv := notifyServer(t, nil)
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	if r := call(t, srv, http.MethodPut, "/bkt?notification", notificationDoc(queueRule("a", testQueueARN, "", "s3:ObjectCreated:*")), nil); r.status != 501 {
		t.Errorf("PutBucketNotificationConfiguration with a destination and no notifier = %d, want 501", r.status)
	}
	if r := call(t, srv, http.MethodPut, "/bkt?notification", notificationDoc(), nil); r.status != 200 {
		t.Errorf("PutBucketNotificationConfiguration with no destination and no notifier = %d, want 200", r.status)
	}
	if r := call(t, srv, http.MethodPut, "/bkt/k", "x", nil); r.status != 200 {
		t.Errorf("PutObject with no notifier = %d, want 200", r.status)
	}
}

func TestNotificationTestEvent(t *testing.T) {
	n := &fakeNotifier{}
	srv := notifyServer(t, n)
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	doc := notificationDoc(queueRule("a", testQueueARN, "", "s3:ObjectCreated:*"))
	_ = call(t, srv, http.MethodPut, "/bkt?notification", doc, nil)
	got := n.take()
	if len(got) != 1 || got[0].arn != testQueueARN {
		t.Fatalf("deliveries after the first PUT = %v, want one to %s", got, testQueueARN)
	}
	var msg map[string]string
	if err := json.Unmarshal([]byte(got[0].message), &msg); err != nil {
		t.Fatal(err)
	}
	if msg["Service"] != "Amazon S3" || msg["Event"] != "s3:TestEvent" || msg["Bucket"] != "bkt" || msg["Time"] == "" || msg["RequestId"] == "" || msg["HostId"] == "" {
		t.Errorf("test event = %v, want Service, Event s3:TestEvent, Bucket bkt, Time, RequestId, and HostId", msg)
	}
	_ = call(t, srv, http.MethodPut, "/bkt?notification", doc, nil)
	if got := n.take(); len(got) != 0 {
		t.Errorf("deliveries after repeating the PUT = %v, want none", got)
	}
}

// record is the part of an event record that the tests compare.
type record struct {
	EventName string
	S3        struct {
		ConfigurationID string `json:"configurationId"`
		Object          struct {
			Key        string
			Size       *int64
			ETag       string `json:"eTag"`
			Annotation *bool  `json:"hasObjectAnnotation"`
			Sequencer  string
		}
	}
}

func records(t *testing.T, sent []delivery) []record {
	t.Helper()
	var out []record
	for _, d := range sent {
		var e struct{ Records []record }
		if err := json.Unmarshal([]byte(d.message), &e); err != nil || len(e.Records) != 1 {
			t.Fatalf("event %q = %v records, %v, want one record", d.message, len(e.Records), err)
		}
		out = append(out, e.Records[0])
	}
	return out
}

func TestObjectEvents(t *testing.T) {
	n := &fakeNotifier{}
	srv := notifyServer(t, n)
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	doc := notificationDoc(queueRule("all", testQueueARN, "", "s3:ObjectCreated:*", "s3:ObjectRemoved:*"))
	_ = call(t, srv, http.MethodPut, "/bkt?notification", doc, nil)
	n.take()

	upload := startUpload(t, srv, "big", nil)
	part := call(t, srv, http.MethodPut, "/bkt/big?partNumber=1&uploadId="+upload, "data", nil)
	complete := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + part.header.Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	steps := []struct {
		name     string
		do       func()
		wantName string
		wantKey  string
		wantSize *int64
	}{
		{"put", func() { call(t, srv, http.MethodPut, "/bkt/a b/ü.txt", "hello", nil) }, "ObjectCreated:Put", "a+b/%C3%BC.txt", new(int64(5))},
		{"empty put", func() { call(t, srv, http.MethodPut, "/bkt/empty", "", nil) }, "ObjectCreated:Put", "empty", new(int64(0))},
		{"copy", func() {
			call(t, srv, http.MethodPut, "/bkt/copy", "", map[string]string{"x-amz-copy-source": "/bkt/empty"})
		}, "ObjectCreated:Copy", "copy", new(int64(0))},
		{"complete", func() { call(t, srv, http.MethodPost, "/bkt/big?uploadId="+upload, complete, nil) }, "ObjectCreated:CompleteMultipartUpload", "big", new(int64(4))},
		{"delete", func() { call(t, srv, http.MethodDelete, "/bkt/copy", "", nil) }, "ObjectRemoved:Delete", "copy", nil},
	}
	var last string
	for _, s := range steps {
		s.do()
		got := records(t, n.take())
		if len(got) != 1 {
			t.Errorf("%s: got %d events, want 1", s.name, len(got))
			continue
		}
		r := got[0]
		o := r.S3.Object
		if r.EventName != s.wantName || o.Key != s.wantKey || r.S3.ConfigurationID != "all" {
			t.Errorf("%s: event = %q key %q config %q, want %q key %q config all", s.name, r.EventName, o.Key, r.S3.ConfigurationID, s.wantName, s.wantKey)
		}
		if (o.Size == nil) != (s.wantSize == nil) || o.Size != nil && *o.Size != *s.wantSize {
			t.Errorf("%s: size = %v, want %v", s.name, o.Size, s.wantSize)
		}
		if removed := s.wantSize == nil; removed != (o.ETag == "") {
			t.Errorf("%s: eTag = %q, want it set only for a creation", s.name, o.ETag)
		}
		if copied := s.wantName == "ObjectCreated:Copy"; copied != (o.Annotation != nil) {
			t.Errorf("%s: hasObjectAnnotation = %v, want it set only for a copy", s.name, o.Annotation)
		}
		if o.Sequencer <= last {
			t.Errorf("%s: sequencer = %q, want it above %q", s.name, o.Sequencer, last)
		}
		last = o.Sequencer
	}

	// DeleteObjects raises one event per deleted key.
	body := `<Delete><Object><Key>a b/ü.txt</Key></Object><Object><Key>empty</Key></Object></Delete>`
	r := call(t, srv, http.MethodPost, "/bkt?delete", body, md5Header(body))
	if got := records(t, n.take()); r.status != 200 || len(got) != 2 || got[0].EventName != "ObjectRemoved:Delete" {
		t.Errorf("DeleteObjects = %d with %d events, want 200 with 2 ObjectRemoved:Delete events", r.status, len(got))
	}
}

func TestNotificationFilterAndFailure(t *testing.T) {
	n := &fakeNotifier{}
	srv := notifyServer(t, n)
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	doc := notificationDoc(queueRule("in", testQueueARN, "in/", "s3:ObjectCreated:Put"))
	_ = call(t, srv, http.MethodPut, "/bkt?notification", doc, nil)
	n.take()
	_ = call(t, srv, http.MethodPut, "/bkt/out/x", "x", nil)
	_ = call(t, srv, http.MethodDelete, "/bkt/out/x", "", nil)
	if got := n.take(); len(got) != 0 {
		t.Errorf("events for a key outside the prefix and for a delete = %v, want none", got)
	}
	n.fail = errors.New("queue is gone")
	if r := call(t, srv, http.MethodPut, "/bkt/in/x", "x", nil); r.status != 200 {
		t.Errorf("PutObject with a failing delivery = %d, want 200", r.status)
	}
	if got := n.take(); len(got) != 1 {
		t.Errorf("deliveries = %v, want 1", got)
	}
}

func TestNotificationMatches(t *testing.T) {
	filtered := func(prefix, suffix string, events ...string) notificationTarget {
		key := &notificationKey{Rules: []filterRule{{"Prefix", prefix}, {"Suffix", suffix}}}
		return notificationTarget{Events: events, Filter: &notificationFilter{Key: key}}
	}
	tests := []struct {
		name  string
		c     notificationTarget
		event string
		key   string
		want  bool
	}{
		{"wildcard", filtered("", "", "s3:ObjectCreated:*"), "ObjectCreated:Copy", "k", true},
		{"exact", filtered("", "", "s3:ObjectCreated:Put"), "ObjectCreated:Put", "k", true},
		{"other kind", filtered("", "", "s3:ObjectCreated:Put"), "ObjectCreated:Copy", "k", false},
		{"other family", filtered("", "", "s3:ObjectCreated:*"), "ObjectRemoved:Delete", "k", false},
		{"prefix hit", filtered("in/", "", "s3:ObjectCreated:*"), "ObjectCreated:Put", "in/a", true},
		{"prefix miss", filtered("in/", "", "s3:ObjectCreated:*"), "ObjectCreated:Put", "out/a", false},
		{"suffix hit", filtered("", ".txt", "s3:ObjectCreated:*"), "ObjectCreated:Put", "a.txt", true},
		{"suffix miss", filtered("", ".txt", "s3:ObjectCreated:*"), "ObjectCreated:Put", "a.png", false},
		{"both", filtered("in/", ".txt", "s3:ObjectCreated:*"), "ObjectCreated:Put", "in/a.png", false},
		{"no filter", notificationTarget{Events: []string{"s3:ObjectRemoved:*"}}, "ObjectRemoved:Delete", "k", true},
	}
	for _, tt := range tests {
		if got := tt.c.matches(tt.event, tt.key); got != tt.want {
			t.Errorf("%s: matches(%q, %q) = %v, want %v", tt.name, tt.event, tt.key, got, tt.want)
		}
	}
}

// cancelAfterPut cancels the request context once the real write has committed.
type cancelAfterPut struct {
	Store
	cancel context.CancelFunc
}

func (s *cancelAfterPut) PutObject(ctx context.Context, bucket, key string, body io.Reader, opts store.PutOptions) (store.ObjectInfo, error) {
	info, err := s.Store.PutObject(ctx, bucket, key, body, opts)
	s.cancel()
	return info, err
}

func TestEventSurvivesCanceledRequest(t *testing.T) {
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	st, err := store.Open(context.Background(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	n := &fakeNotifier{}
	wrapper := &cancelAfterPut{Store: st}
	opts := testOptions("")
	opts.Region, opts.Store, opts.Notifier = "us-east-1", wrapper, n
	h := New(opts)
	if err := st.CreateBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	cfg := store.BucketConfiguration{}
	cfg.XML = []byte(`<NotificationConfiguration><QueueConfiguration><Id>a</Id><Queue>` + testQueueARN + `</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`)
	if err := st.PutBucketConfiguration(context.Background(), "bkt", "notification", &cfg); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wrapper.cancel = cancel
	req := httptest.NewRequestWithContext(ctx, http.MethodPut, "/bkt/k", strings.NewReader("x"))
	signPayload(t, req, time.Now(), "x")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got := n.take(); len(got) != 1 {
		t.Errorf("deliveries after the request was canceled = %v, want 1", got)
	}
}

func TestDeliverBaseURL(t *testing.T) {
	tests := []struct {
		name, domain, host, path, want string
	}{
		{"path-style host starts with the bucket", "example.com", "example.com:9000", "/example/k", "http://example.com:9000"},
		{"virtual-hosted", "example.com", "bkt.example.com:9000", "/k", "http://example.com:9000"},
		{"virtual-hosted in another case", "example.com", "Bkt.Example.com:9000", "/k", "http://Example.com:9000"},
		{"no domain", "", "localhost:9000", "/bkt/k", "http://localhost:9000"},
	}
	for _, tt := range tests {
		n := &fakeNotifier{}
		opts := testOptions(tt.domain)
		opts.Notifier = n
		h := New(opts).(*handler)
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "http://"+tt.host+tt.path, nil)
		target := parseTarget(req, h.opts.Domain)
		h.deliver(context.Background(), req, target, notificationTarget{Queue: testQueueARN}, "m")
		if got := n.take(); len(got) != 1 || got[0].baseURL != tt.want {
			t.Errorf("%s: baseURL = %v, want %s", tt.name, got, tt.want)
		}
	}
}
