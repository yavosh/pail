package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const testRegion = "us-east-1"

func openEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	fsys, err := localdisk.Open(dir)
	if err != nil {
		t.Fatalf("localdisk.Open(%q) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	e, err := Open(t.Context(), fsys, testRegion)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", dir, err)
	}
	return e
}

func newEngine(t *testing.T) *Engine {
	t.Helper()
	return openEngine(t, t.TempDir())
}

func mustCreate(t *testing.T, e *Engine, name string, attrs map[string]string) {
	t.Helper()
	if err := e.CreateQueue(t.Context(), name, attrs, nil); err != nil {
		t.Fatalf("CreateQueue(%q, %v) error = %v", name, attrs, err)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	attrs := map[string]string{"VisibilityTimeout": "77", "Policy": `{"a":1}`}
	tags := map[string]string{"env": "test"}
	if err := e.CreateQueue(t.Context(), "Orders", attrs, tags); err != nil {
		t.Fatal(err)
	}
	wantAttrs, err := e.Attributes(t.Context(), "Orders", []string{"All"})
	if err != nil {
		t.Fatal(err)
	}

	e2 := openEngine(t, dir)
	if err := e2.Lookup(t.Context(), "Orders"); err != nil {
		t.Fatalf("Lookup(Orders) after reopen error = %v", err)
	}
	if err := e2.Lookup(t.Context(), "orders"); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Lookup(orders) error = %v, want ErrQueueDoesNotExist", err)
	}
	gotAttrs, err := e2.Attributes(t.Context(), "Orders", []string{"All"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotAttrs, wantAttrs) {
		t.Errorf("Attributes after reopen = %v, want %v", gotAttrs, wantAttrs)
	}
	gotTags, err := e2.Tags(t.Context(), "Orders")
	if err != nil || !reflect.DeepEqual(gotTags, tags) {
		t.Errorf("Tags after reopen = %v, %v; want %v", gotTags, err, tags)
	}
}

func TestOpenCorruptFile(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustCreate(t, e, "q", nil)
	files, err := filepath.Glob(filepath.Join(dir, "sqs", "queues", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("queue files = %v, %v; want one", files, err)
	}
	if err := os.WriteFile(files[0], []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsys, err := localdisk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	if _, err := Open(t.Context(), fsys, testRegion); err == nil {
		t.Error("Open with a corrupt queue file error = nil, want error")
	}
}

func TestDeleteQueuePersists(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustCreate(t, e, "q", nil)
	if err := e.DeleteQueue(t.Context(), "q"); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteQueue(t.Context(), "q"); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("second DeleteQueue error = %v, want ErrQueueDoesNotExist", err)
	}
	if err := openEngine(t, dir).Lookup(t.Context(), "q"); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Lookup after reopen error = %v, want ErrQueueDoesNotExist", err)
	}
}

func TestCreateQueue(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", map[string]string{"VisibilityTimeout": "10"})
	tests := []struct {
		name  string
		queue string
		attrs map[string]string
		want  error
	}{
		{"no attributes", "q", nil, nil},
		{"matching attribute", "q", map[string]string{"VisibilityTimeout": "10"}, nil},
		{"matching default", "q", map[string]string{"DelaySeconds": "0"}, nil},
		{"conflicting attribute", "q", map[string]string{"VisibilityTimeout": "11"}, ErrQueueNameExists},
		{"unset optional attribute", "q", map[string]string{"KmsMasterKeyId": "k"}, ErrQueueNameExists},
		{"empty name", "", nil, ErrInvalidName},
		{"81 characters", strings.Repeat("a", 81), nil, ErrInvalidName},
		{"80 characters", strings.Repeat("a", 80), nil, nil},
		{"space", "a b", nil, ErrInvalidName},
		{"dot", "a.b", nil, ErrInvalidName},
		{"fifo name", "q.fifo", nil, ErrUnsupported},
		{"fifo attribute", "n1", map[string]string{"FifoQueue": "true"}, ErrUnsupported},
		{"content dedup", "n2", map[string]string{"ContentBasedDeduplication": "true"}, ErrUnsupported},
		{"redrive policy", "n3", map[string]string{"RedrivePolicy": "{}"}, ErrUnsupported},
		{"redrive allow policy", "n4", map[string]string{"RedriveAllowPolicy": "{}"}, ErrUnsupported},
		{"unknown attribute", "n5", map[string]string{"Bogus": "1"}, ErrInvalidAttributeName},
		{"bad value", "n6", map[string]string{"DelaySeconds": "901"}, ErrInvalidAttributeValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := e.CreateQueue(t.Context(), tt.queue, tt.attrs, nil)
			if !errors.Is(err, tt.want) || (tt.want == nil && err != nil) {
				t.Errorf("CreateQueue(%q, %v) error = %v, want %v", tt.queue, tt.attrs, err, tt.want)
			}
		})
	}
}

func TestCreateQueueTooManyTags(t *testing.T) {
	e := newEngine(t)
	tags := map[string]string{}
	for i := range 51 {
		tags["k"+strconv.Itoa(i)] = "v"
	}
	if err := e.CreateQueue(t.Context(), "q", nil, tags); !errors.Is(err, ErrTooManyTags) {
		t.Errorf("CreateQueue with 51 tags error = %v, want ErrTooManyTags", err)
	}
	if err := e.Lookup(t.Context(), "q"); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("queue exists after failed create: %v", err)
	}
}

func TestAttributesDefaults(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	got, err := e.Attributes(t.Context(), "q", []string{"All"})
	if err != nil {
		t.Fatal(err)
	}
	created := got["CreatedTimestamp"]
	if _, err := strconv.ParseInt(created, 10, 64); err != nil {
		t.Errorf("CreatedTimestamp = %q, want decimal seconds", created)
	}
	want := map[string]string{
		"ApproximateNumberOfMessages":           "0",
		"ApproximateNumberOfMessagesDelayed":    "0",
		"ApproximateNumberOfMessagesNotVisible": "0",
		"CreatedTimestamp":                      created,
		"DelaySeconds":                          "0",
		"LastModifiedTimestamp":                 created,
		"MaximumMessageSize":                    "1048576",
		"MessageRetentionPeriod":                "345600",
		"QueueArn":                              "arn:aws:sqs:us-east-1:000000000000:q",
		"ReceiveMessageWaitTimeSeconds":         "0",
		"SqsManagedSseEnabled":                  "true",
		"VisibilityTimeout":                     "30",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Attributes(All) = %v, want %v", got, want)
	}
}

func TestAttributesSelection(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	got, err := e.Attributes(t.Context(), "q", []string{"VisibilityTimeout", "QueueArn", "Policy"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"VisibilityTimeout": "30", "QueueArn": e.ARN("q")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Attributes(list) = %v, want %v", got, want)
	}
	if _, err := e.Attributes(t.Context(), "q", []string{"Nope"}); !errors.Is(err, ErrInvalidAttributeName) {
		t.Errorf("Attributes(Nope) error = %v, want ErrInvalidAttributeName", err)
	}
	if _, err := e.Attributes(t.Context(), "missing", []string{"All"}); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Attributes(missing) error = %v, want ErrQueueDoesNotExist", err)
	}
	if err := e.SetAttributes(t.Context(), "q", map[string]string{"Policy": `{"Version":"x"}`}); err != nil {
		t.Fatal(err)
	}
	got, err = e.Attributes(t.Context(), "q", []string{"Policy"})
	if err != nil || got["Policy"] != `{"Version":"x"}` {
		t.Errorf("Attributes(Policy) = %v, %v; want the policy", got, err)
	}
}

func TestSetAttributesValidation(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	tests := []struct {
		attr, value string
		ok          bool
	}{
		{"DelaySeconds", "0", true}, {"DelaySeconds", "900", true},
		{"DelaySeconds", "901", false}, {"DelaySeconds", "-1", false}, {"DelaySeconds", "+5", false},
		{"DelaySeconds", "", false}, {"DelaySeconds", "1.5", false},
		{"MaximumMessageSize", "1024", true}, {"MaximumMessageSize", "1048576", true},
		{"MaximumMessageSize", "1023", false}, {"MaximumMessageSize", "1048577", false},
		{"MessageRetentionPeriod", "60", true}, {"MessageRetentionPeriod", "1209600", true},
		{"MessageRetentionPeriod", "59", false}, {"MessageRetentionPeriod", "1209601", false},
		{"ReceiveMessageWaitTimeSeconds", "0", true}, {"ReceiveMessageWaitTimeSeconds", "20", true},
		{"ReceiveMessageWaitTimeSeconds", "21", false}, {"ReceiveMessageWaitTimeSeconds", "x", false},
		{"VisibilityTimeout", "0", true}, {"VisibilityTimeout", "43200", true},
		{"VisibilityTimeout", "43201", false}, {"VisibilityTimeout", "99999999999", false},
		{"SqsManagedSseEnabled", "true", true}, {"SqsManagedSseEnabled", "false", true},
		{"SqsManagedSseEnabled", "yes", false}, {"SqsManagedSseEnabled", "True", false},
		{"KmsMasterKeyId", "alias/x", true}, {"KmsMasterKeyId", "", false},
		{"KmsDataKeyReusePeriodSeconds", "60", true}, {"KmsDataKeyReusePeriodSeconds", "86400", true},
		{"KmsDataKeyReusePeriodSeconds", "59", false}, {"KmsDataKeyReusePeriodSeconds", "86401", false},
		{"Policy", `{"a":[1]}`, true}, {"Policy", "{", false}, {"Policy", "", false},
	}
	for _, tt := range tests {
		err := e.SetAttributes(t.Context(), "q", map[string]string{tt.attr: tt.value})
		if tt.ok && err != nil {
			t.Errorf("SetAttributes(%s=%q) error = %v, want nil", tt.attr, tt.value, err)
		}
		if !tt.ok && !errors.Is(err, ErrInvalidAttributeValue) {
			t.Errorf("SetAttributes(%s=%q) error = %v, want ErrInvalidAttributeValue", tt.attr, tt.value, err)
		}
	}
	if err := e.SetAttributes(t.Context(), "q", map[string]string{"Bogus": "1"}); !errors.Is(err, ErrInvalidAttributeName) {
		t.Errorf("SetAttributes(Bogus) error = %v, want ErrInvalidAttributeName", err)
	}
	if err := e.SetAttributes(t.Context(), "q", map[string]string{"FifoQueue": "true"}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("SetAttributes(FifoQueue) error = %v, want ErrUnsupported", err)
	}
	if err := e.SetAttributes(t.Context(), "nope", nil); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("SetAttributes(nope) error = %v, want ErrQueueDoesNotExist", err)
	}
}

func TestSetAttributesUpdatesLastModified(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", nil)
		before, _ := e.Attributes(t.Context(), "q", []string{"All"})
		time.Sleep(10 * time.Second)
		if err := e.SetAttributes(t.Context(), "q", map[string]string{"VisibilityTimeout": "5"}); err != nil {
			t.Fatal(err)
		}
		after, _ := e.Attributes(t.Context(), "q", []string{"All"})
		if after["CreatedTimestamp"] != before["CreatedTimestamp"] {
			t.Errorf("CreatedTimestamp changed: %s -> %s", before["CreatedTimestamp"], after["CreatedTimestamp"])
		}
		b, _ := strconv.ParseInt(before["LastModifiedTimestamp"], 10, 64)
		if a, _ := strconv.ParseInt(after["LastModifiedTimestamp"], 10, 64); a != b+10 {
			t.Errorf("LastModifiedTimestamp = %d, want %d", a, b+10)
		}
		if after["VisibilityTimeout"] != "5" {
			t.Errorf("VisibilityTimeout = %s, want 5", after["VisibilityTimeout"])
		}
	})
}

func TestListQueues(t *testing.T) {
	e := newEngine(t)
	for _, n := range []string{"b2", "a1", "b1", "c1", "b3"} {
		mustCreate(t, e, n, nil)
	}
	tests := []struct {
		name     string
		prefix   string
		limit    int
		after    string
		want     []string
		wantNext string
	}{
		{"all sorted", "", 0, "", []string{"a1", "b1", "b2", "b3", "c1"}, ""},
		{"prefix", "b", 0, "", []string{"b1", "b2", "b3"}, ""},
		{"no match", "z", 0, "", nil, ""},
		{"first page", "", 2, "", []string{"a1", "b1"}, "b1"},
		{"second page", "", 2, "b1", []string{"b2", "b3"}, "b3"},
		{"last page", "", 2, "b3", []string{"c1"}, ""},
		{"exact page", "b", 3, "", []string{"b1", "b2", "b3"}, ""},
		{"after without limit", "", 0, "b2", []string{"b3", "c1"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, next := e.ListQueues(t.Context(), tt.prefix, tt.limit, tt.after)
			if !reflect.DeepEqual(got, tt.want) || next != tt.wantNext {
				t.Errorf("ListQueues(%q, %d, %q) = %v, %q; want %v, %q",
					tt.prefix, tt.limit, tt.after, got, next, tt.want, tt.wantNext)
			}
		})
	}
}

func TestTags(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	got, err := e.Tags(t.Context(), "q")
	if err != nil || len(got) != 0 {
		t.Fatalf("Tags(new queue) = %v, %v; want empty", got, err)
	}
	if err := e.Tag(t.Context(), "q", map[string]string{"a": "1", "b": "2"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Tag(t.Context(), "q", map[string]string{"b": "3"}); err != nil {
		t.Fatal(err)
	}
	got, _ = e.Tags(t.Context(), "q")
	if want := map[string]string{"a": "1", "b": "3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Tags = %v, want %v", got, want)
	}
	got["a"] = "mutated"
	if again, _ := e.Tags(t.Context(), "q"); again["a"] != "1" {
		t.Errorf("Tags returned internal state: a = %q", again["a"])
	}
	if err := e.Untag(t.Context(), "q", []string{"a", "missing"}); err != nil {
		t.Fatal(err)
	}
	got, _ = e.Tags(t.Context(), "q")
	if want := map[string]string{"b": "3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Tags after Untag = %v, want %v", got, want)
	}
	if _, err := e.Tags(t.Context(), "nope"); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Tags(nope) error = %v, want ErrQueueDoesNotExist", err)
	}
}

func TestTagLimit(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	fifty := map[string]string{}
	for i := range 50 {
		fifty["k"+strconv.Itoa(i)] = "v"
	}
	if err := e.Tag(t.Context(), "q", fifty); err != nil {
		t.Fatalf("Tag(50) error = %v", err)
	}
	if err := e.Tag(t.Context(), "q", map[string]string{"k0": "new"}); err != nil {
		t.Errorf("Tag(existing key at limit) error = %v", err)
	}
	if err := e.Tag(t.Context(), "q", map[string]string{"extra": "v"}); !errors.Is(err, ErrTooManyTags) {
		t.Errorf("Tag(51st) error = %v, want ErrTooManyTags", err)
	}
	got, _ := e.Tags(t.Context(), "q")
	if len(got) != 50 || got["k0"] != "new" {
		t.Errorf("Tags after limit = %d entries, want 50 with k0 updated", len(got))
	}
}

func TestPurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", nil)
		sendBodies(t, e, "q", "a", "b")
		if err := e.Purge(t.Context(), "q"); err != nil {
			t.Fatal(err)
		}
		if got := attr(t, e, "q", "ApproximateNumberOfMessages"); got != "0" {
			t.Errorf("messages after purge = %s, want 0", got)
		}
		sendBodies(t, e, "q", "c")
		time.Sleep(59 * time.Second)
		if err := e.Purge(t.Context(), "q"); !errors.Is(err, ErrPurgeInProgress) {
			t.Errorf("Purge after 59s error = %v, want ErrPurgeInProgress", err)
		}
		time.Sleep(time.Second)
		if err := e.Purge(t.Context(), "q"); err != nil {
			t.Errorf("Purge after 60s error = %v, want nil", err)
		}
		if err := e.Purge(t.Context(), "nope"); !errors.Is(err, ErrQueueDoesNotExist) {
			t.Errorf("Purge(nope) error = %v, want ErrQueueDoesNotExist", err)
		}
	})
}

// attr returns one attribute of a queue.
func attr(t *testing.T, e *Engine, name, attribute string) string {
	t.Helper()
	got, err := e.Attributes(t.Context(), name, []string{attribute})
	if err != nil {
		t.Fatalf("Attributes(%q, %q) error = %v", name, attribute, err)
	}
	return got[attribute]
}

func sendBodies(t *testing.T, e *Engine, name string, bodies ...string) {
	t.Helper()
	in := make([]SendInput, len(bodies))
	for i, b := range bodies {
		in[i] = SendInput{Body: b}
	}
	res, err := e.Send(context.Background(), name, in)
	if err != nil {
		t.Fatalf("Send(%q) error = %v", name, err)
	}
	for i, r := range res {
		if r.Err != nil {
			t.Fatalf("Send(%q) entry %d error = %v", name, i, r.Err)
		}
	}
}
