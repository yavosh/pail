package topic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/queue"
)

func mustFIFOTopic(t *testing.T, e *Engine, name string, attrs map[string]string) string {
	t.Helper()
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["FifoTopic"] = "true"
	arn, err := e.CreateTopic(t.Context(), name, attrs, nil)
	if err != nil {
		t.Fatalf("CreateTopic(%q, %v) error = %v", name, attrs, err)
	}
	return arn
}

func fifoInput(arn, message, group, dedup string) PublishInput {
	return PublishInput{TopicARN: arn, Message: message, GroupID: group, DeduplicationID: dedup, BaseURL: "http://localhost:9000"}
}

func TestFIFOTopicAttributes(t *testing.T) {
	e, _ := newEngine(t)
	for _, tt := range []struct {
		name  string
		attrs map[string]string
		want  error
	}{
		{"orders.fifo", map[string]string{"FifoTopic": "true"}, nil},
		{strings.Repeat("a", 251) + ".fifo", map[string]string{"FifoTopic": "true"}, nil},
		{strings.Repeat("a", 252) + ".fifo", map[string]string{"FifoTopic": "true"}, ErrInvalidParameter},
		{".fifo", map[string]string{"FifoTopic": "true"}, ErrInvalidParameter},
		{"missing.fifo", nil, ErrInvalidParameter},
		{"false.fifo", map[string]string{"FifoTopic": "false"}, ErrInvalidParameter},
		{"standard", map[string]string{"FifoTopic": "true"}, ErrInvalidParameter},
		{"badbool.fifo", map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "yes"}, ErrInvalidParameter},
		{"badscope.fifo", map[string]string{"FifoTopic": "true", "FifoThroughputScope": "messageGroup"}, ErrInvalidParameter},
		{"trace.fifo", map[string]string{"FifoTopic": "true", "TracingConfig": "Active"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.CreateTopic(t.Context(), tt.name, tt.attrs, nil)
			if !errors.Is(err, tt.want) {
				t.Errorf("CreateTopic(%q, %v) error = %v, want %v", tt.name, tt.attrs, err, tt.want)
			}
		})
	}
	arn := mustFIFOTopic(t, e, "orders.fifo", map[string]string{"ContentBasedDeduplication": "false", "FifoThroughputScope": "Topic"})
	attrs, err := e.TopicAttributes(t.Context(), arn)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, a := range attrs {
		got[a.Key] = a.Value
	}
	for k, want := range map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"} {
		if got[k] != want {
			t.Errorf("attribute %s = %q, want %q", k, got[k], want)
		}
	}
	if _, ok := got["FifoThroughputScope"]; ok {
		t.Error("default attributes include FifoThroughputScope, want it omitted until set")
	}
	for _, tt := range []struct {
		name, value string
		want        error
	}{
		{"FifoTopic", "false", ErrInvalidParameter},
		{"FifoTopic", "true", nil},
		{"ContentBasedDeduplication", "true", nil},
		{"ContentBasedDeduplication", "", ErrInvalidParameter},
		{"FifoThroughputScope", "Topic", nil},
		{"FifoThroughputScope", "MessageGroup", nil},
		{"FifoThroughputScope", "Topic", ErrInvalidParameter},
	} {
		if err := e.SetTopicAttribute(t.Context(), arn, tt.name, tt.value); !errors.Is(err, tt.want) {
			t.Errorf("SetTopicAttribute(%s, %q) error = %v, want %v", tt.name, tt.value, err, tt.want)
		}
	}
}

func TestFIFOPublishValidation(t *testing.T) {
	e, q := newEngine(t)
	arn := mustFIFOTopic(t, e, "orders.fifo", nil)
	mustSubscribe(t, e, arn, "q.fifo", nil)
	for _, tt := range []struct {
		name, group, dedup string
		want               error
	}{
		{"missing group", "", "d", ErrInvalidParameter},
		{"missing dedup", "g", "", ErrInvalidParameter},
		{"group too long", strings.Repeat("g", 129), "d", ErrInvalidParameter},
		{"dedup too long", "g", strings.Repeat("d", 129), ErrInvalidParameter},
		{"group space", "a b", "d", ErrInvalidParameter},
		{"dedup space", "g", "a b", ErrInvalidParameter},
		{"dedup non ASCII", "g", "é", ErrInvalidParameter},
		{"valid limits", strings.Repeat("g", 128), strings.Repeat("d", 128), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := len(q.sent)
			out, err := e.Publish(t.Context(), fifoInput(arn, "hello", tt.group, tt.dedup))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Publish error = %v, want %v", err, tt.want)
			}
			if err != nil && (out != (PublishOutput{}) || len(q.sent) != before) {
				t.Errorf("invalid publish = %+v, deliveries = %d, want no result or delivery", out, len(q.sent)-before)
			}
		})
	}
}

func TestFIFODeduplication(t *testing.T) {
	for _, scope := range []string{"Topic", "MessageGroup"} {
		t.Run(scope, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, q := newEngine(t)
				arn := mustFIFOTopic(t, e, "orders.fifo", map[string]string{"FifoThroughputScope": scope})
				mustSubscribe(t, e, arn, "standard", map[string]string{attrRaw: "true"})
				publish := func(group string) PublishOutput {
					t.Helper()
					out, err := e.Publish(t.Context(), fifoInput(arn, "body", group, "d"))
					if err != nil {
						t.Fatal(err)
					}
					return out
				}
				first := publish("a")
				time.Sleep(4 * time.Minute)
				if got := publish("a"); got != first {
					t.Errorf("duplicate = %+v, want %+v", got, first)
				}
				other := publish("b")
				wantCount := 1
				if scope == "MessageGroup" {
					wantCount = 2
				}
				if len(q.sent) != wantCount || (other == first) != (scope == "Topic") {
					t.Errorf("scope %s: deliveries = %d, result = %+v, first = %+v", scope, len(q.sent), other, first)
				}
				time.Sleep(time.Minute)
				if got := publish("a"); got == first {
					t.Errorf("expired duplicate = %+v, want new identifiers", got)
				}
				if len(q.sent) != wantCount+1 {
					t.Errorf("deliveries after expiry = %d, want %d", len(q.sent), wantCount+1)
				}
			})
		})
	}
}

func TestFIFOContentDeduplicationAndFilters(t *testing.T) {
	e, q := newEngine(t)
	arn := mustFIFOTopic(t, e, "orders.fifo", map[string]string{"ContentBasedDeduplication": "true"})
	mustSubscribe(t, e, arn, "q.fifo", map[string]string{attrRaw: "true", attrFilter: `{"color":["blue"]}`})
	in := fifoInput(arn, "body", "g", "")
	in.Attributes = map[string]MessageAttribute{"color": {DataType: "String", StringValue: "red"}}
	first, err := e.Publish(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	in.Attributes["color"] = MessageAttribute{DataType: "String", StringValue: "blue"}
	duplicate, err := e.Publish(t.Context(), in)
	if err != nil || duplicate != first || len(q.sent) != 0 {
		t.Fatalf("filtered duplicate = %+v, %v, deliveries %d; want %+v, nil, 0", duplicate, err, len(q.sent), first)
	}
	in.DeduplicationID = "override"
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	in.Message = "new body"
	in.DeduplicationID = ""
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(in.Message))
	want := hex.EncodeToString(sum[:])
	if len(q.sent) != 2 || q.sent[0].In.DeduplicationID != "override" || q.sent[1].In.DeduplicationID != want {
		t.Fatalf("deliveries = %+v, want override and %s", q.sent, want)
	}
	in.DeduplicationID = want
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	if len(q.sent) != 2 {
		t.Errorf("explicit hash duplicate delivered; got %d messages, want 2", len(q.sent))
	}
}

func TestFIFOPersistence(t *testing.T) {
	dir := t.TempDir()
	q := &fakeQueues{}
	e := openEngine(t, dir, q)
	arn := mustFIFOTopic(t, e, "orders.fifo", map[string]string{"ContentBasedDeduplication": "true", "FifoThroughputScope": "MessageGroup"})
	mustSubscribe(t, e, arn, "q.fifo", map[string]string{attrRaw: "true"})
	in := fifoInput(arn, "body", "g", "")
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	want, err := e.TopicAttributes(t.Context(), arn)
	if err != nil {
		t.Fatal(err)
	}
	again := openEngine(t, dir, q)
	got, err := again.TopicAttributes(t.Context(), arn)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("reopened attributes = %v, %v; want %v", got, err, want)
	}
	if _, err := again.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	if len(q.sent) != 2 {
		t.Errorf("deliveries after reopen = %d, want 2", len(q.sent))
	}
	if err := again.DeleteTopic(t.Context(), arn); err != nil {
		t.Fatal(err)
	}
	mustFIFOTopic(t, again, "orders.fifo", nil)
	mustSubscribe(t, again, arn, "q.fifo", nil)
	in.DeduplicationID = "fresh"
	if _, err := again.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
}

// gatedQueues holds the first delivery so a concurrent publish can attempt to overtake it.
type gatedQueues struct {
	fakeQueues
	entered, release chan struct{}
	once             sync.Once
	check            func()
}

func (q *gatedQueues) Send(ctx context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error) {
	q.once.Do(func() { close(q.entered); <-q.release })
	q.check()
	return q.fakeQueues.Send(ctx, name, in)
}

func TestFIFOConcurrentDeliveryOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		arn := mustFIFOTopic(t, e, "orders.fifo", nil)
		mustSubscribe(t, e, arn, "a.fifo", nil)
		mustSubscribe(t, e, arn, "b.fifo", nil)
		q := &gatedQueues{entered: make(chan struct{}), release: make(chan struct{})}
		q.check = func() {
			if _, err := e.TopicAttributes(t.Context(), arn); err != nil {
				t.Error(err)
			}
		}
		e.queues = q
		var wg sync.WaitGroup
		var published [2]PublishOutput
		publish := func(i int) {
			defer wg.Done()
			out, err := e.Publish(t.Context(), fifoInput(arn, fmt.Sprint(i), "g", fmt.Sprint(i)))
			if err != nil {
				t.Error(err)
			}
			published[i-1] = out
		}
		wg.Add(1)
		go publish(1)
		<-q.entered
		wg.Add(1)
		go publish(2)
		synctest.Wait()
		close(q.release)
		wg.Wait()
		got := map[string][]string{}
		for _, sent := range q.sent {
			var env map[string]string
			if err := json.Unmarshal([]byte(sent.In.Body), &env); err != nil {
				t.Fatal(err)
			}
			got[sent.Queue] = append(got[sent.Queue], env["Message"]+":"+env["SequenceNumber"])
		}
		for _, name := range []string{"a.fifo", "b.fifo"} {
			if want := []string{"1:" + published[0].SequenceNumber, "2:" + published[1].SequenceNumber}; !reflect.DeepEqual(got[name], want) {
				t.Errorf("queue %s = %v, want %v", name, got[name], want)
			}
		}
	})
}

func TestFIFOInvalidSubscriptions(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustFIFOTopic(t, e, "orders.fifo", nil)
	for _, tt := range []struct{ protocol, endpoint string }{
		{"http", "http://127.0.0.1:1/hook"},
		{"https", "https://127.0.0.1:1/hook"},
	} {
		_, _, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: arn, Protocol: tt.protocol, Endpoint: tt.endpoint})
		if !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("Subscribe(%s) error = %v, want InvalidParameter", tt.protocol, err)
		}
	}
	subs, _, err := e.ListSubscriptionsByTopic(t.Context(), arn, "")
	if err != nil || len(subs) != 0 {
		t.Errorf("subscriptions = %v, %v; want empty", subs, err)
	}
}

func TestFIFOPublishWaitingCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, q := newEngine(t)
		arn := mustFIFOTopic(t, e, "orders.fifo", nil)
		mustSubscribe(t, e, arn, "q.fifo", nil)
		e.fifoPublish <- struct{}{}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := e.Publish(ctx, fifoInput(arn, "body", "g", "d"))
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("waiting Publish error = %v, want context.Canceled", err)
		}
		<-e.fifoPublish
		if len(q.sent) != 0 {
			t.Errorf("canceled publish delivered %d messages, want 0", len(q.sent))
		}
		if _, err := e.Publish(t.Context(), fifoInput(arn, "body", "g", "d")); err != nil {
			t.Fatal(err)
		}
		if len(q.sent) != 1 {
			t.Errorf("retry delivered %d messages, want 1", len(q.sent))
		}
	})
}

func TestFIFOBatchValidationBeforeDelivery(t *testing.T) {
	for _, tt := range []struct {
		name string
		bad  PublishEntry
	}{
		{"missing group", PublishEntry{ID: "bad", Message: "body", DeduplicationID: "bad"}},
		{"missing dedup", PublishEntry{ID: "bad", Message: "body", GroupID: "g"}},
		{"invalid dedup", PublishEntry{ID: "bad", Message: "body", GroupID: "g", DeduplicationID: "bad id"}},
		{"empty message", PublishEntry{ID: "bad", GroupID: "g", DeduplicationID: "bad"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e, q := newEngine(t)
			arn := mustFIFOTopic(t, e, "orders.fifo", nil)
			mustSubscribe(t, e, arn, "q.fifo", nil)
			good := PublishEntry{ID: "ok", Message: "body", GroupID: "g", DeduplicationID: "ok"}
			got, err := e.PublishBatch(t.Context(), arn, []PublishEntry{good, tt.bad}, "http://localhost:9000")
			if !errors.Is(err, ErrInvalidParameter) || len(got) != 0 || len(q.sent) != 0 {
				t.Fatalf("PublishBatch = %v, %v; deliveries = %d, want whole-batch rejection without delivery", got, err, len(q.sent))
			}
			got, err = e.PublishBatch(t.Context(), arn, []PublishEntry{good}, "http://localhost:9000")
			if err != nil || len(got) != 1 || got[0].Err != nil || len(q.sent) != 1 {
				t.Errorf("retry = %v, %v; deliveries = %d, want one success without deduplication", got, err, len(q.sent))
			}
		})
	}
}
