package topic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/queue"
)

func redrivePolicy(name string) string { return `{"deadLetterTargetArn":"` + queueARN(name) + `"}` }

func TestRedrivePolicyValidation(t *testing.T) {
	for _, tt := range []struct {
		name, topic, endpoint, value string
		want                         error
	}{
		{"standard", "t", "q", redrivePolicy("dead"), nil},
		{"standard accepts FIFO ARN", "t", "q", redrivePolicy("dead.fifo"), nil},
		{"FIFO endpoint", "t.fifo", "q.fifo", redrivePolicy("dead.fifo"), nil},
		{"FIFO endpoint rejects standard", "t.fifo", "q.fifo", redrivePolicy("dead"), ErrInvalidParameter},
		{"standard endpoint of FIFO topic", "t.fifo", "q", redrivePolicy("dead"), nil},
		{"standard endpoint rejects FIFO", "t.fifo", "q", redrivePolicy("dead.fifo"), ErrInvalidParameter},
		{"empty", "t", "q", "", ErrInvalidParameter}, {"object", "t", "q", "{}", ErrInvalidParameter},
		{"null", "t", "q", "null", ErrInvalidParameter}, {"array", "t", "q", "[]", ErrInvalidParameter},
		{"invalid JSON", "t", "q", "x", ErrInvalidParameter},
		{"unknown key", "t", "q", `{"other":"x"}`, ErrInvalidParameter},
		{"extra key", "t", "q", `{"deadLetterTargetArn":"` + queueARN("dead") + `","other":1}`, ErrInvalidParameter},
		{"number", "t", "q", `{"deadLetterTargetArn":1}`, ErrInvalidParameter},
		{"null target", "t", "q", `{"deadLetterTargetArn":null}`, ErrInvalidParameter},
		{"region", "t", "q", `{"deadLetterTargetArn":"arn:aws:sqs:us-west-2:000000000000:q"}`, ErrInvalidParameter},
		{"account", "t", "q", `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:111111111111:q"}`, ErrInvalidParameter},
		{"service", "t", "q", `{"deadLetterTargetArn":"arn:aws:sns:us-east-1:000000000000:q"}`, ErrInvalidParameter},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e, _ := newEngine(t)
			var arn string
			if isFIFO(tt.topic) {
				arn = mustFIFOTopic(t, e, tt.topic, nil)
			} else {
				arn = mustTopic(t, e, tt.topic)
			}
			_, _, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: arn, Protocol: protocolSQS, Endpoint: queueARN(tt.endpoint), Attributes: map[string]string{attrRedrive: tt.value}})
			if !errors.Is(err, tt.want) {
				t.Errorf("Subscribe RedrivePolicy %q error = %v, want %v", tt.value, err, tt.want)
			}
		})
	}
}

func TestRedrivePersistenceAndClear(t *testing.T) {
	dir := t.TempDir()
	fq := &fakeQueues{}
	e := openEngine(t, dir, fq)
	arn := mustTopic(t, e, "persist")
	policy := `{ "deadLetterTargetArn" : "` + queueARN("dead") + `" }`
	sub := mustSubscribe(t, e, arn, "source", map[string]string{attrRedrive: policy})
	e = openEngine(t, dir, fq)
	attrs, err := e.SubscriptionAttributes(t.Context(), sub)
	if err != nil {
		t.Fatal(err)
	}
	found := ""
	for _, a := range attrs {
		if a.Key == attrRedrive {
			found = a.Value
		}
	}
	if found != policy {
		t.Errorf("stored policy = %q, want %q", found, policy)
	}
	if err := e.SetSubscriptionAttribute(t.Context(), sub, attrRedrive, new("")); !errors.Is(err, ErrInvalidParameter) {
		t.Fatalf("empty policy error = %v, want invalid parameter", err)
	}
	if err := e.SetSubscriptionAttribute(t.Context(), sub, attrRedrive, nil); err != nil {
		t.Fatal(err)
	}
	e = openEngine(t, dir, fq)
	if _, ok := e.subs[sub].Attributes[attrRedrive]; ok {
		t.Fatal("cleared policy survived restart")
	}
}

type sendFunc func(context.Context, string, []queue.SendInput) ([]queue.SendResult, error)

func (f sendFunc) Send(ctx context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error) {
	return f(ctx, name, in)
}

func TestRedriveFailures(t *testing.T) {
	for _, mode := range []string{"success", "missing", "entry failure", "no result", "dead failure", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			e, fq := newEngine(t)
			arn := mustTopic(t, e, "failure")
			mustSubscribe(t, e, arn, "source", map[string]string{attrRedrive: redrivePolicy("dead")})
			var calls []string
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			e.queues = sendFunc(func(ctx context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error) {
				if !e.mu.TryLock() {
					t.Fatal("queue called under topic mutex")
				}
				e.mu.Unlock()
				calls = append(calls, name)
				if mode == "success" {
					return fq.Send(ctx, name, in)
				}
				if name == "dead" && mode != "dead failure" {
					return fq.Send(ctx, name, in)
				}
				if mode == "canceled" {
					cancel()
					return nil, ctx.Err()
				}
				if mode == "entry failure" {
					return []queue.SendResult{{Err: queue.ErrInvalidParameterValue}}, nil
				}
				if mode == "no result" {
					return nil, nil
				}
				return nil, queue.ErrQueueDoesNotExist
			})
			if _, err := e.Publish(ctx, publishInput(arn)); err != nil {
				t.Fatalf("Publish error = %v, want nil", err)
			}
			want := []string{"source", "dead"}
			if mode == "success" || mode == "canceled" {
				want = []string{"source"}
			}
			if !reflect.DeepEqual(calls, want) {
				t.Errorf("queue calls = %v, want %v", calls, want)
			}
		})
	}
}

func TestRedriveFIFOAndRaw(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "envelope", true: "raw"}[raw], func(t *testing.T) {
			e, fq := newEngine(t)
			arn := mustFIFOTopic(t, e, "fifo.fifo", nil)
			attrs := map[string]string{attrRedrive: redrivePolicy("dead.fifo")}
			if raw {
				attrs[attrRaw] = "true"
			}
			mustSubscribe(t, e, arn, "source.fifo", attrs)
			fq.missing["source.fifo"] = true
			in := fifoInput(arn, "body", "g", "d")
			in.Attributes = map[string]MessageAttribute{"color": {DataType: "String", StringValue: "blue"}}
			out, err := e.Publish(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.Publish(t.Context(), in); err != nil {
				t.Fatal(err)
			}
			if len(fq.sent) != 1 {
				t.Fatalf("dead-letter sends = %d, want 1 after duplicate", len(fq.sent))
			}
			m := fq.sent[0]
			if m.Queue != "dead.fifo" || m.In.GroupID != "g" || m.In.DeduplicationID != "d" {
				t.Fatalf("FIFO redrive = %+v, want original identifiers", m)
			}
			if raw {
				if m.In.Body != "body" || m.In.Attributes["color"].StringValue != "blue" {
					t.Fatalf("raw redrive = %+v", m.In)
				}
				return
			}
			var env map[string]any
			if err := json.Unmarshal([]byte(m.In.Body), &env); err != nil {
				t.Fatal(err)
			}
			if env["MessageId"] != out.MessageID || env["SequenceNumber"] != out.SequenceNumber || env["Signature"] != nil {
				t.Errorf("FIFO envelope = %v, want original unsigned publication", env)
			}
		})
	}
}

func TestHTTPRedrive(t *testing.T) {
	for _, tt := range []struct {
		name           string
		status         int
		cancel, raw    bool
		attempts, dead int
	}{
		{"success", 200, false, false, 1, 0}, {"recovers", 0, false, false, 2, 0}, {"permanent", 400, false, false, 1, 1},
		{"redirect", 302, false, false, 1, 1}, {"exhaustion", 503, false, false, 3, 1},
		{"throttled", 429, false, true, 3, 1}, {"shutdown", 503, true, false, 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, fq := newEngine(t)
				arn := mustTopic(t, e, "http")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var count atomic.Int32
				srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					n := count.Add(1)
					status := tt.status
					if status == 0 {
						status = http.StatusServiceUnavailable
						if n > 1 {
							status = http.StatusOK
						}
					}
					w.WriteHeader(status)
					if tt.cancel {
						cancel()
					}
				}))
				defer srv.Close()
				e.client = srv.Client()
				attrs := map[string]string{attrRedrive: redrivePolicy("dead")}
				if tt.raw {
					attrs[attrRaw] = "true"
				}
				sub, _ := subscribeHTTP(t, e, arn, srv.URL, attrs)
				confirmation := <-e.jobs
				if confirmation.dead.queue != "" {
					t.Fatal("confirmation has dead-letter destination")
				}
				if _, err := e.ConfirmSubscription(t.Context(), arn, e.subs[sub].Token, true); err != nil {
					t.Fatal(err)
				}
				in := publishInput(arn)
				in.Attributes = map[string]MessageAttribute{"binary": {DataType: "Binary", BinaryValue: []byte("abc")}}
				if _, err := e.Publish(t.Context(), in); err != nil {
					t.Fatal(err)
				}
				in.Attributes["binary"].BinaryValue[0] = 'z'
				j := <-e.jobs
				if err := e.SetSubscriptionAttribute(t.Context(), sub, attrRedrive, new(redrivePolicy("changed"))); err != nil {
					t.Fatal(err)
				}
				j.plan.delays = []time.Duration{time.Second, time.Second}
				e.runJob(ctx, j)
				if int(count.Load()) != tt.attempts || len(fq.sent) != tt.dead {
					t.Fatalf("attempts/dead sends = %d/%d, want %d/%d", count.Load(), len(fq.sent), tt.attempts, tt.dead)
				}
				if tt.dead == 1 && fq.sent[0].Queue != "dead" {
					t.Fatalf("queued destination = %q, want original dead", fq.sent[0].Queue)
				}
				if tt.dead == 1 && tt.raw && string(fq.sent[0].In.Attributes["binary"].BinaryValue) != "abc" {
					t.Fatal("queued raw attributes changed after Publish returned")
				}
			})
		})
	}
}

func TestHTTPRedriveQueueFull(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "full")
	sub, _ := subscribeHTTP(t, e, arn, testEndpoint, map[string]string{attrRedrive: redrivePolicy("dead")})
	<-e.jobs
	if _, err := e.ConfirmSubscription(t.Context(), arn, e.subs[sub].Token, true); err != nil {
		t.Fatal(err)
	}
	e.jobs = make(chan httpJob)
	if _, err := e.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatal(err)
	}
	if len(fq.sent) != 1 || fq.sent[0].Queue != "dead" {
		t.Fatalf("full queue redrive = %+v, want one dead-letter send", fq.sent)
	}
}

func TestRedriveFilterExclusion(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "filtered")
	mustSubscribe(t, e, arn, "missing", map[string]string{attrRedrive: redrivePolicy("dead"), attrFilter: `{"color":["red"]}`})
	fq.missing["missing"] = true
	in := publishInput(arn)
	in.Attributes = map[string]MessageAttribute{"color": {DataType: "String", StringValue: "blue"}}
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	if len(fq.sent) != 0 {
		t.Errorf("filtered redrive = %+v, want no delivery", fq.sent)
	}
}
