package queue

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

var fifoAttrs = map[string]string{"FifoQueue": "true"}

// sendOne sends one entry and returns its result.
func sendOne(t *testing.T, e *Engine, name string, in SendInput) SendResult {
	t.Helper()
	res, err := e.Send(t.Context(), name, []SendInput{in})
	if err != nil {
		t.Fatalf("Send(%q, %+v) error = %v", name, in, err)
	}
	return res[0]
}

// fifoMsg builds a FIFO send whose deduplication ID is its body.
func fifoMsg(body, group string) SendInput {
	return SendInput{Body: body, GroupID: group, DeduplicationID: body}
}

func bodies(msgs []Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Body)
	}
	return out
}

func TestFifoCreateRules(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "a.fifo", fifoAttrs)
	tests := []struct {
		name  string
		queue string
		attrs map[string]string
		want  error
	}{
		{"fifo queue", "b.fifo", fifoAttrs, nil},
		{"same again", "a.fifo", fifoAttrs, nil},
		{"fifo name without FifoQueue", "c.fifo", nil, ErrInvalidParameterValue},
		{"FifoQueue on a standard name", "c", fifoAttrs, ErrInvalidParameterValue},
		{"80 characters", strings.Repeat("a", 75) + ".fifo", fifoAttrs, nil},
		{"81 characters", strings.Repeat("a", 76) + ".fifo", fifoAttrs, ErrInvalidName},
		{"only the suffix", ".fifo", fifoAttrs, ErrInvalidName},
		{"bad character", "a b.fifo", fifoAttrs, ErrInvalidName},
		{"dot before the suffix", "a.b.fifo", fifoAttrs, ErrInvalidName},
		{"content dedup on a fifo queue", "d.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}, nil},
		{"bad content dedup value", "e.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "x"}, ErrInvalidAttributeValue},
		{"bad scope", "f.fifo", map[string]string{"FifoQueue": "true", "DeduplicationScope": "group"}, ErrInvalidAttributeValue},
		{"bad throughput limit", "g.fifo", map[string]string{"FifoQueue": "true", "FifoThroughputLimit": "perGroup"}, ErrInvalidAttributeValue},
		{"conflicting scope", "a.fifo", map[string]string{"FifoQueue": "true", "DeduplicationScope": "messageGroup"}, ErrQueueNameExists},
		{"scope on a standard queue", "h", map[string]string{"DeduplicationScope": "queue"}, ErrInvalidAttributeName},
		{"throughput limit on a standard queue", "i", map[string]string{"FifoThroughputLimit": "perQueue"}, ErrInvalidAttributeName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := e.CreateQueue(t.Context(), tt.queue, tt.attrs, nil)
			if !errors.Is(err, tt.want) || (tt.want == nil && err != nil) {
				t.Errorf("CreateQueue(%q, %v) error = %v, want %v", tt.queue, tt.attrs, err, tt.want)
			}
		})
	}
	if err := e.SetAttributes(t.Context(), "a.fifo", map[string]string{"FifoQueue": "true"}); !errors.Is(err, ErrInvalidAttributeValue) {
		t.Errorf("SetAttributes(FifoQueue) error = %v, want ErrInvalidAttributeValue", err)
	}
	if err := e.SetAttributes(t.Context(), "a.fifo", map[string]string{"ContentBasedDeduplication": "true", "FifoThroughputLimit": "perMessageGroupId"}); err != nil {
		t.Errorf("SetAttributes(FIFO attributes) error = %v, want nil", err)
	}
	if err := e.SetAttributes(t.Context(), "missing.fifo", map[string]string{"ContentBasedDeduplication": "true"}); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("SetAttributes(missing queue) error = %v, want ErrQueueDoesNotExist", err)
	}
	mustCreate(t, e, "std", nil)
	if err := e.SetAttributes(t.Context(), "std", map[string]string{"DeduplicationScope": "queue"}); !errors.Is(err, ErrInvalidAttributeName) {
		t.Errorf("SetAttributes(FIFO attribute on a standard queue) error = %v, want ErrInvalidAttributeName", err)
	}
}

func TestFifoAttributes(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "a.fifo", fifoAttrs)
	mustCreate(t, e, "std", nil)
	got, err := e.Attributes(t.Context(), "a.fifo", []string{"All"})
	if err != nil {
		t.Fatalf("Attributes(a.fifo, All) error = %v", err)
	}
	want := map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "queue", "FifoThroughputLimit": "perQueue"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attribute %s = %q, want %q", k, got[k], v)
		}
	}
	for _, k := range []string{"RedrivePolicy", "RedriveAllowPolicy"} {
		if _, ok := got[k]; ok {
			t.Errorf("attribute %s is present, want absent until set", k)
		}
	}
	std, err := e.Attributes(t.Context(), "std", []string{"All"})
	if err != nil {
		t.Fatalf("Attributes(std, All) error = %v", err)
	}
	for k := range want {
		if _, ok := std[k]; ok {
			t.Errorf("standard queue reports %s", k)
		}
	}
	if len(std) != 12 {
		t.Errorf("standard queue All has %d attributes (%v), want 12", len(std), std)
	}
}

func TestFifoSendRules(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "plain.fifo", fifoAttrs)
	mustCreate(t, e, "cbd.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	long := strings.Repeat("a", 129)
	tests := []struct {
		name  string
		queue string
		in    SendInput
		want  error
	}{
		{"valid", "plain.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d"}, nil},
		{"missing group", "plain.fifo", SendInput{Body: "x", DeduplicationID: "d"}, ErrMissingParameter},
		{"missing dedup id", "plain.fifo", SendInput{Body: "x", GroupID: "g"}, ErrInvalidParameterValue},
		{"missing dedup id with content-based", "cbd.fifo", SendInput{Body: "x", GroupID: "g"}, nil},
		{"explicit dedup id with content-based", "cbd.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d"}, nil},
		{"per-message delay", "plain.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d", DelaySeconds: new(0)}, ErrInvalidParameterValue},
		{"group with a space", "plain.fifo", SendInput{Body: "x", GroupID: "a b", DeduplicationID: "d"}, ErrInvalidParameterValue},
		{"group with a non-ASCII character", "plain.fifo", SendInput{Body: "x", GroupID: "é", DeduplicationID: "d"}, ErrInvalidParameterValue},
		{"group of 129 characters", "plain.fifo", SendInput{Body: "x", GroupID: long, DeduplicationID: "d"}, ErrInvalidParameterValue},
		{"group of 128 characters", "plain.fifo", SendInput{Body: "x", GroupID: long[1:], DeduplicationID: "d"}, nil},
		{"punctuation group", "plain.fifo", SendInput{Body: "x", GroupID: "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", DeduplicationID: "d"}, nil},
		{"dedup id with a space", "plain.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "a b"}, ErrInvalidParameterValue},
		{"dedup id of 129 characters", "plain.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: long}, ErrInvalidParameterValue},
		{"dedup id on a standard queue", "std", SendInput{Body: "x", DeduplicationID: "d"}, ErrInvalidParameterValue},
		{"group on a standard queue", "std", SendInput{Body: "x", GroupID: "g"}, nil},
	}
	mustCreate(t, e, "std", nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := sendOne(t, e, tt.queue, tt.in)
			if !errors.Is(res.Err, tt.want) || (tt.want == nil && res.Err != nil) {
				t.Errorf("Send(%q, %+v) error = %v, want %v", tt.queue, tt.in, res.Err, tt.want)
			}
		})
	}
}

func TestFifoQueueDelayApplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "DelaySeconds": "10"})
		sendOne(t, e, "q.fifo", fifoMsg("a", "g"))
		if got := receive(t, e, "q.fifo", ReceiveInput{}); len(got) != 0 {
			t.Fatalf("Receive before the queue delay = %v, want none", bodies(got))
		}
		time.Sleep(10 * time.Second)
		if got := receive(t, e, "q.fifo", ReceiveInput{}); len(got) != 1 {
			t.Errorf("Receive after the queue delay = %v, want a", bodies(got))
		}
	})
}

func TestFifoDeduplication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
		first := sendOne(t, e, "q.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d1"})
		time.Sleep(4*time.Minute + 59*time.Second)
		dup := sendOne(t, e, "q.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d1"})
		if dup.Err != nil || dup.MessageID != first.MessageID || dup.SequenceNumber != first.SequenceNumber || dup.MD5OfBody != first.MD5OfBody {
			t.Errorf("duplicate at 4m59s = %+v, want the first result %+v", dup, first)
		}
		if got := attr(t, e, "q.fifo", "ApproximateNumberOfMessages"); got != "1" {
			t.Errorf("messages after a duplicate = %s, want 1", got)
		}
		time.Sleep(time.Second)
		again := sendOne(t, e, "q.fifo", SendInput{Body: "x", GroupID: "g", DeduplicationID: "d1"})
		if again.MessageID == first.MessageID || again.SequenceNumber <= first.SequenceNumber {
			t.Errorf("send at 5m = %+v, want a new message after %+v", again, first)
		}
		if got := attr(t, e, "q.fifo", "ApproximateNumberOfMessages"); got != "2" {
			t.Errorf("messages after the window = %s, want 2", got)
		}
	})
}

func TestFifoContentBasedDeduplication(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	a := sendOne(t, e, "q.fifo", SendInput{Body: "same", GroupID: "g"})
	b := sendOne(t, e, "q.fifo", SendInput{Body: "same", GroupID: "other"})
	c := sendOne(t, e, "q.fifo", SendInput{Body: "different", GroupID: "g"})
	if b.MessageID != a.MessageID {
		t.Errorf("same body in another group = %+v, want duplicate of %+v under queue scope", b, a)
	}
	if c.MessageID == a.MessageID {
		t.Errorf("different body = %+v, want a new message", c)
	}
	got := receive(t, e, "q.fifo", ReceiveInput{Max: 10})
	if len(got) != 2 || got[0].DeduplicationID != contentHash("same") {
		t.Errorf("Receive = %+v, want 2 messages, the first with the body hash as DeduplicationID", got)
	}
}

func TestFifoDeduplicationScope(t *testing.T) {
	tests := []struct {
		scope     string
		wantCount string
	}{
		{"queue", "1"},
		{"messageGroup", "2"},
	}
	for _, tt := range tests {
		t.Run(tt.scope, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "DeduplicationScope": tt.scope})
			sendOne(t, e, "q.fifo", SendInput{Body: "x", GroupID: "a", DeduplicationID: "d"})
			sendOne(t, e, "q.fifo", SendInput{Body: "x", GroupID: "b", DeduplicationID: "d"})
			if got := attr(t, e, "q.fifo", "ApproximateNumberOfMessages"); got != tt.wantCount {
				t.Errorf("messages with scope %s = %s, want %s", tt.scope, got, tt.wantCount)
			}
		})
	}
}

func TestFifoSequenceNumbers(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	res, err := e.Send(t.Context(), "q.fifo", []SendInput{fifoMsg("a", "g"), fifoMsg("b", "g"), fifoMsg("c", "h")})
	if err != nil {
		t.Fatalf("Send error = %v", err)
	}
	if res[0].SequenceNumber != "00000000000000000001" || res[1].SequenceNumber != "00000000000000000002" || res[2].SequenceNumber != "00000000000000000003" {
		t.Errorf("sequence numbers = %q, %q, %q; want 1, 2, 3 padded to 20 digits", res[0].SequenceNumber, res[1].SequenceNumber, res[2].SequenceNumber)
	}
	got := receive(t, e, "q.fifo", ReceiveInput{Max: 10})
	if len(got) != 3 || got[1].SequenceNumber != res[1].SequenceNumber || got[1].GroupID != "g" || got[1].DeduplicationID != "b" {
		t.Errorf("Receive = %+v, want messages that carry their sequence number, group, and deduplication ID", got)
	}
	mustCreate(t, e, "std", nil)
	if r := sendOne(t, e, "std", SendInput{Body: "x"}); r.SequenceNumber != "" {
		t.Errorf("standard queue SequenceNumber = %q, want empty", r.SequenceNumber)
	}
}

func TestFifoGroupLocking(t *testing.T) {
	t.Run("in flight message locks its group", func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		sendOne(t, e, "q.fifo", fifoMsg("a1", "a"))
		sendOne(t, e, "q.fifo", fifoMsg("a2", "a"))
		first := receive(t, e, "q.fifo", ReceiveInput{Max: 1})
		if got := bodies(first); len(got) != 1 || got[0] != "a1" {
			t.Fatalf("first Receive = %v, want a1", got)
		}
		if got := receive(t, e, "q.fifo", ReceiveInput{Max: 1}); len(got) != 0 {
			t.Errorf("Receive while a1 is in flight = %v, want none", bodies(got))
		}
		if errs, err := e.Delete(t.Context(), "q.fifo", []string{first[0].ReceiptHandle}); err != nil || errs[0] != nil {
			t.Fatalf("Delete(a1) = %v, %v", errs, err)
		}
		if got := receive(t, e, "q.fifo", ReceiveInput{Max: 1}); len(got) != 1 || got[0].Body != "a2" {
			t.Errorf("Receive after Delete(a1) = %v, want a2", bodies(got))
		}
	})
	t.Run("visibility expiry returns the first message first", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "VisibilityTimeout": "10"})
			sendOne(t, e, "q.fifo", fifoMsg("a1", "a"))
			sendOne(t, e, "q.fifo", fifoMsg("a2", "a"))
			receive(t, e, "q.fifo", ReceiveInput{Max: 1})
			time.Sleep(10 * time.Second)
			got := receive(t, e, "q.fifo", ReceiveInput{Max: 1})
			if len(got) != 1 || got[0].Body != "a1" || got[0].ReceiveCount != 2 {
				t.Errorf("Receive after expiry = %+v, want a1 with count 2", got)
			}
		})
	})
	t.Run("groups interleave", func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		for _, m := range []SendInput{fifoMsg("a1", "a"), fifoMsg("a2", "a"), fifoMsg("b1", "b"), fifoMsg("b2", "b")} {
			sendOne(t, e, "q.fifo", m)
		}
		first := receive(t, e, "q.fifo", ReceiveInput{Max: 1})
		second := receive(t, e, "q.fifo", ReceiveInput{Max: 1})
		third := receive(t, e, "q.fifo", ReceiveInput{Max: 1})
		if bodies(first)[0] != "a1" || bodies(second)[0] != "b1" || len(third) != 0 {
			t.Errorf("Receives = %v, %v, %v; want a1, b1, none", bodies(first), bodies(second), bodies(third))
		}
	})
	t.Run("one receive returns consecutive messages of a group", func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		for _, m := range []SendInput{fifoMsg("a1", "a"), fifoMsg("b1", "b"), fifoMsg("a2", "a")} {
			sendOne(t, e, "q.fifo", m)
		}
		got := bodies(receive(t, e, "q.fifo", ReceiveInput{Max: 10}))
		if len(got) != 3 || got[0] != "a1" || got[1] != "b1" || got[2] != "a2" {
			t.Errorf("Receive(10) = %v, want a1, b1, a2 in arrival order", got)
		}
	})
	t.Run("a hidden message blocks its group", func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "DelaySeconds": "60"})
		sendOne(t, e, "q.fifo", fifoMsg("a1", "a"))
		sendOne(t, e, "q.fifo", fifoMsg("b1", "b"))
		if err := e.SetAttributes(t.Context(), "q.fifo", map[string]string{"DelaySeconds": "0"}); err != nil {
			t.Fatalf("SetAttributes(DelaySeconds) error = %v", err)
		}
		sendOne(t, e, "q.fifo", fifoMsg("a2", "a"))
		sendOne(t, e, "q.fifo", fifoMsg("c1", "c"))
		got := bodies(receive(t, e, "q.fifo", ReceiveInput{Max: 10}))
		if len(got) != 1 || got[0] != "c1" {
			t.Errorf("Receive with a1 and b1 delayed = %v, want c1 only", got)
		}
	})
}

func TestDeleteWakesWaitingReceive(t *testing.T) {
	t.Run("single delete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q.fifo", fifoAttrs)
			sendOne(t, e, "q.fifo", fifoMsg("a1", "a"))
			sendOne(t, e, "q.fifo", fifoMsg("a2", "a"))
			first := receive(t, e, "q.fifo", ReceiveInput{})
			start := time.Now()
			wait := startReceive(t.Context(), e, "q.fifo", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			if _, err := e.Delete(t.Context(), "q.fifo", []string{first[0].ReceiptHandle}); err != nil {
				t.Fatalf("Delete error = %v", err)
			}
			got, err := wait()
			if err != nil || len(got) != 1 || got[0].Body != "a2" || time.Since(start) != 0 {
				t.Errorf("Receive = %v, %v after %v; want a2 at once", bodies(got), err, time.Since(start))
			}
		})
	})
	t.Run("multiple handles", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q.fifo", fifoAttrs)
			for _, m := range []SendInput{fifoMsg("a1", "a"), fifoMsg("b1", "b"), fifoMsg("a2", "a")} {
				sendOne(t, e, "q.fifo", m)
			}
			first := receive(t, e, "q.fifo", ReceiveInput{Max: 2})
			start := time.Now()
			wait := startReceive(t.Context(), e, "q.fifo", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			handles := []string{first[0].ReceiptHandle, "garbage", first[1].ReceiptHandle}
			if _, err := e.Delete(t.Context(), "q.fifo", handles); err != nil {
				t.Fatalf("Delete error = %v", err)
			}
			got, err := wait()
			if err != nil || len(got) != 1 || got[0].Body != "a2" || time.Since(start) != 0 {
				t.Errorf("Receive = %v, %v after %v; want a2 at once", bodies(got), err, time.Since(start))
			}
		})
	})
}

func TestStandardQueueGroupIDRules(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	for _, g := range []string{"a\x01b", "a b", strings.Repeat("a", 129)} {
		if res := sendOne(t, e, "q", SendInput{Body: "x", GroupID: g}); !errors.Is(res.Err, ErrInvalidParameterValue) {
			t.Errorf("Send(group %q) error = %v, want ErrInvalidParameterValue", g, res.Err)
		}
	}
	if res := sendOne(t, e, "q", SendInput{Body: "x", GroupID: "tenant-1"}); res.Err != nil {
		t.Errorf("Send(group tenant-1) error = %v, want nil", res.Err)
	}
}

func TestFifoBatchDeduplication(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	res, err := e.Send(t.Context(), "q.fifo", []SendInput{fifoMsg("a", "g"), fifoMsg("a", "g")})
	if err != nil {
		t.Fatalf("Send error = %v", err)
	}
	if res[0].Err != nil || res[1].Err != nil || res[0].MessageID != res[1].MessageID || res[0].SequenceNumber != res[1].SequenceNumber {
		t.Errorf("results = %+v, %+v; want the same message twice", res[0], res[1])
	}
	if got := attr(t, e, "q.fifo", "ApproximateNumberOfMessages"); got != "1" {
		t.Errorf("messages = %s, want 1", got)
	}
}

func TestPurgeKeepsDeduplication(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	first := sendOne(t, e, "q.fifo", fifoMsg("a", "g"))
	if err := e.Purge(t.Context(), "q.fifo"); err != nil {
		t.Fatalf("Purge error = %v", err)
	}
	if dup := sendOne(t, e, "q.fifo", fifoMsg("a", "g")); dup.MessageID != first.MessageID {
		t.Errorf("send after Purge = %+v, want the first result %+v", dup, first)
	}
}
