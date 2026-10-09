package queue

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func strAttr(v string) MessageAttribute { return MessageAttribute{DataType: "String", StringValue: v} }

func receive(t *testing.T, e *Engine, name string, in ReceiveInput) []Message {
	t.Helper()
	got, err := e.Receive(t.Context(), name, in)
	if err != nil {
		t.Fatalf("Receive(%q, %+v) error = %v", name, in, err)
	}
	return got
}

func TestSendReceiveRoundTrip(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	attrs := map[string]MessageAttribute{
		"s": strAttr("text"),
		"n": {DataType: "Number.int", StringValue: "42"},
		"b": {DataType: "Binary", BinaryValue: []byte{0, 1, 2}},
	}
	sys := map[string]MessageAttribute{"AWSTraceHeader": strAttr("Root=1-abc")}
	res, err := e.Send(t.Context(), "q", []SendInput{{Body: "hello", Attributes: attrs, SystemAttributes: sys}})
	if err != nil || len(res) != 1 || res[0].Err != nil {
		t.Fatalf("Send() = %+v, %v", res, err)
	}
	if want := "5d41402abc4b2a76b9719d911017c592"; res[0].MD5OfBody != want {
		t.Errorf("MD5OfBody = %s, want %s", res[0].MD5OfBody, want)
	}
	if !uuidRE.MatchString(res[0].MessageID) {
		t.Errorf("MessageID = %q, want a lowercase UUID v4", res[0].MessageID)
	}
	if res[0].MD5OfAttributes != MD5OfAttributes(attrs) || res[0].MD5OfSystemAttributes != MD5OfAttributes(sys) {
		t.Errorf("attribute MD5s = %q, %q", res[0].MD5OfAttributes, res[0].MD5OfSystemAttributes)
	}
	got := receive(t, e, "q", ReceiveInput{})
	if len(got) != 1 {
		t.Fatalf("Receive() = %d messages, want 1", len(got))
	}
	m := got[0]
	if m.ID != res[0].MessageID || m.Body != "hello" || m.MD5OfBody != res[0].MD5OfBody || m.ReceiveCount != 1 || m.ReceiptHandle == "" {
		t.Errorf("Receive() = %+v, want the sent message", m)
	}
	if MD5OfAttributes(m.Attributes) != res[0].MD5OfAttributes || string(m.Attributes["b"].BinaryValue) != "\x00\x01\x02" {
		t.Errorf("attributes = %+v, want %+v", m.Attributes, attrs)
	}
	if m.SystemAttributes["AWSTraceHeader"].StringValue != "Root=1-abc" {
		t.Errorf("SystemAttributes = %+v", m.SystemAttributes)
	}
	if m.FirstReceivedAt.IsZero() || m.SentAt.IsZero() {
		t.Errorf("times not set: %+v", m)
	}
	empty, _ := e.Send(t.Context(), "q", []SendInput{{Body: "x"}})
	if empty[0].MD5OfAttributes != "" || empty[0].MD5OfSystemAttributes != "" {
		t.Errorf("empty-map MD5s = %q, %q; want empty", empty[0].MD5OfAttributes, empty[0].MD5OfSystemAttributes)
	}
	if _, err := e.Send(t.Context(), "nope", []SendInput{{Body: "x"}}); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Send(nope) error = %v, want ErrQueueDoesNotExist", err)
	}
}

func TestSendEntryValidation(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", map[string]string{"MaximumMessageSize": "1024"})
	many := map[string]MessageAttribute{}
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"} {
		many[n] = strAttr("v")
	}
	withAttr := func(name string, a MessageAttribute) SendInput {
		return SendInput{Body: "x", Attributes: map[string]MessageAttribute{name: a}}
	}
	tests := []struct {
		name string
		in   SendInput
		want error
	}{
		{"valid", SendInput{Body: "ok"}, nil},
		{"empty body", SendInput{}, ErrInvalidParameterValue},
		{"nul character", SendInput{Body: "a\x00b"}, ErrInvalidMessageContents},
		{"invalid utf-8", SendInput{Body: "a\xffb"}, ErrInvalidMessageContents},
		{"allowed controls", SendInput{Body: "a\tb\nc\rd"}, nil},
		{"oversize", SendInput{Body: strings.Repeat("a", 1025)}, ErrMessageTooLong},
		{"max size", SendInput{Body: strings.Repeat("a", 1024)}, nil},
		{"size counts attributes", SendInput{Body: strings.Repeat("a", 1024), Attributes: map[string]MessageAttribute{"n": strAttr("v")}}, ErrMessageTooLong},
		{"too many attributes", SendInput{Body: "x", Attributes: many}, ErrInvalidParameterValue},
		{"empty attribute name", withAttr("", strAttr("v")), ErrInvalidParameterValue},
		{"bad attribute name", withAttr("a b", strAttr("v")), ErrInvalidParameterValue},
		{"aws prefix", withAttr("aws.x", strAttr("v")), ErrInvalidParameterValue},
		{"amazon prefix", withAttr("Amazon.x", strAttr("v")), ErrInvalidParameterValue},
		{"double period", withAttr("a..b", strAttr("v")), ErrInvalidParameterValue},
		{"leading period", withAttr(".a", strAttr("v")), ErrInvalidParameterValue},
		{"trailing period", withAttr("a.", strAttr("v")), ErrInvalidParameterValue},
		{"period inside", withAttr("a.b-c_d", strAttr("v")), nil},
		{"bad data type", withAttr("a", MessageAttribute{DataType: "Blob", StringValue: "v"}), ErrInvalidParameterValue},
		{"empty label", withAttr("a", MessageAttribute{DataType: "String.", StringValue: "v"}), ErrInvalidParameterValue},
		{"missing string value", withAttr("a", MessageAttribute{DataType: "String"}), ErrInvalidParameterValue},
		{"missing number value", withAttr("a", MessageAttribute{DataType: "Number"}), ErrInvalidParameterValue},
		{"missing binary value", withAttr("a", MessageAttribute{DataType: "Binary", StringValue: "v"}), ErrInvalidParameterValue},
		{"binary label", withAttr("a", MessageAttribute{DataType: "Binary.png", BinaryValue: []byte{1}}), nil},
		{"bad system attribute", SendInput{Body: "x", SystemAttributes: map[string]MessageAttribute{"Other": strAttr("v")}}, ErrInvalidParameterValue},
		{"bad system type", SendInput{Body: "x", SystemAttributes: map[string]MessageAttribute{"AWSTraceHeader": {DataType: "Number", StringValue: "1"}}}, ErrInvalidParameterValue},
		{"delay too high", SendInput{Body: "x", DelaySeconds: new(901)}, ErrInvalidParameterValue},
		{"delay negative", SendInput{Body: "x", DelaySeconds: new(-1)}, ErrInvalidParameterValue},
		{"delay max", SendInput{Body: "x", DelaySeconds: new(900)}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A valid entry on each side proves one bad entry does not stop the batch.
			res, err := e.Send(t.Context(), "q", []SendInput{{Body: "before"}, tt.in, {Body: "after"}})
			if err != nil {
				t.Fatal(err)
			}
			if res[0].Err != nil || res[2].Err != nil || res[0].MessageID == "" || res[2].MessageID == "" {
				t.Errorf("neighbors = %+v, %+v; want sent", res[0], res[2])
			}
			if !errors.Is(res[1].Err, tt.want) || (tt.want == nil && res[1].Err != nil) {
				t.Errorf("Send(%s) error = %v, want %v", tt.name, res[1].Err, tt.want)
			}
		})
	}
}

func TestReceiveValidationAndMax(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	for i := range 12 {
		sendBodies(t, e, "q", string(rune('a'+i)))
	}
	bad := []ReceiveInput{
		{Max: 11}, {Max: -1},
		{WaitTimeSeconds: new(21)}, {WaitTimeSeconds: new(-1)},
		{VisibilityTimeout: new(43201)}, {VisibilityTimeout: new(-1)},
	}
	for _, in := range bad {
		if _, err := e.Receive(t.Context(), "q", in); !errors.Is(err, ErrInvalidParameterValue) {
			t.Errorf("Receive(%+v) error = %v, want ErrInvalidParameterValue", in, err)
		}
	}
	if _, err := e.Receive(t.Context(), "nope", ReceiveInput{}); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Receive(nope) error = %v, want ErrQueueDoesNotExist", err)
	}
	if got := receive(t, e, "q", ReceiveInput{}); len(got) != 1 || got[0].Body != "a" {
		t.Errorf("Receive(default) = %+v, want one message \"a\"", got)
	}
	got := receive(t, e, "q", ReceiveInput{Max: 10})
	if len(got) != 10 || got[0].Body != "b" || got[9].Body != "k" {
		t.Errorf("Receive(10) = %d messages, want 10 in arrival order b..k", len(got))
	}
}

func TestVisibilityTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", map[string]string{"VisibilityTimeout": "30"})
		sendBodies(t, e, "q", "m")
		first := receive(t, e, "q", ReceiveInput{})
		if len(first) != 1 || first[0].ReceiveCount != 1 {
			t.Fatalf("first Receive = %+v", first)
		}
		if got := attr(t, e, "q", "ApproximateNumberOfMessagesNotVisible"); got != "1" {
			t.Errorf("NotVisible = %s, want 1", got)
		}
		time.Sleep(29 * time.Second)
		if got := receive(t, e, "q", ReceiveInput{}); len(got) != 0 {
			t.Errorf("Receive at 29s = %d messages, want 0", len(got))
		}
		time.Sleep(time.Second)
		second := receive(t, e, "q", ReceiveInput{VisibilityTimeout: new(5)})
		if len(second) != 1 || second[0].ReceiveCount != 2 || second[0].ID != first[0].ID {
			t.Fatalf("Receive at 30s = %+v, want the same message with count 2", second)
		}
		if !second[0].FirstReceivedAt.Equal(first[0].FirstReceivedAt) {
			t.Errorf("FirstReceivedAt changed: %v -> %v", first[0].FirstReceivedAt, second[0].FirstReceivedAt)
		}
		time.Sleep(4 * time.Second)
		if got := receive(t, e, "q", ReceiveInput{}); len(got) != 0 {
			t.Errorf("Receive at 4s of 5s override = %d messages, want 0", len(got))
		}
		time.Sleep(time.Second)
		if got := receive(t, e, "q", ReceiveInput{}); len(got) != 1 || got[0].ReceiveCount != 3 {
			t.Errorf("Receive after override = %+v, want count 3", got)
		}
	})
}

func TestDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", map[string]string{"DelaySeconds": "10"})
		sendBodies(t, e, "q", "queue-delay")
		if _, err := e.Send(t.Context(), "q", []SendInput{{Body: "own-delay", DelaySeconds: new(3)}, {Body: "no-delay", DelaySeconds: new(0)}}); err != nil {
			t.Fatal(err)
		}
		counts := func() [3]string {
			return [3]string{
				attr(t, e, "q", "ApproximateNumberOfMessages"),
				attr(t, e, "q", "ApproximateNumberOfMessagesDelayed"),
				attr(t, e, "q", "ApproximateNumberOfMessagesNotVisible"),
			}
		}
		if got, want := counts(), [3]string{"1", "2", "0"}; got != want {
			t.Errorf("counts at 0s = %v, want %v", got, want)
		}
		got := receive(t, e, "q", ReceiveInput{Max: 10})
		if len(got) != 1 || got[0].Body != "no-delay" {
			t.Errorf("Receive at 0s = %+v, want no-delay", got)
		}
		if got, want := counts(), [3]string{"0", "2", "1"}; got != want {
			t.Errorf("counts after receive = %v, want %v", got, want)
		}
		time.Sleep(3 * time.Second)
		if got := receive(t, e, "q", ReceiveInput{Max: 10}); len(got) != 1 || got[0].Body != "own-delay" {
			t.Errorf("Receive at 3s = %+v, want own-delay", got)
		}
		time.Sleep(7 * time.Second)
		if got := receive(t, e, "q", ReceiveInput{Max: 10}); len(got) != 1 || got[0].Body != "queue-delay" {
			t.Errorf("Receive at 10s = %+v, want queue-delay", got)
		}
	})
}

func TestRetention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", map[string]string{"MessageRetentionPeriod": "60"})
		sendBodies(t, e, "q", "old")
		time.Sleep(59 * time.Second)
		if got := attr(t, e, "q", "ApproximateNumberOfMessages"); got != "1" {
			t.Errorf("messages at 59s = %s, want 1", got)
		}
		time.Sleep(time.Second)
		if got := attr(t, e, "q", "ApproximateNumberOfMessages"); got != "0" {
			t.Errorf("messages at 60s = %s, want 0", got)
		}
		sendBodies(t, e, "q", "second")
		time.Sleep(60 * time.Second)
		if got := receive(t, e, "q", ReceiveInput{}); len(got) != 0 {
			t.Errorf("Receive after retention = %+v, want none", got)
		}
	})
}

// startReceive runs Receive in a goroutine and returns a function that waits for the result.
func startReceive(ctx context.Context, e *Engine, name string, in ReceiveInput) func() ([]Message, error) {
	var (
		wg  sync.WaitGroup
		got []Message
		err error
	)
	wg.Go(func() { got, err = e.Receive(ctx, name, in) })
	return func() ([]Message, error) {
		wg.Wait()
		return got, err
	}
}

func TestLongPoll(t *testing.T) {
	t.Run("send wakes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			start := time.Now()
			wait := startReceive(t.Context(), e, "q", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			time.Sleep(5 * time.Second)
			sendBodies(t, e, "q", "m")
			got, err := wait()
			if err != nil || len(got) != 1 || time.Since(start) != 5*time.Second {
				t.Errorf("Receive = %v, %v after %v; want 1 message after 5s", got, err, time.Since(start))
			}
		})
	})
	t.Run("times out empty", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			start := time.Now()
			got, err := e.Receive(t.Context(), "q", ReceiveInput{WaitTimeSeconds: new(20)})
			if err != nil || len(got) != 0 || time.Since(start) != 20*time.Second {
				t.Errorf("Receive = %v, %v after %v; want empty after 20s", got, err, time.Since(start))
			}
		})
	})
	t.Run("queue default wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", map[string]string{"ReceiveMessageWaitTimeSeconds": "7"})
			start := time.Now()
			if got := receive(t, e, "q", ReceiveInput{}); len(got) != 0 || time.Since(start) != 7*time.Second {
				t.Errorf("Receive = %v after %v; want empty after 7s", got, time.Since(start))
			}
		})
	})
	t.Run("delayed message becomes visible", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			if _, err := e.Send(t.Context(), "q", []SendInput{{Body: "d", DelaySeconds: new(7)}}); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			got, err := e.Receive(t.Context(), "q", ReceiveInput{WaitTimeSeconds: new(20)})
			if err != nil || len(got) != 1 || time.Since(start) != 7*time.Second {
				t.Errorf("Receive = %v, %v after %v; want 1 message after 7s", got, err, time.Since(start))
			}
		})
	})
	t.Run("context canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			ctx, cancel := context.WithCancel(t.Context())
			wait := startReceive(ctx, e, "q", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			cancel()
			if got, err := wait(); !errors.Is(err, context.Canceled) || len(got) != 0 {
				t.Errorf("Receive = %v, %v; want context.Canceled", got, err)
			}
		})
	})
	t.Run("stop waiters", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			start := time.Now()
			wait := startReceive(t.Context(), e, "q", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			e.StopWaiters()
			e.StopWaiters()
			if got, err := wait(); err != nil || len(got) != 0 || time.Since(start) != 0 {
				t.Errorf("Receive = %v, %v after %v; want empty at once", got, err, time.Since(start))
			}
			if got := receive(t, e, "q", ReceiveInput{WaitTimeSeconds: new(20)}); len(got) != 0 || time.Since(start) != 0 {
				t.Errorf("later Receive = %v after %v; want empty at once", got, time.Since(start))
			}
			sendBodies(t, e, "q", "m")
			if got := receive(t, e, "q", ReceiveInput{WaitTimeSeconds: new(20)}); len(got) != 1 {
				t.Errorf("Receive after stop = %v, want the available message", got)
			}
		})
	})
	t.Run("queue deleted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q", nil)
			wait := startReceive(t.Context(), e, "q", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			if err := e.DeleteQueue(t.Context(), "q"); err != nil {
				t.Fatal(err)
			}
			if _, err := wait(); !errors.Is(err, ErrQueueDoesNotExist) {
				t.Errorf("Receive error = %v, want ErrQueueDoesNotExist", err)
			}
		})
	})
}

func TestDeleteHandles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q", map[string]string{"VisibilityTimeout": "10"})
		mustCreate(t, e, "other", nil)
		sendBodies(t, e, "q", "m")
		old := receive(t, e, "q", ReceiveInput{})[0].ReceiptHandle
		time.Sleep(10 * time.Second)
		latest := receive(t, e, "q", ReceiveInput{})[0].ReceiptHandle
		if old == latest {
			t.Fatal("receipt handle did not change on the second receive")
		}
		del := func(name, handle string) error {
			t.Helper()
			errs, err := e.Delete(t.Context(), name, []string{handle})
			if err != nil || len(errs) != 1 {
				t.Fatalf("Delete(%q, %q) = %v, %v", name, handle, errs, err)
			}
			return errs[0]
		}
		if err := del("q", old); err != nil {
			t.Errorf("Delete(old handle) error = %v, want nil", err)
		}
		if got := attr(t, e, "q", "ApproximateNumberOfMessagesNotVisible"); got != "1" {
			t.Errorf("NotVisible after old-handle delete = %s, want 1 (not deleted)", got)
		}
		for _, bad := range []string{"garbage", "", latest[:len(latest)-2]} {
			if err := del("q", bad); !errors.Is(err, ErrReceiptHandleInvalid) {
				t.Errorf("Delete(%q) error = %v, want ErrReceiptHandleInvalid", bad, err)
			}
		}
		if err := del("other", latest); !errors.Is(err, ErrReceiptHandleInvalid) {
			t.Errorf("Delete(other queue's handle) error = %v, want ErrReceiptHandleInvalid", err)
		}
		if err := del("q", latest); err != nil {
			t.Errorf("Delete(latest) error = %v, want nil", err)
		}
		if got := attr(t, e, "q", "ApproximateNumberOfMessagesNotVisible"); got != "0" {
			t.Errorf("NotVisible after delete = %s, want 0", got)
		}
		if err := del("q", latest); err != nil {
			t.Errorf("Delete(deleted message) error = %v, want nil", err)
		}
		if _, err := e.Delete(t.Context(), "nope", []string{latest}); !errors.Is(err, ErrQueueDoesNotExist) {
			t.Errorf("Delete(nope) error = %v, want ErrQueueDoesNotExist", err)
		}
	})
}

func TestChangeVisibility(t *testing.T) {
	change := func(t *testing.T, e *Engine, handle string, timeout int) error {
		t.Helper()
		errs, err := e.ChangeVisibility(t.Context(), "q", []VisibilityChange{{ReceiptHandle: handle, Timeout: timeout}})
		if err != nil || len(errs) != 1 {
			t.Fatalf("ChangeVisibility(%d) = %v, %v", timeout, errs, err)
		}
		return errs[0]
	}
	setup := func(t *testing.T) (*Engine, string) {
		t.Helper()
		e := newEngine(t)
		mustCreate(t, e, "q", nil)
		sendBodies(t, e, "q", "m")
		return e, receive(t, e, "q", ReceiveInput{})[0].ReceiptHandle
	}
	t.Run("extend", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, h := setup(t)
			time.Sleep(20 * time.Second)
			if err := change(t, e, h, 100); err != nil {
				t.Fatal(err)
			}
			time.Sleep(99 * time.Second)
			if got := receive(t, e, "q", ReceiveInput{}); len(got) != 0 {
				t.Errorf("Receive at 99s = %v, want none", got)
			}
			time.Sleep(time.Second)
			if got := receive(t, e, "q", ReceiveInput{}); len(got) != 1 {
				t.Errorf("Receive at 100s = %v, want 1", got)
			}
		})
	})
	t.Run("zero wakes a waiting receive", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, h := setup(t)
			start := time.Now()
			wait := startReceive(t.Context(), e, "q", ReceiveInput{WaitTimeSeconds: new(20)})
			synctest.Wait()
			time.Sleep(4 * time.Second)
			if err := change(t, e, h, 0); err != nil {
				t.Fatal(err)
			}
			if got, err := wait(); err != nil || len(got) != 1 || time.Since(start) != 4*time.Second {
				t.Errorf("Receive = %v, %v after %v; want 1 message after 4s", got, err, time.Since(start))
			}
		})
	})
	t.Run("errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, h := setup(t)
			if err := change(t, e, "garbage", 5); !errors.Is(err, ErrReceiptHandleInvalid) {
				t.Errorf("garbage handle error = %v, want ErrReceiptHandleInvalid", err)
			}
			for _, bad := range []int{-1, 43201} {
				if err := change(t, e, h, bad); !errors.Is(err, ErrInvalidParameterValue) {
					t.Errorf("timeout %d error = %v, want ErrInvalidParameterValue", bad, err)
				}
			}
			time.Sleep(11*time.Hour + 59*time.Minute)
			if err := change(t, e, h, 43200); !errors.Is(err, ErrMessageNotInflight) {
				t.Errorf("visible message error = %v, want ErrMessageNotInflight", err)
			}
		})
	})
	t.Run("beyond 12 hours", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, h := setup(t)
			if err := change(t, e, h, 43200); err != nil {
				t.Errorf("12 hours from receive error = %v, want nil", err)
			}
			time.Sleep(time.Hour)
			if err := change(t, e, h, 43200); !errors.Is(err, ErrInvalidParameterValue) {
				t.Errorf("13 hours from receive error = %v, want ErrInvalidParameterValue", err)
			}
		})
	})
	t.Run("stale or deleted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, h := setup(t)
			time.Sleep(30 * time.Second)
			h2 := receive(t, e, "q", ReceiveInput{})[0].ReceiptHandle
			if err := change(t, e, h, 60); !errors.Is(err, ErrMessageNotAvailable) {
				t.Errorf("stale handle error = %v, want ErrMessageNotAvailable", err)
			}
			if _, err := e.Delete(t.Context(), "q", []string{h2}); err != nil {
				t.Fatal(err)
			}
			if err := change(t, e, h2, 60); !errors.Is(err, ErrMessageNotAvailable) {
				t.Errorf("deleted message error = %v, want ErrMessageNotAvailable", err)
			}
			if _, err := e.ChangeVisibility(t.Context(), "nope", nil); !errors.Is(err, ErrQueueDoesNotExist) {
				t.Errorf("ChangeVisibility(nope) error = %v, want ErrQueueDoesNotExist", err)
			}
		})
	})
}
