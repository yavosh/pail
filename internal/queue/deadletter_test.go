package queue

import (
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

// redrive returns a RedrivePolicy that targets the queue called target.
func redrive(target string, count string) map[string]string {
	return map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:` + target + `","maxReceiveCount":` + count + `}`}
}

func TestRedrivePolicyValidation(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "dlq", nil)
	mustCreate(t, e, "dlq.fifo", fifoAttrs)
	mustCreate(t, e, "deny", map[string]string{"RedriveAllowPolicy": `{"redrivePermission":"denyAll"}`})
	mustCreate(t, e, "by", map[string]string{"RedriveAllowPolicy": `{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:000000000000:ok"]}`})
	tests := []struct {
		name   string
		source string
		attrs  map[string]string
		want   error
	}{
		{"valid", "s0", redrive("dlq", "3"), nil},
		{"count as a string", "s1", redrive("dlq", `"5"`), nil},
		{"count 1000", "s2", redrive("dlq", "1000"), nil},
		{"count 0", "s3", redrive("dlq", "0"), ErrInvalidAttributeValue},
		{"count 1001", "s4", redrive("dlq", "1001"), ErrInvalidAttributeValue},
		{"count not an integer", "s5", redrive("dlq", "1.5"), ErrInvalidAttributeValue},
		{"count not a number", "s6", redrive("dlq", `"many"`), ErrInvalidAttributeValue},
		{"not JSON", "s7", map[string]string{"RedrivePolicy": "x"}, ErrInvalidAttributeValue},
		{"missing target", "s8", map[string]string{"RedrivePolicy": `{"maxReceiveCount":1}`}, ErrInvalidAttributeValue},
		{"missing count", "s9", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`}, ErrInvalidAttributeValue},
		{"target does not exist", "s10", redrive("gone", "1"), ErrInvalidParameterValue},
		{"not an ARN", "s11", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"dlq","maxReceiveCount":1}`}, ErrInvalidParameterValue},
		{"other region", "s12", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:eu-west-1:000000000000:dlq","maxReceiveCount":1}`}, ErrInvalidParameterValue},
		{"other account", "s13", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:111111111111:dlq","maxReceiveCount":1}`}, ErrInvalidParameterValue},
		{"standard source, fifo target", "s14", redrive("dlq.fifo", "1"), ErrInvalidParameterValue},
		{"fifo source, standard target", "s15.fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": redrive("dlq", "1")["RedrivePolicy"]}, ErrInvalidParameterValue},
		{"fifo source, fifo target", "s16.fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": redrive("dlq.fifo", "1")["RedrivePolicy"]}, nil},
		{"target denies all", "s17", redrive("deny", "1"), ErrInvalidParameterValue},
		{"byQueue lists the source", "ok2", redrive("by", "1"), ErrInvalidParameterValue},
		{"empty policy", "s19", map[string]string{"RedrivePolicy": ""}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := e.CreateQueue(t.Context(), tt.source, tt.attrs, nil)
			if !errors.Is(err, tt.want) || (tt.want == nil && err != nil) {
				t.Errorf("CreateQueue(%q, %v) error = %v, want %v", tt.source, tt.attrs, err, tt.want)
			}
		})
	}
	// A byQueue target accepts only the listed source.
	if err := e.CreateQueue(t.Context(), "ok", redrive("by", "1"), nil); err != nil {
		t.Errorf("CreateQueue(ok, redrive to by) error = %v, want nil: the policy lists it", err)
	}
	// SetAttributes applies the same checks.
	mustCreate(t, e, "late", nil)
	if err := e.SetAttributes(t.Context(), "late", redrive("deny", "1")); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("SetAttributes(redrive to deny) error = %v, want ErrInvalidParameterValue", err)
	}
	if err := e.SetAttributes(t.Context(), "late", redrive("late", "1")); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("SetAttributes(redrive to itself) error = %v, want ErrInvalidParameterValue", err)
	}
}

func TestRedriveCanonicalForm(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "dlq", nil)
	policy := ` { "maxReceiveCount": "7", "deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:dlq", "extra": 1 } `
	mustCreate(t, e, "src", map[string]string{"RedrivePolicy": policy})
	want := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":7}`
	if got := attr(t, e, "src", "RedrivePolicy"); got != want {
		t.Errorf("RedrivePolicy = %q, want %q", got, want)
	}
	if err := e.CreateQueue(t.Context(), "src", map[string]string{"RedrivePolicy": want}, nil); err != nil {
		t.Errorf("CreateQueue(src, canonical policy) error = %v, want nil", err)
	}
	// Removing the policy hides the attribute.
	if err := e.SetAttributes(t.Context(), "src", map[string]string{"RedrivePolicy": ""}); err != nil {
		t.Fatalf("SetAttributes(RedrivePolicy empty) error = %v", err)
	}
	all, err := e.Attributes(t.Context(), "src", []string{"All"})
	if _, ok := all["RedrivePolicy"]; err != nil || ok {
		t.Errorf("Attributes after removing the policy = %v, %v; want no RedrivePolicy", all, err)
	}
}

func TestRedriveAllowPolicyValidation(t *testing.T) {
	e := newEngine(t)
	const arn = `"arn:aws:sqs:us-east-1:000000000000:a"`
	tests := []struct {
		name, value, want string
		ok                bool
	}{
		{"allowAll", `{"redrivePermission":"allowAll"}`, `{"redrivePermission":"allowAll"}`, true},
		{"denyAll", ` {"redrivePermission":"denyAll"}`, `{"redrivePermission":"denyAll"}`, true},
		{"byQueue", `{"redrivePermission":"byQueue","sourceQueueArns":[` + arn + `]}`, `{"redrivePermission":"byQueue","sourceQueueArns":[` + arn + `]}`, true},
		{"byQueue without ARNs", `{"redrivePermission":"byQueue"}`, "", false},
		{"byQueue with an empty list", `{"redrivePermission":"byQueue","sourceQueueArns":[]}`, "", false},
		{"byQueue with 11 ARNs", `{"redrivePermission":"byQueue","sourceQueueArns":[` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `,` + arn + `]}`, "", false},
		{"allowAll with ARNs", `{"redrivePermission":"allowAll","sourceQueueArns":[` + arn + `]}`, "", false},
		{"unknown permission", `{"redrivePermission":"some"}`, "", false},
		{"missing permission", `{}`, "", false},
		{"not JSON", `nope`, "", false},
		{"ARNs not strings", `{"redrivePermission":"byQueue","sourceQueueArns":[1]}`, "", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := "q" + string(rune('a'+i))
			err := e.CreateQueue(t.Context(), name, map[string]string{"RedriveAllowPolicy": tt.value}, nil)
			if tt.ok && err != nil || !tt.ok && !errors.Is(err, ErrInvalidAttributeValue) {
				t.Fatalf("CreateQueue(RedriveAllowPolicy=%s) error = %v, ok want %v", tt.value, err, tt.ok)
			}
			if tt.ok {
				if got := attr(t, e, name, "RedriveAllowPolicy"); got != tt.want {
					t.Errorf("RedriveAllowPolicy = %q, want %q", got, tt.want)
				}
			}
		})
	}
}

func TestMoveToDeadLetterQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "dlq", nil)
		mustCreate(t, e, "src", redrive("dlq", "1"))
		if _, err := e.Send(t.Context(), "src", []SendInput{{Body: "poison", Attributes: map[string]MessageAttribute{"k": strAttr("v")}, GroupID: "tenant"}}); err != nil {
			t.Fatalf("Send error = %v", err)
		}
		first := receive(t, e, "src", ReceiveInput{VisibilityTimeout: new(0)})
		if len(first) != 1 || first[0].ReceiveCount != 1 {
			t.Fatalf("first Receive = %+v, want one message with count 1", first)
		}
		// Waiting receive on the DLQ wakes when the move happens.
		start := time.Now()
		wait := startReceive(t.Context(), e, "dlq", ReceiveInput{WaitTimeSeconds: new(20)})
		synctest.Wait()
		time.Sleep(3 * time.Second)
		if got := receive(t, e, "src", ReceiveInput{}); len(got) != 0 {
			t.Fatalf("Receive after maxReceiveCount = %v, want none", bodies(got))
		}
		moved, err := wait()
		if err != nil || len(moved) != 1 || time.Since(start) != 3*time.Second {
			t.Fatalf("DLQ Receive = %+v, %v after %v; want the message after 3s", moved, err, time.Since(start))
		}
		m := moved[0]
		if m.ID != first[0].ID || m.Body != "poison" || m.DeadLetterSourceARN != e.ARN("src") || m.ReceiveCount != 2 ||
			m.Attributes["k"].StringValue != "v" || m.GroupID != "tenant" {
			t.Errorf("moved message = %+v, want the original ID, body, attributes, and group, source ARN %s, and receive count 2", m, e.ARN("src"))
		}
		if got := attr(t, e, "src", "ApproximateNumberOfMessages"); got != "0" {
			t.Errorf("source messages = %s, want 0", got)
		}
		if got := first[0].DeadLetterSourceARN; got != "" {
			t.Errorf("DeadLetterSourceARN on the source = %q, want empty", got)
		}
	})
}

func TestMoveRespectsMaxReceiveCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "dlq", nil)
		mustCreate(t, e, "src", redrive("dlq", "3"))
		sendBodies(t, e, "src", "m")
		for i := 1; i <= 3; i++ {
			got := receive(t, e, "src", ReceiveInput{VisibilityTimeout: new(0)})
			if len(got) != 1 || got[0].ReceiveCount != i {
				t.Fatalf("Receive %d = %+v, want count %d", i, got, i)
			}
		}
		if got := receive(t, e, "src", ReceiveInput{}); len(got) != 0 {
			t.Errorf("fourth Receive = %v, want none", bodies(got))
		}
		if got := attr(t, e, "dlq", "ApproximateNumberOfMessages"); got != "1" {
			t.Errorf("DLQ messages = %s, want 1", got)
		}
	})
}

func TestMoveFifoToFifo(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "dlq.fifo", fifoAttrs)
	mustCreate(t, e, "src.fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": redrive("dlq.fifo", "1")["RedrivePolicy"]})
	sendOne(t, e, "dlq.fifo", fifoMsg("existing", "x"))
	sent := sendOne(t, e, "src.fifo", fifoMsg("poison", "g"))
	receive(t, e, "src.fifo", ReceiveInput{VisibilityTimeout: new(0)})
	receive(t, e, "src.fifo", ReceiveInput{})
	got := receive(t, e, "dlq.fifo", ReceiveInput{Max: 10})
	if len(got) != 2 || got[1].Body != "poison" || got[1].GroupID != "g" || got[1].DeduplicationID != "poison" {
		t.Fatalf("DLQ Receive = %+v, want existing then poison with group and deduplication ID", got)
	}
	if got[1].SequenceNumber == sent.SequenceNumber || got[1].SequenceNumber != "00000000000000000002" {
		t.Errorf("moved SequenceNumber = %s, want a new number from the target (2), not %s", got[1].SequenceNumber, sent.SequenceNumber)
	}
}

func TestMoveWithoutTarget(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "dlq", nil)
	mustCreate(t, e, "src", redrive("dlq", "1"))
	sendBodies(t, e, "src", "m")
	receive(t, e, "src", ReceiveInput{VisibilityTimeout: new(0)})
	if err := e.DeleteQueue(t.Context(), "dlq"); err != nil {
		t.Fatalf("DeleteQueue(dlq) error = %v", err)
	}
	got := receive(t, e, "src", ReceiveInput{})
	if len(got) != 1 || got[0].ReceiveCount != 2 {
		t.Errorf("Receive after the target is deleted = %+v, want the message delivered with count 2", got)
	}
}

func TestDeadLetterSourceQueues(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "dlq", nil)
	mustCreate(t, e, "other", nil)
	for _, n := range []string{"c", "a", "b"} {
		mustCreate(t, e, n, redrive("dlq", "1"))
	}
	mustCreate(t, e, "plain", nil)
	names, next, err := e.DeadLetterSourceQueues(t.Context(), "dlq", 0, "")
	if err != nil || !slices.Equal(names, []string{"a", "b", "c"}) || next != "" {
		t.Errorf("DeadLetterSourceQueues(dlq) = %v, %q, %v; want a, b, c", names, next, err)
	}
	names, next, err = e.DeadLetterSourceQueues(t.Context(), "dlq", 2, "")
	if err != nil || !slices.Equal(names, []string{"a", "b"}) || next != "b" {
		t.Errorf("DeadLetterSourceQueues(dlq, 2) = %v, %q, %v; want a, b and next b", names, next, err)
	}
	names, next, err = e.DeadLetterSourceQueues(t.Context(), "dlq", 2, next)
	if err != nil || !slices.Equal(names, []string{"c"}) || next != "" {
		t.Errorf("DeadLetterSourceQueues(dlq, 2, b) = %v, %q, %v; want c", names, next, err)
	}
	if names, _, err := e.DeadLetterSourceQueues(t.Context(), "other", 0, ""); err != nil || len(names) != 0 {
		t.Errorf("DeadLetterSourceQueues(other) = %v, %v; want none", names, err)
	}
	if _, _, err := e.DeadLetterSourceQueues(t.Context(), "gone", 0, ""); !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("DeadLetterSourceQueues(gone) error = %v, want ErrQueueDoesNotExist", err)
	}
}

func TestRedrivePersists(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustCreate(t, e, "dlq", nil)
	mustCreate(t, e, "src", redrive("dlq", "2"))
	want := attr(t, e, "src", "RedrivePolicy")
	reopened := openEngine(t, dir)
	if got := attr(t, reopened, "src", "RedrivePolicy"); got != want {
		t.Errorf("RedrivePolicy after reopen = %q, want %q", got, want)
	}
}

func TestStandardQueueKeepsGroupID(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q", nil)
	sendOne(t, e, "q", SendInput{Body: "m", GroupID: "tenant-a"})
	got := receive(t, e, "q", ReceiveInput{})
	if len(got) != 1 || got[0].GroupID != "tenant-a" || got[0].SequenceNumber != "" || got[0].DeduplicationID != "" {
		t.Errorf("Receive = %+v, want the group ID and no sequence number or deduplication ID", got)
	}
}
