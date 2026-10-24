package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestReceiveAttemptValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		id   *string
		bad  bool
	}{
		{"absent", nil, false}, {"empty", new(""), true},
		{"space", new("a b"), true}, {"unicode", new("é"), true},
		{"too long", new(strings.Repeat("a", 129)), true},
		{"max length", new(strings.Repeat("a", 128)), false},
		{"punctuation", new("!~"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEngine(t)
			mustCreate(t, e, "q.fifo", fifoAttrs)
			mustCreate(t, e, "standard", nil)
			_, err := e.Receive(t.Context(), "q.fifo", ReceiveInput{AttemptID: tt.id})
			if errors.Is(err, ErrInvalidParameterValue) != tt.bad || !tt.bad && err != nil {
				t.Errorf("Receive FIFO attempt %v error = %v, want invalid = %v", tt.id, err, tt.bad)
			}
			if _, err := e.Receive(t.Context(), "standard", ReceiveInput{AttemptID: tt.id}); err != nil {
				t.Errorf("Receive standard attempt %v error = %v, want nil", tt.id, err)
			}
		})
	}
}

func TestReceiveAttemptVisibility(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		sendOne(t, e, "q.fifo", fifoMsg("second", "g"))
		in := ReceiveInput{AttemptID: new("attempt"), VisibilityTimeout: new(10)}
		first := receive(t, e, "q.fifo", in)
		time.Sleep(4 * time.Second)
		retry := receive(t, e, "q.fifo", in)
		if len(retry) != 1 || retry[0].ID != first[0].ID || retry[0].ReceiptHandle != first[0].ReceiptHandle || retry[0].ReceiveCount != 1 {
			t.Fatalf("retry = %+v, want first selection with count 1", retry)
		}
		time.Sleep(6 * time.Second)
		if got := receive(t, e, "q.fifo", ReceiveInput{}); len(got) != 0 {
			t.Errorf("Receive at original visibility expiry = %+v, want empty", got)
		}
		time.Sleep(4 * time.Second)
		if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
			t.Errorf("retry at visibility expiry error = %v, want invalid parameter", err)
		}
		if got := receive(t, e, "q.fifo", ReceiveInput{}); len(got) != 1 || got[0].Body != "first" || got[0].ReceiveCount != 2 {
			t.Errorf("Receive after visibility expiry = %+v, want first with count 2", got)
		}
	})
}

func TestReceiveAttemptExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		sendOne(t, e, "q.fifo", fifoMsg("first", "a"))
		in := ReceiveInput{AttemptID: new("attempt"), VisibilityTimeout: new(600)}
		first := receive(t, e, "q.fifo", in)
		time.Sleep(5*time.Minute - time.Nanosecond)
		if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].ID != first[0].ID {
			t.Fatalf("retry before expiry = %+v, want first message", got)
		}
		sendOne(t, e, "q.fifo", fifoMsg("second", "b"))
		time.Sleep(time.Nanosecond)
		if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].Body != "second" || got[0].ReceiveCount != 1 {
			t.Errorf("retry at five minutes = %+v, want a new selection", got)
		}
	})
}

func TestReceiveAttemptZeroVisibility(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
	in := ReceiveInput{AttemptID: new("attempt"), VisibilityTimeout: new(0)}
	receive(t, e, "q.fifo", in)
	if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("retry zero visibility error = %v, want invalid parameter", err)
	}
}

func TestReceiveAttemptValidationIsAtomic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		sendOne(t, e, "q.fifo", fifoMsg("first", "a"))
		sendOne(t, e, "q.fifo", fifoMsg("second", "b"))
		in := ReceiveInput{AttemptID: new("attempt"), Max: 2, VisibilityTimeout: new(10)}
		first := receive(t, e, "q.fifo", in)
		if len(first) != 2 {
			t.Fatalf("first Receive = %+v, want two messages", first)
		}
		time.Sleep(5 * time.Second)
		if _, err := e.Delete(t.Context(), "q.fifo", []string{first[1].ReceiptHandle}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("retry partially deleted selection error = %v, want invalid parameter", err)
		}
		time.Sleep(5 * time.Second)
		if got := receive(t, e, "q.fifo", ReceiveInput{}); len(got) != 1 || got[0].ID != first[0].ID || got[0].ReceiveCount != 2 {
			t.Errorf("Receive at original expiry = %+v, want remaining message with count 2", got)
		}
	})
}

func TestReceiveAttemptResetsLongPollVisibility(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		in := ReceiveInput{AttemptID: new("attempt"), VisibilityTimeout: new(10)}
		first := receive(t, e, "q.fifo", in)
		start := time.Now()
		wait := startReceive(t.Context(), e, "q.fifo", ReceiveInput{WaitTimeSeconds: new(20)})
		synctest.Wait()
		time.Sleep(5 * time.Second)
		receive(t, e, "q.fifo", in)
		got, err := wait()
		if err != nil || len(got) != 1 || got[0].ID != first[0].ID || got[0].ReceiveCount != 2 {
			t.Fatalf("waiting Receive = %+v, %v, want original message with count 2", got, err)
		}
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Errorf("waiting Receive elapsed = %v, want 15s", elapsed)
		}
	})
}

func TestReceiveAttemptEmptyAndPurge(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	in := ReceiveInput{AttemptID: new("empty")}
	if got := receive(t, e, "q.fifo", in); len(got) != 0 {
		t.Fatalf("first Receive = %+v, want empty", got)
	}
	sendOne(t, e, "q.fifo", fifoMsg("before-purge", "g"))
	if got := receive(t, e, "q.fifo", in); len(got) != 0 {
		t.Errorf("retry after send = %+v, want cached empty result", got)
	}
	if err := e.Purge(t.Context(), "q.fifo"); err != nil {
		t.Fatal(err)
	}
	sendOne(t, e, "q.fifo", fifoMsg("after-purge", "g"))
	if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].Body != "after-purge" {
		t.Fatalf("retry after purge = %+v, want new message", got)
	}
	if err := e.Purge(t.Context(), "q.fifo"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("second Purge error = %v, want purge in progress", err)
	}
	if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].Body != "after-purge" || got[0].ReceiveCount != 1 {
		t.Errorf("retry after rejected purge = %+v, want preserved attempt", got)
	}
}

func TestReceiveAttemptEmptyVisibility(t *testing.T) {
	for _, tt := range []struct {
		name       string
		visibility int
		wait       int
	}{
		{"zero", 0, 0}, {"short poll", 2, 0}, {"long poll", 2, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newEngine(t)
				mustCreate(t, e, "q.fifo", fifoAttrs)
				in := ReceiveInput{AttemptID: new("empty"), VisibilityTimeout: new(tt.visibility), WaitTimeSeconds: new(tt.wait)}
				started := time.Now()
				if got := receive(t, e, "q.fifo", in); len(got) != 0 {
					t.Fatalf("first Receive = %+v, want empty", got)
				}
				in.WaitTimeSeconds = new(0)
				if tt.visibility > 0 {
					if got := receive(t, e, "q.fifo", in); len(got) != 0 {
						t.Fatalf("retry after completion = %+v, want empty", got)
					}
					time.Sleep(time.Second)
					if got := receive(t, e, "q.fifo", in); len(got) != 0 {
						t.Fatalf("retry before deadline = %+v, want empty", got)
					}
					time.Sleep(time.Second)
				}
				sendOne(t, e, "q.fifo", fifoMsg("new", "g"))
				if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
					t.Errorf("retry at original visibility deadline error = %v, want invalid parameter", err)
				}
				time.Sleep(time.Until(started.Add(dedupWindow - time.Nanosecond)))
				if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
					t.Errorf("retry before five-minute expiry error = %v, want invalid parameter", err)
				}
				time.Sleep(time.Nanosecond)
				if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].Body != "new" || got[0].ReceiveCount != 1 {
					t.Errorf("retry at five-minute expiry = %+v, want new message with count 1", got)
				}
			})
		})
	}
}

func TestReceiveAttemptChangedSelection(t *testing.T) {
	for _, change := range []string{"delete", "visibility zero", "visibility unchanged", "retention", "another receive"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newEngine(t)
				mustCreate(t, e, "q.fifo", map[string]string{"FifoQueue": "true", "MessageRetentionPeriod": "60"})
				sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
				in := ReceiveInput{AttemptID: new("attempt")}
				if change == "retention" {
					in.VisibilityTimeout = new(600)
				}
				first := receive(t, e, "q.fifo", in)
				switch change {
				case "delete":
					if _, err := e.Delete(t.Context(), "q.fifo", []string{first[0].ReceiptHandle}); err != nil {
						t.Fatal(err)
					}
				case "visibility zero", "visibility unchanged":
					timeout := 0
					if change == "visibility unchanged" {
						timeout = 30
					}
					if _, err := e.ChangeVisibility(t.Context(), "q.fifo", []VisibilityChange{{first[0].ReceiptHandle, timeout}}); err != nil {
						t.Fatal(err)
					}
				case "retention":
					time.Sleep(time.Minute)
				case "another receive":
					time.Sleep(30 * time.Second)
					receive(t, e, "q.fifo", ReceiveInput{})
				}
				if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrInvalidParameterValue) {
					t.Errorf("retry after %s error = %v, want invalid parameter", change, err)
				}
			})
		})
	}
}

func TestReceiveAttemptChangedParameters(t *testing.T) {
	e := newEngine(t)
	mustCreate(t, e, "q.fifo", fifoAttrs)
	sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
	in := ReceiveInput{AttemptID: new("attempt")}
	receive(t, e, "q.fifo", in)
	for _, changed := range []ReceiveInput{
		{AttemptID: in.AttemptID, Max: 2}, {AttemptID: in.AttemptID, VisibilityTimeout: new(31)},
	} {
		if _, err := e.Receive(t.Context(), "q.fifo", changed); !errors.Is(err, ErrInvalidParameterValue) {
			t.Errorf("retry changed %+v error = %v, want invalid parameter", changed, err)
		}
	}
	in.Max, in.VisibilityTimeout, in.WaitTimeSeconds = 1, new(30), new(20)
	if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].ReceiveCount != 1 {
		t.Errorf("retry explicit defaults and changed wait = %+v, want original selection", got)
	}
}

func TestReceiveAttemptQueueLifecycle(t *testing.T) {
	for _, change := range []string{"delete", "restart"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			e := openEngine(t, dir)
			mustCreate(t, e, "q.fifo", fifoAttrs)
			sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
			in := ReceiveInput{AttemptID: new("attempt")}
			receive(t, e, "q.fifo", in)
			if change == "delete" {
				if err := e.DeleteQueue(t.Context(), "q.fifo"); err != nil {
					t.Fatal(err)
				}
				mustCreate(t, e, "q.fifo", fifoAttrs)
			} else {
				e = openEngine(t, dir)
			}
			sendOne(t, e, "q.fifo", fifoMsg("second", "g"))
			if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].Body != "second" {
				t.Errorf("retry after %s = %+v, want new selection", change, got)
			}
		})
	}
}

func TestReceiveAttemptConcurrentLongPolls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		in := ReceiveInput{AttemptID: new("attempt"), WaitTimeSeconds: new(20)}
		first := startReceive(t.Context(), e, "q.fifo", in)
		second := startReceive(t.Context(), e, "q.fifo", in)
		synctest.Wait()
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		a, errA := first()
		b, errB := second()
		if errA != nil || errB != nil || len(a) != 1 || len(b) != 1 {
			t.Fatalf("concurrent Receive = %v, %v; %v, %v; want one each", a, errA, b, errB)
		}
		if a[0].ID != b[0].ID || a[0].ReceiveCount != 1 || b[0].ReceiveCount != 1 {
			t.Errorf("concurrent selection = %+v, %+v; want same ID with count 1", a, b)
		}
	})
}

func TestReceiveAttemptCanceledLongPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		in := ReceiveInput{AttemptID: new("attempt"), WaitTimeSeconds: new(20)}
		ctx, cancel := context.WithCancel(t.Context())
		wait := startReceive(ctx, e, "q.fifo", in)
		synctest.Wait()
		cancel()
		if _, err := wait(); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Receive error = %v, want context canceled", err)
		}
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		if got := receive(t, e, "q.fifo", in); len(got) != 1 {
			t.Errorf("retry canceled attempt = %+v, want new selection", got)
		}
	})
}

func TestReceiveAttemptDeadlineLongPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		in := ReceiveInput{AttemptID: new("attempt"), WaitTimeSeconds: new(20)}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := e.Receive(ctx, "q.fifo", in); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired Receive error = %v, want deadline exceeded", err)
		}
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		if got := receive(t, e, "q.fifo", in); len(got) != 1 {
			t.Errorf("retry expired attempt = %+v, want new selection", got)
		}
	})
}

func TestReceiveAttemptCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(t)
		mustCreate(t, e, "q.fifo", fifoAttrs)
		q := e.queues["q.fifo"]
		q.attempts = map[string]receiveAttempt{}
		for i := range maxReceiveAttempts {
			q.attempts[fmt.Sprint(i)] = receiveAttempt{
				expires: time.Now().Add(dedupWindow), validUntil: time.Now().Add(30 * time.Second), limit: 1, timeout: 30,
			}
		}
		if got := receive(t, e, "q.fifo", ReceiveInput{AttemptID: new("0")}); len(got) != 0 {
			t.Fatalf("existing attempt at capacity = %+v, want cached empty result", got)
		}
		sendOne(t, e, "q.fifo", fifoMsg("first", "g"))
		in := ReceiveInput{AttemptID: new("new")}
		if _, err := e.Receive(t.Context(), "q.fifo", in); !errors.Is(err, ErrOverLimit) {
			t.Errorf("new attempt at capacity error = %v, want over limit", err)
		}
		time.Sleep(dedupWindow)
		if got := receive(t, e, "q.fifo", in); len(got) != 1 || got[0].ReceiveCount != 1 {
			t.Errorf("new attempt after expiry = %+v, want unconsumed message", got)
		}
		if len(q.attempts) != 1 {
			t.Errorf("attempt count after expiry = %d, want 1", len(q.attempts))
		}
	})
}
