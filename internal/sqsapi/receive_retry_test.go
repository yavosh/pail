package sqsapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yavosh/pail/internal/queue"
)

type overLimitQueues struct{ Queues }

func (overLimitQueues) Receive(context.Context, string, queue.ReceiveInput) ([]queue.Message, error) {
	return nil, fmt.Errorf("receive attempt capacity: %w", queue.ErrOverLimit)
}

func TestReceiveAttemptOverLimit(t *testing.T) {
	srv := httptest.NewTestServer(t, New(Options{
		AccessKeyID: testKey, SecretAccessKey: testSecret, Queues: overLimitQueues{},
	}))
	srv.Client()
	e := &env{t: t, srv: srv}
	e.fail("ReceiveMessage", `{"QueueUrl":"http://pail.test:9000/000000000000/q.fifo","ReceiveRequestAttemptId":"attempt"}`,
		http.StatusBadRequest, "com.amazonaws.sqs#OverLimit", "OverLimit")
}
