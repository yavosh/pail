// Package sqsapi serves the Amazon SQS API over the AWS JSON 1.0 protocol.
package sqsapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/sigv4"
)

// SQS messages and batches reach 1 MiB, and JSON escaping can triple them.
const maxRequestBytes = 4 << 20

const contentType = "application/x-amz-json-1.0"

// Queues is the queue engine the handler serves. *queue.Engine implements it.
type Queues interface {
	CreateQueue(ctx context.Context, name string, attrs, tags map[string]string) error
	DeleteQueue(ctx context.Context, name string) error
	Lookup(ctx context.Context, name string) error
	ListQueues(ctx context.Context, prefix string, limit int, after string) (names []string, next string, err error)
	Attributes(ctx context.Context, name string, names []string) (map[string]string, error)
	SetAttributes(ctx context.Context, name string, attrs map[string]string) error
	Purge(ctx context.Context, name string) error
	Send(ctx context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error)
	Receive(ctx context.Context, name string, in queue.ReceiveInput) ([]queue.Message, error)
	Delete(ctx context.Context, name string, handles []string) ([]error, error)
	ChangeVisibility(ctx context.Context, name string, changes []queue.VisibilityChange) ([]error, error)
	Tag(ctx context.Context, name string, tags map[string]string) error
	Untag(ctx context.Context, name string, keys []string) error
	Tags(ctx context.Context, name string) (map[string]string, error)
}

// Options configures the handler.
type Options struct {
	AccessKeyID     string
	SecretAccessKey string
	Queues          Queues
}

// modelOnly lists SQS operations that exist in the service model and that pail
// does not implement. Any other name outside the table is not an SQS operation.
var modelOnly = []string{
	"AddPermission", "RemovePermission", "StartMessageMoveTask", "CancelMessageMoveTask",
	"ListMessageMoveTasks", "ListDeadLetterSourceQueues",
}

// opFunc runs one operation. It returns the response value, or nil for an empty body.
type opFunc func(r *http.Request, body []byte) (any, error)

type handler struct {
	verifier    *sigv4.Verifier
	accessKeyID string
	queues      Queues
	ops         map[string]opFunc
}

// New returns the SQS handler. An operation outside the table answers UnsupportedOperation or UnknownOperation.
func New(opts Options) http.Handler {
	h := &handler{
		verifier:    sigv4.New(opts.AccessKeyID, opts.SecretAccessKey),
		accessKeyID: opts.AccessKeyID,
		queues:      opts.Queues,
	}
	h.ops = map[string]opFunc{
		"CreateQueue":                  operation(h.createQueue),
		"GetQueueUrl":                  operation(h.getQueueURL),
		"DeleteQueue":                  operation(h.deleteQueue),
		"PurgeQueue":                   operation(h.purgeQueue),
		"ListQueues":                   operation(h.listQueues),
		"GetQueueAttributes":           operation(h.getQueueAttributes),
		"SetQueueAttributes":           operation(h.setQueueAttributes),
		"TagQueue":                     operation(h.tagQueue),
		"UntagQueue":                   operation(h.untagQueue),
		"ListQueueTags":                operation(h.listQueueTags),
		"SendMessage":                  operation(h.sendMessage),
		"SendMessageBatch":             operation(h.sendMessageBatch),
		"ReceiveMessage":               operation(h.receiveMessage),
		"DeleteMessage":                operation(h.deleteMessage),
		"DeleteMessageBatch":           operation(h.deleteMessageBatch),
		"ChangeMessageVisibility":      operation(h.changeMessageVisibility),
		"ChangeMessageVisibilityBatch": operation(h.changeMessageVisibilityBatch),
	}
	return h
}

// operation decodes the JSON request body into In before it calls fn.
func operation[In any](fn func(r *http.Request, in In) (any, error)) opFunc {
	return func(r *http.Request, body []byte) (any, error) {
		var in In
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, fmt.Errorf("the request body is not valid for this operation: %w", errBadJSON)
		}
		return fn(r, in)
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("x-amzn-RequestId", newRequestID())
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")
	status := h.serve(w, r, op)
	log := clogSqsapi().With("method", r.Method, "op", op, "status", status, "duration", time.Since(start))
	if status == 0 {
		log = log.With("client_gone", true)
	}
	log.Info("request")
}

// serve answers the request and returns the status it wrote, or 0 when the
// client left during a long poll and nothing was written.
func (h *handler) serve(w http.ResponseWriter, r *http.Request, op string) int {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	if err := h.verifier.VerifyService(r, "sqs"); err != nil {
		return writeError(w, authError(err), err.Error())
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return writeError(w, authError(err), err.Error())
	}
	fn, ok := h.ops[op]
	if !ok {
		if slices.Contains(modelOnly, op) {
			return writeError(w, errUnsupportedOperation, fmt.Sprintf("%s is not supported", op))
		}
		return writeError(w, errUnknownOperation, fmt.Sprintf("unknown operation %q", op))
	}
	v, err := fn(r, body)
	if err != nil {
		if r.Context().Err() != nil {
			return 0
		}
		apiErr := mapError(err)
		return writeError(w, apiErr, err.Error())
	}
	return writeResult(w, v)
}

// writeResult answers 200 with v as JSON, or with an empty body when v is nil.
func writeResult(w http.ResponseWriter, v any) int {
	var body []byte
	if v != nil {
		var err error
		if body, err = json.Marshal(v); err != nil {
			return writeError(w, errInternalFailure, "encode response: "+err.Error())
		}
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
	return http.StatusOK
}

// writeError answers with the JSON 1.0 error body and the legacy query code.
func writeError(w http.ResponseWriter, e apiError, message string) int {
	body, _ := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{e.typ, message})
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("x-amzn-query-error", e.queryCode+";Sender")
	w.WriteHeader(e.status)
	_, _ = w.Write(body)
	return e.status
}

// newRequestID returns a random UUID v4.
func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
