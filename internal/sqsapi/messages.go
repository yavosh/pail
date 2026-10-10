package sqsapi

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/queue"
)

// messageAttribute is MessageAttributeValue in the SQS model. encoding/json
// writes BinaryValue as base64, which is the JSON 1.0 blob encoding.
type messageAttribute struct {
	DataType         string
	StringValue      string   `json:",omitempty"`
	BinaryValue      []byte   `json:",omitempty"`
	StringListValues []string `json:",omitempty"`
	BinaryListValues [][]byte `json:",omitempty"`
}

// toEngine converts attrs. AWS does not support list values.
func toEngine(attrs map[string]messageAttribute) (map[string]queue.MessageAttribute, error) {
	if attrs == nil {
		return nil, nil
	}
	out := make(map[string]queue.MessageAttribute, len(attrs))
	for name, a := range attrs {
		if len(a.StringListValues) > 0 || len(a.BinaryListValues) > 0 {
			return nil, fmt.Errorf("message attribute %q: list values are not supported: %w", name, queue.ErrUnsupportedOperation)
		}
		out[name] = queue.MessageAttribute{DataType: a.DataType, StringValue: a.StringValue, BinaryValue: a.BinaryValue}
	}
	return out, nil
}

func fromEngine(attrs map[string]queue.MessageAttribute) map[string]messageAttribute {
	out := make(map[string]messageAttribute, len(attrs))
	for name, a := range attrs {
		out[name] = messageAttribute{DataType: a.DataType, StringValue: a.StringValue, BinaryValue: a.BinaryValue}
	}
	return out
}

// sendEntry holds the fields that SendMessage and a SendMessageBatch entry share.
type sendEntry struct {
	MessageBody             string
	DelaySeconds            *int
	MessageAttributes       map[string]messageAttribute
	MessageSystemAttributes map[string]messageAttribute
	MessageDeduplicationID  string `json:"MessageDeduplicationId"`
	MessageGroupID          string `json:"MessageGroupId"`
}

// toInput converts e. The engine applies the group and deduplication rules of
// the queue type.
func (e sendEntry) toInput() (queue.SendInput, error) {
	if e.MessageBody == "" {
		return queue.SendInput{}, fmt.Errorf("MessageBody is required: %w", errMissingParam)
	}
	attrs, err := toEngine(e.MessageAttributes)
	if err != nil {
		return queue.SendInput{}, err
	}
	sys, err := toEngine(e.MessageSystemAttributes)
	if err != nil {
		return queue.SendInput{}, err
	}
	return queue.SendInput{
		Body: e.MessageBody, DelaySeconds: e.DelaySeconds, Attributes: attrs, SystemAttributes: sys,
		GroupID: e.MessageGroupID, DeduplicationID: e.MessageDeduplicationID,
	}, nil
}

// size is the part of the batch size limit that this entry adds.
func (e sendEntry) size() int {
	n := len(e.MessageBody)
	for name, a := range e.MessageAttributes {
		n += len(name) + len(a.DataType) + len(a.StringValue) + len(a.BinaryValue)
	}
	return n
}

type sendMessageRequest struct {
	QueueURL string `json:"QueueUrl"`
	sendEntry
}

type sendMessageResponse struct {
	MD5OfMessageBody             string
	MessageID                    string `json:"MessageId"`
	MD5OfMessageAttributes       string `json:",omitempty"`
	MD5OfMessageSystemAttributes string `json:",omitempty"`
	SequenceNumber               string `json:",omitempty"`
}

func (h *handler) sendMessage(r *http.Request, in sendMessageRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	input, err := in.toInput()
	if err != nil {
		return nil, err
	}
	res, err := h.queues.Send(r.Context(), name, []queue.SendInput{input})
	if err != nil {
		return nil, err
	}
	if res[0].Err != nil {
		return nil, res[0].Err
	}
	return sendMessageResponse{
		MD5OfMessageBody:             res[0].MD5OfBody,
		MessageID:                    res[0].MessageID,
		MD5OfMessageAttributes:       res[0].MD5OfAttributes,
		MD5OfMessageSystemAttributes: res[0].MD5OfSystemAttributes,
		SequenceNumber:               res[0].SequenceNumber,
	}, nil
}

type receiveMessageRequest struct {
	QueueURL                    string `json:"QueueUrl"`
	AttributeNames              []string
	MessageSystemAttributeNames []string
	MessageAttributeNames       []string
	MaxNumberOfMessages         *int
	VisibilityTimeout           *int
	WaitTimeSeconds             *int
	// ReceiveRequestAttemptId only retries a FIFO receive. pail ignores it.
}

type receivedMessage struct {
	MessageID              string                      `json:"MessageId"`
	ReceiptHandle          string                      `json:"ReceiptHandle"`
	MD5OfBody              string                      `json:"MD5OfBody"`
	Body                   string                      `json:"Body"`
	Attributes             map[string]string           `json:",omitempty"`
	MD5OfMessageAttributes string                      `json:",omitempty"`
	MessageAttributes      map[string]messageAttribute `json:",omitempty"`
}

type receiveMessageResponse struct {
	Messages []receivedMessage `json:",omitempty"`
}

func (h *handler) receiveMessage(r *http.Request, in receiveMessageRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	var limit int // 0 asks the engine for its default
	if in.MaxNumberOfMessages != nil {
		limit = *in.MaxNumberOfMessages
	}
	msgs, err := h.queues.Receive(r.Context(), name, queue.ReceiveInput{
		Max:               limit,
		VisibilityTimeout: in.VisibilityTimeout,
		WaitTimeSeconds:   in.WaitTimeSeconds,
	})
	if err != nil {
		return nil, err
	}
	sysNames := slices.Concat(in.AttributeNames, in.MessageSystemAttributeNames)
	var out receiveMessageResponse
	for _, m := range msgs {
		attrs := filterAttributes(m.Attributes, in.MessageAttributeNames)
		rm := receivedMessage{
			MessageID:     m.ID,
			ReceiptHandle: m.ReceiptHandle,
			MD5OfBody:     m.MD5OfBody,
			Body:          m.Body,
			Attributes:    h.systemAttributes(m, sysNames),
		}
		if len(attrs) > 0 {
			rm.MessageAttributes = fromEngine(attrs)
			rm.MD5OfMessageAttributes = queue.MD5OfAttributes(attrs)
		}
		out.Messages = append(out.Messages, rm)
	}
	return out, nil
}

// systemAttributes returns the system attributes of m that names ask for.
func (h *handler) systemAttributes(m queue.Message, names []string) map[string]string {
	all := slices.Contains(names, "All")
	want := func(n string) bool { return all || slices.Contains(names, n) }
	out := map[string]string{}
	if want("SenderId") {
		// AWS returns the caller's IAM unique ID. pail has one account.
		out["SenderId"] = h.accessKeyID
	}
	if want("SentTimestamp") {
		out["SentTimestamp"] = strconv.FormatInt(m.SentAt.UnixMilli(), 10)
	}
	if want("ApproximateReceiveCount") {
		out["ApproximateReceiveCount"] = strconv.Itoa(m.ReceiveCount)
	}
	if want("ApproximateFirstReceiveTimestamp") {
		out["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10)
	}
	if m.SequenceNumber != "" && want("SequenceNumber") {
		out["SequenceNumber"] = m.SequenceNumber
	}
	if m.GroupID != "" && want("MessageGroupId") {
		out["MessageGroupId"] = m.GroupID
	}
	if m.DeduplicationID != "" && want("MessageDeduplicationId") {
		out["MessageDeduplicationId"] = m.DeduplicationID
	}
	if m.DeadLetterSourceARN != "" && want("DeadLetterQueueSourceArn") {
		out["DeadLetterQueueSourceArn"] = m.DeadLetterSourceARN
	}
	if trace, ok := m.SystemAttributes["AWSTraceHeader"]; ok && want("AWSTraceHeader") {
		out["AWSTraceHeader"] = trace.StringValue
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// filterAttributes keeps the attributes that a name in names selects. "All" and
// ".*" select every attribute, and "prefix.*" selects names starting "prefix"
// (AWS matches the text before ".*", without the dot).
func filterAttributes(attrs map[string]queue.MessageAttribute, names []string) map[string]queue.MessageAttribute {
	if len(names) == 0 || len(attrs) == 0 {
		return nil
	}
	if slices.Contains(names, "All") || slices.Contains(names, ".*") {
		return attrs
	}
	out := map[string]queue.MessageAttribute{}
	for n, a := range attrs {
		if slices.ContainsFunc(names, func(p string) bool {
			if prefix, ok := strings.CutSuffix(p, ".*"); ok {
				return strings.HasPrefix(n, prefix)
			}
			return n == p
		}) {
			out[n] = a
		}
	}
	return out
}

type deleteMessageRequest struct {
	QueueURL      string `json:"QueueUrl"`
	ReceiptHandle string
}

func (h *handler) deleteMessage(r *http.Request, in deleteMessageRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	if in.ReceiptHandle == "" {
		return nil, fmt.Errorf("ReceiptHandle is required: %w", errMissingParam)
	}
	errs, err := h.queues.Delete(r.Context(), name, []string{in.ReceiptHandle})
	if err != nil {
		return nil, err
	}
	return nil, errs[0]
}

type changeMessageVisibilityRequest struct {
	QueueURL          string `json:"QueueUrl"`
	ReceiptHandle     string
	VisibilityTimeout *int
}

func (h *handler) changeMessageVisibility(r *http.Request, in changeMessageVisibilityRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	if in.ReceiptHandle == "" {
		return nil, fmt.Errorf("ReceiptHandle is required: %w", errMissingParam)
	}
	if in.VisibilityTimeout == nil {
		return nil, fmt.Errorf("VisibilityTimeout is required: %w", errMissingParam)
	}
	errs, err := h.queues.ChangeVisibility(r.Context(), name, []queue.VisibilityChange{{ReceiptHandle: in.ReceiptHandle, Timeout: *in.VisibilityTimeout}})
	if err != nil {
		return nil, err
	}
	return nil, errs[0]
}
