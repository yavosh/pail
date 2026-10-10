package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/queue"
)

const (
	maxMessageBytes = 262144
	maxSubject      = 100
	maxAttributes   = 10
)

var numberRE = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)

// MessageAttribute is a typed message attribute. String, String.Array, and
// Number use StringValue; Binary uses BinaryValue.
type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// PublishInput is one message to publish. BaseURL is the scheme and host of the
// request; it forms the SigningCertURL and UnsubscribeURL of an envelope.
type PublishInput struct {
	TopicARN         string
	Message          string
	Subject          string
	MessageStructure string
	Attributes       map[string]MessageAttribute
	GroupID          string
	DeduplicationID  string
	BaseURL          string
}

// PublishEntry is one message of a batch.
type PublishEntry struct {
	ID               string
	Message          string
	Subject          string
	MessageStructure string
	Attributes       map[string]MessageAttribute
	GroupID          string
	DeduplicationID  string
}

// PublishResult is the outcome of one PublishEntry.
type PublishResult struct {
	ID        string
	MessageID string
	Err       error
}

// delivery is one SQS subscription that receives a message.
type delivery struct {
	arn, queue string
	raw        bool
}

// Publish sends a message to every subscription of a topic and returns its
// message ID. A failed delivery is logged and does not fail the publish.
func (e *Engine) Publish(ctx context.Context, in PublishInput) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validatePublish(in); err != nil {
		return "", err
	}
	e.mu.Lock()
	t, err := e.findTopic(in.TopicARN)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	version := topicAttrValue(t, in.TopicARN, "SignatureVersion")
	var targets []delivery
	for _, s := range e.topicSubs(in.TopicARN) {
		name, _ := e.checkEndpoint(s.Protocol, s.Endpoint) // checked when stored
		targets = append(targets, delivery{s.ARN, name, s.rawDelivery()})
	}
	e.mu.Unlock()

	n := notification{
		messageID: newUUID(), topicARN: in.TopicARN, subject: in.Subject,
		message: selectMessage(in.MessageStructure, in.Message), timestamp: time.Now().UTC().Format(timestampFormat),
		signatureVersion: version, baseURL: in.BaseURL, attrs: in.Attributes,
	}
	if slices.ContainsFunc(targets, func(d delivery) bool { return !d.raw }) {
		if n.signature, err = e.signString(version, n.stringToSign()); err != nil {
			return "", err
		}
	}
	for _, d := range targets {
		// Forwarding the group ID to SQS is unverified.
		send := queue.SendInput{Body: n.message, Attributes: rawAttributes(in.Attributes), GroupID: in.GroupID}
		if !d.raw {
			n.subscriptionARN = d.arn
			send = queue.SendInput{Body: n.envelope(), GroupID: in.GroupID}
		}
		e.deliver(ctx, in.TopicARN, d.queue, send)
	}
	return n.messageID, nil
}

// deliver sends one message to a queue and logs a failure.
func (e *Engine) deliver(ctx context.Context, topicARN, queueName string, in queue.SendInput) {
	res, err := e.queues.Send(ctx, queueName, []queue.SendInput{in})
	if err == nil && len(res) == 1 {
		err = res[0].Err
	}
	if err != nil {
		clogTopic().Warn("delivery failed", "topic", topicARN, "queue", queueName, "error", err)
	}
}

// PublishBatch publishes each entry like Publish. A bad entry fails alone;
// an unknown topic fails the call. The API checks the batch-level rules.
func (e *Engine) PublishBatch(ctx context.Context, topicARN string, entries []PublishEntry, baseURL string) ([]PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	_, err := e.findTopic(topicARN)
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out := make([]PublishResult, len(entries))
	for i, en := range entries {
		id, err := e.Publish(ctx, PublishInput{
			TopicARN: topicARN, Message: en.Message, Subject: en.Subject, MessageStructure: en.MessageStructure,
			Attributes: en.Attributes, GroupID: en.GroupID, DeduplicationID: en.DeduplicationID, BaseURL: baseURL,
		})
		out[i] = PublishResult{ID: en.ID, MessageID: id, Err: err}
	}
	return out, nil
}

// selectMessage returns the text for the SQS protocol: with a json message
// structure, the "sqs" value when present, else "default".
func selectMessage(structure, message string) string {
	if structure != "json" {
		return message
	}
	var m map[string]string
	_ = json.Unmarshal([]byte(message), &m) // checked in validatePublish
	if v, ok := m["sqs"]; ok {
		return v
	}
	return m["default"]
}

func invalid(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, ErrInvalidParameter)...)
}

// validatePublish applies the Publish rules that need no topic. Which of
// these AWS reports as InvalidParameter is unverified.
func validatePublish(in PublishInput) error {
	switch {
	case in.Message == "":
		return invalid("empty message")
	case in.DeduplicationID != "":
		// Unverified. A standard topic does accept MessageGroupId (sns-errors).
		return invalid("MessageDeduplicationId is for FIFO topics, which are not supported")
	case len(in.Subject) > maxSubject:
		return invalid("subject is longer than %d characters", maxSubject)
	case strings.ContainsFunc(in.Subject, func(r rune) bool { return r < 0x20 || r > 0x7e }):
		return invalid("subject must be printable ASCII with no line breaks")
	case len(in.Attributes) > maxAttributes:
		return invalid("%d message attributes, at most %d", len(in.Attributes), maxAttributes)
	}
	switch in.MessageStructure {
	case "":
	case "json":
		var m map[string]string
		if err := json.Unmarshal([]byte(in.Message), &m); err != nil {
			return invalid("with MessageStructure json, the message must be a JSON object of strings")
		}
		if _, ok := m["default"]; !ok {
			return invalid(`with MessageStructure json, the message needs a "default" key`)
		}
	default:
		return invalid("MessageStructure %q must be json", in.MessageStructure)
	}
	size := len(in.Message)
	for _, name := range slices.Sorted(maps.Keys(in.Attributes)) {
		a := in.Attributes[name]
		if err := validateAttribute(name, a); err != nil {
			return err
		}
		size += len(name) + len(a.DataType) + len(a.StringValue) + len(a.BinaryValue)
	}
	if size > maxMessageBytes {
		return invalid("message too long: %d bytes, at most %d", size, maxMessageBytes)
	}
	return nil
}

func validateAttribute(name string, a MessageAttribute) error {
	bad := func(why string) error { return invalid("message attribute %q: %s", name, why) }
	lower := strings.ToLower(name)
	switch {
	case name == "" || len(name) > 256:
		return bad("name must be 1 to 256 characters")
	case strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon."):
		return bad("name uses a reserved prefix")
	case strings.Contains(name, "..") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, "."):
		return bad("name has a misplaced period")
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
		default:
			return bad("name has an invalid character")
		}
	}
	base, label, hasLabel := strings.Cut(a.DataType, ".")
	if hasLabel && label == "" {
		return bad("data type has an empty custom label")
	}
	switch base {
	case "String", "Number":
		if a.StringValue == "" || len(a.BinaryValue) != 0 {
			return bad("a string or number attribute needs a string value and no binary value")
		}
		if base == "Number" && !numberRE.MatchString(a.StringValue) {
			return bad("number value is not a decimal number")
		}
		if a.DataType == "String.Array" {
			var list []any
			if err := json.Unmarshal([]byte(a.StringValue), &list); err != nil {
				return bad("a String.Array value must be a JSON array")
			}
		}
	case "Binary":
		if len(a.BinaryValue) == 0 || a.StringValue != "" {
			return bad("a binary attribute needs a binary value and no string value")
		}
	default:
		return bad("data type must be String, String.Array, Number, or Binary")
	}
	return nil
}
