package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yavosh/pail/internal/queue"
)

const attrRedrive = "RedrivePolicy"

func redriveARN(value string) (string, error) {
	var policy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &policy); err != nil || len(policy) != 1 {
		return "", invalid("RedrivePolicy must contain only deadLetterTargetArn")
	}
	var arn string
	if err := json.Unmarshal(policy["deadLetterTargetArn"], &arn); err != nil || arn == "" {
		return "", invalid("RedrivePolicy requires a deadLetterTargetArn")
	}
	return arn, nil
}

// redriveTarget validates the policy without requiring its queue to exist.
// The caller holds e.mu when it reads subscription attributes.
func (e *Engine) redriveTarget(s *subDef) (string, error) {
	value, ok := s.Attributes[attrRedrive]
	if !ok {
		return "", nil
	}
	arn, err := redriveARN(value)
	if err != nil {
		return "", err
	}
	name, ok := strings.CutPrefix(arn, sqsARNPrefix+e.region+":"+queue.Account+":")
	if !ok || name == "" || strings.Contains(name, ":") {
		return "", invalid("deadLetterTargetArn must be an SQS queue ARN of this region and account")
	}
	if isFIFO(s.TopicARN) && isFIFO(s.Endpoint) != isFIFO(name) {
		return "", invalid("dead-letter queue type must match the FIFO topic subscription endpoint")
	}
	return name, nil
}

type deadLetter struct {
	queue string
	input queue.SendInput
}

// redrive attempts the configured dead-letter delivery once, outside e.mu.
func (e *Engine) redrive(ctx context.Context, topicARN string, d deadLetter) {
	if d.queue == "" || ctx.Err() != nil {
		return
	}
	if err := e.deliver(ctx, d.queue, d.input); err != nil {
		clogTopic().Warn("dead-letter delivery failed", "topic", topicARN, "queue", d.queue, "error", err)
	}
}

func (e *Engine) deliver(ctx context.Context, name string, in queue.SendInput) error {
	res, err := e.queues.Send(ctx, name, []queue.SendInput{in})
	if err != nil {
		return err
	}
	if len(res) != 1 {
		return fmt.Errorf("queue returned %d results for one message", len(res))
	}
	return res[0].Err
}
