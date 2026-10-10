package topic

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/queue"
)

// checkEndpoint checks the protocol and endpoint of a subscription and returns
// the queue name. The queue need not exist yet.
func (e *Engine) checkEndpoint(protocol, endpoint string) (string, error) {
	if protocol != protocolSQS {
		return "", fmt.Errorf("protocol %q is not supported: %w", protocol, ErrInvalidParameter)
	}
	name, ok := strings.CutPrefix(endpoint, sqsARNPrefix+e.region+":"+queue.Account+":")
	if !ok || name == "" || strings.Contains(name, ":") {
		return "", fmt.Errorf("endpoint %q is not an SQS queue ARN of this region and account: %w", endpoint, ErrInvalidParameter)
	}
	if strings.HasSuffix(name, ".fifo") {
		// AWS rejects this pairing too; the message text is unverified.
		return "", fmt.Errorf("endpoint %q: a FIFO queue cannot subscribe to a standard topic: %w", endpoint, ErrInvalidParameter)
	}
	return name, nil
}

// checkSubAttrs validates subscription attributes. RawMessageDelivery is the
// only one that pail supports; filter policies come later.
func checkSubAttrs(attrs map[string]string) error {
	for k, v := range attrs {
		if k != attrRaw {
			return fmt.Errorf("subscription attribute %q is not supported: %w", k, ErrInvalidParameter)
		}
		if v != "true" && v != "false" {
			return fmt.Errorf("subscription attribute %s must be true or false: %w", k, ErrInvalidParameter)
		}
	}
	return nil
}

// rawDelivery reports whether s delivers the message without the envelope.
func (s *subDef) rawDelivery() bool { return s.Attributes[attrRaw] == "true" }

// Subscribe adds a subscription to a topic and returns its ARN. SQS
// subscriptions are confirmed at once. It succeeds without change when the
// same endpoint already subscribes with the same attributes.
func (e *Engine) Subscribe(ctx context.Context, topicARN, protocol, endpoint string, attrs map[string]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.findTopic(topicARN); err != nil {
		return "", err
	}
	if _, err := e.checkEndpoint(protocol, endpoint); err != nil {
		return "", err
	}
	if err := checkSubAttrs(attrs); err != nil {
		return "", err
	}
	for _, s := range e.topicSubs(topicARN) {
		if s.Protocol != protocol || s.Endpoint != endpoint {
			continue
		}
		for k, v := range attrs {
			if subAttrValue(s, k) != v {
				// The message text is unverified.
				return "", fmt.Errorf("subscription %s already exists with different attributes: %w", s.ARN, ErrInvalidParameter)
			}
		}
		return s.ARN, nil
	}
	def := &subDef{ARN: topicARN + ":" + newUUID(), TopicARN: topicARN, Protocol: protocol, Endpoint: endpoint, Attributes: maps.Clone(attrs), Created: time.Now().Unix()}
	if err := e.persistSub(def); err != nil {
		return "", err
	}
	e.subs[def.ARN] = def
	return def.ARN, nil
}

// subAttrValue returns the stored value of a subscription attribute, or its default.
func subAttrValue(s *subDef, k string) string {
	if v, ok := s.Attributes[k]; ok {
		return v
	}
	return "false"
}

// Unsubscribe removes a subscription. A missing one is ErrNotFound (unverified).
func (e *Engine) Unsubscribe(ctx context.Context, arn string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.findSub(arn)
	if err != nil {
		return err
	}
	if err := e.fs.Remove(subFile(s.ARN)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete subscription %s: %w", s.ARN, err)
	}
	delete(e.subs, s.ARN)
	return nil
}

func listing(s *subDef) Subscription {
	return Subscription{ARN: s.ARN, Owner: queue.Account, Protocol: s.Protocol, Endpoint: s.Endpoint, TopicARN: s.TopicARN}
}

// listSubs returns the page of the sorted subscription ARNs after next. The caller holds e.mu.
func (e *Engine) listSubs(arns []string, next string) ([]Subscription, string) {
	arns, after := page(arns, next)
	out := make([]Subscription, len(arns))
	for i, a := range arns {
		out[i] = listing(e.subs[a])
	}
	return out, after
}

// ListSubscriptions returns every subscription sorted by ARN, one page after next.
func (e *Engine) ListSubscriptions(ctx context.Context, next string) ([]Subscription, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out, after := e.listSubs(slices.Sorted(maps.Keys(e.subs)), next)
	return out, after, nil
}

// ListSubscriptionsByTopic is ListSubscriptions for one topic.
func (e *Engine) ListSubscriptionsByTopic(ctx context.Context, topicARN, next string) ([]Subscription, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.findTopic(topicARN); err != nil {
		return nil, "", err
	}
	var arns []string
	for _, s := range e.topicSubs(topicARN) {
		arns = append(arns, s.ARN)
	}
	out, after := e.listSubs(arns, next)
	return out, after, nil
}

// SubscriptionAttributes returns the attributes of a subscription in the order
// AWS lists them (recorded in sns-sqs-delivery).
func (e *Engine) SubscriptionAttributes(ctx context.Context, arn string) ([]Attribute, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.findSub(arn)
	if err != nil {
		return nil, err
	}
	// AWS returns the caller's ARN as SubscriptionPrincipal; the value's shape is unverified.
	return []Attribute{
		{"SubscriptionPrincipal", "arn:aws:iam::" + queue.Account + ":root"},
		{"Owner", queue.Account},
		{attrRaw, subAttrValue(s, attrRaw)},
		{"TopicArn", s.TopicARN},
		{"Endpoint", s.Endpoint},
		{"Protocol", s.Protocol},
		{"PendingConfirmation", "false"},
		{"ConfirmationWasAuthenticated", "true"},
		{"SubscriptionArn", s.ARN},
	}, nil
}

// SetSubscriptionAttribute sets one attribute of a subscription.
func (e *Engine) SetSubscriptionAttribute(ctx context.Context, arn, name, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkSubAttrs(map[string]string{name: value}); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.findSub(arn)
	if err != nil {
		return err
	}
	updated := *s
	updated.Attributes = maps.Clone(s.Attributes)
	if updated.Attributes == nil {
		updated.Attributes = map[string]string{}
	}
	updated.Attributes[name] = value
	if err := e.persistSub(&updated); err != nil {
		return err
	}
	*s = updated
	return nil
}
