package topic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/queue"
)

// checkEndpoint checks the protocol and endpoint of a subscription and returns
// the queue name, which is empty for an HTTP or HTTPS endpoint. The queue need
// not exist yet.
func (e *Engine) checkEndpoint(protocol, endpoint string) (string, error) {
	if protocol == protocolHTTP || protocol == protocolHTTPS {
		// The message text is unverified.
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != protocol || u.Host == "" || u.User != nil {
			return "", fmt.Errorf("endpoint %q is not a valid %s URL without user info: %w", endpoint, protocol, ErrInvalidParameter)
		}
		return "", nil
	}
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

// checkSubAttrs validates subscription attributes. pail supports
// RawMessageDelivery, FilterPolicy, and FilterPolicyScope; the policy is
// checked against its scope.
func checkSubAttrs(attrs map[string]string) error {
	for k, v := range attrs {
		if err := checkSubAttr(k, v); err != nil {
			return err
		}
	}
	_, err := compileFilter(attrs)
	return err
}

// checkSubAttr checks one attribute name and value without the filter policy.
func checkSubAttr(k, v string) error {
	switch k {
	case attrFilter, attrScope:
	case attrRaw:
		if v != "true" && v != "false" {
			return fmt.Errorf("subscription attribute %s must be true or false: %w", k, ErrInvalidParameter)
		}
	default:
		return fmt.Errorf("subscription attribute %q is not supported: %w", k, ErrInvalidParameter)
	}
	return nil
}

// rawDelivery reports whether s delivers the message without the envelope.
func (s *subDef) rawDelivery() bool { return s.Attributes[attrRaw] == "true" }

// authenticated reports ConfirmationWasAuthenticated: the subscription is
// confirmed, and its confirmation was signed with AuthenticateOnUnsubscribe.
func (s *subDef) authenticated() bool { return !s.Pending && !s.Unauthenticated }

// SubscribeInput is one Subscribe request. BaseURL is the scheme and host of
// the request; it forms the SubscribeURL and SigningCertURL of a confirmation.
type SubscribeInput struct {
	TopicARN, Protocol, Endpoint string
	Attributes                   map[string]string
	BaseURL                      string
}

// Subscribe adds a subscription to a topic and returns its ARN. SQS
// subscriptions are confirmed at once. An HTTP or HTTPS subscription is
// pending until its endpoint confirms, and pail sends the endpoint a
// SubscriptionConfirmation. It succeeds without change when the same endpoint
// already subscribes with the same attributes.
func (e *Engine) Subscribe(ctx context.Context, in SubscribeInput) (arn string, pending bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	sub, version, err := e.addSub(in)
	if err != nil {
		return "", false, err
	}
	if sub.Pending {
		e.sendConfirmation(sub, version, in.BaseURL)
	}
	return sub.ARN, sub.Pending, nil
}

// addSub stores the subscription, or finds the matching one, and returns a
// copy with the topic's signature version.
func (e *Engine) addSub(in SubscribeInput) (subDef, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.findTopic(in.TopicARN)
	if err != nil {
		return subDef{}, "", err
	}
	if _, err := e.checkEndpoint(in.Protocol, in.Endpoint); err != nil {
		return subDef{}, "", err
	}
	if err := checkSubAttrs(in.Attributes); err != nil {
		return subDef{}, "", err
	}
	version := topicAttrValue(t, in.TopicARN, "SignatureVersion")
	stored := maps.Clone(in.Attributes)
	maps.DeleteFunc(stored, unsetValue)
	for _, s := range e.topicSubs(in.TopicARN) {
		if s.Protocol != in.Protocol || s.Endpoint != in.Endpoint {
			continue
		}
		for k, v := range stored {
			if subAttrValue(s, k) != v {
				// The message text is unverified.
				return subDef{}, "", fmt.Errorf("subscription %s already exists with different attributes: %w", s.ARN, ErrInvalidParameter)
			}
		}
		return *s, version, nil
	}
	def := &subDef{ARN: in.TopicARN + ":" + newUUID(), TopicARN: in.TopicARN, Protocol: in.Protocol, Endpoint: in.Endpoint, Attributes: stored, Created: time.Now().Unix()}
	if in.Protocol != protocolSQS {
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		def.Pending, def.Token = true, hex.EncodeToString(b)
	}
	if err := e.persistSub(def); err != nil {
		return subDef{}, "", err
	}
	e.subs[def.ARN] = def
	return *def, version, nil
}

// ConfirmSubscription confirms the pending subscription of a topic that holds
// token and returns its ARN. authenticated records whether the caller signed
// the request and asked to authenticate Unsubscribe. Confirming a confirmed
// subscription again changes nothing (unverified).
func (e *Engine) ConfirmSubscription(ctx context.Context, topicARN, token string, authenticated bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.findTopic(topicARN); err != nil {
		return "", err
	}
	for _, s := range e.topicSubs(topicARN) {
		if token == "" || s.Token != token {
			continue
		}
		if s.Pending {
			updated := *s
			updated.Pending, updated.Unauthenticated = false, !authenticated
			if err := e.persistSub(&updated); err != nil {
				return "", err
			}
			*s = updated
		}
		return s.ARN, nil
	}
	// The message text is unverified.
	return "", fmt.Errorf("invalid token: %w", ErrInvalidParameter)
}

// subAttrValue returns the stored value of a subscription attribute, or its default.
func subAttrValue(s *subDef, k string) string {
	if v, ok := s.Attributes[k]; ok {
		return v
	}
	switch k {
	case attrRaw:
		return "false"
	case attrScope:
		return scopeAttributes
	}
	return ""
}

// unsetValue reports whether v removes attribute k: "" does, and so does an
// empty FilterPolicy object (recorded in sns-filter-policies).
func unsetValue(k, v string) bool {
	var m map[string]json.RawMessage
	return v == "" || k == attrFilter && json.Unmarshal([]byte(v), &m) == nil && m != nil && len(m) == 0
}

// Unsubscribe removes a subscription. A missing one is ErrNotFound (unverified).
// An unsigned call may remove only a subscription whose confirmation was not
// authenticated; otherwise it is ErrAuthorization (unverified).
func (e *Engine) Unsubscribe(ctx context.Context, arn string, signed bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.findSub(arn)
	if err != nil {
		return err
	}
	if !signed && s.authenticated() {
		return fmt.Errorf("subscription %s needs a signed request: %w", arn, ErrAuthorization)
	}
	if err := e.fs.Remove(subFile(s.ARN)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete subscription %s: %w", s.ARN, err)
	}
	delete(e.subs, s.ARN)
	return nil
}

func listing(s *subDef) Subscription {
	arn := s.ARN
	if s.Pending {
		arn = "PendingConfirmation"
	}
	return Subscription{ARN: arn, Owner: queue.Account, Protocol: s.Protocol, Endpoint: s.Endpoint, TopicARN: s.TopicARN}
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
// AWS lists them (recorded in sns-sqs-delivery and sns-filter-policies).
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
	policy := s.Attributes[attrFilter]
	out := []Attribute{
		{"SubscriptionPrincipal", "arn:aws:iam::" + queue.Account + ":root"},
		{"Owner", queue.Account},
		{attrRaw, subAttrValue(s, attrRaw)},
		{attrFilter, policy},
		{"TopicArn", s.TopicARN},
		{"Endpoint", s.Endpoint},
		{attrScope, subAttrValue(s, attrScope)},
		{"Protocol", s.Protocol},
		{"PendingConfirmation", strconv.FormatBool(s.Pending)},
		{"ConfirmationWasAuthenticated", strconv.FormatBool(s.authenticated())},
		{"SubscriptionArn", s.ARN},
	}
	if s.Protocol != protocolSQS {
		// The value and its position are unverified until the sns-http-subscriptions recording.
		out = append(out, Attribute{"EffectiveDeliveryPolicy", httpDeliveryPolicy})
	}
	if policy == "" {
		// Without a policy, AWS lists neither filter attribute, even with a scope set.
		out = slices.DeleteFunc(out, func(a Attribute) bool { return a.Key == attrFilter || a.Key == attrScope })
	}
	return out, nil
}

// SetSubscriptionAttribute sets one attribute of a subscription.
func (e *Engine) SetSubscriptionAttribute(ctx context.Context, arn, name, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkSubAttr(name, value); err != nil {
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
	if unsetValue(name, value) {
		delete(updated.Attributes, name)
	} else {
		updated.Attributes[name] = value
	}
	if err := checkSubAttrs(updated.Attributes); err != nil {
		return err
	}
	if err := e.persistSub(&updated); err != nil {
		return err
	}
	*s = updated
	return nil
}
