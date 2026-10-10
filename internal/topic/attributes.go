package topic

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/queue"
)

// defaultDeliveryPolicy is what AWS reports as EffectiveDeliveryPolicy for a
// topic with no delivery policy (recorded in test/diff, sns-topic-basics).
const defaultDeliveryPolicy = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}`

// defaultPolicy is AWS's default topic policy for the topic arn.
func defaultPolicy(arn string) string {
	return `{"Version":"2008-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},"Action":["SNS:GetTopicAttributes","SNS:SetTopicAttributes","SNS:AddPermission","SNS:RemovePermission","SNS:DeleteTopic","SNS:Subscribe","SNS:ListSubscriptionsByTopic","SNS:Publish"],"Resource":"` + arn + `","Condition":{"StringEquals":{"AWS:SourceOwner":"` + queue.Account + `"}}}]}`
}

// topicAttrNames are the attributes a client can set.
var topicAttrNames = []string{"DisplayName", "Policy", "DeliveryPolicy", "KmsMasterKeyId", "SignatureVersion", "TracingConfig"}

// noopAttr reports whether an attribute turns off a FIFO feature that pail never
// has. pail accepts it and stores nothing.
func noopAttr(name, value string) bool {
	return (name == "FifoTopic" || name == "ContentBasedDeduplication") && value == "false"
}

// checkTopicAttr validates one attribute. An empty value means unset.
func checkTopicAttr(name, value string) error {
	if noopAttr(name, value) {
		return nil
	}
	if !slices.Contains(topicAttrNames, name) {
		return fmt.Errorf("topic attribute %q is not supported: %w", name, ErrInvalidParameter)
	}
	ok := true
	switch name {
	case "DisplayName":
		// The 100-character limit is AWS's documented SMS limit; the rule is unverified.
		ok = utf8.RuneCountInString(value) <= 100 && !strings.ContainsFunc(value, unicode.IsControl)
	case "Policy":
		ok = value == "" || isJSONObject(value)
	case attrDelivery:
		if value != "" {
			_, err := normalizeTopicPolicy(value)
			return err
		}
	case "SignatureVersion":
		ok = value == "" || value == "1" || value == "2"
	case "TracingConfig":
		ok = value == "" || value == "PassThrough" || value == "Active"
	}
	if !ok {
		return fmt.Errorf("topic attribute %s has an invalid value: %w", name, ErrInvalidParameter)
	}
	return nil
}

// topicAttrValue returns the stored value of attribute k, or its default.
func topicAttrValue(t *topicDef, arn, k string) string {
	if v, ok := t.Attributes[k]; ok {
		return v
	}
	if k == "Policy" {
		return defaultPolicy(arn)
	}
	if k == "SignatureVersion" {
		return "1"
	}
	return ""
}

// TopicAttributes returns the attributes of a topic in the order AWS lists them.
func (e *Engine) TopicAttributes(ctx context.Context, arn string) ([]Attribute, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.lock()
	defer e.mu.Unlock()
	t, err := e.findTopic(arn)
	if err != nil {
		return nil, err
	}
	confirmed, pending := 0, 0
	for _, s := range e.topicSubs(arn) {
		if s.Pending {
			pending++
		} else {
			confirmed++
		}
	}
	// AWS updates the subscription counts with a delay (unverified).
	// The order is recorded in sns-edge-cases and sns-delivery-policy. The
	// settable attributes appear only when set.
	var out []Attribute
	add := func(k, v string) { out = append(out, Attribute{k, v}) }
	addIfSet := func(k string) {
		if v, ok := t.Attributes[k]; ok {
			add(k, v)
		}
	}
	add("Policy", topicAttrValue(t, arn, "Policy"))
	addIfSet("SignatureVersion")
	add("Owner", queue.Account)
	add("SubscriptionsPending", fmt.Sprint(pending))
	addIfSet("KmsMasterKeyId")
	add("TopicArn", arn)
	addIfSet("TracingConfig")
	add("EffectiveDeliveryPolicy", effectiveTopicPolicy(t))
	add("SubscriptionsConfirmed", fmt.Sprint(confirmed))
	add("DisplayName", t.Attributes["DisplayName"])
	addIfSet(attrDelivery)
	add("SubscriptionsDeleted", "0")
	return out, nil
}

// SetTopicAttribute sets one attribute of a topic. An empty value unsets it.
func (e *Engine) SetTopicAttribute(ctx context.Context, arn, name, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkTopicAttr(name, value); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.findTopic(arn)
	if err != nil {
		return err
	}
	if noopAttr(name, value) {
		return nil
	}
	updated := *t
	updated.Attributes = maps.Clone(t.Attributes)
	if updated.Attributes == nil {
		updated.Attributes = map[string]string{}
	}
	if value == "" {
		delete(updated.Attributes, name)
	} else {
		updated.Attributes[name] = canonTopicAttr(name, value)
	}
	if err := e.persistTopic(&updated); err != nil {
		return err
	}
	*t = updated
	return nil
}
