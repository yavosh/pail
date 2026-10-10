package topic

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/yavosh/pail/internal/queue"
)

// defaultDeliveryPolicy is what AWS reports as EffectiveDeliveryPolicy for a
// topic with no delivery policy (recorded in test/diff, sns-topic-basics).
const defaultDeliveryPolicy = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}`

// defaultPolicy is AWS's default topic policy for the topic arn.
func defaultPolicy(arn string) string {
	return `{"Version":"2008-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},"Action":["SNS:GetTopicAttributes","SNS:SetTopicAttributes","SNS:AddPermission","SNS:RemovePermission","SNS:DeleteTopic","SNS:Subscribe","SNS:ListSubscriptionsByTopic","SNS:Publish"],"Resource":"` + arn + `","Condition":{"StringEquals":{"AWS:SourceOwner":"` + queue.Account + `"}}}]}`
}

// topicAttrNames are the attributes a client can set. The same order lists the
// optional ones in TopicAttributes (positions unverified).
var topicAttrNames = []string{"DisplayName", "Policy", "DeliveryPolicy", "KmsMasterKeyId", "SignatureVersion", "TracingConfig"}

// checkTopicAttr validates one attribute. An empty value means unset.
func checkTopicAttr(name, value string) error {
	if !slices.Contains(topicAttrNames, name) {
		return fmt.Errorf("topic attribute %q is not supported: %w", name, ErrInvalidParameter)
	}
	ok := true
	switch name {
	case "Policy", "DeliveryPolicy":
		ok = value == "" || isJSONObject(value)
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
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.findTopic(arn)
	if err != nil {
		return nil, err
	}
	// The default delivery policy stays even when DeliveryPolicy is set (unverified).
	out := []Attribute{
		{"Policy", topicAttrValue(t, arn, "Policy")},
		{"Owner", queue.Account},
		{"SubscriptionsPending", "0"},
		{"TopicArn", arn},
		{"EffectiveDeliveryPolicy", defaultDeliveryPolicy},
		{"SubscriptionsConfirmed", fmt.Sprint(len(e.topicSubs(arn)))},
		{"DisplayName", t.Attributes["DisplayName"]},
		{"SubscriptionsDeleted", "0"},
	}
	for _, k := range topicAttrNames[2:] {
		if v, ok := t.Attributes[k]; ok {
			out = append(out, Attribute{k, v})
		}
	}
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
	updated := *t
	updated.Attributes = maps.Clone(t.Attributes)
	if updated.Attributes == nil {
		updated.Attributes = map[string]string{}
	}
	if value == "" {
		delete(updated.Attributes, name)
	} else {
		updated.Attributes[name] = value
	}
	if err := e.persistTopic(&updated); err != nil {
		return err
	}
	*t = updated
	return nil
}
