package snsapi

import (
	"net/http"

	"github.com/yavosh/pail/internal/topic"
)

func (h *handler) subscribe(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	protocol, err := p.required("Protocol")
	if err != nil {
		return "", err
	}
	// ReturnSubscriptionArn is ignored: SQS subscriptions are confirmed, so the ARN always returns.
	sub, err := h.topics.Subscribe(r.Context(), arn, protocol, p.get("Endpoint"), p.attributes("Attributes"))
	if err != nil {
		return "", err
	}
	var x xmlBuf
	x.elem("SubscriptionArn", sub)
	return x.String(), nil
}

func (h *handler) unsubscribe(r *http.Request, p params) (string, error) {
	arn, err := p.required("SubscriptionArn")
	if err != nil {
		return "", err
	}
	return "", h.topics.Unsubscribe(r.Context(), arn)
}

func (h *handler) listSubscriptions(r *http.Request, p params) (string, error) {
	next, err := decodeToken(p.get("NextToken"))
	if err != nil {
		return "", err
	}
	subs, after, err := h.topics.ListSubscriptions(r.Context(), next)
	return subscriptionList(subs, after, err)
}

func (h *handler) listSubscriptionsByTopic(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	next, err := decodeToken(p.get("NextToken"))
	if err != nil {
		return "", err
	}
	subs, after, err := h.topics.ListSubscriptionsByTopic(r.Context(), arn, next)
	return subscriptionList(subs, after, err)
}

// subscriptionList writes the Subscriptions list in the model's member order.
func subscriptionList(subs []topic.Subscription, after string, err error) (string, error) {
	if err != nil {
		return "", err
	}
	var x xmlBuf
	if len(subs) == 0 {
		x.WriteString("<Subscriptions/>")
	} else {
		x.open("Subscriptions")
		for _, s := range subs {
			x.open("member")
			x.elem("SubscriptionArn", s.ARN)
			x.elem("Owner", s.Owner)
			x.elem("Protocol", s.Protocol)
			x.elem("Endpoint", s.Endpoint)
			x.elem("TopicArn", s.TopicARN)
			x.close("member")
		}
		x.close("Subscriptions")
	}
	if after != "" {
		x.elem("NextToken", encodeToken(after))
	}
	return x.String(), nil
}

func (h *handler) getSubscriptionAttributes(r *http.Request, p params) (string, error) {
	arn, err := p.required("SubscriptionArn")
	if err != nil {
		return "", err
	}
	attrs, err := h.topics.SubscriptionAttributes(r.Context(), arn)
	if err != nil {
		return "", err
	}
	var x xmlBuf
	x.attributeMap(attrs)
	return x.String(), nil
}

func (h *handler) setSubscriptionAttributes(r *http.Request, p params) (string, error) {
	arn, err := p.required("SubscriptionArn")
	if err != nil {
		return "", err
	}
	name, err := p.required("AttributeName")
	if err != nil {
		return "", err
	}
	return "", h.topics.SetSubscriptionAttribute(r.Context(), arn, name, p.get("AttributeValue"))
}
