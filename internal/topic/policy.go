package topic

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	attrDelivery       = "DeliveryPolicy"
	defaultContentType = "text/plain; charset=UTF-8"
	maxRetries         = 100  // recorded: 101 is InvalidParameter
	maxDelayTarget     = 3600 // seconds; the AWS documented bound, unverified
)

// defaultRetry is AWS's default healthyRetryPolicy (recorded in sns-topic-basics).
var defaultRetry = retryPolicy{MinDelayTarget: 20, MaxDelayTarget: 20, NumRetries: 3, BackoffFunction: "linear"}

var backoffFunctions = []string{"linear", "arithmetic", "geometric", "exponential"}

// The field order is the order AWS writes in EffectiveDeliveryPolicy.
type retryPolicy struct {
	MinDelayTarget     int    `json:"minDelayTarget"`
	MaxDelayTarget     int    `json:"maxDelayTarget"`
	NumRetries         int    `json:"numRetries"`
	NumMaxDelayRetries int    `json:"numMaxDelayRetries"`
	NumNoDelayRetries  int    `json:"numNoDelayRetries"`
	NumMinDelayRetries int    `json:"numMinDelayRetries"`
	BackoffFunction    string `json:"backoffFunction"`
}

// UnmarshalJSON starts from the default policy, so a key left out keeps its
// default (unverified), and rejects unknown keys.
func (p *retryPolicy) UnmarshalJSON(b []byte) error {
	type plain retryPolicy
	q := plain(defaultRetry)
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&q); err != nil {
		return err
	}
	*p = retryPolicy(q)
	return nil
}

// check applies the recorded rules (retries, backoff, min <= max, phase sum)
// and the documented delay bounds (unverified).
func (p *retryPolicy) check() error {
	switch {
	case p.NumRetries < 0 || p.NumRetries > maxRetries:
		return invalid("numRetries %d must be 0 to %d", p.NumRetries, maxRetries)
	case min(p.NumNoDelayRetries, p.NumMinDelayRetries, p.NumMaxDelayRetries) < 0:
		return invalid("phase retry counts must not be negative")
	case !slices.Contains(backoffFunctions, p.BackoffFunction):
		return invalid("backoffFunction %q must be one of %s", p.BackoffFunction, strings.Join(backoffFunctions, ", "))
	case p.MinDelayTarget < 1 || p.MaxDelayTarget > maxDelayTarget:
		return invalid("delay targets must be 1 to %d seconds", maxDelayTarget)
	case p.MinDelayTarget > p.MaxDelayTarget:
		return invalid("minDelayTarget %d is greater than maxDelayTarget %d", p.MinDelayTarget, p.MaxDelayTarget)
	case p.NumNoDelayRetries+p.NumMinDelayRetries+p.NumMaxDelayRetries > p.NumRetries:
		return invalid("numNoDelayRetries, numMinDelayRetries, and numMaxDelayRetries add up to more than numRetries")
	}
	return nil
}

// delays returns the wait before each retry: the no-delay, min-delay, backoff,
// and max-delay phases. The backoff formulas are pail's choice (unverified);
// docs/sqs-sns-compatibility.md lists them.
func (p *retryPolicy) delays() []time.Duration {
	out := make([]time.Duration, 0, p.NumRetries)
	repeat := func(n, seconds int) {
		for range n {
			out = append(out, time.Duration(seconds)*time.Second)
		}
	}
	repeat(p.NumNoDelayRetries, 0)
	repeat(p.NumMinDelayRetries, p.MinDelayTarget)
	lo, hi := float64(p.MinDelayTarget), float64(p.MaxDelayTarget)
	n := p.NumRetries - p.NumNoDelayRetries - p.NumMinDelayRetries - p.NumMaxDelayRetries
	for i := 1; i <= n; i++ {
		var d float64
		switch p.BackoffFunction {
		case "arithmetic":
			d = lo + (hi-lo)*float64(i*(i+1))/float64((n+1)*(n+2))
		case "geometric":
			d = lo * math.Pow(hi/lo, float64(i)/float64(n+1))
		case "exponential":
			d = lo + (hi-lo)*(math.Pow(2, float64(i))-1)/(math.Pow(2, float64(n+1))-1)
		default:
			d = lo + (hi-lo)*float64(i)/float64(n+1)
		}
		out = append(out, time.Duration(math.Round(d))*time.Second)
	}
	repeat(p.NumMaxDelayRetries, p.MaxDelayTarget)
	return out
}

type throttlePolicy struct {
	MaxReceivesPerSecond int `json:"maxReceivesPerSecond"`
}

type requestPolicy struct {
	HeaderContentType string `json:"headerContentType"`
}

// validContentType accepts the media types the AWS docs list for
// headerContentType, with an optional UTF-8 charset. Recorded: application/json
// and text/plain pass and text/bogus fails; the rest of the list is unverified.
func validContentType(v string) bool {
	base, params, hasParams := strings.Cut(v, ";")
	if !slices.Contains([]string{"text/plain", "text/csv", "application/json", "application/xml"}, base) {
		return false
	}
	return !hasParams || strings.EqualFold(strings.TrimSpace(params), "charset=UTF-8")
}

func (p *throttlePolicy) check() error {
	if p.MaxReceivesPerSecond < 1 {
		return invalid("maxReceivesPerSecond %d must be at least 1", p.MaxReceivesPerSecond)
	}
	return nil
}

func (p *requestPolicy) check() error {
	if p.HeaderContentType == "" {
		p.HeaderContentType = defaultContentType
	}
	if !validContentType(p.HeaderContentType) {
		return invalid("headerContentType %q is not supported", p.HeaderContentType)
	}
	return nil
}

// topicDoc is a topic DeliveryPolicy. The field order is AWS's storage order.
type topicDoc struct {
	HTTP *httpDoc `json:"http,omitempty"`
}

type httpDoc struct {
	DefaultHealthyRetryPolicy    *retryPolicy    `json:"defaultHealthyRetryPolicy,omitempty"`
	DisableSubscriptionOverrides bool            `json:"disableSubscriptionOverrides"`
	DefaultThrottlePolicy        *throttlePolicy `json:"defaultThrottlePolicy,omitempty"`
	DefaultRequestPolicy         *requestPolicy  `json:"defaultRequestPolicy,omitempty"`
}

// subDoc is a subscription DeliveryPolicy, and also the EffectiveDeliveryPolicy.
type subDoc struct {
	HealthyRetryPolicy *retryPolicy    `json:"healthyRetryPolicy"`
	SicklyRetryPolicy  *retryPolicy    `json:"sicklyRetryPolicy"`
	ThrottlePolicy     *throttlePolicy `json:"throttlePolicy"`
	RequestPolicy      *requestPolicy  `json:"requestPolicy"`
	Guaranteed         bool            `json:"guaranteed"`
}

// decodePolicy decodes the JSON object v into dst and rejects unknown keys.
func decodePolicy(v string, dst any) error {
	if !isJSONObject(v) {
		return invalid("DeliveryPolicy must be a JSON object")
	}
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return policyError(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return invalid("DeliveryPolicy has data after the JSON object")
	}
	return nil
}

// policyError turns a decoding error into a message without Go type names.
func policyError(err error) error {
	if te, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		field := te.Field
		if i := strings.LastIndex(field, "."); i >= 0 {
			field = field[i+1:]
		}
		want := map[reflect.Kind]string{reflect.Bool: "true or false", reflect.String: "a string", reflect.Struct: "an object", reflect.Pointer: "an object"}[te.Type.Kind()]
		if want == "" {
			want = "an integer"
		}
		return invalid("DeliveryPolicy: %s must be %s", cmp.Or(field, "a value"), want)
	}
	if key, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return invalid("DeliveryPolicy has an unknown key %s", key)
	}
	return invalid("DeliveryPolicy is not valid JSON or has a value of the wrong type")
}

// checks runs check on each non-nil policy.
func checks(policies ...interface{ check() error }) error {
	for _, p := range policies {
		if err := p.check(); err != nil {
			return err
		}
	}
	return nil
}

// normalizeTopicPolicy validates a topic DeliveryPolicy and returns it in the
// form AWS stores (recorded in sns-delivery-policy).
func normalizeTopicPolicy(v string) (string, error) {
	var doc topicDoc
	if err := decodePolicy(v, &doc); err != nil {
		return "", err
	}
	if h := doc.HTTP; h != nil {
		var list []interface{ check() error }
		if h.DefaultHealthyRetryPolicy != nil {
			list = append(list, h.DefaultHealthyRetryPolicy)
		}
		if h.DefaultThrottlePolicy != nil {
			list = append(list, h.DefaultThrottlePolicy)
		}
		if h.DefaultRequestPolicy != nil {
			list = append(list, h.DefaultRequestPolicy)
		}
		if err := checks(list...); err != nil {
			return "", err
		}
	}
	return marshalPolicy(doc), nil
}

// normalizeSubPolicy is normalizeTopicPolicy for a subscription DeliveryPolicy.
func normalizeSubPolicy(v string) (string, error) {
	var doc subDoc
	if err := decodePolicy(v, &doc); err != nil {
		return "", err
	}
	var list []interface{ check() error }
	for _, p := range []*retryPolicy{doc.HealthyRetryPolicy, doc.SicklyRetryPolicy} {
		if p != nil {
			list = append(list, p)
		}
	}
	if doc.ThrottlePolicy != nil {
		list = append(list, doc.ThrottlePolicy)
	}
	if doc.RequestPolicy != nil {
		list = append(list, doc.RequestPolicy)
	}
	if err := checks(list...); err != nil {
		return "", err
	}
	return marshalPolicy(doc), nil
}

func marshalPolicy(doc any) string {
	b, _ := json.Marshal(doc) // plain structs always marshal
	return string(b)
}

// effectiveTopicPolicy overlays the stored topic policy on the defaults, as
// effectivePolicy does for subscriptions (unverified for a partial policy).
func effectiveTopicPolicy(t *topicDef) string {
	v := t.Attributes[attrDelivery]
	if v == "" {
		return defaultDeliveryPolicy
	}
	h := httpDoc{DefaultHealthyRetryPolicy: new(defaultRetry), DefaultRequestPolicy: &requestPolicy{defaultContentType}}
	var td topicDoc
	_ = json.Unmarshal([]byte(v), &td) // checked when stored
	if s := td.HTTP; s != nil {
		h.DisableSubscriptionOverrides, h.DefaultThrottlePolicy = s.DisableSubscriptionOverrides, s.DefaultThrottlePolicy
		if s.DefaultHealthyRetryPolicy != nil {
			h.DefaultHealthyRetryPolicy = s.DefaultHealthyRetryPolicy
		}
		if s.DefaultRequestPolicy != nil {
			h.DefaultRequestPolicy = s.DefaultRequestPolicy
		}
	}
	return marshalPolicy(topicDoc{HTTP: &h})
}

// canonTopicAttr returns the stored form of a checked topic attribute value.
func canonTopicAttr(name, value string) string {
	if name != attrDelivery || value == "" {
		return value
	}
	out, _ := normalizeTopicPolicy(value) // checked by the caller
	return out
}

// canonSubAttr is canonTopicAttr for a subscription attribute.
func canonSubAttr(name, value string) string {
	if name != attrDelivery || value == "" {
		return value
	}
	out, _ := normalizeSubPolicy(value) // checked by the caller
	return out
}

// effectivePolicy derives the policy of a subscription from its topic's
// DeliveryPolicy and its own. With disableSubscriptionOverrides the
// subscription's policy is ignored (unverified).
func effectivePolicy(t *topicDef, s *subDef) subDoc {
	doc := subDoc{HealthyRetryPolicy: new(defaultRetry), RequestPolicy: &requestPolicy{defaultContentType}}
	overlay := func(h *retryPolicy, th *throttlePolicy, r *requestPolicy) {
		if h != nil {
			doc.HealthyRetryPolicy = h
		}
		if th != nil {
			doc.ThrottlePolicy = th
		}
		if r != nil {
			doc.RequestPolicy = r
		}
	}
	var td topicDoc
	if v := t.Attributes[attrDelivery]; v != "" {
		_ = json.Unmarshal([]byte(v), &td) // checked when stored
	}
	if h := td.HTTP; h != nil {
		overlay(h.DefaultHealthyRetryPolicy, h.DefaultThrottlePolicy, h.DefaultRequestPolicy)
	}
	if v := s.Attributes[attrDelivery]; v != "" && (td.HTTP == nil || !td.HTTP.DisableSubscriptionOverrides) {
		var sd subDoc
		_ = json.Unmarshal([]byte(v), &sd) // checked when stored
		overlay(sd.HealthyRetryPolicy, sd.ThrottlePolicy, sd.RequestPolicy)
		doc.SicklyRetryPolicy, doc.Guaranteed = sd.SicklyRetryPolicy, sd.Guaranteed
	}
	return doc
}

// deliveryPlan is what a delivery job needs from the effective policy.
type deliveryPlan struct {
	delays      []time.Duration // wait before each retry
	perSecond   int             // posts per second per subscription; 0 is unlimited
	contentType string
}

var defaultPlan = deliveryPlan{delays: defaultRetry.delays(), contentType: defaultContentType}

func (d subDoc) plan() deliveryPlan {
	p := deliveryPlan{delays: d.HealthyRetryPolicy.delays(), contentType: d.RequestPolicy.HeaderContentType}
	if d.ThrottlePolicy != nil {
		p.perSecond = d.ThrottlePolicy.MaxReceivesPerSecond
	}
	return p
}
