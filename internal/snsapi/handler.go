// Package snsapi serves the Amazon SNS API over the AWS query protocol.
package snsapi

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/topic"
)

// SNS messages reach 1 MiB with a raised topic limit, and form encoding can triple them.
const maxRequestBytes = 4 << 20

const xmlns = "http://sns.amazonaws.com/doc/2010-03-31/"

// Topics is the topic engine the handler serves. *topic.Engine implements it.
type Topics interface {
	CreateTopic(ctx context.Context, name string, attrs, tags map[string]string) (string, error)
	DeleteTopic(ctx context.Context, arn string) error
	ListTopics(ctx context.Context, next string) ([]string, string, error)
	TopicAttributes(ctx context.Context, arn string) ([]topic.Attribute, error)
	SetTopicAttribute(ctx context.Context, arn, name, value string) error
	Subscribe(ctx context.Context, in topic.SubscribeInput) (arn string, pending bool, err error)
	ConfirmSubscription(ctx context.Context, topicARN, token string, authenticated bool) (string, error)
	Unsubscribe(ctx context.Context, arn string, signed bool, baseURL string) error
	ListSubscriptions(ctx context.Context, next string) ([]topic.Subscription, string, error)
	ListSubscriptionsByTopic(ctx context.Context, topicARN, next string) ([]topic.Subscription, string, error)
	SubscriptionAttributes(ctx context.Context, arn string) ([]topic.Attribute, error)
	SetSubscriptionAttribute(ctx context.Context, arn, name, value string) error
	Publish(ctx context.Context, in topic.PublishInput) (string, error)
	PublishBatch(ctx context.Context, topicARN string, entries []topic.PublishEntry, baseURL string) ([]topic.PublishResult, error)
	TagResource(ctx context.Context, arn string, tags map[string]string) error
	UntagResource(ctx context.Context, arn string, keys []string) error
	ListTagsForResource(ctx context.Context, arn string) (map[string]string, error)
}

// Options configures the handler.
type Options struct {
	AccessKeyID     string
	SecretAccessKey string
	Topics          Topics
}

// operation runs one action and returns the XML inside the Result element.
type operation struct {
	run func(r *http.Request, p params) (string, error)
	// hasResult marks actions with a result wrapper; DeleteTopic has none
	// (verified by sns-topic-basics).
	hasResult bool
	// anonymous marks actions that SNS endpoints call without a signature.
	anonymous bool
}

type signedKey struct{}

// signed reports whether the request carried valid credentials.
func signed(r *http.Request) bool {
	v, _ := r.Context().Value(signedKey{}).(bool)
	return v
}

type handler struct {
	verifier *sigv4.Verifier
	topics   Topics
	ops      map[string]operation
}

// New returns the SNS handler. An action outside the table answers InvalidAction.
func New(opts Options) http.Handler {
	h := &handler{verifier: sigv4.New(opts.AccessKeyID, opts.SecretAccessKey), topics: opts.Topics}
	h.ops = map[string]operation{
		"CreateTopic":               {run: h.createTopic, hasResult: true},
		"DeleteTopic":               {run: h.deleteTopic},
		"ListTopics":                {run: h.listTopics, hasResult: true},
		"GetTopicAttributes":        {run: h.getTopicAttributes, hasResult: true},
		"SetTopicAttributes":        {run: h.setTopicAttributes},
		"TagResource":               {run: h.tagResource, hasResult: true},
		"UntagResource":             {run: h.untagResource, hasResult: true},
		"ListTagsForResource":       {run: h.listTagsForResource, hasResult: true},
		"Subscribe":                 {run: h.subscribe, hasResult: true},
		"ConfirmSubscription":       {run: h.confirmSubscription, hasResult: true, anonymous: true},
		"Unsubscribe":               {run: h.unsubscribe, anonymous: true},
		"ListSubscriptions":         {run: h.listSubscriptions, hasResult: true},
		"ListSubscriptionsByTopic":  {run: h.listSubscriptionsByTopic, hasResult: true},
		"GetSubscriptionAttributes": {run: h.getSubscriptionAttributes, hasResult: true},
		"SetSubscriptionAttributes": {run: h.setSubscriptionAttributes},
		"Publish":                   {run: h.publish, hasResult: true},
		"PublishBatch":              {run: h.publishBatch, hasResult: true},
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := newRequestID()
	w.Header().Set("x-amzn-RequestId", requestID)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	op, status := h.serve(w, r, requestID)
	clogSnsapi().Info("request", "method", r.Method, "op", op, "status", status, "duration", time.Since(start))
}

// serve answers the request and returns the action name and the status it wrote.
func (h *handler) serve(w http.ResponseWriter, r *http.Request, requestID string) (string, int) {
	authErr := h.verifier.VerifyService(r, "sns")
	if authErr != nil && !errors.Is(authErr, sigv4.ErrMissingAuth) {
		return "", writeError(w, authError(authErr), authErr.Error(), requestID)
	}
	_ = r.ParseForm() // a parse error leaves Action empty
	name := r.Form.Get("Action")
	op, ok := h.ops[name]
	if authErr != nil && (!ok || !op.anonymous) {
		return "", writeError(w, authError(authErr), authErr.Error(), requestID)
	}
	r = r.WithContext(context.WithValue(r.Context(), signedKey{}, authErr == nil))
	if !ok {
		msg := "the action is not supported"
		if name != "" {
			msg = fmt.Sprintf("%s is not supported", name)
		}
		return name, writeError(w, errInvalidAction, msg, requestID)
	}
	inner, err := op.run(r, params(r.Form))
	if err != nil {
		return name, writeError(w, mapError(err), err.Error(), requestID)
	}
	return name, writeResult(w, name, op.hasResult, inner, requestID)
}

// writeResult answers 200 with the query protocol's XML response document.
func writeResult(w http.ResponseWriter, action string, hasResult bool, inner, requestID string) int {
	var b strings.Builder
	b.WriteString(`<` + action + `Response xmlns="` + xmlns + `">`)
	switch {
	case !hasResult:
	case inner == "":
		b.WriteString(`<` + action + `Result/>`)
	default:
		b.WriteString(`<` + action + `Result>` + inner + `</` + action + `Result>`)
	}
	b.WriteString(`<ResponseMetadata><RequestId>` + requestID + `</RequestId></ResponseMetadata></` + action + `Response>`)
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
	return http.StatusOK
}

// writeError answers with the query protocol's XML error document.
func writeError(w http.ResponseWriter, e apiError, message, requestID string) int {
	type errorBody struct {
		Type    string
		Code    string
		Message string
	}
	body, _ := xml.Marshal(struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Error     errorBody
		RequestID string `xml:"RequestId"`
	}{Xmlns: xmlns, Error: errorBody{e.typ, e.code, message}, RequestID: requestID})
	w.Header().Set("Content-Type", "text/xml")
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
