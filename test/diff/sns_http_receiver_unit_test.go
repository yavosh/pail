package diff

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSNSCallbackReceiverLimits(t *testing.T) {
	for _, tt := range []struct {
		name, method, path, body string
		full                     bool
		status                   int
	}{
		{name: "unknown path", method: http.MethodPost, path: "/unknown", status: http.StatusNotFound},
		{name: "method", method: http.MethodGet, path: "/callback", status: http.StatusMethodNotAllowed},
		{name: "body limit", method: http.MethodPost, path: "/callback", body: strings.Repeat("x", 1<<20+1), status: http.StatusRequestEntityTooLarge},
		{name: "full receiver", method: http.MethodPost, path: "/callback", full: true, status: http.StatusTooManyRequests},
		{name: "accepted", method: http.MethodPost, path: "/callback", body: "message", status: http.StatusNoContent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			route := &snsCallbackRoute{callbacks: make(chan snsCallback, 1), statuses: []int{http.StatusNoContent}}
			if tt.full {
				route.callbacks <- snsCallback{}
			}
			r := &snsCallbackReceiver{routes: map[string]*snsCallbackRoute{"/callback": route}}
			srv := httptest.NewTestServer(t, http.HandlerFunc(r.serveHTTP))
			req := httptest.NewRequestWithContext(t.Context(), tt.method, "http://example.com"+tt.path, strings.NewReader(tt.body))
			req.RequestURI = ""
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != tt.status {
				t.Errorf("receiver status = %d, want %d", resp.StatusCode, tt.status)
			}
		})
	}
}

func TestSNSCallbackLinkOrigins(t *testing.T) {
	tg := &target{scheme: "https", serviceHost: func(string) string { return "sns.us-east-1.amazonaws.com" }}
	const topic = "arn:aws:sns:us-east-1:000000000000:topic"
	const subscription = topic + ":subscription"
	for _, tt := range []struct {
		name, link, action string
		valid              bool
	}{
		{"confirmation", "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=" + topic + "&Token=test-token", "ConfirmSubscription", true},
		{"unsubscribe", "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + subscription, "Unsubscribe", true},
		{"other host", "https://example.com/?Action=Unsubscribe&SubscriptionArn=" + subscription, "Unsubscribe", false},
		{"userinfo", "https://user@sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + subscription, "Unsubscribe", false},
		{"other subscription", "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=other", "Unsubscribe", false},
		{"missing token", "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=" + topic, "ConfirmSubscription", false},
		{"wrong action", "https://sns.us-east-1.amazonaws.com/?Action=DeleteTopic&TopicArn=" + topic, "ConfirmSubscription", false},
		{"other path", "https://sns.us-east-1.amazonaws.com/other?Action=Unsubscribe&SubscriptionArn=" + subscription, "Unsubscribe", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := snsLinkURL(tg, tt.link, tt.action, topic, subscription)
			if (err == nil) != tt.valid {
				t.Errorf("link validity = %t, want %t", err == nil, tt.valid)
			}
		})
	}
}

func TestSNSHTTPAttributeOrder(t *testing.T) {
	prefix := "GetSubscriptionAttributesResponse\n  GetSubscriptionAttributesResult\n    Attributes\n"
	entryA := "      entry\n        key: FilterPolicy\n        value: {\"kind\":[\"accept\"]}\n"
	entryB := "      entry\n        key: Protocol\n        value: https\n"
	suffix := "  ResponseMetadata\n"
	want := orderedSNSAttributes(prefix + entryA + entryB + suffix)
	if got := orderedSNSAttributes(prefix + entryB + entryA + suffix); got != want {
		t.Errorf("reordered attributes = %q, want %q", got, want)
	}
	changed := "      entry\n        key: Protocol\n        value: http\n"
	if got := orderedSNSAttributes(prefix + changed + entryA + suffix); got == want {
		t.Fatal("changed protocol compares equal after sorting attributes")
	}
}
