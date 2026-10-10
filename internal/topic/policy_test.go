package topic

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const (
	// goldenTopicIn and goldenTopicOut are the input and stored form recorded in sns-delivery-policy.
	goldenTopicIn  = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":5,"maxDelayTarget":30,"numRetries":10,"numNoDelayRetries":1,"numMinDelayRetries":2,"numMaxDelayRetries":3,"backoffFunction":"exponential"},"disableSubscriptionOverrides":false,"defaultThrottlePolicy":{"maxReceivesPerSecond":5},"defaultRequestPolicy":{"headerContentType":"application/json"}}}`
	goldenTopicOut = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":5,"maxDelayTarget":30,"numRetries":10,"numMaxDelayRetries":3,"numNoDelayRetries":1,"numMinDelayRetries":2,"backoffFunction":"exponential"},"disableSubscriptionOverrides":false,"defaultThrottlePolicy":{"maxReceivesPerSecond":5},"defaultRequestPolicy":{"headerContentType":"application/json"}}}`
	goldenSubIn    = `{"healthyRetryPolicy":{"minDelayTarget":2,"maxDelayTarget":4,"numRetries":2,"numNoDelayRetries":0,"numMinDelayRetries":0,"numMaxDelayRetries":0,"backoffFunction":"linear"},"throttlePolicy":{"maxReceivesPerSecond":1},"requestPolicy":{"headerContentType":"text/plain"}}`
	goldenSubOut   = `{"healthyRetryPolicy":{"minDelayTarget":2,"maxDelayTarget":4,"numRetries":2,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"sicklyRetryPolicy":null,"throttlePolicy":{"maxReceivesPerSecond":1},"requestPolicy":{"headerContentType":"text/plain"},"guaranteed":false}`
)

func TestNormalizeTopicPolicy(t *testing.T) {
	replace := func(old, new string) string { return strings.Replace(goldenTopicIn, old, new, 1) }
	tests := []struct {
		name, in, want string
		wantErr        bool
	}{
		{"recorded policy is reordered", goldenTopicIn, goldenTopicOut, false},
		{"stored form is stable", goldenTopicOut, goldenTopicOut, false},
		{"empty object", `{}`, `{}`, false},
		{"empty http section", `{"http":{}}`, `{"http":{"disableSubscriptionOverrides":false}}`, false},
		{"missing retry keys take the default", `{"http":{"defaultHealthyRetryPolicy":{"numRetries":5}}}`,
			`{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":5,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false}}`, false},
		{"100 retries", replace(`"numRetries":10`, `"numRetries":100`), "", false},
		{"101 retries", replace(`"numRetries":10`, `"numRetries":101`), "", true},
		{"negative retries", replace(`"numRetries":10`, `"numRetries":-1`), "", true},
		{"unknown backoff", replace("exponential", "cubic"), "", true},
		{"min over max", replace(`"minDelayTarget":5`, `"minDelayTarget":40`), "", true},
		{"zero min delay", replace(`"minDelayTarget":5`, `"minDelayTarget":0`), "", true},
		{"max delay over 3600", replace(`"maxDelayTarget":30`, `"maxDelayTarget":3601`), "", true},
		{"phases over retries", replace(`"numMaxDelayRetries":3`, `"numMaxDelayRetries":30`), "", true},
		{"phases equal retries", replace(`"numMaxDelayRetries":3`, `"numMaxDelayRetries":7`), "", false},
		{"bad content type", replace("application/json", "text/bogus"), "", true},
		{"charset variant", replace("application/json", "application/json; charset=UTF-8"), "", false},
		{"other charset", replace("application/json", "application/json; charset=latin1"), "", true},
		{"zero throttle", replace(`"maxReceivesPerSecond":5`, `"maxReceivesPerSecond":0`), "", true},
		{"unknown key", `{"http":{"bogus":1}}`, "", true},
		{"unknown top-level key", `{"https":{}}`, "", true},
		{"wrong type", `{"http":{"disableSubscriptionOverrides":"yes"}}`, "", true},
		{"fractional retries", replace(`"numRetries":10`, `"numRetries":1.5`), "", true},
		{"not JSON", `{`, "", true},
		{"JSON null", `null`, "", true},
		{"JSON array", `[]`, "", true},
		{"trailing data", `{} {}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeTopicPolicy(tt.in)
			if (err != nil) != tt.wantErr || tt.wantErr && !errors.Is(err, ErrInvalidParameter) {
				t.Fatalf("normalizeTopicPolicy(%s) error = %v, want error %v of ErrInvalidParameter", tt.in, err, tt.wantErr)
			}
			if tt.want != "" && got != tt.want {
				t.Errorf("normalizeTopicPolicy(%s)\n got %s\nwant %s", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeSubPolicy(t *testing.T) {
	tests := []struct {
		name, in, want string
		wantErr        bool
	}{
		{"recorded policy", goldenSubIn, goldenSubOut, false},
		{"stored form is stable", goldenSubOut, goldenSubOut, false},
		{"empty object", `{}`, `{"healthyRetryPolicy":null,"sicklyRetryPolicy":null,"throttlePolicy":null,"requestPolicy":null,"guaranteed":false}`, false},
		{"sickly policy is checked", `{"sicklyRetryPolicy":{"numRetries":101}}`, "", true},
		{"101 retries", strings.Replace(goldenSubIn, `"numRetries":2`, `"numRetries":101`, 1), "", true},
		{"unknown key", `{"bogus":1}`, "", true},
		{"topic form is not a subscription form", goldenTopicIn, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSubPolicy(tt.in)
			if (err != nil) != tt.wantErr || tt.wantErr && !errors.Is(err, ErrInvalidParameter) {
				t.Fatalf("normalizeSubPolicy(%s) error = %v, want error %v of ErrInvalidParameter", tt.in, err, tt.wantErr)
			}
			if tt.want != "" && got != tt.want {
				t.Errorf("normalizeSubPolicy(%s)\n got %s\nwant %s", tt.in, got, tt.want)
			}
		})
	}
}

func secs(n ...int) []time.Duration {
	out := make([]time.Duration, len(n))
	for i, s := range n {
		out[i] = time.Duration(s) * time.Second
	}
	return out
}

func TestRetryDelays(t *testing.T) {
	p := func(minD, maxD, retries, noDelay, minRetries, maxRetries int, f string) retryPolicy {
		return retryPolicy{minD, maxD, retries, maxRetries, noDelay, minRetries, f}
	}
	tests := []struct {
		name string
		in   retryPolicy
		want []time.Duration
	}{
		{"default", defaultRetry, secs(20, 20, 20)},
		{"no retries", p(1, 5, 0, 0, 0, 0, "linear"), secs()},
		{"linear", p(2, 32, 4, 0, 0, 0, "linear"), secs(8, 14, 20, 26)},
		{"arithmetic", p(2, 32, 4, 0, 0, 0, "arithmetic"), secs(4, 8, 14, 22)},
		{"geometric", p(2, 32, 4, 0, 0, 0, "geometric"), secs(3, 6, 11, 18)},
		{"exponential", p(2, 32, 4, 0, 0, 0, "exponential"), secs(3, 5, 9, 17)},
		{"phases", p(5, 30, 10, 1, 2, 3, "linear"), secs(0, 5, 5, 10, 15, 20, 25, 30, 30, 30)}, // 1 immediate, 2 at 5 s, 4 backoff, 3 at 30 s
		{"all phases, no backoff", p(5, 30, 6, 1, 2, 3, "exponential"), secs(0, 5, 5, 30, 30, 30)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.delays(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("%+v delays = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// confirmedHTTP subscribes an endpoint and confirms it without authentication.
func confirmedHTTP(t *testing.T, e *Engine, topicARN, endpoint string, attrs map[string]string) string {
	t.Helper()
	sub, _ := subscribeHTTP(t, e, topicARN, endpoint, attrs)
	if _, err := e.ConfirmSubscription(t.Context(), topicARN, e.subs[sub].Token, false); err != nil {
		t.Fatal(err)
	}
	return sub
}

func publishText(t *testing.T, e *Engine, arn, text string) {
	t.Helper()
	in := publishInput(arn)
	in.Message, in.BaseURL = text, testBase
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
}

// attemptTimes returns when the posts after the first arrived, relative to the second.
func attemptTimes(ep *recordingEndpoint) []time.Duration {
	posts := ep.got()[1:]
	var out []time.Duration
	for _, p := range posts {
		out = append(out, p.at-posts[0].at)
	}
	return out
}

func TestHTTPDeliveryPolicySchedule(t *testing.T) {
	retry := func(f string) string {
		return `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":2,"maxDelayTarget":32,"numRetries":4,"backoffFunction":"` + f + `"}}}`
	}
	tests := []struct {
		name, policy string
		want         []time.Duration
	}{
		{"linear", retry("linear"), secs(0, 8, 22, 42, 68)},
		{"arithmetic", retry("arithmetic"), secs(0, 4, 12, 26, 48)},
		{"geometric", retry("geometric"), secs(0, 3, 9, 20, 38)},
		{"exponential", retry("exponential"), secs(0, 3, 8, 17, 34)},
		{"phases", `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":5,"maxDelayTarget":9,"numRetries":6,"numNoDelayRetries":1,"numMinDelayRetries":1,"numMaxDelayRetries":1,"backoffFunction":"linear"}}}`,
			secs(0, 0, 5, 11, 18, 26, 35)}, // 1 immediate, 1 at 5 s, 3 backoff (6, 7, 8 s), 1 at 9 s
		{"no retries", `{"http":{"defaultHealthyRetryPolicy":{"numRetries":0}}}`, secs(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, _ := newEngine(t)
				// The confirmation is the first post and succeeds; every notification post fails.
				ep := startEndpoint(t, e, func(n int) int {
					if n == 1 {
						return 200
					}
					return 500
				})
				stop := runDeliveries(e)
				defer stop()
				arn := mustTopic(t, e, "sched")
				if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", tt.policy); err != nil {
					t.Fatal(err)
				}
				confirmedHTTP(t, e, arn, ep.srv.URL, nil)
				synctest.Wait()
				publishText(t, e, arn, "m")
				time.Sleep(10 * time.Minute)
				synctest.Wait()
				if got := attemptTimes(ep); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("policy %s: attempts at %v, want %v", tt.policy, got, tt.want)
				}
			})
		})
	}
}

func TestHTTPDeliveryThrottle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		ep := startEndpoint(t, e, ok)
		stop := runDeliveries(e)
		defer stop()
		arn := mustTopic(t, e, "throttle")
		if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", `{"http":{"defaultThrottlePolicy":{"maxReceivesPerSecond":2}}}`); err != nil {
			t.Fatal(err)
		}
		slow := confirmedHTTP(t, e, arn, ep.srv.URL+"/slow", nil)
		fast := confirmedHTTP(t, e, arn, ep.srv.URL+"/fast", map[string]string{"DeliveryPolicy": `{"throttlePolicy":{"maxReceivesPerSecond":100}}`})
		synctest.Wait()
		before := len(ep.got())
		for range 4 {
			publishText(t, e, arn, "m")
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		bySub := map[string][]time.Duration{}
		base := ep.got()[before].at
		for _, p := range ep.got()[before:] {
			s := p.header.Get("X-Amz-Sns-Subscription-Arn")
			bySub[s] = append(bySub[s], p.at-base)
		}
		// Each subscription has its own limiter, and the subscription's policy overrides the topic's.
		if len(bySub[fast]) != 4 || bySub[fast][3] > time.Second {
			t.Errorf("fast subscription posts at %v, want 4 within a second", bySub[fast])
		}
		got := bySub[slow]
		if len(got) != 4 {
			t.Fatalf("slow subscription received %d posts, want 4", len(got))
		}
		for i := 1; i < len(got); i++ {
			if gap := got[i] - got[i-1]; gap != 500*time.Millisecond {
				t.Errorf("slow subscription gap %d = %v, want 500ms (posts at %v)", i, gap, got)
			}
		}
	})
}

func TestHTTPDeliveryContentType(t *testing.T) {
	tests := []struct {
		name, topicPolicy, subPolicy, want string
	}{
		{"default", "", "", "text/plain; charset=UTF-8"},
		{"topic policy", `{"http":{"defaultRequestPolicy":{"headerContentType":"application/json"}}}`, "", "application/json"},
		{"subscription overrides", `{"http":{"defaultRequestPolicy":{"headerContentType":"application/json"}}}`,
			`{"requestPolicy":{"headerContentType":"application/xml"}}`, "application/xml"},
		{"topic disables overrides", `{"http":{"disableSubscriptionOverrides":true,"defaultRequestPolicy":{"headerContentType":"application/json"}}}`,
			`{"requestPolicy":{"headerContentType":"application/xml"}}`, "application/json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, _ := newEngine(t)
				ep := startEndpoint(t, e, ok)
				stop := runDeliveries(e)
				defer stop()
				arn := mustTopic(t, e, "ctype")
				if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", tt.topicPolicy); err != nil {
					t.Fatal(err)
				}
				attrs := map[string]string{}
				if tt.subPolicy != "" {
					attrs["DeliveryPolicy"] = tt.subPolicy
				}
				confirmedHTTP(t, e, arn, ep.srv.URL, attrs)
				synctest.Wait()
				before := len(ep.got())
				publishText(t, e, arn, "hello")
				synctest.Wait()
				posts := ep.got()[before:]
				if len(posts) != 1 || posts[0].header.Get("Content-Type") != tt.want {
					t.Errorf("notification posts = %v, want one with Content-Type %q", posts, tt.want)
				}
				if got := ep.got()[0].header.Get("Content-Type"); got != "text/plain; charset=UTF-8" {
					t.Errorf("confirmation Content-Type = %q, want the default", got)
				}
			})
		})
	}
}

func TestEffectiveSubscriptionPolicy(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "eff")
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", goldenTopicIn); err != nil {
		t.Fatal(err)
	}
	sub, _ := subscribeHTTP(t, e, arn, testEndpoint, nil)
	get := func(key string) string {
		t.Helper()
		attrs, err := e.SubscriptionAttributes(t.Context(), sub)
		if err != nil {
			t.Fatal(err)
		}
		if i := slicesIndex(attrs, key); i >= 0 {
			return attrs[i].Value
		}
		return "<absent>"
	}
	const fromTopic = `{"healthyRetryPolicy":{"minDelayTarget":5,"maxDelayTarget":30,"numRetries":10,"numMaxDelayRetries":3,"numNoDelayRetries":1,"numMinDelayRetries":2,"backoffFunction":"exponential"},"sicklyRetryPolicy":null,"throttlePolicy":{"maxReceivesPerSecond":5},"requestPolicy":{"headerContentType":"application/json"},"guaranteed":false}`
	if got := get("EffectiveDeliveryPolicy"); got != fromTopic {
		t.Errorf("EffectiveDeliveryPolicy = %s, want %s", got, fromTopic)
	}
	if got := get("DeliveryPolicy"); got != "<absent>" {
		t.Errorf("DeliveryPolicy = %s, want it absent", got)
	}
	if err := e.SetSubscriptionAttribute(t.Context(), sub, "DeliveryPolicy", goldenSubIn); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"EffectiveDeliveryPolicy", "DeliveryPolicy"} {
		if got := get(key); got != goldenSubOut {
			t.Errorf("%s = %s, want %s", key, got, goldenSubOut)
		}
	}
	attrs, _ := e.SubscriptionAttributes(t.Context(), sub)
	if i := slicesIndex(attrs, "EffectiveDeliveryPolicy"); attrs[i+1].Key != "DeliveryPolicy" || attrs[i+2].Key != "Protocol" {
		t.Errorf("attribute order near EffectiveDeliveryPolicy = %v, want DeliveryPolicy then Protocol", attrs[i:i+3])
	}
	// A partial subscription policy keeps the topic's other sections.
	if err := e.SetSubscriptionAttribute(t.Context(), sub, "DeliveryPolicy", `{"requestPolicy":{"headerContentType":"text/plain"}}`); err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(fromTopic, `"application/json"`, `"text/plain"`, 1)
	if got := get("EffectiveDeliveryPolicy"); got != want {
		t.Errorf("EffectiveDeliveryPolicy with a partial policy = %s, want %s", got, want)
	}
	// The topic can disable overrides.
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", strings.Replace(goldenTopicIn, `"disableSubscriptionOverrides":false`, `"disableSubscriptionOverrides":true`, 1)); err != nil {
		t.Fatal(err)
	}
	if got := get("EffectiveDeliveryPolicy"); got != fromTopic {
		t.Errorf("EffectiveDeliveryPolicy with overrides disabled = %s, want %s", got, fromTopic)
	}
	if err := e.SetSubscriptionAttribute(t.Context(), sub, "DeliveryPolicy", ""); err != nil {
		t.Fatal(err)
	}
	if got := get("DeliveryPolicy"); got != "<absent>" {
		t.Errorf("DeliveryPolicy after unset = %s, want it absent", got)
	}
}

func TestDeliveryPolicyOnSQSSubscription(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "sqspolicy")
	sub := mustSubscribe(t, e, arn, "q1", nil)
	if err := e.SetSubscriptionAttribute(t.Context(), sub, "DeliveryPolicy", goldenSubIn); !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("SetSubscriptionAttribute(sqs, DeliveryPolicy) error = %v, want %v", err, ErrInvalidParameter)
	}
	_, _, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: arn, Protocol: "sqs", Endpoint: queueARN("q2"), Attributes: map[string]string{"DeliveryPolicy": goldenSubIn}})
	if !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("Subscribe(sqs, DeliveryPolicy) error = %v, want %v", err, ErrInvalidParameter)
	}
	_, pending, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: arn, Protocol: "http", Endpoint: testEndpoint, Attributes: map[string]string{"DeliveryPolicy": goldenSubIn}, BaseURL: testBase})
	if err != nil || !pending {
		t.Errorf("Subscribe(http, DeliveryPolicy) = pending %v, %v; want pending, nil", pending, err)
	}
}

func TestDeliveryPolicyPersists(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	arn := mustTopic(t, e, "persist")
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", goldenTopicIn); err != nil {
		t.Fatal(err)
	}
	sub, _ := subscribeHTTP(t, e, arn, testEndpoint, map[string]string{"DeliveryPolicy": goldenSubIn})
	again := openEngine(t, dir, &fakeQueues{})
	if got := again.topics["persist"].Attributes["DeliveryPolicy"]; got != goldenTopicOut {
		t.Errorf("topic DeliveryPolicy after reopen = %s, want %s", got, goldenTopicOut)
	}
	if got := again.subs[sub].Attributes["DeliveryPolicy"]; got != goldenSubOut {
		t.Errorf("subscription DeliveryPolicy after reopen = %s, want %s", got, goldenSubOut)
	}
	if err := again.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", ""); err != nil {
		t.Fatal(err)
	}
	attrs, _ := again.TopicAttributes(t.Context(), arn)
	if got := attrs[slicesIndex(attrs, "EffectiveDeliveryPolicy")].Value; got != defaultDeliveryPolicy {
		t.Errorf("EffectiveDeliveryPolicy after unset = %s, want the default", got)
	}
}

func TestPendingTokenExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		arn := mustTopic(t, e, "expiry")
		young, _ := subscribeHTTP(t, e, arn, "http://192.0.2.1/young", nil)
		youngToken := e.subs[young].Token
		time.Sleep(tokenTTL - time.Hour)
		old, _ := subscribeHTTP(t, e, arn, "http://192.0.2.1/old", nil)
		oldToken := e.subs[old].Token
		time.Sleep(2 * time.Hour) // the first subscription is now older than 3 days
		if _, err := e.ConfirmSubscription(t.Context(), arn, youngToken, true); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("ConfirmSubscription(expired token) error = %v, want %v", err, ErrInvalidParameter)
		}
		if _, err := e.SubscriptionAttributes(t.Context(), young); !errors.Is(err, ErrNotFound) {
			t.Errorf("SubscriptionAttributes(expired) error = %v, want %v", err, ErrNotFound)
		}
		list, _, _ := e.ListSubscriptions(t.Context(), "")
		byTopic, _, _ := e.ListSubscriptionsByTopic(t.Context(), arn, "")
		if len(list) != 1 || len(byTopic) != 1 {
			t.Errorf("listings hold %d and %d subscriptions, want 1 each (the expired one is gone)", len(list), len(byTopic))
		}
		ta, _ := e.TopicAttributes(t.Context(), arn)
		if ta[2] != (Attribute{"SubscriptionsPending", "1"}) {
			t.Errorf("topic attribute %v, want SubscriptionsPending 1", ta[2])
		}
		if entries, _ := e.fs.ReadDir(subsDir); len(entries) != 1 {
			t.Errorf("%d subscription files on disk, want 1", len(entries))
		}
		if got, err := e.ConfirmSubscription(t.Context(), arn, oldToken, true); err != nil || got != old {
			t.Errorf("ConfirmSubscription(unexpired token) = %q, %v; want %q", got, err, old)
		}
		time.Sleep(2 * tokenTTL)
		if _, err := e.SubscriptionAttributes(t.Context(), old); err != nil {
			t.Errorf("SubscriptionAttributes(confirmed, old) error = %v, want nil: confirmed subscriptions never expire", err)
		}
	})
}

func TestUnsubscribeConfirmation(t *testing.T) {
	for _, version := range []string{"1", "2"} {
		t.Run("signature version "+version, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, _ := newEngine(t)
				ep := startEndpoint(t, e, ok)
				stop := runDeliveries(e)
				defer stop()
				arn, err := e.CreateTopic(t.Context(), "unsub", map[string]string{"SignatureVersion": version}, nil)
				if err != nil {
					t.Fatal(err)
				}
				sub := confirmedHTTP(t, e, arn, ep.srv.URL+"/hook", map[string]string{"RawMessageDelivery": "true"})
				synctest.Wait()
				before := len(ep.got())

				if err := e.Unsubscribe(t.Context(), sub, false, testBase); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				posts := ep.got()[before:]
				if len(posts) != 1 {
					t.Fatalf("Unsubscribe produced %d posts, want 1", len(posts))
				}
				p := posts[0]
				if got := p.header.Get("X-Amz-Sns-Message-Type"); got != "UnsubscribeConfirmation" {
					t.Errorf("message type header = %q, want UnsubscribeConfirmation", got)
				}
				wantKeys := []string{"Type", "MessageId", "Token", "TopicArn", "Message", "SubscribeURL", "Timestamp", "SignatureVersion", "Signature", "SigningCertURL"}
				if keys := topKeys(t, p.body); !reflect.DeepEqual(keys, wantKeys) {
					t.Errorf("keys = %v, want %v", keys, wantKeys)
				}
				env := map[string]string{}
				if err := unmarshalStrings(p.body, env); err != nil {
					t.Fatal(err)
				}
				wantMessage := "You have chosen to deactivate subscription " + sub + ".\nTo cancel this operation and restore the subscription, visit the SubscribeURL included in this message."
				if env["Type"] != "UnsubscribeConfirmation" || env["TopicArn"] != arn || env["Message"] != wantMessage || env["SignatureVersion"] != version {
					t.Errorf("envelope = %v, want an UnsubscribeConfirmation of %s for %s", env, sub, arn)
				}
				cert, _ := e.CertPEM(t.Context())
				toSign := "Message\n" + env["Message"] + "\nMessageId\n" + env["MessageId"] + "\nSubscribeURL\n" + env["SubscribeURL"] +
					"\nTimestamp\n" + env["Timestamp"] + "\nToken\n" + env["Token"] + "\nTopicArn\n" + env["TopicArn"] + "\nType\nUnsubscribeConfirmation\n"
				verifySignature(t, cert, version, toSign, env["Signature"])
				if _, err := e.SubscriptionAttributes(t.Context(), sub); !errors.Is(err, ErrNotFound) {
					t.Fatalf("SubscriptionAttributes(removed) error = %v, want %v", err, ErrNotFound)
				}

				u, err := url.Parse(env["SubscribeURL"])
				if err != nil || u.Query().Get("Action") != "ConfirmSubscription" {
					t.Fatalf("SubscribeURL = %q, %v; want a ConfirmSubscription URL", env["SubscribeURL"], err)
				}
				if got, err := e.ConfirmSubscription(t.Context(), u.Query().Get("TopicArn"), u.Query().Get("Token"), false); err != nil || got != sub {
					t.Fatalf("ConfirmSubscription from SubscribeURL = %q, %v; want %q", got, err, sub)
				}
				attrs, err := e.SubscriptionAttributes(t.Context(), sub)
				if err != nil {
					t.Fatal(err)
				}
				if got := attrs[slicesIndex(attrs, "RawMessageDelivery")].Value; got != "true" {
					t.Errorf("RawMessageDelivery after restore = %q, want true", got)
				}
				if got := attrs[slicesIndex(attrs, "PendingConfirmation")].Value; got != "false" {
					t.Errorf("PendingConfirmation after restore = %q, want false", got)
				}
				if _, err := e.ConfirmSubscription(t.Context(), arn, env["Token"], false); err != nil {
					t.Errorf("ConfirmSubscription again error = %v, want nil (the subscription is confirmed)", err)
				}
			})
		})
	}
}

func TestUnsubscribeConfirmationRules(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		ep := startEndpoint(t, e, ok)
		stop := runDeliveries(e)
		defer stop()
		arn := mustTopic(t, e, "rules")
		signed := confirmedHTTP(t, e, arn, ep.srv.URL+"/signed", nil)
		expiring := confirmedHTTP(t, e, arn, ep.srv.URL+"/expiring", nil)
		synctest.Wait()
		before := len(ep.got())
		if err := e.Unsubscribe(t.Context(), signed, true, testBase); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if n := len(ep.got()) - before; n != 0 {
			t.Errorf("a signed Unsubscribe posted %d messages, want none", n)
		}
		if err := e.Unsubscribe(t.Context(), expiring, false, testBase); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		var token string
		for _, p := range ep.got()[before:] {
			if p.header.Get("X-Amz-Sns-Message-Type") == "UnsubscribeConfirmation" {
				env := map[string]string{}
				_ = unmarshalStrings(p.body, env)
				token = env["Token"]
			}
		}
		time.Sleep(tokenTTL + time.Minute)
		if _, err := e.ConfirmSubscription(t.Context(), arn, token, false); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("ConfirmSubscription(expired restore token) error = %v, want %v", err, ErrInvalidParameter)
		}
		if len(e.restores) != 0 {
			t.Errorf("%d restore entries remain after expiry, want 0", len(e.restores))
		}
	})
}

func TestTLSSkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(srv.Close)
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{"default verifies", nil, true},
		{"skip off verifies", []Option{WithTLSSkipVerify(false)}, true},
		{"skip on accepts the certificate", []Option{WithTLSSkipVerify(true)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := openEngine(t, t.TempDir(), &fakeQueues{}).fs
			e, err := Open(t.Context(), fsys, testRegion, &fakeQueues{}, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.post(t.Context(), newJob(srv.URL, "x", "Notification", "id", "arn", "sub", false))
			if (err != nil) != tt.wantErr {
				t.Errorf("post to a self-signed HTTPS endpoint error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

// unmarshalStrings decodes a JSON object of strings into dst.
func unmarshalStrings(body string, dst map[string]string) error {
	return json.Unmarshal([]byte(body), &dst)
}

func TestOpenRepairsLegacyDeliveryPolicy(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	write := func(name, policy string) {
		t.Helper()
		def := &topicDef{Name: name, Attributes: map[string]string{"DisplayName": "kept", "DeliveryPolicy": policy}}
		if err := e.persistTopic(def); err != nil {
			t.Fatal(err)
		}
	}
	write("legacy-bad", `{"anything":1}`)
	write("legacy-valid", `{"http":{"disableSubscriptionOverrides":false,"defaultRequestPolicy":{"headerContentType":"application/json"},"defaultThrottlePolicy":{"maxReceivesPerSecond":2}}}`)
	arn := e.topicARN("legacy-valid")
	httpSub := arn + ":11111111-1111-4111-8111-111111111111"
	sqsSub := arn + ":22222222-2222-4222-8222-222222222222"
	for _, s := range []*subDef{
		{ARN: httpSub, TopicARN: arn, Protocol: "http", Endpoint: testEndpoint, Attributes: map[string]string{"DeliveryPolicy": `{"bogus":1}`}},
		{ARN: sqsSub, TopicARN: arn, Protocol: "sqs", Endpoint: queueARN("q"), Attributes: map[string]string{"DeliveryPolicy": goldenSubIn}},
	} {
		if err := e.persistSub(s); err != nil {
			t.Fatal(err)
		}
	}

	again := openEngine(t, dir, &fakeQueues{}) // must not fail
	if got := again.topics["legacy-bad"].Attributes; got["DeliveryPolicy"] != "" || got["DisplayName"] != "kept" {
		t.Errorf("legacy-bad attributes = %v, want DeliveryPolicy dropped and DisplayName kept", got)
	}
	const want = `{"http":{"disableSubscriptionOverrides":false,"defaultThrottlePolicy":{"maxReceivesPerSecond":2},"defaultRequestPolicy":{"headerContentType":"application/json"}}}`
	if got := again.topics["legacy-valid"].Attributes["DeliveryPolicy"]; got != want {
		t.Errorf("legacy-valid DeliveryPolicy = %s, want %s", got, want)
	}
	for _, sub := range []string{httpSub, sqsSub} {
		if got := again.subs[sub].Attributes["DeliveryPolicy"]; got != "" {
			t.Errorf("subscription %s DeliveryPolicy = %q, want it dropped", sub, got)
		}
	}
}

func TestPolicyErrorsHaveNoGoTypes(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"float retries", `{"http":{"defaultHealthyRetryPolicy":{"numRetries":5.5}}}`, "numRetries must be an integer"},
		{"string flag", `{"http":{"disableSubscriptionOverrides":"yes"}}`, "disableSubscriptionOverrides must be true or false"},
		{"string content type", `{"http":{"defaultRequestPolicy":{"headerContentType":5}}}`, "headerContentType must be a string"},
		{"unknown key", `{"http":{"bogus":1}}`, `unknown key "bogus"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizeTopicPolicy(tt.in)
			if !errors.Is(err, ErrInvalidParameter) || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "Go ") {
				t.Errorf("normalizeTopicPolicy(%s) error = %v, want %q without Go internals", tt.in, err, tt.want)
			}
		})
	}
}

func TestEffectiveTopicPolicyOverlaysDefaults(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "overlay")
	effective := func() string {
		attrs, err := e.TopicAttributes(t.Context(), arn)
		if err != nil {
			t.Fatal(err)
		}
		return attrs[slicesIndex(attrs, "EffectiveDeliveryPolicy")].Value
	}
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", `{}`); err != nil {
		t.Fatal(err)
	}
	if got := effective(); got != defaultDeliveryPolicy {
		t.Errorf("EffectiveDeliveryPolicy after {} = %s, want the default", got)
	}
	attrs, _ := e.TopicAttributes(t.Context(), arn)
	if got := attrs[slicesIndex(attrs, "DeliveryPolicy")].Value; got != `{}` {
		t.Errorf("DeliveryPolicy after {} = %s, want {}", got)
	}
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", `{"http":{"defaultThrottlePolicy":{"maxReceivesPerSecond":3}}}`); err != nil {
		t.Fatal(err)
	}
	const partial = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultThrottlePolicy":{"maxReceivesPerSecond":3},"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}`
	if got := effective(); got != partial {
		t.Errorf("EffectiveDeliveryPolicy with only a throttle = %s, want %s", got, partial)
	}
	if err := e.SetTopicAttribute(t.Context(), arn, "DeliveryPolicy", goldenTopicIn); err != nil {
		t.Fatal(err)
	}
	if got := effective(); got != goldenTopicOut {
		t.Errorf("EffectiveDeliveryPolicy with the full policy = %s, want %s", got, goldenTopicOut)
	}
}

func TestRestoreEdgeCases(t *testing.T) {
	restoreToken := func(t *testing.T, e *Engine, ep *recordingEndpoint, before int) string {
		t.Helper()
		synctest.Wait()
		for _, p := range ep.got()[before:] {
			if p.header.Get("X-Amz-Sns-Message-Type") == "UnsubscribeConfirmation" {
				env := map[string]string{}
				_ = unmarshalStrings(p.body, env)
				return env["Token"]
			}
		}
		t.Fatal("no UnsubscribeConfirmation received")
		return ""
	}
	t.Run("endpoint subscribed again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, _ := newEngine(t)
			ep := startEndpoint(t, e, ok)
			stop := runDeliveries(e)
			defer stop()
			arn := mustTopic(t, e, "again")
			old := confirmedHTTP(t, e, arn, ep.srv.URL, nil)
			synctest.Wait()
			before := len(ep.got())
			if err := e.Unsubscribe(t.Context(), old, false, testBase); err != nil {
				t.Fatal(err)
			}
			token := restoreToken(t, e, ep, before)
			fresh := confirmedHTTP(t, e, arn, ep.srv.URL, nil)
			if fresh == old {
				t.Fatalf("new subscription reuses ARN %s", old)
			}
			if got, err := e.ConfirmSubscription(t.Context(), arn, token, false); err != nil || got != fresh {
				t.Errorf("ConfirmSubscription(restore token) = %q, %v; want the new subscription %q", got, err, fresh)
			}
			if _, err := e.SubscriptionAttributes(t.Context(), old); !errors.Is(err, ErrNotFound) {
				t.Errorf("old subscription error = %v, want %v: nothing is restored", err, ErrNotFound)
			}
		})
	})
	t.Run("topic deleted and created again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			e, _ := newEngine(t)
			ep := startEndpoint(t, e, ok)
			stop := runDeliveries(e)
			defer stop()
			arn := mustTopic(t, e, "recreate")
			sub := confirmedHTTP(t, e, arn, ep.srv.URL, nil)
			synctest.Wait()
			before := len(ep.got())
			if err := e.Unsubscribe(t.Context(), sub, false, testBase); err != nil {
				t.Fatal(err)
			}
			token := restoreToken(t, e, ep, before)
			if err := e.DeleteTopic(t.Context(), arn); err != nil {
				t.Fatal(err)
			}
			mustTopic(t, e, "recreate")
			if _, err := e.ConfirmSubscription(t.Context(), arn, token, false); !errors.Is(err, ErrInvalidParameter) {
				t.Errorf("ConfirmSubscription(restore token of a deleted topic) error = %v, want %v", err, ErrInvalidParameter)
			}
		})
	})
}
