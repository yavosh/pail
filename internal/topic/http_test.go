package topic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// defaultSubPolicy is the EffectiveDeliveryPolicy of an HTTP subscription of a topic with no policy.
const defaultSubPolicy = `{"healthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"sicklyRetryPolicy":null,"throttlePolicy":null,"requestPolicy":{"headerContentType":"text/plain; charset=UTF-8"},"guaranteed":false}`

const (
	testBase     = "http://pail.test:9000"
	testEndpoint = "http://192.0.2.1/pail"
)

// subscribeHTTP subscribes an endpoint and returns the ARN and the pending state.
func subscribeHTTP(t *testing.T, e *Engine, topicARN, endpoint string, attrs map[string]string) (string, bool) {
	t.Helper()
	protocol, _, _ := strings.Cut(endpoint, ":")
	arn, pending, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: topicARN, Protocol: protocol, Endpoint: endpoint, Attributes: attrs, BaseURL: testBase})
	if err != nil {
		t.Fatalf("Subscribe(%q, %q) error = %v", topicARN, endpoint, err)
	}
	return arn, pending
}

func TestHTTPEndpointValidation(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "ep")
	tests := []struct {
		name, protocol, endpoint string
		want                     error
	}{
		{"http", "http", "http://example.com/hook", nil},
		{"https with port and query", "https", "https://example.com:8443/hook?a=b", nil},
		{"protocol and scheme differ", "http", "https://example.com/hook", ErrInvalidParameter},
		{"https scheme on http", "https", "http://example.com/hook", ErrInvalidParameter},
		{"not a URL", "https", "not-a-url", ErrInvalidParameter},
		{"empty", "http", "", ErrInvalidParameter},
		{"no host", "http", "http:///hook", ErrInvalidParameter},
		{"user info", "http", "http://user:pass@example.com/hook", ErrInvalidParameter},
		{"bad escape", "http", "http://example.com/%zz", ErrInvalidParameter},
		{"email", "email", "http://example.com/hook", ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := e.Subscribe(t.Context(), SubscribeInput{TopicARN: arn, Protocol: tt.protocol, Endpoint: tt.endpoint, BaseURL: testBase})
			if !errors.Is(err, tt.want) {
				t.Errorf("Subscribe(%q, %q) error = %v, want %v", tt.protocol, tt.endpoint, err, tt.want)
			}
		})
	}
}

func TestHTTPPendingSubscription(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	arn := mustTopic(t, e, "pend")
	sub, pending := subscribeHTTP(t, e, arn, testEndpoint, nil)
	if !pending {
		t.Fatalf("Subscribe http pending = false, want true")
	}
	if again, pending := subscribeHTTP(t, e, arn, testEndpoint, nil); again != sub || !pending {
		t.Errorf("Subscribe again = %q, pending %v; want %q, true", again, pending, sub)
	}
	if got := len(e.jobs); got != 2 {
		t.Errorf("queued %d confirmations after Subscribe twice, want 2 (resent while pending)", got)
	}
	if tok := e.subs[sub].Token; len(tok) != 64 {
		t.Errorf("token %q has %d characters, want 64 hex characters", tok, len(tok))
	}

	attrs, err := e.SubscriptionAttributes(t.Context(), sub)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"PendingConfirmation": "true", "ConfirmationWasAuthenticated": "false", "Protocol": "http", "Endpoint": testEndpoint, "EffectiveDeliveryPolicy": defaultSubPolicy}
	got := map[string]string{}
	for _, a := range attrs {
		got[a.Key] = a.Value
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("pending attribute %s = %q, want %q", k, got[k], v)
		}
	}
	// AWS's order, recorded in sns-http-subscriptions.
	var keys []string
	for _, a := range attrs {
		keys = append(keys, a.Key)
	}
	wantKeys := []string{"SubscriptionPrincipal", "Owner", "RawMessageDelivery", "TopicArn", "Endpoint", "EffectiveDeliveryPolicy",
		"Protocol", "PendingConfirmation", "ConfirmationWasAuthenticated", "SubscriptionArn"}
	if !slices.Equal(keys, wantKeys) {
		t.Errorf("attribute order = %v, want %v", keys, wantKeys)
	}

	list, _, err := e.ListSubscriptionsByTopic(t.Context(), arn, "")
	if err != nil || len(list) != 1 || list[0].ARN != "PendingConfirmation" {
		t.Errorf("ListSubscriptionsByTopic = %v, %v; want one PendingConfirmation", list, err)
	}
	all, _, _ := e.ListSubscriptions(t.Context(), "")
	if len(all) != 1 || all[0].ARN != "PendingConfirmation" {
		t.Errorf("ListSubscriptions = %v, want one PendingConfirmation", all)
	}
	mustSubscribe(t, e, arn, "q1", nil)
	ta, _ := e.TopicAttributes(t.Context(), arn)
	if ta[2] != (Attribute{"SubscriptionsPending", "1"}) || ta[5] != (Attribute{"SubscriptionsConfirmed", "1"}) {
		t.Errorf("topic counts = %v and %v, want 1 pending and 1 confirmed", ta[2], ta[5])
	}

	again := openEngine(t, dir, &fakeQueues{})
	if got, err := again.SubscriptionAttributes(t.Context(), sub); err != nil || !reflect.DeepEqual(got, attrs) {
		t.Errorf("SubscriptionAttributes after reopen = %v, %v; want %v", got, err, attrs)
	}
	if got, err := again.ConfirmSubscription(t.Context(), arn, e.subs[sub].Token, true); err != nil || got != sub {
		t.Errorf("ConfirmSubscription after reopen = %q, %v; want %q", got, err, sub)
	}
}

func TestHTTPOpenRejectsPendingWithoutToken(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	arn := mustTopic(t, e, "t")
	sub, _ := subscribeHTTP(t, e, arn, testEndpoint, nil)
	def := *e.subs[sub]
	def.Token = ""
	if err := e.persistSub(&def); err != nil {
		t.Fatal(err)
	}
	if err := e.checkSub(def); err == nil {
		t.Errorf("checkSub(pending without token) = nil, want an error")
	}
}

func TestConfirmSubscription(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "conf")
	other := mustTopic(t, e, "other")
	sub, _ := subscribeHTTP(t, e, arn, testEndpoint, nil)
	token := e.subs[sub].Token
	tests := []struct {
		name, topic, token string
		want               error
	}{
		{"bad token", arn, "bad", ErrInvalidParameter},
		{"empty token", arn, "", ErrInvalidParameter},
		{"token of another topic", other, token, ErrInvalidParameter},
		{"missing topic", arn + "x", token, ErrNotFound},
		{"not an ARN", "x", token, ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := e.ConfirmSubscription(t.Context(), tt.topic, tt.token, true); !errors.Is(err, tt.want) {
				t.Errorf("ConfirmSubscription(%q, %q) = %q, %v; want error %v", tt.topic, tt.token, got, err, tt.want)
			}
		})
	}
	if !e.subs[sub].Pending {
		t.Fatalf("failed confirmations left the subscription confirmed")
	}
	if got, err := e.ConfirmSubscription(t.Context(), arn, token, true); err != nil || got != sub {
		t.Fatalf("ConfirmSubscription = %q, %v; want %q", got, err, sub)
	}
	if got, err := e.ConfirmSubscription(t.Context(), arn, token, false); err != nil || got != sub {
		t.Errorf("ConfirmSubscription again = %q, %v; want %q", got, err, sub)
	}
	if s := e.subs[sub]; s.Pending || s.Unauthenticated {
		t.Errorf("subscription after authenticated confirmation: pending %v, unauthenticated %v; a repeat must not change it", s.Pending, s.Unauthenticated)
	}
	attrs, _ := e.SubscriptionAttributes(t.Context(), sub)
	if i := slicesIndex(attrs, "PendingConfirmation"); attrs[i].Value != "false" || attrs[i+1] != (Attribute{"ConfirmationWasAuthenticated", "true"}) {
		t.Errorf("attributes after confirmation = %v, want confirmed and authenticated", attrs)
	}
	if list, _, _ := e.ListSubscriptionsByTopic(t.Context(), arn, ""); len(list) != 1 || list[0].ARN != sub {
		t.Errorf("ListSubscriptionsByTopic after confirmation = %v, want %q", list, sub)
	}

	unauth, _ := subscribeHTTP(t, e, arn, "https://example.com/hook", nil)
	if _, err := e.ConfirmSubscription(t.Context(), arn, e.subs[unauth].Token, false); err != nil {
		t.Fatal(err)
	}
	attrs, _ = e.SubscriptionAttributes(t.Context(), unauth)
	if i := slicesIndex(attrs, "PendingConfirmation"); attrs[i].Value != "false" || attrs[i+1] != (Attribute{"ConfirmationWasAuthenticated", "false"}) {
		t.Errorf("attributes after unauthenticated confirmation = %v, want confirmed and not authenticated", attrs)
	}
}

func slicesIndex(attrs []Attribute, key string) int {
	for i, a := range attrs {
		if a.Key == key {
			return i
		}
	}
	return -1
}

func TestHTTPUnsignedUnsubscribe(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "unsub")
	pending, _ := subscribeHTTP(t, e, arn, "http://example.com/a", nil)
	unauth, _ := subscribeHTTP(t, e, arn, "http://example.com/b", nil)
	auth, _ := subscribeHTTP(t, e, arn, "http://example.com/c", nil)
	sqs := mustSubscribe(t, e, arn, "q1", nil)
	for sub, authenticated := range map[string]bool{unauth: false, auth: true} {
		if _, err := e.ConfirmSubscription(t.Context(), arn, e.subs[sub].Token, authenticated); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name, sub string
		want      error
	}{
		{"sqs", sqs, ErrAuthorization},
		{"confirmed authenticated", auth, ErrAuthorization},
		{"missing", arn + ":x", ErrNotFound},
		{"pending", pending, ErrInvalidParameter},
		{"confirmed unauthenticated", unauth, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := e.Unsubscribe(t.Context(), tt.sub, false, testBase); !errors.Is(err, tt.want) {
				t.Errorf("Unsubscribe(%q, unsigned) error = %v, want %v", tt.sub, err, tt.want)
			}
		})
	}
	if err := e.Unsubscribe(t.Context(), pending, true, testBase); !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("Unsubscribe(pending, signed) error = %v, want %v", err, ErrInvalidParameter)
	}
	if _, ok := e.subs[pending]; !ok {
		t.Error("pending subscription was removed, want it kept")
	}
	if err := e.Unsubscribe(t.Context(), auth, true, testBase); err != nil {
		t.Errorf("Unsubscribe(authenticated, signed) error = %v, want nil", err)
	}
}

// post is one request that a test endpoint received.
type post struct {
	header http.Header
	body   string
	at     time.Duration // since the endpoint started
}

// recordingEndpoint records every POST and answers with status(n) for the nth.
type recordingEndpoint struct {
	srv   *httptest.Server
	mu    sync.Mutex
	posts []post
}

func startEndpoint(t *testing.T, e *Engine, status func(n int) int) *recordingEndpoint {
	t.Helper()
	ep := &recordingEndpoint{}
	start := time.Now()
	ep.srv = httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ep.mu.Lock()
		ep.posts = append(ep.posts, post{r.Header.Clone(), string(body), time.Since(start)})
		n := len(ep.posts)
		ep.mu.Unlock()
		code := status(n)
		if code/100 == 3 {
			w.Header().Set("Location", "/elsewhere")
		}
		w.WriteHeader(code)
	}))
	// Keep pail's client for its timeout and redirect rule; swap only the transport.
	e.client.Transport = ep.srv.Client().Transport // starts the in-memory network
	return ep
}

func (ep *recordingEndpoint) got() []post {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	return append([]post(nil), ep.posts...)
}

func ok(int) int { return http.StatusOK }

// runDeliveries starts the worker. The returned func stops it and waits.
func runDeliveries(e *Engine) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.RunDeliveries(ctx) }()
	return func() { cancel(); <-done }
}

// topKeys returns the keys of a JSON object in document order.
func topKeys(t *testing.T, body string) []string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(body))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestHTTPConfirmationDelivery(t *testing.T) {
	for _, version := range []string{"1", "2"} {
		t.Run("signature version "+version, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, _ := newEngine(t)
				ep := startEndpoint(t, e, ok)
				stop := runDeliveries(e)
				defer stop()
				arn, err := e.CreateTopic(t.Context(), "confirm", map[string]string{"SignatureVersion": version}, nil)
				if err != nil {
					t.Fatal(err)
				}
				sub, _ := subscribeHTTP(t, e, arn, ep.srv.URL+"/hook", nil)
				synctest.Wait()

				posts := ep.got()
				if len(posts) != 1 {
					t.Fatalf("endpoint received %d requests, want 1", len(posts))
				}
				p := posts[0]
				wantHeaders := map[string]string{
					"Content-Type":           "text/plain; charset=UTF-8",
					"User-Agent":             "Amazon Simple Notification Service Agent",
					"X-Amz-Sns-Message-Type": "SubscriptionConfirmation",
					"X-Amz-Sns-Topic-Arn":    arn,
				}
				for k, v := range wantHeaders {
					if got := p.header.Get(k); got != v {
						t.Errorf("header %s = %q, want %q", k, got, v)
					}
				}
				if p.header.Get("X-Amz-Sns-Subscription-Arn") != "" {
					t.Errorf("confirmation has a subscription ARN header %q, want none", p.header.Get("X-Amz-Sns-Subscription-Arn"))
				}
				wantKeys := []string{"Type", "MessageId", "Token", "TopicArn", "Message", "SubscribeURL", "Timestamp", "SignatureVersion", "Signature", "SigningCertURL"}
				if keys := topKeys(t, p.body); !reflect.DeepEqual(keys, wantKeys) {
					t.Errorf("confirmation keys = %v, want %v", keys, wantKeys)
				}
				var env map[string]string
				if err := json.Unmarshal([]byte(p.body), &env); err != nil {
					t.Fatal(err)
				}
				token := e.subs[sub].Token
				wantSubscribeURL := testBase + "/?Action=ConfirmSubscription&TopicArn=" + arn + "&Token=" + token
				checks := map[string]string{
					"Type": "SubscriptionConfirmation", "Token": token, "TopicArn": arn, "SubscribeURL": wantSubscribeURL,
					"SignatureVersion": version, "SigningCertURL": testBase + "/_pail/sns/signing-cert.pem",
					"Message": "You have chosen to subscribe to the topic " + arn + ".\nTo confirm the subscription, visit the SubscribeURL included in this message.",
				}
				for k, v := range checks {
					if env[k] != v {
						t.Errorf("confirmation %s = %q, want %q", k, env[k], v)
					}
				}
				if p.header.Get("X-Amz-Sns-Message-Id") != env["MessageId"] {
					t.Errorf("message ID header %q, body %q; want equal", p.header.Get("X-Amz-Sns-Message-Id"), env["MessageId"])
				}
				cert, err := e.CertPEM(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				toSign := "Message\n" + env["Message"] + "\nMessageId\n" + env["MessageId"] + "\nSubscribeURL\n" + env["SubscribeURL"] +
					"\nTimestamp\n" + env["Timestamp"] + "\nToken\n" + env["Token"] + "\nTopicArn\n" + env["TopicArn"] + "\nType\nSubscriptionConfirmation\n"
				verifySignature(t, cert, version, toSign, env["Signature"])

				u, err := url.Parse(env["SubscribeURL"])
				if err != nil {
					t.Fatal(err)
				}
				if got, err := e.ConfirmSubscription(t.Context(), u.Query().Get("TopicArn"), u.Query().Get("Token"), false); err != nil || got != sub {
					t.Errorf("ConfirmSubscription from SubscribeURL = %q, %v; want %q", got, err, sub)
				}
			})
		})
	}
}

func TestHTTPNotificationDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		ep := startEndpoint(t, e, ok)
		stop := runDeliveries(e)
		defer stop()
		arn := mustTopic(t, e, "notify")
		hook := ep.srv.URL + "/envelope"
		rawHook := ep.srv.URL + "/raw"
		skipped := ep.srv.URL + "/filtered"
		unconfirmed := ep.srv.URL + "/pending"
		subs := map[string]string{}
		for _, h := range []struct {
			url   string
			attrs map[string]string
		}{
			{hook, nil},
			{rawHook, map[string]string{"RawMessageDelivery": "true"}},
			{skipped, map[string]string{"FilterPolicy": `{"color":["red"]}`}},
		} {
			sub, _ := subscribeHTTP(t, e, arn, h.url, h.attrs)
			subs[h.url] = sub
			if _, err := e.ConfirmSubscription(t.Context(), arn, e.subs[sub].Token, false); err != nil {
				t.Fatal(err)
			}
		}
		subscribeHTTP(t, e, arn, unconfirmed, nil)
		synctest.Wait()
		before := len(ep.got()) // the three confirmations and one more for the pending subscription

		in := publishInput(arn)
		in.BaseURL = testBase
		in.Subject = "greeting"
		in.Message = "hello"
		in.Attributes = map[string]MessageAttribute{"color": {DataType: "String", StringValue: "blue"}}
		id, err := e.Publish(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		posts := ep.got()[before:]
		if len(posts) != 2 {
			t.Fatalf("publish produced %d requests, want 2 (envelope and raw)", len(posts))
		}
		cert, _ := e.CertPEM(t.Context())
		for _, p := range posts {
			if p.header.Get("User-Agent") != "Amazon Simple Notification Service Agent" || p.header.Get("Content-Type") != "text/plain; charset=UTF-8" ||
				p.header.Get("X-Amz-Sns-Message-Type") != "Notification" || p.header.Get("X-Amz-Sns-Message-Id") != id || p.header.Get("X-Amz-Sns-Topic-Arn") != arn {
				t.Errorf("notification headers = %v, want Notification for message %s on %s", p.header, id, arn)
			}
			switch sub := p.header.Get("X-Amz-Sns-Subscription-Arn"); sub {
			case subs[hook]:
				if p.header.Get("X-Amz-Sns-Rawdelivery") != "" {
					t.Errorf("envelope delivery has the raw header %q", p.header.Get("X-Amz-Sns-Rawdelivery"))
				}
				verify(t, cert, p.body)
				var env map[string]any
				if err := json.Unmarshal([]byte(p.body), &env); err != nil {
					t.Fatal(err)
				}
				if env["MessageId"] != id || env["UnsubscribeURL"] != testBase+"/?Action=Unsubscribe&SubscriptionArn="+sub || env["MessageAttributes"] == nil {
					t.Errorf("envelope = %s, want message %s and the UnsubscribeURL of %s", p.body, id, sub)
				}
			case subs[rawHook]:
				if p.header.Get("X-Amz-Sns-Rawdelivery") != "true" || p.body != "hello" {
					t.Errorf("raw delivery: header %q, body %q; want true and hello", p.header.Get("X-Amz-Sns-Rawdelivery"), p.body)
				}
			default:
				t.Errorf("notification for subscription %q, want the envelope or raw subscription", sub)
			}
		}
	})
}

func TestHTTPDeliveryRetries(t *testing.T) {
	always := func(code int) func(int) int { return func(int) int { return code } }
	tests := []struct {
		name   string
		status func(int) int
		want   []time.Duration
	}{
		{"500 retries three times", always(500), []time.Duration{0, 20 * time.Second, 40 * time.Second, 60 * time.Second}},
		{"429 retries", always(429), []time.Duration{0, 20 * time.Second, 40 * time.Second, 60 * time.Second}},
		{"400 does not retry", always(400), []time.Duration{0}},
		{"302 with a Location is not followed or retried", always(302), []time.Duration{0}},
		{"a handler slower than the timeout is retried", func(int) int {
			time.Sleep(time.Minute)
			return 200
		}, []time.Duration{0, 35 * time.Second, 70 * time.Second, 105 * time.Second}}, // 15 s timeout + 20 s delay
		{"200 succeeds", always(200), []time.Duration{0}},
		{"204 succeeds", always(204), []time.Duration{0}},
		{"success stops retries", func(n int) int {
			if n < 3 {
				return 503
			}
			return 200
		}, []time.Duration{0, 20 * time.Second, 40 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, _ := newEngine(t)
				ep := startEndpoint(t, e, tt.status)
				stop := runDeliveries(e)
				defer stop()
				arn := mustTopic(t, e, "retry")
				subscribeHTTP(t, e, arn, ep.srv.URL, nil)
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				var got []time.Duration
				for _, p := range ep.got() {
					got = append(got, p.at)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("attempts at %v, want %v", got, tt.want)
				}
			})
		})
	}
}

func TestHTTPDeliveryStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		ep := startEndpoint(t, e, func(int) int { return 500 })
		stop := runDeliveries(e)
		arn := mustTopic(t, e, "cancel")
		subscribeHTTP(t, e, arn, ep.srv.URL, nil)
		time.Sleep(25 * time.Second) // the job waits for its second attempt
		stop()                       // returns only when RunDeliveries and its job have ended
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		if n := len(ep.got()); n != 2 {
			t.Errorf("endpoint received %d requests, want 2 (none after cancel)", n)
		}
	})
}

func TestHTTPDeliveryQueueFull(t *testing.T) {
	e, _ := newEngine(t)
	e.jobs = make(chan httpJob, 1)
	e.enqueue(httpJob{url: "http://one.test/", header: http.Header{}})
	e.enqueue(httpJob{url: "http://two.test/?secret=1", header: http.Header{}})
	if len(e.jobs) != 1 {
		t.Fatalf("queue holds %d jobs, want 1", len(e.jobs))
	}
	if j := <-e.jobs; j.url != "http://one.test/" {
		t.Errorf("queued job = %q, want the first", j.url)
	}
}

func TestHTTPDeliveryWorkersAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, _ := newEngine(t)
		var mu sync.Mutex
		running, peak := 0, 0
		ep := startEndpoint(t, e, func(int) int {
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()
			time.Sleep(time.Second)
			mu.Lock()
			running--
			mu.Unlock()
			return 200
		})
		stop := runDeliveries(e)
		defer stop()
		const jobs = 3 * deliveryWorkers
		for range jobs {
			e.enqueue(newJob(ep.srv.URL, "x", "Notification", "id", "arn", "sub", false))
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if got := len(ep.got()); got != jobs {
			t.Errorf("endpoint received %d requests, want %d", got, jobs)
		}
		if peak != deliveryWorkers {
			t.Errorf("peak concurrent posts = %d, want %d", peak, deliveryWorkers)
		}
	})
}
