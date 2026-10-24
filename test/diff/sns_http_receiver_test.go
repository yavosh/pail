package diff

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type snsCallback struct {
	header http.Header
	body   []byte
	status int
	at     time.Time
}

type snsCallbackRoute struct {
	callbacks chan snsCallback
	statuses  []int
	attempts  int
	redirects int
}

type snsCallbackReceiver struct {
	mu     sync.Mutex
	routes map[string]*snsCallbackRoute
	base   string
}

func newSNSCallbackReceiver(t *testing.T, publicURL, listen string) *snsCallbackReceiver {
	t.Helper()
	r := &snsCallbackReceiver{routes: map[string]*snsCallbackRoute{}}
	if *record {
		u, err := url.Parse(publicURL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			t.Fatal("recording needs a public HTTPS receiver URL without credentials, query, or fragment")
		}
		host, _, err := net.SplitHostPort(listen)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			t.Fatal("recording listener must use a loopback IP address")
		}
		var lc net.ListenConfig
		listener, err := lc.Listen(t.Context(), "tcp", listen)
		if err != nil {
			t.Fatalf("listen for callbacks: %v", err)
		}
		srv := &http.Server{
			Handler: http.HandlerFunc(r.serveHTTP), ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 8192,
			BaseContext: func(net.Listener) context.Context { return t.Context() },
		}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(listener) }()
		t.Cleanup(func() {
			_ = srv.Close()
			if err := <-done; !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("callback listener stopped: %v", err)
			}
		})
		r.base = strings.TrimRight(publicURL, "/")
		return r
	}
	srv := httptest.NewTestServer(t, http.HandlerFunc(r.serveHTTP))
	srv.Config.ReadHeaderTimeout = 5 * time.Second
	srv.Config.ReadTimeout = 10 * time.Second
	srv.Config.WriteTimeout = 10 * time.Second
	srv.Config.MaxHeaderBytes = 8192
	srv.StartTLS()
	r.base = srv.URL
	return r
}

func (r *snsCallbackReceiver) add(statuses []int) (string, *snsCallbackRoute) {
	r.mu.Lock()
	defer r.mu.Unlock()
	path := "/" + newBucketName()
	route := &snsCallbackRoute{callbacks: make(chan snsCallback, 64), statuses: statuses}
	r.routes[path] = route
	return r.base + path, route
}

func (r *snsCallbackReceiver) serveHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	path, redirect := strings.CutSuffix(req.URL.Path, "/redirect")
	route := r.routes[path]
	if route == nil {
		r.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if redirect {
		route.redirects++
		r.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	r.mu.Unlock()
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20))
	if err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	status := http.StatusNoContent
	if req.Header.Get("X-Amz-Sns-Message-Type") == "Notification" {
		status = route.statuses[min(route.attempts, len(route.statuses)-1)]
		route.attempts++
	}
	if status/100 == 3 {
		w.Header().Set("Location", r.base+path+"/redirect")
	}
	cb := snsCallback{header: req.Header.Clone(), body: body, status: status, at: time.Now()}
	select {
	case route.callbacks <- cb:
		w.WriteHeader(status)
	default:
		w.WriteHeader(http.StatusTooManyRequests)
	}
}

func (r *snsCallbackReceiver) redirectCount(route *snsCallbackRoute) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return route.redirects
}

func nextSNSCallback(t *testing.T, route *snsCallbackRoute) snsCallback {
	t.Helper()
	wait := 10 * time.Second
	if *record {
		wait = 90 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case cb := <-route.callbacks:
		return cb
	case <-timer.C:
		t.Fatal("timed out waiting for SNS callback")
	case <-t.Context().Done():
		t.Fatal("SNS callback wait canceled")
	}
	return snsCallback{}
}

func snsCallbackEnvelope(t *testing.T, cb snsCallback) map[string]any {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal(cb.body, &env); err != nil || env == nil {
		t.Fatal("SNS callback is not a JSON envelope")
	}
	return env
}

func snsString(env map[string]any, name string) string {
	v, _ := env[name].(string)
	return v
}

// The callback is untrusted until its certificate host and signature are checked.
// Errors deliberately omit callback bodies, tokens, and signed URLs.
func verifySNSCallback(t *testing.T, tg *target, cb snsCallback, topicARN, subscriptionARN, version string) map[string]any {
	t.Helper()
	env := snsCallbackEnvelope(t, cb)
	if snsString(env, "TopicArn") != topicARN || cb.header.Get("X-Amz-Sns-Topic-Arn") != topicARN {
		t.Fatal("SNS callback topic does not match the created topic")
	}
	messageID := snsString(env, "MessageId")
	if messageID == "" || cb.header.Get("X-Amz-Sns-Message-Id") != messageID || cb.header.Get("X-Amz-Sns-Message-Type") != snsString(env, "Type") {
		t.Fatal("SNS callback headers do not match its envelope")
	}
	if snsString(env, "Type") == "SubscriptionConfirmation" {
		if cb.header.Get("X-Amz-Sns-Subscription-Arn") != "" {
			t.Fatal("initial confirmation unexpectedly includes a subscription ARN header")
		}
	} else if subscriptionARN == "" || cb.header.Get("X-Amz-Sns-Subscription-Arn") != subscriptionARN {
		t.Fatal("SNS callback subscription does not match the confirmed subscription")
	}
	if snsString(env, "SignatureVersion") != version {
		t.Fatalf("SNS signature version = %q, want %q", snsString(env, "SignatureVersion"), version)
	}
	stamp, err := time.Parse(time.RFC3339Nano, snsString(env, "Timestamp"))
	if err != nil || time.Since(stamp) < -time.Minute || time.Since(stamp) > 10*time.Minute {
		t.Fatal("SNS callback timestamp is invalid or outside the test window")
	}
	u, err := url.Parse(snsString(env, "SigningCertURL"))
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Scheme != tg.scheme || u.Host != tg.serviceHost("sns") {
		t.Fatal("SNS signing certificate URL has an unexpected origin")
	}
	if *record && (!strings.HasPrefix(u.Path, "/SimpleNotificationService-") || !strings.HasSuffix(u.Path, ".pem")) {
		t.Fatal("SNS signing certificate URL has an unexpected path")
	}
	if !*record && u.Path != "/_pail/sns/signing-cert.pem" {
		t.Fatal("local SNS signing certificate URL has an unexpected path")
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatal("build SNS certificate request")
	}
	resp, err := tg.client.Do(req)
	if err != nil {
		t.Fatal("fetch SNS signing certificate failed")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatal("read SNS signing certificate failed")
	}
	block, _ := pem.Decode(body)
	if block == nil {
		t.Fatal("SNS signing certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal("SNS signing certificate is invalid")
	}
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		t.Fatal("SNS signing certificate is not RSA")
	}
	keys := []string{"Message", "MessageId"}
	if snsString(env, "Type") == "Notification" {
		if _, ok := env["Subject"]; ok {
			keys = append(keys, "Subject")
		}
	} else {
		keys = append(keys, "SubscribeURL")
	}
	keys = append(keys, "Timestamp")
	if snsString(env, "Type") != "Notification" {
		keys = append(keys, "Token")
	}
	keys = append(keys, "TopicArn", "Type")
	var signed strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&signed, "%s\n%s\n", key, snsString(env, key))
	}
	sig, err := base64.StdEncoding.DecodeString(snsString(env, "Signature"))
	if err != nil {
		t.Fatal("SNS signature is not base64")
	}
	hash := crypto.SHA256
	sum256 := sha256.Sum256([]byte(signed.String()))
	sum := sum256[:]
	if version == "1" {
		hash = crypto.SHA1
		sum1 := sha1.Sum([]byte(signed.String()))
		sum = sum1[:]
	}
	if err := rsa.VerifyPKCS1v15(pub, hash, sum, sig); err != nil {
		t.Fatal("SNS callback signature does not verify")
	}
	if snsString(env, "Type") == "Notification" {
		if _, err := snsLinkURL(tg, snsString(env, "UnsubscribeURL"), "Unsubscribe", topicARN, subscriptionARN); err != nil {
			t.Fatal("notification unsubscribe link does not match its subscription")
		}
	} else {
		u, err := snsLinkURL(tg, snsString(env, "SubscribeURL"), "ConfirmSubscription", topicARN, "")
		if err != nil || u.Query().Get("Token") != snsString(env, "Token") {
			t.Fatal("confirmation link does not match its topic and token")
		}
	}
	return env
}

func snsLinkURL(tg *target, link, action, topicARN, subscriptionARN string) (*url.URL, error) {
	u, err := url.Parse(link)
	if err != nil || u.User != nil || u.Fragment != "" || u.Scheme != tg.scheme || u.Host != tg.serviceHost("sns") || u.Path != "/" {
		return nil, fmt.Errorf("unexpected SNS link origin")
	}
	q := u.Query()
	if q.Get("Action") != action || (action == "ConfirmSubscription" && (q.Get("TopicArn") != topicARN || q.Get("Token") == "")) || (action == "Unsubscribe" && q.Get("SubscriptionArn") != subscriptionARN) {
		return nil, fmt.Errorf("unexpected SNS link parameters")
	}
	return u, nil
}

func snsLink(ctx context.Context, tg *target, link, action, topicARN, subscriptionARN string) (response, error) {
	u, err := snsLinkURL(tg, link, action, topicARN, subscriptionARN)
	if err != nil {
		return response{}, err
	}
	return tg.do(ctx, step{method: http.MethodGet, service: "sns", auth: authNone, query: u.RawQuery}, "")
}
