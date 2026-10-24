package diff

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	snsHTTPPublicURL = flag.String("sns-http-public-url", "", "public HTTPS URL forwarding to the SNS callback recording listener")
	snsHTTPListen    = flag.String("sns-http-listen", "127.0.0.1:18786", "loopback address for live AWS SNS callback recording")
)

type snsHTTPCase struct {
	name, version string
	statuses      []int
	authenticated bool
	raw, jsonBody bool
	filter        bool
	subject       bool
	contentType   string
}

type snsHTTPRun struct {
	t        *testing.T
	tg       *target
	name     string
	endpoint string
	vars     map[string]string
	golden   golden
	cfg      snsHTTPCase
}

func TestSNSHTTPDelivery(t *testing.T) {
	if *record && *snsHTTPPublicURL == "" {
		t.Skip("live callback recording needs -sns-http-public-url")
	}
	receiver := newSNSCallbackReceiver(t, *snsHTTPPublicURL, *snsHTTPListen)
	var target *target
	if *record {
		target = awsTarget(t)
	} else {
		target = pailTargetTLS(t, true)
	}
	for _, cfg := range []snsHTTPCase{
		{name: "version-1-links", version: "1", statuses: []int{204}, subject: true},
		{name: "version-2-authenticated", version: "2", statuses: []int{204}, authenticated: true},
		{name: "raw", version: "1", statuses: []int{204}, raw: true},
		{name: "json-content-type", version: "2", statuses: []int{204}, jsonBody: true, contentType: "application/json"},
		{name: "filter", version: "1", statuses: []int{204}, filter: true},
		{name: "permanent-400", version: "1", statuses: []int{400}},
		{name: "redirect-302", version: "1", statuses: []int{302}},
		{name: "retry-429", version: "1", statuses: []int{429, 429, 429}},
		{name: "retry-503", version: "2", statuses: []int{503, 503, 503}},
		{name: "recover-503", version: "1", statuses: []int{503, 204}},
		{name: "raw-dead-letter", version: "1", statuses: []int{429, 429, 429}, raw: true},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			endpoint, route := receiver.add(cfg.statuses)
			run := &snsHTTPRun{t: t, tg: target, name: newBucketName(), endpoint: endpoint, cfg: cfg, vars: map[string]string{}, golden: golden{Scenario: "sns-http-live-" + cfg.name}}
			run.exercise(route)
			if receiver.redirectCount(route) != 0 {
				t.Fatal("SNS followed the notification redirect")
			}
			run.compare()
		})
	}
}

func (r *snsHTTPRun) exercise(route *snsCallbackRoute) {
	t := r.t
	t.Cleanup(func() { cleanupTopic(t, r.tg, r.name, r.vars["topicArn"]) })
	t.Cleanup(func() { cleanupQueue(t, r.tg, r.name) })
	r.sns("create-topic", url.Values{"Action": {"CreateTopic"}, "Name": {r.name}, "Attributes.entry.1.key": {"SignatureVersion"}, "Attributes.entry.1.value": {r.cfg.version}}, 200)
	r.sqs("create-dead-letter", "CreateQueue", map[string]any{"QueueName": r.name})
	r.sqs("dead-letter-arn", "GetQueueAttributes", map[string]any{"QueueUrl": r.vars["queueUrl"], "AttributeNames": []string{"QueueArn"}})
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage","Resource":"` + r.vars["queueArn"] + `","Condition":{"ArnEquals":{"aws:SourceArn":"` + r.vars["topicArn"] + `"}}}]}`
	r.sqs("allow-topic", "SetQueueAttributes", map[string]any{"QueueUrl": r.vars["queueUrl"], "Attributes": map[string]string{"Policy": policy}})
	attrs := map[string]string{
		"RedrivePolicy":  `{"deadLetterTargetArn":"` + r.vars["queueArn"] + `"}`,
		"DeliveryPolicy": `{"healthyRetryPolicy":{"minDelayTarget":1,"maxDelayTarget":1,"numRetries":2,"numNoDelayRetries":0,"numMinDelayRetries":2,"numMaxDelayRetries":0,"backoffFunction":"linear"}}`,
	}
	if r.cfg.contentType != "" {
		attrs["DeliveryPolicy"] = strings.TrimSuffix(attrs["DeliveryPolicy"], "}") + `,"requestPolicy":{"headerContentType":"` + r.cfg.contentType + `"}}`
	}
	if r.cfg.raw {
		attrs["RawMessageDelivery"] = "true"
	}
	if r.cfg.filter {
		attrs["FilterPolicy"] = `{"kind":["accept"]}`
	}
	subscribe := url.Values{"Action": {"Subscribe"}, "TopicArn": {r.vars["topicArn"]}, "Protocol": {"https"}, "Endpoint": {r.endpoint}, "ReturnSubscriptionArn": {"true"}}
	attributeIndex := 0
	for _, key := range []string{"RedrivePolicy", "DeliveryPolicy", "RawMessageDelivery", "FilterPolicy"} {
		if value, ok := attrs[key]; ok {
			attributeIndex++
			subscribe.Set(fmt.Sprintf("Attributes.entry.%d.key", attributeIndex), key)
			subscribe.Set(fmt.Sprintf("Attributes.entry.%d.value", attributeIndex), value)
		}
	}
	r.sns("subscribe", subscribe, 200)
	confirmation := nextSNSCallback(t, route)
	env := verifySNSCallback(t, r.tg, confirmation, r.vars["topicArn"], "", r.cfg.version)
	if snsString(env, "Type") != "SubscriptionConfirmation" || snsString(env, "Token") == "" {
		t.Fatal("expected a subscription confirmation with a token")
	}
	r.callback("confirmation", confirmation, env)
	if r.cfg.authenticated {
		r.sns("confirm-authenticated", url.Values{"Action": {"ConfirmSubscription"}, "TopicArn": {r.vars["topicArn"]}, "Token": {snsString(env, "Token")}, "AuthenticateOnUnsubscribe": {"true"}}, 200)
	} else {
		r.link("confirm-unsigned", snsString(env, "SubscribeURL"), "ConfirmSubscription", 200)
	}
	r.sns("get-subscription", url.Values{"Action": {"GetSubscriptionAttributes"}, "SubscriptionArn": {r.vars["subscriptionArn"]}}, 200)
	messageID, message := r.publish("publish", "accept")
	var last map[string]any
	var first time.Time
	for i := range r.cfg.statuses {
		cb := nextSNSCallback(t, route)
		if i == 0 {
			first = cb.at
		} else {
			delay := cb.at.Sub(first)
			if delay < 0 || delay > 60*time.Second {
				t.Fatalf("retry %d elapsed = %v, want within one minute", i, delay)
			}
			t.Logf("retry %d elapsed: %s", i, delay.Round(time.Millisecond))
		}
		last = r.notification(cb, messageID, message)
		r.callback(fmt.Sprintf("notification-%d", i+1), cb, last)
	}
	status := r.cfg.statuses[len(r.cfg.statuses)-1]
	failed := status/100 != 2
	retryable := status == 429 || status >= 500
	if failed && retryable {
		r.deadLetter(messageID, message)
	}
	if !failed {
		if r.cfg.filter {
			r.publish("publish-excluded", "reject")
			id, body := r.publish("publish-filter-barrier", "accept")
			cb := nextSNSCallback(t, route)
			r.callback("notification-filter-barrier", cb, r.notification(cb, id, body))
		}
		r.noDeadLetter()
	}
	if failed && !retryable {
		r.quiet(route, 5*time.Second)
	}
	select {
	case <-route.callbacks:
		t.Fatal("unexpected extra SNS callback after delivery completed")
	default:
	}
	if r.cfg.name == "version-1-links" || r.cfg.authenticated {
		originalSubscription := r.vars["subscriptionArn"]
		status := 200
		if r.cfg.authenticated {
			status = 403
		}
		r.link("unsubscribe-unsigned", snsString(last, "UnsubscribeURL"), "Unsubscribe", status)
		if !r.cfg.authenticated {
			cb := nextSNSCallback(t, route)
			restore := verifySNSCallback(t, r.tg, cb, r.vars["topicArn"], r.vars["subscriptionArn"], r.cfg.version)
			if snsString(restore, "Type") != "UnsubscribeConfirmation" {
				t.Fatal("expected an unsubscribe confirmation")
			}
			r.callback("unsubscribe-confirmation", cb, restore)
			r.link("restore-unsigned", snsString(restore, "SubscribeURL"), "ConfirmSubscription", 200)
			st := step{name: "restored-identity", body: "compare original and restored subscription ARN"}
			r.golden.Exchanges = append(r.golden.Exchanges, exchange{Step: st.name, Request: "compare subscription identities", Fingerprint: fingerprint(st), Status: 200, Body: fmt.Sprint(originalSubscription == r.vars["subscriptionArn"])})
			id, body := r.publish("publish-restored", "accept")
			cb = nextSNSCallback(t, route)
			r.callback("notification-restored", cb, r.notification(cb, id, body))
		}
	}
	r.sns("unsubscribe-signed", url.Values{"Action": {"Unsubscribe"}, "SubscriptionArn": {r.vars["subscriptionArn"]}}, 200)
}

func (r *snsHTTPRun) publish(name, kind string) (string, string) {
	message := "line one\n\"quoted\" <body>&"
	v := url.Values{"Action": {"Publish"}, "TopicArn": {r.vars["topicArn"]}, "Message": {message},
		"MessageAttributes.entry.1.Name": {"kind"}, "MessageAttributes.entry.1.Value.DataType": {"String"}, "MessageAttributes.entry.1.Value.StringValue": {kind},
		"MessageAttributes.entry.2.Name": {"number"}, "MessageAttributes.entry.2.Value.DataType": {"Number"}, "MessageAttributes.entry.2.Value.StringValue": {"3.5"},
		"MessageAttributes.entry.3.Name": {"binary"}, "MessageAttributes.entry.3.Value.DataType": {"Binary"}, "MessageAttributes.entry.3.Value.BinaryValue": {"YWJj"},
	}
	if r.cfg.subject {
		v.Set("Subject", "callback subject")
	}
	if r.cfg.jsonBody {
		v.Set("MessageStructure", "json")
		v.Set("Message", `{"default":"fallback","http":"http selection","https":"https selection"}`)
		message = "https selection"
	}
	r.sns(name, v, 200)
	return r.vars["messageId"], message
}

func (r *snsHTTPRun) notification(cb snsCallback, messageID, message string) map[string]any {
	r.t.Helper()
	t := r.t
	if cb.header.Get("X-Amz-Sns-Message-Id") != messageID || cb.header.Get("X-Amz-Sns-Topic-Arn") != r.vars["topicArn"] || cb.header.Get("X-Amz-Sns-Subscription-Arn") != r.vars["subscriptionArn"] {
		t.Fatal("notification headers do not match the publication and subscription")
	}
	if r.cfg.raw {
		if string(cb.body) != message || cb.header.Get("X-Amz-Sns-Rawdelivery") != "true" {
			t.Fatal("raw notification body or raw-delivery header changed")
		}
		return nil
	}
	env := verifySNSCallback(t, r.tg, cb, r.vars["topicArn"], r.vars["subscriptionArn"], r.cfg.version)
	if snsString(env, "Type") != "Notification" || snsString(env, "MessageId") != messageID || snsString(env, "Message") != message {
		t.Fatal("notification changed publication identity or message")
	}
	_, subject := env["Subject"]
	if subject != r.cfg.subject || (subject && snsString(env, "Subject") != "callback subject") {
		t.Fatal("notification subject presence or value changed")
	}
	attrs, _ := env["MessageAttributes"].(map[string]any)
	want := map[string]any{
		"kind":   map[string]any{"Type": "String", "Value": "accept"},
		"number": map[string]any{"Type": "Number", "Value": "3.5"},
		"binary": map[string]any{"Type": "Binary", "Value": "YWJj"},
	}
	if !reflect.DeepEqual(attrs, want) {
		t.Fatalf("notification message attributes = %v, want %v", attrs, want)
	}
	return env
}

func (r *snsHTTPRun) deadLetter(messageID, message string) {
	r.t.Helper()
	body, _ := json.Marshal(map[string]any{"QueueUrl": r.vars["queueUrl"], "WaitTimeSeconds": 20, "MessageAttributeNames": []string{"All"}})
	st := sqsStep("receive-dead-letter", "ReceiveMessage", string(body))
	deadline := time.Now().Add(2 * time.Minute)
	for {
		resp, err := r.tg.do(r.t.Context(), st, r.name)
		if err != nil || resp.status != http.StatusOK {
			r.t.Fatal("receive dead-letter message failed")
		}
		var out struct{ Messages []struct{ Body string } }
		if json.Unmarshal(resp.body, &out) != nil {
			r.t.Fatal("dead-letter response is not JSON")
		}
		if len(out.Messages) == 0 && time.Now().Before(deadline) {
			continue
		}
		if len(out.Messages) != 1 {
			r.t.Fatal("exhausted delivery did not reach its dead-letter queue within two minutes")
		}
		st.body = r.mask(st.body)
		r.addResponse(st, resp)
		if r.cfg.raw {
			if out.Messages[0].Body != message {
				r.t.Fatal("raw dead-letter body changed")
			}
			return
		}
		var env map[string]any
		if json.Unmarshal([]byte(out.Messages[0].Body), &env) != nil || snsString(env, "MessageId") != messageID || snsString(env, "Message") != message {
			r.t.Fatal("dead-letter notification changed publication identity or message")
		}
		return
	}
}

func (r *snsHTTPRun) noDeadLetter() {
	r.t.Helper()
	wait := 3
	if r.cfg.filter {
		wait = 5
	}
	resp := r.sqs("receive-no-dead-letter", "ReceiveMessage", map[string]any{"QueueUrl": r.vars["queueUrl"], "WaitTimeSeconds": wait})
	var out struct{ Messages []json.RawMessage }
	if json.Unmarshal(resp.body, &out) != nil || len(out.Messages) != 0 {
		r.t.Fatal("successful or filtered delivery unexpectedly reached its dead-letter queue")
	}
}

func (r *snsHTTPRun) quiet(route *snsCallbackRoute, duration time.Duration) {
	r.t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-route.callbacks:
		r.t.Fatal("unexpected SNS callback during the quiet observation window")
	case <-timer.C:
	case <-r.t.Context().Done():
		r.t.Fatal("SNS quiet observation canceled")
	}
	st := step{name: "quiet", body: duration.String()}
	r.golden.Exchanges = append(r.golden.Exchanges, exchange{Step: "quiet", Request: "observe callbacks", Fingerprint: fingerprint(st), Status: 200, Body: "no additional callbacks"})
}

func (r *snsHTTPRun) sns(name string, values url.Values, status int) response {
	r.t.Helper()
	values.Set("Version", "2010-03-31")
	st := snsStep(name, values.Encode())
	resp, err := r.tg.do(r.t.Context(), st, r.name)
	if err != nil {
		r.t.Fatalf("%s request failed", name)
	}
	if resp.status != status {
		var out struct{ Error struct{ Code string } }
		_ = xml.Unmarshal(resp.body, &out)
		r.t.Fatalf("%s status = %d (%s), want %d", name, resp.status, out.Error.Code, status)
	}
	captureVars(r.vars, resp.body)
	if values.Has("Token") {
		values.Set("Token", "<token>")
	}
	st.body = r.mask(values.Encode())
	r.addResponse(st, resp)
	return resp
}

func (r *snsHTTPRun) sqs(name, action string, input any) response {
	r.t.Helper()
	body, _ := json.Marshal(input)
	st := sqsStep(name, action, string(body))
	resp, err := r.tg.do(r.t.Context(), st, r.name)
	if err != nil || resp.status != http.StatusOK {
		r.t.Fatalf("%s SQS request failed with status %d", name, resp.status)
	}
	captureVars(r.vars, resp.body)
	st.body = r.mask(st.body)
	r.addResponse(st, resp)
	return resp
}

func (r *snsHTTPRun) link(name, link, action string, status int) {
	r.t.Helper()
	resp, err := snsLink(r.t.Context(), r.tg, link, action, r.vars["topicArn"], r.vars["subscriptionArn"])
	if err != nil || resp.status != status {
		r.t.Fatalf("%s status = %d, want %d; valid link = %t", name, resp.status, status, err == nil)
	}
	captureVars(r.vars, resp.body)
	st := snsStep(name, "Action="+action)
	st.method, st.auth = http.MethodGet, authNone
	r.addResponse(st, resp)
}

func (r *snsHTTPRun) addResponse(st step, resp response) {
	ex := r.tg.normalize(st, r.name, resp)
	ex.Body = r.mask(ex.Body)
	r.golden.Exchanges = append(r.golden.Exchanges, ex)
}

func (r *snsHTTPRun) mask(value string) string {
	for _, from := range []string{r.endpoint, r.vars["queueUrl"], r.vars["queueArn"], r.vars["topicArn"], r.vars["subscriptionArn"]} {
		if from != "" {
			value = strings.ReplaceAll(value, from, "<target>")
			value = strings.ReplaceAll(value, url.QueryEscape(from), "%3Ctarget%3E")
		}
	}
	value = strings.ReplaceAll(value, r.name, "{name}")
	value = uuidPattern.ReplaceAllString(value, "{uuid}")
	return accountPattern.ReplaceAllString(value, "{account}")
}

func (r *snsHTTPRun) callback(name string, cb snsCallback, env map[string]any) {
	body := string(cb.body)
	if env != nil {
		var b strings.Builder
		writeEnvelope(&b, "Body", env)
		body = b.String()
		if token := snsString(env, "Token"); token != "" {
			body = strings.ReplaceAll(body, token, "<token>")
			body = strings.ReplaceAll(body, url.QueryEscape(token), "<token>")
		}
	}
	header := map[string]string{}
	for _, key := range []string{"Content-Type", "User-Agent", "X-Amz-Sns-Message-Type", "X-Amz-Sns-Message-Id", "X-Amz-Sns-Topic-Arn", "X-Amz-Sns-Subscription-Arn", "X-Amz-Sns-Rawdelivery"} {
		if value := cb.header.Get(key); value != "" {
			header[key] = r.mask(value)
		}
	}
	input, _ := json.Marshal(r.cfg.statuses)
	st := step{name: name, method: http.MethodPost, body: string(input)}
	r.golden.Exchanges = append(r.golden.Exchanges, exchange{Step: name, Request: "POST callback", Fingerprint: fingerprint(st), Status: cb.status, Headers: header, Body: r.mask(body)})
}

func (r *snsHTTPRun) compare() {
	r.t.Helper()
	path := filepath.Join(goldenDir, r.golden.Scenario+".json")
	if *record {
		if err := writeGolden(path, r.golden); err != nil {
			r.t.Fatal(err)
		}
		r.t.Logf("recorded %s", path)
		return
	}
	want, err := readGolden(path)
	if err != nil {
		r.t.Fatalf("read AWS callback recording: %v", err)
	}
	if len(want.Exchanges) != len(r.golden.Exchanges) {
		r.t.Fatalf("callback trace length = %d, want %d", len(r.golden.Exchanges), len(want.Exchanges))
	}
	for i, got := range r.golden.Exchanges {
		if got.Step != want.Exchanges[i].Step || got.Fingerprint != want.Exchanges[i].Fingerprint {
			r.t.Errorf("step %s has a stale AWS recording", got.Step)
		}
		expected := want.Exchanges[i]
		if got.Step == "get-subscription" {
			expected.Body = orderedSNSAttributes(expected.Body)
			got.Body = orderedSNSAttributes(got.Body)
		}
		for field, diff := range compare(expected, got) {
			r.t.Errorf("%s: %s", field, diff)
		}
	}
}

// SNS attributes are an unordered map. Keep each key and value together when
// sorting the canonical XML, including recordings made before this comparison.
func orderedSNSAttributes(body string) string {
	prefix, entries, ok := strings.Cut(body, "    Attributes\n")
	if !ok {
		return body
	}
	entries, suffix, ok := strings.Cut(entries, "  ResponseMetadata\n")
	if !ok {
		return body
	}
	blocks := strings.Split(strings.TrimPrefix(entries, "      entry\n"), "      entry\n")
	slices.Sort(blocks)
	return prefix + "    Attributes\n      entry\n" + strings.Join(blocks, "      entry\n") + "  ResponseMetadata\n" + suffix
}
