package topic

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/vfs"
)

func publishInput(arn string) PublishInput {
	return PublishInput{TopicARN: arn, Message: "hello", BaseURL: "http://localhost:9000"}
}

func TestPublishValidation(t *testing.T) {
	e, _ := newEngine(t)
	arn := mustTopic(t, e, "pub")
	attr := func(dataType, value string) map[string]MessageAttribute {
		return map[string]MessageAttribute{"a": {DataType: dataType, StringValue: value}}
	}
	tooMany := map[string]MessageAttribute{}
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"} {
		tooMany[n] = MessageAttribute{DataType: "String", StringValue: "v"}
	}
	tests := []struct {
		name   string
		modify func(*PublishInput)
		want   error
	}{
		{"valid", func(*PublishInput) {}, nil},
		{"missing topic", func(in *PublishInput) { in.TopicARN += "x" }, ErrNotFound},
		{"no topic ARN", func(in *PublishInput) { in.TopicARN = "" }, ErrInvalidParameter},
		{"empty message", func(in *PublishInput) { in.Message = "" }, ErrInvalidParameter},
		{"message at the limit", func(in *PublishInput) { in.Message = strings.Repeat("x", 262144) }, nil},
		{"message too long", func(in *PublishInput) { in.Message = strings.Repeat("x", 262145) }, ErrInvalidParameter},
		{"attributes count toward the size", func(in *PublishInput) {
			in.Message = strings.Repeat("x", 262144)
			in.Attributes = attr("String", "v")
		}, ErrInvalidParameter},
		{"subject of 100", func(in *PublishInput) { in.Subject = strings.Repeat("s", 100) }, nil},
		{"subject of 101", func(in *PublishInput) { in.Subject = strings.Repeat("s", 101) }, ErrInvalidParameter},
		{"subject with a line break", func(in *PublishInput) { in.Subject = "a\nb" }, ErrInvalidParameter},
		{"subject with non-ASCII", func(in *PublishInput) { in.Subject = "café" }, ErrInvalidParameter},
		{"group ID on a standard topic", func(in *PublishInput) { in.GroupID = "g" }, nil},
		{"group ID of 128", func(in *PublishInput) { in.GroupID = strings.Repeat("g", 128) }, nil},
		{"group ID of 129", func(in *PublishInput) { in.GroupID = strings.Repeat("g", 129) }, ErrInvalidParameter},
		{"group ID with a space", func(in *PublishInput) { in.GroupID = "a b" }, ErrInvalidParameter},
		{"deduplication ID on a standard topic", func(in *PublishInput) { in.DeduplicationID = "d" }, ErrInvalidParameter},
		{"unknown message structure", func(in *PublishInput) { in.MessageStructure = "xml" }, ErrInvalidParameter},
		{"json without default", func(in *PublishInput) { in.MessageStructure = "json"; in.Message = `{"sqs":"x"}` }, ErrInvalidParameter},
		{"json not an object", func(in *PublishInput) { in.MessageStructure = "json"; in.Message = `["a"]` }, ErrInvalidParameter},
		{"json with a non-string value", func(in *PublishInput) { in.MessageStructure = "json"; in.Message = `{"default":1}` }, ErrInvalidParameter},
		{"json", func(in *PublishInput) { in.MessageStructure = "json"; in.Message = `{"default":"d"}` }, nil},
		{"11 attributes", func(in *PublishInput) { in.Attributes = tooMany }, ErrInvalidParameter},
		{"number attribute", func(in *PublishInput) { in.Attributes = attr("Number", "3.5") }, nil},
		{"bad number", func(in *PublishInput) { in.Attributes = attr("Number", "three") }, ErrInvalidParameter},
		{"custom label", func(in *PublishInput) { in.Attributes = attr("String.json", "{}") }, nil},
		{"string array", func(in *PublishInput) { in.Attributes = attr("String.Array", `["a",1,true]`) }, nil},
		{"string array not an array", func(in *PublishInput) { in.Attributes = attr("String.Array", `"a"`) }, ErrInvalidParameter},
		{"empty string value", func(in *PublishInput) { in.Attributes = attr("String", "") }, ErrInvalidParameter},
		{"unknown data type", func(in *PublishInput) { in.Attributes = attr("Blob", "x") }, ErrParameterValueInvalid},
		{"empty custom label", func(in *PublishInput) { in.Attributes = attr("String.", "x") }, ErrInvalidParameter},
		{"binary attribute", func(in *PublishInput) {
			in.Attributes = map[string]MessageAttribute{"a": {DataType: "Binary", BinaryValue: []byte{1}}}
		}, nil},
		{"binary without value", func(in *PublishInput) { in.Attributes = attr("Binary", "") }, ErrInvalidParameter},
		{"reserved name", func(in *PublishInput) {
			in.Attributes = map[string]MessageAttribute{"AWS.x": {DataType: "String", StringValue: "v"}}
		}, ErrInvalidParameter},
		{"bad name character", func(in *PublishInput) {
			in.Attributes = map[string]MessageAttribute{"a b": {DataType: "String", StringValue: "v"}}
		}, ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := publishInput(arn)
			tt.modify(&in)
			id, err := e.Publish(t.Context(), in)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Publish error = %v, want %v", err, tt.want)
			}
			if (err == nil) != uuidRE.MatchString(id) {
				t.Errorf("Publish MessageId = %q with error %v, want a UUID only on success", id, err)
			}
		})
	}
}

func TestPublishEnvelope(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "fan")
	mustSubscribe(t, e, arn, "q1", nil)
	mustSubscribe(t, e, arn, "q2", nil)
	in := publishInput(arn)
	in.Subject = "greeting"
	in.Message = `a "quote" <b> & more`
	in.Attributes = map[string]MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
		"blob":  {DataType: "Binary", BinaryValue: []byte{1, 2, 3}},
	}
	id, err := e.Publish(t.Context(), in)
	if err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(fq.sent) != 2 {
		t.Fatalf("sent %d messages, want 2", len(fq.sent))
	}
	byQueue := map[string]queue.SendInput{}
	for _, s := range fq.sent {
		byQueue[s.Queue] = s.In
		if len(s.In.Attributes) != 0 {
			t.Errorf("envelope delivery to %s has SQS attributes %v, want none", s.Queue, s.In.Attributes)
		}
	}
	if len(byQueue) != 2 {
		t.Fatalf("deliveries went to queues %v, want q1 and q2", byQueue)
	}
	subs, _, _ := e.ListSubscriptionsByTopic(t.Context(), arn, "")
	for _, sub := range subs {
		name := sub.Endpoint[strings.LastIndex(sub.Endpoint, ":")+1:]
		body := byQueue[name].Body
		var keys []string
		dec := json.NewDecoder(strings.NewReader(body))
		dec.Token()
		for dec.More() {
			k, _ := dec.Token()
			keys = append(keys, k.(string))
			var skip json.RawMessage
			_ = dec.Decode(&skip)
		}
		wantKeys := []string{"Type", "MessageId", "TopicArn", "Subject", "Message", "Timestamp", "SignatureVersion", "Signature", "SigningCertURL", "UnsubscribeURL", "MessageAttributes"}
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Errorf("%s envelope keys = %v, want %v\n%s", name, keys, wantKeys, body)
		}
		var env map[string]any
		if err := json.Unmarshal([]byte(body), &env); err != nil {
			t.Fatalf("envelope %s is not JSON: %v", body, err)
		}
		if env["Message"] != in.Message || env["MessageId"] != id || env["TopicArn"] != arn || env["Subject"] != "greeting" || env["Type"] != "Notification" || env["SignatureVersion"] != "1" {
			t.Errorf("%s envelope = %v", name, env)
		}
		if want := "http://localhost:9000/_pail/sns/signing-cert.pem"; env["SigningCertURL"] != want {
			t.Errorf("SigningCertURL = %v, want %s", env["SigningCertURL"], want)
		}
		if want := "http://localhost:9000/?Action=Unsubscribe&SubscriptionArn=" + sub.ARN; env["UnsubscribeURL"] != want {
			t.Errorf("UnsubscribeURL = %v, want %s", env["UnsubscribeURL"], want)
		}
		wantAttrs := map[string]any{
			"color": map[string]any{"Type": "String", "Value": "blue"},
			"blob":  map[string]any{"Type": "Binary", "Value": "AQID"},
		}
		if !reflect.DeepEqual(env["MessageAttributes"], wantAttrs) {
			t.Errorf("MessageAttributes = %v, want %v", env["MessageAttributes"], wantAttrs)
		}
		if strings.Contains(body, "\\u003c") || strings.Contains(body, "\\u0026") {
			t.Errorf("envelope %s escapes HTML characters", body)
		}
		if ts, _ := env["Timestamp"].(string); len(ts) != len("2006-01-02T15:04:05.000Z") {
			t.Errorf("Timestamp = %q, want millisecond precision", ts)
		} else if _, err := time.Parse(timestampFormat, ts); err != nil {
			t.Errorf("Timestamp %q: %v", ts, err)
		}
	}
}

func TestPublishEnvelopeOmitsEmptyFields(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "plain")
	mustSubscribe(t, e, arn, "q1", nil)
	if _, err := e.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatal(err)
	}
	body := fq.sent[0].In.Body
	for _, key := range []string{`"Subject"`, `"MessageAttributes"`} {
		if strings.Contains(body, key) {
			t.Errorf("envelope %s has %s, want it omitted", body, key)
		}
	}
}

func TestPublishRawDelivery(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "raw")
	mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": "true"})
	in := publishInput(arn)
	in.Attributes = map[string]MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
		"count": {DataType: "Number", StringValue: "3"},
		"blob":  {DataType: "Binary", BinaryValue: []byte{9}},
		"tags":  {DataType: "String.Array", StringValue: `["a","b"]`},
	}
	if _, err := e.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	want := queue.SendInput{Body: "hello", Attributes: map[string]queue.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
		"count": {DataType: "Number", StringValue: "3"},
		"blob":  {DataType: "Binary", BinaryValue: []byte{9}},
		"tags":  {DataType: "String.Array", StringValue: `["a","b"]`},
	}}
	if len(fq.sent) != 1 || !reflect.DeepEqual(fq.sent[0].In, want) {
		t.Errorf("raw delivery = %+v, want one send of %+v", fq.sent, want)
	}
}

func TestPublishMessageStructure(t *testing.T) {
	tests := []struct {
		name, message, want string
	}{
		{"sqs key", `{"default":"d","sqs":"s"}`, "s"},
		{"default only", `{"default":"d","email":"e"}`, "d"},
	}
	for _, raw := range []bool{false, true} {
		for _, tt := range tests {
			mode, rawValue := "envelope", "false"
			if raw {
				mode, rawValue = "raw", "true"
			}
			t.Run(tt.name+" "+mode, func(t *testing.T) {
				e, fq := newEngine(t)
				arn := mustTopic(t, e, "structured")
				mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": rawValue})
				in := publishInput(arn)
				in.MessageStructure = "json"
				in.Message = tt.message
				if _, err := e.Publish(t.Context(), in); err != nil {
					t.Fatal(err)
				}
				got := fq.sent[0].In.Body
				if !raw {
					var env struct{ Message string }
					if err := json.Unmarshal([]byte(got), &env); err != nil {
						t.Fatal(err)
					}
					got = env.Message
				}
				if got != tt.want {
					t.Errorf("delivered message = %q, want %q", got, tt.want)
				}
			})
		}
	}
}

func TestPublishForwardsGroupID(t *testing.T) {
	for _, raw := range []string{"false", "true"} {
		e, fq := newEngine(t)
		arn := mustTopic(t, e, "grouped")
		mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": raw})
		in := publishInput(arn)
		in.GroupID = "tenant"
		if _, err := e.Publish(t.Context(), in); err != nil {
			t.Fatal(err)
		}
		if len(fq.sent) != 1 || fq.sent[0].In.GroupID != "tenant" {
			t.Errorf("RawMessageDelivery %s: sent = %+v, want one message with GroupID tenant", raw, fq.sent)
		}
	}
}

func TestPublishMissingQueue(t *testing.T) {
	e, fq := newEngine(t)
	fq.missing["gone"] = true
	arn := mustTopic(t, e, "lenient")
	mustSubscribe(t, e, arn, "gone", nil)
	mustSubscribe(t, e, arn, "here", nil)
	if _, err := e.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatalf("Publish error = %v, want success when a queue is missing", err)
	}
	if len(fq.sent) != 1 || fq.sent[0].Queue != "here" {
		t.Errorf("sent = %+v, want one message to here", fq.sent)
	}
}

func TestPublishWithoutSubscriptions(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "empty")
	if _, err := e.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(fq.sent) != 0 {
		t.Errorf("sent = %+v, want nothing", fq.sent)
	}
	if _, err := e.CertPEM(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPublishBatch(t *testing.T) {
	e, fq := newEngine(t)
	arn := mustTopic(t, e, "batch")
	mustSubscribe(t, e, arn, "q1", nil)
	res, err := e.PublishBatch(t.Context(), arn, []PublishEntry{{ID: "a", Message: "one"}, {ID: "b", Message: ""}, {ID: "c", Message: "three"}}, "http://h")
	if err != nil || len(res) != 3 {
		t.Fatalf("PublishBatch = %v, %v; want 3 results", res, err)
	}
	if res[0].ID != "a" || !uuidRE.MatchString(res[0].MessageID) || res[0].Err != nil {
		t.Errorf("result a = %+v, want a MessageId", res[0])
	}
	if res[1].ID != "b" || !errors.Is(res[1].Err, ErrInvalidParameter) {
		t.Errorf("result b = %+v, want %v", res[1], ErrInvalidParameter)
	}
	if res[2].ID != "c" || res[2].Err != nil || len(fq.sent) != 2 {
		t.Errorf("result c = %+v with %d sends, want success and 2 sends", res[2], len(fq.sent))
	}
	if _, err := e.PublishBatch(t.Context(), arn+"x", []PublishEntry{{ID: "a", Message: "m"}}, ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("PublishBatch(missing topic) error = %v, want %v", err, ErrNotFound)
	}
}

// verify checks the envelope signature against the certificate.
func verify(t *testing.T, cert []byte, body string) {
	t.Helper()
	var env struct {
		Type, TopicArn, Subject, Message, Timestamp, SignatureVersion, Signature string
		MessageID                                                                string `json:"MessageId"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	toSign := "Message\n" + env.Message + "\nMessageId\n" + env.MessageID + "\n"
	if env.Subject != "" {
		toSign += "Subject\n" + env.Subject + "\n"
	}
	toSign += "Timestamp\n" + env.Timestamp + "\nTopicArn\n" + env.TopicArn + "\nType\nNotification\n"
	verifySignature(t, cert, env.SignatureVersion, toSign, env.Signature)
}

// verifySignature checks that signature signs toSign under the certificate.
func verifySignature(t *testing.T, cert []byte, version, toSign, signature string) {
	t.Helper()
	block, _ := pem.Decode(cert)
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		t.Fatal(err)
	}
	hash, sum := crypto.SHA1, sha1.Sum([]byte(toSign))
	digest := sum[:]
	if version == "2" {
		s := sha256.Sum256([]byte(toSign))
		hash, digest = crypto.SHA256, s[:]
	}
	pub, ok := c.PublicKey.(*rsa.PublicKey)
	if !ok {
		t.Fatal("certificate key is not RSA")
	}
	if err := rsa.VerifyPKCS1v15(pub, hash, digest, sig); err != nil {
		t.Errorf("signature of version %s does not verify: %v\n%s", version, err, toSign)
	}
}

func TestSignature(t *testing.T) {
	for _, version := range []string{"", "1", "2"} {
		for _, subject := range []string{"", "greeting"} {
			t.Run("version "+version+" subject "+subject, func(t *testing.T) {
				e, fq := newEngine(t)
				attrs := map[string]string{}
				if version != "" {
					attrs["SignatureVersion"] = version
				}
				arn, err := e.CreateTopic(t.Context(), "signed", attrs, nil)
				if err != nil {
					t.Fatal(err)
				}
				mustSubscribe(t, e, arn, "q1", nil)
				in := publishInput(arn)
				in.Subject = subject
				if _, err := e.Publish(t.Context(), in); err != nil {
					t.Fatal(err)
				}
				cert, err := e.CertPEM(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				verify(t, cert, fq.sent[0].In.Body)
				want := cmpVersion(version)
				if !strings.Contains(fq.sent[0].In.Body, `"SignatureVersion":"`+want+`"`) {
					t.Errorf("envelope %s lacks SignatureVersion %s", fq.sent[0].In.Body, want)
				}
			})
		}
	}
}

func cmpVersion(v string) string {
	if v == "" {
		return "1"
	}
	return v
}

func TestSigningKeyReusedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	fq := &fakeQueues{}
	e := openEngine(t, dir, fq)
	arn := mustTopic(t, e, "reuse")
	mustSubscribe(t, e, arn, "q1", nil)
	if _, err := e.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatal(err)
	}
	again := openEngine(t, dir, fq)
	if _, err := again.Publish(t.Context(), publishInput(arn)); err != nil {
		t.Fatal(err)
	}
	cert, _ := again.CertPEM(t.Context())
	for _, s := range fq.sent {
		verify(t, cert, s.In.Body)
	}
}

func TestOpenRejectsBadSigningMaterial(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, &fakeQueues{})
	if _, err := e.CertPEM(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := vfs.WriteFile(e.fs, signingKeyFile, []byte("not pem")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), e.fs, testRegion, &fakeQueues{}); err == nil {
		t.Error("Open with a bad signing key = nil error, want a failure")
	}
}

func TestPublishFilterPolicy(t *testing.T) {
	str := func(v string) map[string]MessageAttribute {
		return map[string]MessageAttribute{"color": {DataType: "String", StringValue: v}}
	}
	tests := []struct {
		name   string
		attrs  map[string]string
		modify func(*PublishInput)
		want   bool
	}{
		{"attribute match", map[string]string{"FilterPolicy": `{"color":["blue"]}`}, func(in *PublishInput) { in.Attributes = str("blue") }, true},
		{"attribute miss", map[string]string{"FilterPolicy": `{"color":["blue"]}`}, func(in *PublishInput) { in.Attributes = str("red") }, false},
		{"attribute policy ignores the body", map[string]string{"FilterPolicy": `{"color":["blue"]}`}, func(in *PublishInput) { in.Message = `{"color":"blue"}` }, false},
		{"body match", map[string]string{"FilterPolicy": `{"o":{"k":["book"]}}`, "FilterPolicyScope": "MessageBody"}, func(in *PublishInput) { in.Message = `{"o":{"k":"book"}}` }, true},
		{"body miss", map[string]string{"FilterPolicy": `{"o":{"k":["book"]}}`, "FilterPolicyScope": "MessageBody"}, func(in *PublishInput) { in.Message = `{"o":{"k":"pen"}}` }, false},
		{"body not JSON", map[string]string{"FilterPolicy": `{"o":{"k":["book"]}}`, "FilterPolicyScope": "MessageBody"}, func(in *PublishInput) { in.Message = "plain" }, false},
		{"body policy sees the SQS message", map[string]string{"FilterPolicy": `{"k":["book"]}`, "FilterPolicyScope": "MessageBody"}, func(in *PublishInput) {
			in.MessageStructure = "json"
			in.Message = `{"default":"plain","sqs":"{\"k\":\"book\"}"}`
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, fq := newEngine(t)
			arn := mustTopic(t, e, "filtered")
			mustSubscribe(t, e, arn, "filtered", tt.attrs)
			mustSubscribe(t, e, arn, "everything", nil)
			in := publishInput(arn)
			tt.modify(&in)
			if _, err := e.Publish(t.Context(), in); err != nil {
				t.Fatalf("Publish error = %v", err)
			}
			var got []string
			for _, s := range fq.sent {
				got = append(got, s.Queue)
			}
			want := []string{"everything"}
			if tt.want {
				want = []string{"filtered", "everything"}
			}
			if !reflect.DeepEqual(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				t.Errorf("Publish(%+v) delivered to %v, want %v", in, got, want)
			}
		})
	}
}

func TestPublishMessageStructurePerProtocol(t *testing.T) {
	tests := []struct {
		name, message, sqs, http, https string
	}{
		{"own keys", `{"default":"d","sqs":"s","http":"h","https":"hs"}`, "s", "h", "hs"},
		{"default fallback", `{"default":"d"}`, "d", "d", "d"},
		{"only http", `{"default":"d","http":"h"}`, "d", "h", "d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e, fq := newEngine(t)
				ep := startEndpoint(t, e, ok)
				stop := runDeliveries(e)
				defer stop()
				arn := mustTopic(t, e, "per-protocol")
				mustSubscribe(t, e, arn, "q1", map[string]string{"RawMessageDelivery": "true"})
				for _, proto := range []string{"http", "https"} {
					// The in-memory server speaks plain HTTP, so an https subscription uses an http URL
					// that its protocol field marks as https.
					sub, _ := subscribeHTTP(t, e, arn, "http://"+proto+".example.com/", map[string]string{"RawMessageDelivery": "true"})
					e.subs[sub].Protocol = proto
					e.subs[sub].Token = ""
					e.subs[sub].Pending = false
				}
				synctest.Wait()
				before := len(ep.got())
				in := publishInput(arn)
				in.MessageStructure, in.Message = "json", tt.message
				if _, err := e.Publish(t.Context(), in); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				got := map[string]string{}
				for _, p := range ep.got()[before:] {
					got[p.header.Get("X-Amz-Sns-Subscription-Arn")] = p.body
				}
				want := map[string]string{}
				for _, s := range e.subs {
					switch s.Protocol {
					case "http":
						want[s.ARN] = tt.http
					case "https":
						want[s.ARN] = tt.https
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("HTTP deliveries = %v, want %v", got, want)
				}
				if len(fq.sent) != 1 || fq.sent[0].In.Body != tt.sqs {
					t.Errorf("SQS deliveries = %v, want one with body %q", fq.sent, tt.sqs)
				}
			})
		})
	}
}
