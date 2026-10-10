package test

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// snsEnvelope is the JSON body that an SNS-to-SQS delivery puts in the queue.
type snsEnvelope struct {
	Type, TopicArn, Subject, Message, Timestamp string
	MessageID                                   string `json:"MessageId"`
	SignatureVersion, Signature, SigningCertURL string
	UnsubscribeURL                              string
	MessageAttributes                           map[string]struct{ Type, Value string }
}

// mustSubscribeQueue creates a queue and a topic, subscribes the queue, and
// returns the queue URL, the topic ARN, and the subscription ARN.
func mustSubscribeQueue(t *testing.T, sc *sns.Client, qc *sqs.Client, name string, attrs map[string]string) (queueURL, topicARN, subARN string) {
	t.Helper()
	ctx := t.Context()
	topic, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name)})
	if err != nil {
		t.Fatalf("CreateTopic(%q) error = %v", name, err)
	}
	queueURL = mustQueue(t, qc, name)
	got, err := qc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &queueURL, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes error = %v", err)
	}
	sub, err := sc.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(got.Attributes["QueueArn"]),
		Attributes: attrs, ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	return queueURL, aws.ToString(topic.TopicArn), aws.ToString(sub.SubscriptionArn)
}

// verifyEnvelope fetches the certificate named in the envelope through p and
// checks the SHA-1 signature.
func verifyEnvelope(t *testing.T, p *pail, env snsEnvelope) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, env.SigningCertURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s error = %v", env.SigningCertURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/x-pem-file" {
		t.Fatalf("GET %s = %d %q, want 200 application/x-pem-file", env.SigningCertURL, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	block, _ := pem.Decode(body)
	if block == nil {
		t.Fatalf("certificate body %q is not PEM", body)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	toSign := "Message\n" + env.Message + "\nMessageId\n" + env.MessageID + "\n"
	if env.Subject != "" {
		toSign += "Subject\n" + env.Subject + "\n"
	}
	toSign += "Timestamp\n" + env.Timestamp + "\nTopicArn\n" + env.TopicArn + "\nType\nNotification\n"
	sig, err := base64.StdEncoding.DecodeString(env.Signature)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha1.Sum([]byte(toSign))
	pub, _ := cert.PublicKey.(*rsa.PublicKey)
	if pub == nil {
		t.Fatal("certificate key is not RSA")
	}
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA1, sum[:], sig); err != nil {
		t.Errorf("signature does not verify: %v\n%s", err, toSign)
	}
}

func TestSNSDelivery(t *testing.T) {
	for name, start := range map[string]func(*testing.T) *pail{"plain": startPail, "tls": startPailTLS} {
		t.Run(name, func(t *testing.T) {
			p := start(t)
			sc, qc := p.snsClient(), p.sqsClient()
			ctx := t.Context()
			queueURL, topicARN, subARN := mustSubscribeQueue(t, sc, qc, "delivery", nil)

			out, err := sc.Publish(ctx, &sns.PublishInput{
				TopicArn: &topicARN, Message: aws.String("hello"), Subject: aws.String("greeting"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
					"count": {DataType: aws.String("Number"), StringValue: aws.String("3")},
					"blob":  {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2, 3}},
				},
			})
			if err != nil {
				t.Fatalf("Publish error = %v", err)
			}
			msg := mustReceive(t, qc, &sqs.ReceiveMessageInput{QueueUrl: &queueURL})
			var env snsEnvelope
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &env); err != nil {
				t.Fatalf("body %q is not JSON: %v", aws.ToString(msg.Body), err)
			}
			if env.Type != "Notification" || env.MessageID != aws.ToString(out.MessageId) || env.TopicArn != topicARN || env.Subject != "greeting" || env.Message != "hello" || env.SignatureVersion != "1" {
				t.Errorf("envelope = %+v, want Notification %s on %s", env, aws.ToString(out.MessageId), topicARN)
			}
			if got := env.MessageAttributes["blob"]; got.Type != "Binary" || got.Value != "AQID" {
				t.Errorf("blob attribute = %+v, want Binary AQID", got)
			}
			if got := env.MessageAttributes["count"]; got.Type != "Number" || got.Value != "3" {
				t.Errorf("count attribute = %+v, want Number 3", got)
			}
			if want := p.endpoint + "/_pail/sns/signing-cert.pem"; env.SigningCertURL != want {
				t.Errorf("SigningCertURL = %q, want %q", env.SigningCertURL, want)
			}
			verifyEnvelope(t, p, env)
			if _, err := qc.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &queueURL, ReceiptHandle: msg.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}

			// Raw delivery.
			if _, err := sc.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
				SubscriptionArn: &subARN, AttributeName: aws.String("RawMessageDelivery"), AttributeValue: aws.String("true"),
			}); err != nil {
				t.Fatalf("SetSubscriptionAttributes error = %v", err)
			}
			subAttrs, err := sc.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: &subARN})
			if err != nil || subAttrs.Attributes["RawMessageDelivery"] != "true" || subAttrs.Attributes["TopicArn"] != topicARN {
				t.Errorf("GetSubscriptionAttributes = %v, %v; want RawMessageDelivery true", subAttrs, err)
			}
			if _, err := sc.Publish(ctx, &sns.PublishInput{
				TopicArn: &topicARN, Message: aws.String("raw"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
					"tags":  {DataType: aws.String("String.Array"), StringValue: aws.String(`["a","b"]`)},
				},
			}); err != nil {
				t.Fatalf("Publish raw error = %v", err)
			}
			raw := mustReceive(t, qc, &sqs.ReceiveMessageInput{QueueUrl: &queueURL, MessageAttributeNames: []string{"All"}})
			if aws.ToString(raw.Body) != "raw" || aws.ToString(raw.MessageAttributes["color"].StringValue) != "blue" ||
				aws.ToString(raw.MessageAttributes["tags"].DataType) != "String.Array" || aws.ToString(raw.MessageAttributes["tags"].StringValue) != `["a","b"]` {
				t.Errorf("raw message = %q %v, want body raw with color and tags", aws.ToString(raw.Body), raw.MessageAttributes)
			}
			if _, err := qc.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &queueURL, ReceiptHandle: raw.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}

			// A JSON message structure delivers the "sqs" value.
			if _, err := sc.Publish(ctx, &sns.PublishInput{
				TopicArn: &topicARN, MessageStructure: aws.String("json"), Message: aws.String(`{"default":"d","sqs":"s"}`),
			}); err != nil {
				t.Fatalf("Publish json error = %v", err)
			}
			if got := mustReceive(t, qc, &sqs.ReceiveMessageInput{QueueUrl: &queueURL}); aws.ToString(got.Body) != "s" {
				t.Errorf("structured message body = %q, want s", aws.ToString(got.Body))
			}
		})
	}
}

func TestSNSPublishBatch(t *testing.T) {
	p := startPail(t)
	sc, qc := p.snsClient(), p.sqsClient()
	queueURL, topicARN, _ := mustSubscribeQueue(t, sc, qc, "batch", map[string]string{"RawMessageDelivery": "true"})
	out, err := sc.PublishBatch(t.Context(), &sns.PublishBatchInput{TopicArn: &topicARN, PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
		{Id: aws.String("a"), Message: aws.String("one"), MessageAttributes: map[string]types.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
		}},
		{Id: aws.String("b"), Message: aws.String("")},
		{Id: aws.String("c"), Message: aws.String("three")},
	}})
	if err != nil {
		t.Fatalf("PublishBatch error = %v", err)
	}
	if len(out.Successful) != 2 || len(out.Failed) != 1 || aws.ToString(out.Failed[0].Id) != "b" || aws.ToString(out.Failed[0].Code) != "InvalidParameter" || !out.Failed[0].SenderFault {
		t.Errorf("PublishBatch = %+v, want a and c delivered and b failed", out)
	}
	got, err := p.sqsClient().ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: &queueURL, MaxNumberOfMessages: 10, MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for _, m := range got.Messages {
		bodies = append(bodies, aws.ToString(m.Body))
	}
	if !slices.Equal(bodies, []string{"one", "three"}) {
		t.Errorf("queue bodies = %v, want [one three]", bodies)
	}

	_, err = sc.PublishBatch(t.Context(), &sns.PublishBatchInput{TopicArn: &topicARN, PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
		{Id: aws.String("x"), Message: aws.String("m")}, {Id: aws.String("x"), Message: aws.String("m")},
	}})
	if _, ok := errors.AsType[*types.BatchEntryIdsNotDistinctException](err); !ok {
		t.Errorf("PublishBatch with duplicate Ids error = %v, want *types.BatchEntryIdsNotDistinctException", err)
	}
}

func TestSNSTopicsAndSubscriptions(t *testing.T) {
	p := startPail(t)
	sc, qc := p.snsClient(), p.sqsClient()
	ctx := t.Context()
	_, topicARN, subARN := mustSubscribeQueue(t, sc, qc, "admin", nil)

	list, err := sc.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &topicARN})
	if err != nil || len(list.Subscriptions) != 1 || aws.ToString(list.Subscriptions[0].SubscriptionArn) != subARN || aws.ToString(list.Subscriptions[0].Protocol) != "sqs" {
		t.Errorf("ListSubscriptionsByTopic = %v, %v; want the sqs subscription", list, err)
	}
	topics, err := sc.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil || len(topics.Topics) != 1 || aws.ToString(topics.Topics[0].TopicArn) != topicARN {
		t.Errorf("ListTopics = %v, %v; want the topic", topics, err)
	}
	attrs, err := sc.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicARN})
	if err != nil || attrs.Attributes["SubscriptionsConfirmed"] != "1" || attrs.Attributes["Owner"] != "000000000000" || attrs.Attributes["DisplayName"] != "" {
		t.Errorf("GetTopicAttributes = %v, %v", attrs, err)
	}
	if _, err := sc.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: &topicARN, AttributeName: aws.String("DisplayName"), AttributeValue: aws.String("pail")}); err != nil {
		t.Fatalf("SetTopicAttributes error = %v", err)
	}
	if _, err := sc.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &topicARN, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}}}); err != nil {
		t.Fatalf("TagResource error = %v", err)
	}
	tags, err := sc.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicARN})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "env" {
		t.Errorf("ListTagsForResource = %v, %v; want env", tags, err)
	}
	if _, err := sc.UntagResource(ctx, &sns.UntagResourceInput{ResourceArn: &topicARN, TagKeys: []string{"env"}}); err != nil {
		t.Fatalf("UntagResource error = %v", err)
	}

	if _, err := sc.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: &subARN}); err != nil {
		t.Fatalf("Unsubscribe error = %v", err)
	}
	if _, err := sc.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: &topicARN}); err != nil {
		t.Fatalf("DeleteTopic error = %v", err)
	}
}

func TestSNSErrors(t *testing.T) {
	p := startPail(t)
	sc := p.snsClient()
	ctx := t.Context()
	topic, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("errors")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sc.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: topic.TopicArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := sc.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn}); err == nil {
		t.Error("GetTopicAttributes on a deleted topic error = nil, want *types.NotFoundException")
	} else if _, ok := errors.AsType[*types.NotFoundException](err); !ok {
		t.Errorf("GetTopicAttributes on a deleted topic error = %v, want *types.NotFoundException", err)
	}
	if _, err := sc.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("x")}); err == nil {
		t.Error("Publish to a deleted topic error = nil, want *types.NotFoundException")
	} else if _, ok := errors.AsType[*types.NotFoundException](err); !ok {
		t.Errorf("Publish to a deleted topic error = %v, want *types.NotFoundException", err)
	}

	live, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("errors-live")})
	if err != nil {
		t.Fatal(err)
	}
	invalid := map[string]func() error{
		"empty message": func() error {
			_, err := sc.Publish(ctx, &sns.PublishInput{TopicArn: live.TopicArn, Message: aws.String("")})
			return err
		},
		"bad protocol": func() error {
			_, err := sc.Subscribe(ctx, &sns.SubscribeInput{TopicArn: live.TopicArn, Protocol: aws.String("smoke"), Endpoint: aws.String("x")})
			return err
		},
		"FIFO topic name": func() error {
			_, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("t.fifo")})
			return err
		},
	}
	for name, fn := range invalid {
		err := fn()
		if _, ok := errors.AsType[*types.InvalidParameterException](err); !ok {
			t.Errorf("%s: error = %v, want *types.InvalidParameterException", name, err)
		}
	}
}

func TestSNSFilterPolicy(t *testing.T) {
	p := startPail(t)
	sc, qc := p.snsClient(), p.sqsClient()
	ctx := t.Context()
	queueURL, topicARN, subARN := mustSubscribeQueue(t, sc, qc, "filter", map[string]string{
		"RawMessageDelivery": "true", "FilterPolicy": `{"color":["blue"]}`,
	})
	publish := func(message string, color string) {
		t.Helper()
		in := &sns.PublishInput{TopicArn: &topicARN, Message: aws.String(message)}
		if color != "" {
			in.MessageAttributes = map[string]types.MessageAttributeValue{
				"color": {DataType: aws.String("String"), StringValue: aws.String(color)},
			}
		}
		if _, err := sc.Publish(ctx, in); err != nil {
			t.Fatalf("Publish(%q) error = %v", message, err)
		}
	}
	bodies := func() []string {
		t.Helper()
		got, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &queueURL, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatalf("ReceiveMessage error = %v", err)
		}
		var out []string
		for _, m := range got.Messages {
			out = append(out, aws.ToString(m.Body))
		}
		return out
	}

	publish("miss", "red")
	publish("hit", "blue")
	if got := bodies(); !slices.Equal(got, []string{"hit"}) {
		t.Errorf("attribute scope: queue bodies = %v, want [hit]", got)
	}

	for name, value := range map[string]string{"FilterPolicyScope": "MessageBody", "FilterPolicy": `{"order":{"kind":["book"]}}`} {
		if _, err := sc.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
			SubscriptionArn: &subARN, AttributeName: aws.String(name), AttributeValue: aws.String(value),
		}); err != nil {
			t.Fatalf("SetSubscriptionAttributes(%s) error = %v", name, err)
		}
	}
	publish(`{"order":{"kind":"pen"}}`, "blue")
	publish(`{"order":{"kind":"book"}}`, "")
	if got := bodies(); !slices.Equal(got, []string{`{"order":{"kind":"book"}}`}) {
		t.Errorf("body scope: queue bodies = %v, want the book order", got)
	}

	_, err := sc.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: &subARN, AttributeName: aws.String("FilterPolicy"), AttributeValue: aws.String(`{"a":[{"bogus":"x"}]}`),
	})
	if _, ok := errors.AsType[*types.InvalidParameterException](err); !ok {
		t.Errorf("SetSubscriptionAttributes with a bad policy error = %v, want *types.InvalidParameterException", err)
	}
}
