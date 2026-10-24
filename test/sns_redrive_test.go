package test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestSNSSubscriptionRedrive(t *testing.T) {
	p := startPail(t)
	sc, qc := p.snsClient(), p.sqsClient()
	source, topicARN, subARN := mustSubscribeQueue(t, sc, qc, "redrive", nil)
	dead := mustQueue(t, qc, "dead")
	attrs, err := qc.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: &dead, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"` + attrs.Attributes["QueueArn"] + `"}`
	if _, err := sc.SetSubscriptionAttributes(t.Context(), &sns.SetSubscriptionAttributesInput{SubscriptionArn: &subARN, AttributeName: new("RedrivePolicy"), AttributeValue: &policy}); err != nil {
		t.Fatal(err)
	}
	if _, err := qc.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: &source}); err != nil {
		t.Fatal(err)
	}
	pub, err := sc.Publish(t.Context(), &sns.PublishInput{TopicArn: &topicARN, Message: new("lost"), Subject: new("subject")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := qc.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: &dead})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("dead-letter messages = %d, want 1", len(got.Messages))
	}
	var env snsEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(got.Messages[0].Body)), &env); err != nil {
		t.Fatal(err)
	}
	if env.Message != "lost" || env.Subject != "subject" || env.MessageID != aws.ToString(pub.MessageId) {
		t.Fatalf("dead-letter envelope = %+v, want original publication", env)
	}
	verifyEnvelope(t, p, env)
	if _, err := sc.SetSubscriptionAttributes(t.Context(), &sns.SetSubscriptionAttributesInput{SubscriptionArn: &subARN, AttributeName: new("RedrivePolicy")}); err != nil {
		t.Fatal(err)
	}
	sub, err := sc.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{SubscriptionArn: &subARN})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sub.Attributes["RedrivePolicy"]; ok {
		t.Fatal("RedrivePolicy remains after clearing")
	}
}
