package test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestSQSBooleanAttributeCasing(t *testing.T) {
	for _, tt := range []struct{ yes, no string }{{"True", "False"}, {"TRUE", "FALSE"}, {"tRuE", "fAlSe"}} {
		t.Run(tt.yes, func(t *testing.T) {
			c := startPail(t).sqsClient()
			ctx := t.Context()
			attrs := map[string]string{"FifoQueue": tt.yes, "ContentBasedDeduplication": tt.no, "SqsManagedSseEnabled": tt.no}
			q, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("booleans.fifo"), Attributes: attrs})
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range attrs {
				attrs[k] = strings.ToLower(v)
			}
			if _, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("booleans.fifo"), Attributes: attrs}); err != nil {
				t.Fatalf("CreateQueue with lowercase attributes: %v", err)
			}
			got, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll}})
			if err != nil {
				t.Fatal(err)
			}
			for k, want := range attrs {
				if got.Attributes[k] != want {
					t.Errorf("attribute %s = %q, want %q", k, got.Attributes[k], want)
				}
			}
			_, err = c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: map[string]string{"ContentBasedDeduplication": tt.yes, "SqsManagedSseEnabled": tt.yes}})
			if err != nil {
				t.Fatal(err)
			}
			got, err = c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll}})
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"ContentBasedDeduplication", "SqsManagedSseEnabled"} {
				if got.Attributes[key] != "true" {
					t.Errorf("attribute %s after set = %q, want true", key, got.Attributes[key])
				}
			}
			for range 2 {
				_, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("body"), MessageGroupId: aws.String("g")})
				if err != nil {
					t.Fatalf("SendMessage with content deduplication %q: %v", tt.yes, err)
				}
			}
			messages, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10})
			if err != nil || len(messages.Messages) != 1 {
				t.Fatalf("ReceiveMessage = %+v, %v; want one deduplicated message", messages, err)
			}
			_, err = c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: map[string]string{"ContentBasedDeduplication": tt.no}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("other"), MessageGroupId: aws.String("g")})
			if got := errorCode(err); got != "InvalidParameterValue" {
				t.Errorf("SendMessage with content deduplication %q error = %q, want InvalidParameterValue", tt.no, got)
			}
			for _, key := range []string{"ContentBasedDeduplication", "SqsManagedSseEnabled"} {
				for _, value := range []string{"", "1", "0", "yes", " true", "false ", "falſe"} {
					_, err := c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: map[string]string{key: value}})
					if got := errorCode(err); got != "InvalidAttributeValue" {
						t.Errorf("SetQueueAttributes(%s=%q) error = %q, want InvalidAttributeValue", key, value, got)
					}
				}
			}
		})
	}
}

func TestSNSBooleanAttributeCasing(t *testing.T) {
	for _, tt := range []struct{ yes, no string }{{"True", "False"}, {"TRUE", "FALSE"}, {"tRuE", "fAlSe"}} {
		t.Run(tt.yes, func(t *testing.T) {
			p := startPail(t)
			sc, qc := p.snsClient(), p.sqsClient()
			ctx := t.Context()
			attrs := map[string]string{"FifoTopic": tt.yes, "ContentBasedDeduplication": tt.no}
			topic, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("booleans.fifo"), Attributes: attrs})
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range attrs {
				attrs[k] = strings.ToLower(v)
			}
			if _, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("booleans.fifo"), Attributes: attrs}); err != nil {
				t.Fatalf("CreateTopic with lowercase attributes: %v", err)
			}
			got, err := sc.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn})
			if err != nil {
				t.Fatal(err)
			}
			for k, want := range attrs {
				if got.Attributes[k] != want {
					t.Errorf("attribute %s = %q, want %q", k, got.Attributes[k], want)
				}
			}
			url := mustQueue(t, qc, "booleans")
			qa, err := qc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
			if err != nil {
				t.Fatal(err)
			}
			subscribe := &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(qa.Attributes["QueueArn"]), Attributes: map[string]string{"RawMessageDelivery": tt.no}}
			sub, err := sc.Subscribe(ctx, subscribe)
			if err != nil {
				t.Fatal(err)
			}
			subscribe.Attributes["RawMessageDelivery"] = "false"
			if _, err := sc.Subscribe(ctx, subscribe); err != nil {
				t.Fatalf("Subscribe with lowercase attributes: %v", err)
			}
			subAttrs, err := sc.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn})
			if err != nil {
				t.Fatal(err)
			}
			if got := subAttrs.Attributes["RawMessageDelivery"]; got != "false" {
				t.Errorf("RawMessageDelivery after subscribe = %q, want false", got)
			}
			for _, key := range []string{"FifoTopic", "ContentBasedDeduplication"} {
				_, err := sc.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: topic.TopicArn, AttributeName: &key, AttributeValue: &tt.yes})
				if err != nil {
					t.Fatalf("SetTopicAttributes(%s=%q): %v", key, tt.yes, err)
				}
			}
			_, err = sc.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("RawMessageDelivery"), AttributeValue: &tt.yes})
			if err != nil {
				t.Fatal(err)
			}
			subAttrs, err = sc.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn})
			if err != nil {
				t.Fatal(err)
			}
			if got := subAttrs.Attributes["RawMessageDelivery"]; got != "true" {
				t.Errorf("RawMessageDelivery after set = %q, want true", got)
			}
			for range 2 {
				_, err := sc.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("body"), MessageGroupId: aws.String("g")})
				if err != nil {
					t.Fatalf("Publish with content deduplication %q: %v", tt.yes, err)
				}
			}
			messages, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &url, MaxNumberOfMessages: 10})
			if err != nil || len(messages.Messages) != 1 {
				t.Fatalf("ReceiveMessage = %+v, %v; want one deduplicated message", messages, err)
			}
			if got := aws.ToString(messages.Messages[0].Body); got != "body" {
				t.Errorf("raw delivery body = %q, want body", got)
			}
			for _, value := range []string{"", "1", "0", "yes", " true", "false ", "falſe"} {
				_, err := sc.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: topic.TopicArn, AttributeName: aws.String("ContentBasedDeduplication"), AttributeValue: &value})
				if got := errorCode(err); got != "InvalidParameter" {
					t.Errorf("SetTopicAttributes(ContentBasedDeduplication=%q) error = %q, want InvalidParameter", value, got)
				}
				_, err = sc.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("RawMessageDelivery"), AttributeValue: &value})
				if got := errorCode(err); got != "InvalidParameter" {
					t.Errorf("SetSubscriptionAttributes(RawMessageDelivery=%q) error = %q, want InvalidParameter", value, got)
				}
			}
			_, err = sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("standard"), Attributes: map[string]string{"FifoTopic": tt.no, "ContentBasedDeduplication": tt.no}})
			if got := errorCode(err); got != "InvalidParameter" {
				t.Errorf("CreateTopic with disabled FIFO attributes %q error = %q, want InvalidParameter", tt.no, got)
			}
			_, err = sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("standard"), Attributes: map[string]string{"FifoTopic": "false", "ContentBasedDeduplication": "false"}})
			if err != nil {
				t.Fatalf("CreateTopic with lowercase disabled FIFO attributes: %v", err)
			}
		})
	}
}
