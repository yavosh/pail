package test

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestSNSFIFOEndToEnd(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw=%t", raw), func(t *testing.T) {
			p := startPail(t)
			sc, qc := p.snsClient(), p.sqsClient()
			ctx := t.Context()
			topic, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("orders.fifo"), Attributes: map[string]string{"FifoTopic": "true"}})
			if err != nil {
				t.Fatal(err)
			}
			urls := map[string]*string{}
			for _, name := range []string{"orders.fifo", "audit"} {
				attrs := map[string]string{}
				if name == "orders.fifo" {
					attrs["FifoQueue"] = "true"
				}
				q, err := qc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
				if err != nil {
					t.Fatal(err)
				}
				urls[name] = q.QueueUrl
				qa, err := qc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
				if err != nil {
					t.Fatal(err)
				}
				_, err = sc.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(qa.Attributes["QueueArn"]), Attributes: map[string]string{"RawMessageDelivery": fmt.Sprint(raw)}})
				if err != nil {
					t.Fatal(err)
				}
			}
			first, err := sc.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("one"), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("one")})
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := sc.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("ignored duplicate"), MessageGroupId: aws.String("other"), MessageDeduplicationId: aws.String("one")})
			if err != nil {
				t.Fatal(err)
			}
			if aws.ToString(duplicate.MessageId) != aws.ToString(first.MessageId) || aws.ToString(duplicate.SequenceNumber) != aws.ToString(first.SequenceNumber) {
				t.Errorf("duplicate = %+v, want original identifiers %+v", duplicate, first)
			}
			_, err = sc.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: topic.TopicArn, PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{
				{Id: aws.String("valid"), Message: aws.String("must not deliver"), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("two")},
				{Id: aws.String("invalid"), Message: aws.String(""), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("invalid")},
			}})
			if got := errorCode(err); got != "InvalidParameter" {
				t.Fatalf("invalid FIFO batch error = %q (%v), want InvalidParameter", got, err)
			}
			batch, err := sc.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: topic.TopicArn, PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{
				{Id: aws.String("two"), Message: aws.String("two"), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("two")},
				{Id: aws.String("three"), Message: aws.String("three"), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("three")},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Successful) != 2 || len(batch.Failed) != 0 {
				t.Fatalf("PublishBatch = %+v, want two successes and no failures", batch)
			}
			sequences := []string{aws.ToString(first.SequenceNumber), aws.ToString(batch.Successful[0].SequenceNumber), aws.ToString(batch.Successful[1].SequenceNumber)}
			previous := new(big.Int)
			for _, seq := range sequences {
				n, ok := new(big.Int).SetString(seq, 10)
				if !ok || n.BitLen() > 128 || n.Cmp(previous) <= 0 {
					t.Fatalf("sequences = %v, want increasing 128-bit integers", sequences)
				}
				previous = n
			}
			for name, url := range urls {
				for i, want := range []string{"one", "two", "three"} {
					got, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 1, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll}})
					if err != nil || len(got.Messages) != 1 {
						t.Fatalf("ReceiveMessage(%s) = %+v, %v; want one message", name, got, err)
					}
					m := got.Messages[0]
					body := aws.ToString(m.Body)
					if !raw {
						var env struct{ Message, SequenceNumber string }
						if err := json.Unmarshal([]byte(body), &env); err != nil {
							t.Fatal(err)
						}
						body = env.Message
						if env.SequenceNumber != sequences[i] {
							t.Errorf("envelope sequence = %q, want %q", env.SequenceNumber, sequences[i])
						}
					}
					if body != want {
						t.Errorf("queue %s message %d = %q, want %q", name, i, body, want)
					}
					if name == "orders.fifo" {
						if m.Attributes["MessageGroupId"] != "g" || m.Attributes["MessageDeduplicationId"] != want {
							t.Errorf("FIFO attributes = %v, want group g and dedup %s", m.Attributes, want)
						}
						blocked, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url})
						if err != nil || len(blocked.Messages) != 0 {
							t.Fatalf("receive with group in flight = %+v, %v; want no messages", blocked, err)
						}
					} else if _, ok := m.Attributes["MessageGroupId"]; ok {
						t.Errorf("standard queue received FIFO group: %v", m.Attributes)
					}
					if _, err := qc.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: url, ReceiptHandle: m.ReceiptHandle}); err != nil {
						t.Fatal(err)
					}
				}
				empty, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url})
				if err != nil || len(empty.Messages) != 0 {
					t.Errorf("queue %s after drain = %+v, %v; want empty", name, empty, err)
				}
			}
		})
	}
}

func TestSNSFIFOContentDeduplication(t *testing.T) {
	for _, scope := range []string{"Topic", "MessageGroup"} {
		t.Run(scope, func(t *testing.T) {
			p := startPail(t)
			sc, qc := p.snsClient(), p.sqsClient()
			ctx := t.Context()
			topic, err := sc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("content.fifo"), Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true", "FifoThroughputScope": scope}})
			if err != nil {
				t.Fatal(err)
			}
			url := mustQueue(t, qc, "content")
			qa, err := qc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = sc.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(qa.Attributes["QueueArn"]), Attributes: map[string]string{"RawMessageDelivery": "true"}})
			if err != nil {
				t.Fatal(err)
			}
			for _, group := range []string{"a", "a", "b"} {
				_, err = sc.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("same body"), MessageGroupId: aws.String(group)})
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := qc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &url, MaxNumberOfMessages: 10})
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if scope == "MessageGroup" {
				want = 2
			}
			if len(got.Messages) != want {
				t.Errorf("scope %s: messages = %d, want %d", scope, len(got.Messages), want)
			}
		})
	}
}

func TestSNSFIFORejectsS3Notifications(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		mustBucket(t, c, "fifo-events")
		topic, err := p.snsClient().CreateTopic(t.Context(), &sns.CreateTopicInput{
			Name: aws.String("events.fifo"), Attributes: map[string]string{"FifoTopic": "true"},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.PutBucketNotificationConfiguration(t.Context(), &s3.PutBucketNotificationConfigurationInput{
			Bucket: aws.String("fifo-events"),
			NotificationConfiguration: &s3types.NotificationConfiguration{TopicConfigurations: []s3types.TopicConfiguration{{
				TopicArn: topic.TopicArn, Events: []s3types.Event{s3types.EventS3ObjectCreated},
			}}},
		})
		if got := errorCode(err); got != "InvalidArgument" {
			t.Errorf("FIFO notification error = %q (%v), want InvalidArgument", got, err)
		}
	})
}
