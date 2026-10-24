package test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestSQSReceiveAttemptRetry(t *testing.T) {
	c := startPail(t).sqsClient()
	q, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String("receive-retry.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: q.QueueUrl, MessageBody: aws.String("hello"), MessageGroupId: aws.String("group"),
	}); err != nil {
		t.Fatal(err)
	}
	in := &sqs.ReceiveMessageInput{
		QueueUrl: q.QueueUrl, ReceiveRequestAttemptId: aws.String("attempt"),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	}
	first, err := c.ReceiveMessage(t.Context(), in)
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("first ReceiveMessage = %v, %v; want one message", first, err)
	}
	retry, err := c.ReceiveMessage(t.Context(), in)
	if err != nil || len(retry.Messages) != 1 {
		t.Fatalf("retry ReceiveMessage = %v, %v; want the same message", retry, err)
	}
	if aws.ToString(retry.Messages[0].MessageId) != aws.ToString(first.Messages[0].MessageId) || retry.Messages[0].Attributes["ApproximateReceiveCount"] != "1" {
		t.Errorf("retry message = %+v, want first ID and receive count 1", retry.Messages[0])
	}
	if _, err := c.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: retry.Messages[0].ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage with retry handle error = %v", err)
	}
}
