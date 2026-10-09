package test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// mustQueue creates a queue and returns its URL.
func mustQueue(t *testing.T, c *sqs.Client, name string) string {
	t.Helper()
	out, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("CreateQueue(%q) error = %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// mustReceive receives one message, waiting up to 5 s, and fails on none.
func mustReceive(t *testing.T, c *sqs.Client, in *sqs.ReceiveMessageInput) types.Message {
	t.Helper()
	in.MaxNumberOfMessages = 1
	in.WaitTimeSeconds = 5
	out, err := c.ReceiveMessage(t.Context(), in)
	if err != nil {
		t.Fatalf("ReceiveMessage error = %v", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(out.Messages))
	}
	return out.Messages[0]
}

func TestSQSQueueLifecycle(t *testing.T) {
	for name, start := range map[string]func(*testing.T) *pail{"plain": startPail, "tls": startPailTLS} {
		t.Run(name, func(t *testing.T) {
			c := start(t).sqsClient()
			ctx := t.Context()
			url := mustQueue(t, c, "lifecycle-a")
			mustQueue(t, c, "lifecycle-b")
			mustQueue(t, c, "other")

			got, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("lifecycle-a")})
			if err != nil || aws.ToString(got.QueueUrl) != url {
				t.Errorf("GetQueueUrl = %v, %v; want %s", got, err, url)
			}
			list, err := c.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("lifecycle-")})
			if err != nil || len(list.QueueUrls) != 2 {
				t.Errorf("ListQueues prefix = %v, %v; want 2 URLs", list, err)
			}
			page, err := c.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int32(2)})
			if err != nil || len(page.QueueUrls) != 2 || page.NextToken == nil {
				t.Fatalf("ListQueues page = %v, %v; want 2 URLs and a token", page, err)
			}
			rest, err := c.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int32(2), NextToken: page.NextToken})
			if err != nil || len(rest.QueueUrls) != 1 || rest.NextToken != nil {
				t.Errorf("ListQueues second page = %v, %v; want 1 URL and no token", rest, err)
			}

			attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
			if err != nil {
				t.Fatal(err)
			}
			wantAttrs := map[string]string{
				"VisibilityTimeout": "30", "DelaySeconds": "0", "MaximumMessageSize": "1048576", "MessageRetentionPeriod": "345600",
				"ReceiveMessageWaitTimeSeconds": "0", "QueueArn": "arn:aws:sqs:us-east-1:000000000000:lifecycle-a",
				"ApproximateNumberOfMessages": "0",
			}
			for k, v := range wantAttrs {
				if attrs.Attributes[k] != v {
					t.Errorf("attribute %s = %q, want %q", k, attrs.Attributes[k], v)
				}
			}
			if _, err := c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: &url, Attributes: map[string]string{"VisibilityTimeout": "60"}}); err != nil {
				t.Fatal(err)
			}
			attrs, err = c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []types.QueueAttributeName{"VisibilityTimeout"}})
			if err != nil || attrs.Attributes["VisibilityTimeout"] != "60" {
				t.Errorf("VisibilityTimeout after SetQueueAttributes = %v, %v; want 60", attrs, err)
			}

			if _, err := c.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: &url}); err != nil {
				t.Fatal(err)
			}
			_, err = c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("lifecycle-a")})
			if _, ok := errors.AsType[*types.QueueDoesNotExist](err); !ok {
				t.Errorf("GetQueueUrl after DeleteQueue error = %v, want *types.QueueDoesNotExist", err)
			}
		})
	}
}

func TestSQSMessageAttributes(t *testing.T) {
	c := startPail(t).sqsClient()
	url := mustQueue(t, c, "attributes")
	blob := []byte{1, 2, 3}
	sent, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:    &url,
		MessageBody: aws.String("hello"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
			"count": {DataType: aws.String("Number"), StringValue: aws.String("3")},
			"blob":  {DataType: aws.String("Binary"), BinaryValue: blob},
		},
	})
	if err != nil || sent.MD5OfMessageAttributes == nil {
		t.Fatalf("SendMessage = %v, %v; want an attribute MD5", sent, err)
	}
	m := mustReceive(t, c, &sqs.ReceiveMessageInput{
		QueueUrl:                    &url,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if aws.ToString(m.Body) != "hello" || aws.ToString(m.MessageId) != aws.ToString(sent.MessageId) {
		t.Errorf("received %q (%s), want hello (%s)", aws.ToString(m.Body), aws.ToString(m.MessageId), aws.ToString(sent.MessageId))
	}
	if aws.ToString(m.MD5OfMessageAttributes) != aws.ToString(sent.MD5OfMessageAttributes) {
		t.Errorf("received attribute MD5 = %s, sent %s", aws.ToString(m.MD5OfMessageAttributes), aws.ToString(sent.MD5OfMessageAttributes))
	}
	if got := m.MessageAttributes["color"]; aws.ToString(got.StringValue) != "blue" || aws.ToString(got.DataType) != "String" {
		t.Errorf("color = %+v, want String blue", got)
	}
	if got := m.MessageAttributes["count"]; aws.ToString(got.StringValue) != "3" || aws.ToString(got.DataType) != "Number" {
		t.Errorf("count = %+v, want Number 3", got)
	}
	if got := m.MessageAttributes["blob"]; !slices.Equal(got.BinaryValue, blob) || aws.ToString(got.DataType) != "Binary" {
		t.Errorf("blob = %+v, want Binary %v", got, blob)
	}
	if m.Attributes["ApproximateReceiveCount"] != "1" || m.Attributes["SenderId"] != testKey || m.Attributes["SentTimestamp"] == "" {
		t.Errorf("system attributes = %v, want count 1, the sender, and a timestamp", m.Attributes)
	}

	// The attribute MD5 covers the returned attributes only.
	if _, err := c.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{QueueUrl: &url, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	m = mustReceive(t, c, &sqs.ReceiveMessageInput{QueueUrl: &url, MessageAttributeNames: []string{"color"}})
	if len(m.MessageAttributes) != 1 || m.MessageAttributes["color"].StringValue == nil {
		t.Errorf("filtered attributes = %v, want color only", m.MessageAttributes)
	}
	if _, err := c.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: &url, ReceiptHandle: m.ReceiptHandle}); err != nil {
		t.Errorf("DeleteMessage error = %v", err)
	}
}

func TestSQSVisibility(t *testing.T) {
	c := startPail(t).sqsClient()
	ctx := t.Context()
	url := mustQueue(t, c, "visibility")
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &url, MessageBody: aws.String("m")}); err != nil {
		t.Fatal(err)
	}
	first := mustReceive(t, c, &sqs.ReceiveMessageInput{QueueUrl: &url, VisibilityTimeout: 30})
	out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &url})
	if err != nil || len(out.Messages) != 0 {
		t.Fatalf("receive while in flight = %v, %v; want no messages", out, err)
	}
	if _, err := c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: &url, ReceiptHandle: first.ReceiptHandle, VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	second := mustReceive(t, c, &sqs.ReceiveMessageInput{QueueUrl: &url, MessageSystemAttributeNames: []types.MessageSystemAttributeName{"ApproximateReceiveCount"}})
	if second.Attributes["ApproximateReceiveCount"] != "2" {
		t.Errorf("ApproximateReceiveCount = %q, want 2", second.Attributes["ApproximateReceiveCount"])
	}
	// The first handle is stale: it deletes nothing, and the message stays in flight.
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &url, ReceiptHandle: first.ReceiptHandle}); err != nil {
		t.Errorf("DeleteMessage with a stale handle error = %v, want success", err)
	}
	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessagesNotVisible"}})
	if err != nil || attrs.Attributes["ApproximateNumberOfMessagesNotVisible"] != "1" {
		t.Errorf("in flight after a stale delete = %v, %v; want 1", attrs, err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &url, ReceiptHandle: second.ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	attrs, err = c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &url, AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessagesNotVisible"}})
	if err != nil || attrs.Attributes["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Errorf("in flight after delete = %v, %v; want 0", attrs, err)
	}
	_, err = c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: &url, ReceiptHandle: aws.String("garbage"), VisibilityTimeout: 5})
	if _, ok := errors.AsType[*types.ReceiptHandleIsInvalid](err); !ok {
		t.Errorf("ChangeMessageVisibility with a garbage handle error = %v, want *types.ReceiptHandleIsInvalid", err)
	}
}

func TestSQSLongPoll(t *testing.T) {
	c := startPail(t).sqsClient()
	url := mustQueue(t, c, "longpoll")
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: &url, MessageBody: aws.String("late")}); err != nil {
			t.Errorf("SendMessage error = %v", err)
		}
	})
	start := time.Now()
	m := mustReceive(t, c, &sqs.ReceiveMessageInput{QueueUrl: &url}) // waits up to 5 s
	wg.Wait()
	if aws.ToString(m.Body) != "late" || time.Since(start) > 3*time.Second {
		t.Errorf("long poll got %q after %v, want late within 3 s", aws.ToString(m.Body), time.Since(start))
	}
}

func TestSQSBatches(t *testing.T) {
	c := startPail(t).sqsClient()
	ctx := t.Context()
	url := mustQueue(t, c, "batches")

	sent, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &url, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("ok"), MessageBody: aws.String("fine")},
		{Id: aws.String("bad"), MessageBody: aws.String("\x00")},
		{Id: aws.String("also-ok"), MessageBody: aws.String("fine too")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.Successful) != 2 || len(sent.Failed) != 1 || aws.ToString(sent.Failed[0].Id) != "bad" || !sent.Failed[0].SenderFault || aws.ToString(sent.Failed[0].Code) == "" {
		t.Fatalf("SendMessageBatch = %+v, %+v; want 2 successes and bad failed", sent.Successful, sent.Failed)
	}

	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &url, MaxNumberOfMessages: 10, WaitTimeSeconds: 2})
	if err != nil || len(recv.Messages) != 2 {
		t.Fatalf("ReceiveMessage = %v, %v; want 2 messages", recv, err)
	}
	var visibility []types.ChangeMessageVisibilityBatchRequestEntry
	for i, m := range recv.Messages {
		visibility = append(visibility, types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String("v" + strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0})
	}
	visibility = append(visibility, types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String("garbage"), ReceiptHandle: aws.String("garbage"), VisibilityTimeout: 0})
	vis, err := c.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: &url, Entries: visibility})
	if err != nil || len(vis.Successful) != 2 || len(vis.Failed) != 1 || aws.ToString(vis.Failed[0].Code) != "ReceiptHandleIsInvalid" {
		t.Fatalf("ChangeMessageVisibilityBatch = %+v, %+v, %v; want 2 successes and garbage failed", vis.Successful, vis.Failed, err)
	}

	recv, err = c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &url, MaxNumberOfMessages: 10, WaitTimeSeconds: 2})
	if err != nil || len(recv.Messages) != 2 {
		t.Fatalf("second ReceiveMessage = %v, %v; want 2 messages", recv, err)
	}
	var deletes []types.DeleteMessageBatchRequestEntry
	for i, m := range recv.Messages {
		deletes = append(deletes, types.DeleteMessageBatchRequestEntry{Id: aws.String("d" + strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle})
	}
	del, err := c.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: &url, Entries: deletes})
	if err != nil || len(del.Successful) != 2 || len(del.Failed) != 0 {
		t.Errorf("DeleteMessageBatch = %+v, %+v, %v; want 2 successes", del.Successful, del.Failed, err)
	}
}

func TestSQSErrors(t *testing.T) {
	c := startPail(t).sqsClient()
	ctx := t.Context()
	url := mustQueue(t, c, "errors")
	entries := func(n int) []types.SendMessageBatchRequestEntry {
		var out []types.SendMessageBatchRequestEntry
		for i := range n {
			out = append(out, types.SendMessageBatchRequestEntry{Id: aws.String("e" + strconv.Itoa(i)), MessageBody: aws.String("b")})
		}
		return out
	}
	batch := func(e []types.SendMessageBatchRequestEntry) error {
		_, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &url, Entries: e})
		return err
	}

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("errors"), Attributes: map[string]string{"VisibilityTimeout": "99"}})
	if _, ok := errors.AsType[*types.QueueNameExists](err); !ok {
		t.Errorf("CreateQueue with other attributes error = %v, want *types.QueueNameExists", err)
	}
	_, err = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &url, ReceiptHandle: aws.String("garbage")})
	if _, ok := errors.AsType[*types.ReceiptHandleIsInvalid](err); !ok {
		t.Errorf("DeleteMessage with garbage error = %v, want *types.ReceiptHandleIsInvalid", err)
	}
	if _, ok := errors.AsType[*types.EmptyBatchRequest](batch([]types.SendMessageBatchRequestEntry{})); !ok {
		t.Error("SendMessageBatch with no entries: want *types.EmptyBatchRequest")
	}
	if _, ok := errors.AsType[*types.TooManyEntriesInBatchRequest](batch(entries(11))); !ok {
		t.Error("SendMessageBatch with 11 entries: want *types.TooManyEntriesInBatchRequest")
	}
	dup := entries(2)
	dup[1].Id = dup[0].Id
	if _, ok := errors.AsType[*types.BatchEntryIdsNotDistinct](batch(dup)); !ok {
		t.Error("SendMessageBatch with duplicate Ids: want *types.BatchEntryIdsNotDistinct")
	}
	bad := entries(1)
	bad[0].Id = aws.String("bad id")
	if _, ok := errors.AsType[*types.InvalidBatchEntryId](batch(bad)); !ok {
		t.Error("SendMessageBatch with a bad Id: want *types.InvalidBatchEntryId")
	}
	long := entries(2)
	long[0].MessageBody = aws.String(strings.Repeat("x", 600000))
	long[1].MessageBody = long[0].MessageBody
	if _, ok := errors.AsType[*types.BatchRequestTooLong](batch(long)); !ok {
		t.Error("SendMessageBatch over 1 MiB: want *types.BatchRequestTooLong")
	}
	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &url, MessageBody: aws.String("\x00")})
	if _, ok := errors.AsType[*types.InvalidMessageContents](err); !ok {
		t.Errorf("SendMessage with a NUL error = %v, want *types.InvalidMessageContents", err)
	}

	if _, err := c.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &url}); err != nil {
		t.Fatal(err)
	}
	_, err = c.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &url})
	if _, ok := errors.AsType[*types.PurgeQueueInProgress](err); !ok {
		t.Errorf("second PurgeQueue error = %v, want *types.PurgeQueueInProgress", err)
	}

	_, err = c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("missing")})
	_, ok := errors.AsType[*types.QueueDoesNotExist](err)
	if !ok {
		t.Fatalf("GetQueueUrl of a missing queue error = %v, want *types.QueueDoesNotExist", err)
	}
	if status, code := apiFailure(t, err); status != 400 || code != "AWS.SimpleQueueService.NonExistentQueue" {
		t.Errorf("missing queue: status %d, code %q, want 400 AWS.SimpleQueueService.NonExistentQueue", status, code)
	}
}

func TestSQSTags(t *testing.T) {
	c := startPail(t).sqsClient()
	ctx := t.Context()
	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tags"), Tags: map[string]string{"env": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	url := out.QueueUrl
	if _, err := c.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: url, Tags: map[string]string{"team": "a", "tier": "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagQueue(ctx, &sqs.UntagQueueInput{QueueUrl: url, TagKeys: []string{"tier"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: url})
	if err != nil || len(tags.Tags) != 2 || tags.Tags["env"] != "test" || tags.Tags["team"] != "a" {
		t.Errorf("ListQueueTags = %v, %v; want env and team", tags, err)
	}
}

func TestSQSQueueURLHost(t *testing.T) {
	p := startPail(t)
	url := mustQueue(t, p.sqsClient(), "hosts")
	if want := "http://localhost:" + p.port + "/000000000000/hosts"; url != want {
		t.Errorf("queue URL = %q, want %q", url, want)
	}
	// The URL from localhost works through a client that connects as 127.0.0.1.
	other := p.sqsClient(func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://127.0.0.1:" + p.port) })
	if _, err := other.SendMessage(t.Context(), &sqs.SendMessageInput{QueueUrl: &url, MessageBody: aws.String("hi")}); err != nil {
		t.Errorf("SendMessage with a localhost URL through 127.0.0.1 error = %v", err)
	}
	got, err := other.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("hosts")})
	if want := "http://127.0.0.1:" + p.port + "/000000000000/hosts"; err != nil || aws.ToString(got.QueueUrl) != want {
		t.Errorf("GetQueueUrl via 127.0.0.1 = %v, %v; want %s", got, err, want)
	}
}
