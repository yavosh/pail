package test

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// queueARN returns the ARN of the queue at queueURL.
func queueARN(t *testing.T, qc *sqs.Client, queueURL string) string {
	t.Helper()
	out, err := qc.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: &queueURL, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes error = %v", err)
	}
	return out.Attributes["QueueArn"]
}

// drain receives and deletes the messages in a queue without waiting.
// pail delivers events before it answers, so none is still in flight.
func drain(t *testing.T, qc *sqs.Client, queueURL string) []string {
	t.Helper()
	var bodies []string
	for {
		out, err := qc.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: &queueURL, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatalf("ReceiveMessage error = %v", err)
		}
		if len(out.Messages) == 0 {
			return bodies
		}
		for _, m := range out.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
			if _, err := qc.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: &queueURL, ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatalf("DeleteMessage error = %v", err)
			}
		}
	}
}

// eventRecord is the part of an S3 event that the tests read.
type eventRecord struct {
	EventName string
	S3        struct {
		ConfigurationID string `json:"configurationId"`
		Bucket          struct{ Name, ARN string }
		Object          struct {
			Key  string
			Size int64
			ETag string `json:"eTag"`
		}
	}
}

func oneRecord(t *testing.T, body string) eventRecord {
	t.Helper()
	var e struct{ Records []eventRecord }
	if err := json.Unmarshal([]byte(body), &e); err != nil || len(e.Records) != 1 {
		t.Fatalf("event %q has %d records, %v, want one record", body, len(e.Records), err)
	}
	return e.Records[0]
}

func TestBucketNotifications(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		qc, sc := p.sqsClient(), p.snsClient()
		mustBucket(t, c, "events")
		bucket := aws.String("events")
		createdURL := mustQueue(t, qc, "created")
		removedURL, topicARN, _ := mustSubscribeQueue(t, sc, qc, "removed", nil)
		prefix := &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{FilterRules: []types.FilterRule{{Name: types.FilterRuleNamePrefix, Value: aws.String("in/")}}}}
		put := func(queueARN string) error {
			_, err := c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
				Bucket: bucket,
				NotificationConfiguration: &types.NotificationConfiguration{
					QueueConfigurations: []types.QueueConfiguration{{
						Id: aws.String("created"), QueueArn: aws.String(queueARN), Filter: prefix,
						Events: []types.Event{types.EventS3ObjectCreated},
					}},
					TopicConfigurations: []types.TopicConfiguration{{
						Id: aws.String("removed"), TopicArn: aws.String(topicARN),
						Events: []types.Event{types.EventS3ObjectRemoved},
					}},
				},
			})
			return err
		}

		if err := put(queueARN(t, qc, createdURL) + "-missing"); errorCode(err) != "InvalidArgument" {
			t.Fatalf("PutBucketNotificationConfiguration with a missing queue error = %v, want InvalidArgument", err)
		}
		if err := put(queueARN(t, qc, createdURL)); err != nil {
			t.Fatal(err)
		}
		got, err := c.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil || len(got.QueueConfigurations) != 1 || len(got.TopicConfigurations) != 1 ||
			!strings.EqualFold(string(got.QueueConfigurations[0].Filter.Key.FilterRules[0].Name), "prefix") {
			t.Fatalf("GetBucketNotificationConfiguration = %+v, %v, want one queue and one topic configuration", got, err)
		}

		// Each destination gets a test event.
		for _, url := range []string{createdURL, removedURL} {
			bodies := drain(t, qc, url)
			if len(bodies) != 1 || !strings.Contains(bodies[0], "s3:TestEvent") {
				t.Errorf("messages after the PUT = %q, want one test event", bodies)
			}
		}

		for _, key := range []string{"out/skipped.txt", "in/a b.txt"} {
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String(key), Body: strings.NewReader("hello")}); err != nil {
				t.Fatal(err)
			}
		}
		bodies := drain(t, qc, createdURL)
		if len(bodies) != 1 {
			t.Fatalf("created events = %q, want one for the key under in/", bodies)
		}
		rec := oneRecord(t, bodies[0])
		if rec.EventName != "ObjectCreated:Put" || rec.S3.ConfigurationID != "created" || rec.S3.Bucket.ARN != "arn:aws:s3:::events" ||
			rec.S3.Object.Key != "in/a+b.txt" || rec.S3.Object.Size != 5 || rec.S3.Object.ETag != "5d41402abc4b2a76b9719d911017c592" {
			t.Errorf("created event = %+v, want ObjectCreated:Put for in/a+b.txt", rec)
		}
		if bodies := drain(t, qc, removedURL); len(bodies) != 0 {
			t.Errorf("removed queue got %q after creations, want nothing", bodies)
		}

		if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: aws.String("in/a b.txt")}); err != nil {
			t.Fatal(err)
		}
		bodies = drain(t, qc, removedURL)
		if len(bodies) != 1 {
			t.Fatalf("removed events = %q, want one", bodies)
		}
		// The topic is not raw, so the event is inside an SNS envelope.
		var env snsEnvelope
		if err := json.Unmarshal([]byte(bodies[0]), &env); err != nil {
			t.Fatal(err)
		}
		rec = oneRecord(t, env.Message)
		if env.Subject != "Amazon S3 Notification" || env.TopicArn != topicARN || rec.EventName != "ObjectRemoved:Delete" || rec.S3.Object.Key != "in/a+b.txt" {
			t.Errorf("removed envelope = %+v, record %+v, want ObjectRemoved:Delete for in/a+b.txt", env, rec)
		}

		// DeleteObjects raises one event per key.
		if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("out/skipped.txt")}, {Key: aws.String("gone")}}}}); err != nil {
			t.Fatal(err)
		}
		if bodies := drain(t, qc, removedURL); len(bodies) != 2 {
			t.Errorf("removed events after DeleteObjects = %d, want 2", len(bodies))
		}

		// An empty configuration clears the notifications.
		if _, err := c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: bucket, NotificationConfiguration: &types.NotificationConfiguration{}}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("in/b"), Body: strings.NewReader("x")}); err != nil {
			t.Fatal(err)
		}
		if bodies := drain(t, qc, createdURL); len(bodies) != 0 {
			t.Errorf("events after clearing = %q, want none", bodies)
		}
	})
}

func TestBucketNotificationCopyAndMultipart(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		qc := p.sqsClient()
		mustBucket(t, c, "events")
		bucket := aws.String("events")
		queueURL := mustQueue(t, qc, "all")
		if _, err := c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
			Bucket: bucket,
			NotificationConfiguration: &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
				QueueArn: aws.String(queueARN(t, qc, queueURL)), Events: []types.Event{types.EventS3ObjectCreated},
			}}},
		}); err != nil {
			t.Fatal(err)
		}
		drain(t, qc, queueURL)

		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("src"), Body: strings.NewReader("hello")}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: aws.String("dst"), CopySource: aws.String("events/src")}); err != nil {
			t.Fatal(err)
		}
		upload := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("big")})
		parts := uploadParts(t, c, "events", "big", aws.ToString(upload.UploadId), []byte("part"))
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: aws.String("big"), UploadId: upload.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}); err != nil {
			t.Fatal(err)
		}
		post, err := s3.NewPresignClient(c).PresignPostObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("form")})
		if err != nil {
			t.Fatal(err)
		}
		if status, _, body := sendForm(t, p, post.URL, maps.Clone(post.Values), "form bytes", false); status != 204 {
			t.Fatalf("POST status = %d, want 204: %s", status, body)
		}

		var names []string
		for _, body := range drain(t, qc, queueURL) {
			names = append(names, oneRecord(t, body).EventName)
		}
		want := "ObjectCreated:Put,ObjectCreated:Copy,ObjectCreated:CompleteMultipartUpload,ObjectCreated:Post"
		if got := strings.Join(names, ","); got != want {
			t.Errorf("event names = %s, want %s", got, want)
		}
	})
}
