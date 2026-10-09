package test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

func wrongSecret() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: testKey, SecretAccessKey: "wrong"}, nil
	})
}

// apiFailure returns the HTTP status and API error code of err.
func apiFailure(t *testing.T, err error) (int, string) {
	t.Helper()
	respErr, ok := errors.AsType[*awshttp.ResponseError](err)
	if !ok {
		t.Fatalf("error %v: want *awshttp.ResponseError", err)
	}
	apiErr, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		t.Fatalf("error %v: want smithy.APIError", err)
	}
	return respErr.HTTPStatusCode(), apiErr.ErrorCode()
}

func TestSQSStub(t *testing.T) {
	servers := map[string]func(*testing.T) *pail{"plain": startPail, "tls": startPailTLS}
	for name, start := range servers {
		t.Run(name+" list queues is unsupported", func(t *testing.T) {
			_, err := start(t).sqsClient().ListQueues(context.Background(), &sqs.ListQueuesInput{})
			if _, ok := errors.AsType[*types.UnsupportedOperation](err); !ok {
				t.Fatalf("ListQueues error = %v, want *types.UnsupportedOperation", err)
			}
		})
	}

	t.Run("wrong secret", func(t *testing.T) {
		c := startPail(t).sqsClient(func(o *sqs.Options) { o.Credentials = wrongSecret() })
		_, err := c.ListQueues(context.Background(), &sqs.ListQueuesInput{})
		status, code := apiFailure(t, err)
		if status != 403 || code != "SignatureDoesNotMatch" {
			t.Errorf("wrong secret: status %d, code %q, want 403 SignatureDoesNotMatch", status, code)
		}
	})
}

func TestSNSStub(t *testing.T) {
	t.Run("list topics is unsupported", func(t *testing.T) {
		_, err := startPail(t).snsClient().ListTopics(context.Background(), &sns.ListTopicsInput{})
		status, code := apiFailure(t, err)
		if status != 400 || code != "InvalidAction" {
			t.Errorf("ListTopics: status %d, code %q, want 400 InvalidAction", status, code)
		}
	})

	t.Run("wrong secret", func(t *testing.T) {
		c := startPail(t).snsClient(func(o *sns.Options) { o.Credentials = wrongSecret() })
		_, err := c.ListTopics(context.Background(), &sns.ListTopicsInput{})
		status, code := apiFailure(t, err)
		if status != 403 || code != "SignatureDoesNotMatch" {
			t.Errorf("wrong secret: status %d, code %q, want 403 SignatureDoesNotMatch", status, code)
		}
	})
}
