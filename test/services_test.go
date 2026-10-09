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
	status, code, _ := apiFailureMessage(t, err)
	return status, code
}

// apiFailureMessage is apiFailure with the error message.
func apiFailureMessage(t *testing.T, err error) (int, string, string) {
	t.Helper()
	respErr, ok := errors.AsType[*awshttp.ResponseError](err)
	if !ok {
		t.Fatalf("error %v: want *awshttp.ResponseError", err)
	}
	apiErr, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		t.Fatalf("error %v: want smithy.APIError", err)
	}
	return respErr.HTTPStatusCode(), apiErr.ErrorCode(), apiErr.ErrorMessage()
}

func TestSQSStub(t *testing.T) {
	servers := map[string]func(*testing.T) *pail{"plain": startPail, "tls": startPailTLS}
	for name, start := range servers {
		t.Run(name+" list queues is unsupported", func(t *testing.T) {
			_, err := start(t).sqsClient().ListQueues(context.Background(), &sqs.ListQueuesInput{})
			unsupported, ok := errors.AsType[*types.UnsupportedOperation](err)
			if !ok {
				t.Fatalf("ListQueues error = %v, want *types.UnsupportedOperation", err)
			}
			if got, want := unsupported.ErrorMessage(), "ListQueues is not supported"; got != want {
				t.Errorf("ListQueues message = %q, want %q", got, want)
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
		status, code, msg := apiFailureMessage(t, err)
		if status != 400 || code != "InvalidAction" || msg != "ListTopics is not supported" {
			t.Errorf("ListTopics: status %d, code %q, message %q, want 400 InvalidAction %q", status, code, msg, "ListTopics is not supported")
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
