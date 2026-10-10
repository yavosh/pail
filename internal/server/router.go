package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/s3api"
	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/snsapi"
	"github.com/yavosh/pail/internal/sqsapi"
	"github.com/yavosh/pail/internal/topic"
)

// router sends each request to one service without reading the body.
type router struct{ s3, sqs, sns http.Handler }

// NewHandler returns the handler for one listener that serves S3, SQS, and SNS.
func NewHandler(cfg config.Config, st s3api.Store, queues sqsapi.Queues, topics *topic.Engine) http.Handler {
	return &router{
		s3: s3api.New(s3api.Options{
			Domain:          cfg.Domain,
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			Region:          cfg.Region,
			Store:           st,
			Internal:        map[string]http.Handler{"GET /_pail/sns/signing-cert.pem": signingCert(topics)},
			Notifier:        notifier{region: cfg.Region, queues: queues, topics: topics},
		}),
		sqs: sqsapi.New(sqsapi.Options{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey, Queues: queues}),
		sns: snsapi.New(snsapi.Options{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey, Topics: topics}),
	}
}

// notifier delivers S3 event notifications to queues and topics by ARN.
type notifier struct {
	region string
	queues sqsapi.Queues
	topics *topic.Engine
}

// split returns the service and resource name of an ARN in pail's region and account.
func (n notifier) split(arn string) (service, name string, ok bool) {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[3] != n.region || parts[4] != queue.Account || parts[5] == "" {
		return "", "", false
	}
	return parts[2], parts[5], parts[2] == "sqs" || parts[2] == "sns"
}

// Exists reports whether arn names a queue or topic.
func (n notifier) Exists(ctx context.Context, arn string) bool {
	service, name, ok := n.split(arn)
	switch {
	case !ok:
		return false
	case service == "sqs":
		// AWS refuses a FIFO queue, and a delivery without a group ID would fail.
		return !strings.HasSuffix(name, ".fifo") && n.queues.Lookup(ctx, name) == nil
	}
	_, err := n.topics.TopicAttributes(ctx, arn)
	return err == nil
}

// Deliver sends message to the queue or topic. It holds no engine mutex.
func (n notifier) Deliver(ctx context.Context, arn, message, baseURL string) error {
	service, name, ok := n.split(arn)
	switch {
	case !ok:
		return errors.New("unsupported destination " + arn)
	case service == "sns":
		_, err := n.topics.Publish(ctx, topic.PublishInput{TopicARN: arn, Message: message, Subject: "Amazon S3 Notification", BaseURL: baseURL})
		return err
	}
	res, err := n.queues.Send(ctx, name, []queue.SendInput{{Body: message}})
	if err == nil && len(res) == 1 {
		err = res[0].Err
	}
	return err
}

// signingCert serves the certificate that verifies SNS notification signatures.
// It needs no credentials, as on AWS.
func signingCert(topics *topic.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pem, err := topics.CertPEM(r.Context())
		if err != nil {
			clogServer().Error("signing certificate", "error", err)
			http.Error(w, "signing certificate unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(pem)
	})
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch service := sigv4.Service(r); {
	case service == "sqs":
		rt.sqs.ServeHTTP(w, r)
	case service == "sns":
		rt.sns.ServeHTTP(w, r)
	case service == "" && strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS."):
		rt.sqs.ServeHTTP(w, r)
	case service == "" && snsLink(r):
		rt.sns.ServeHTTP(w, r)
	default:
		rt.s3.ServeHTTP(w, r)
	}
}

// snsLink reports whether r is a link from an SNS message: an unsigned GET of
// "/" that confirms a subscription or unsubscribes. It reads only the query.
func snsLink(r *http.Request) bool {
	if r.Method != http.MethodGet || r.URL.Path != "/" {
		return false
	}
	action := r.URL.Query().Get("Action")
	return action == "ConfirmSubscription" || action == "Unsubscribe"
}
