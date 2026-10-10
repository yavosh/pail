package server

import (
	"net/http"
	"strings"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/s3api"
	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/snsapi"
	"github.com/yavosh/pail/internal/sqsapi"
)

// router sends each request to one service without reading the body.
type router struct{ s3, sqs, sns http.Handler }

// NewHandler returns the handler for one listener that serves S3, SQS, and SNS.
func NewHandler(cfg config.Config, st s3api.Store, queues sqsapi.Queues) http.Handler {
	return &router{
		s3: s3api.New(s3api.Options{
			Domain:          cfg.Domain,
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			Region:          cfg.Region,
			Store:           st,
		}),
		sqs: sqsapi.New(sqsapi.Options{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey, Queues: queues}),
		sns: snsapi.New(snsapi.Options{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey}),
	}
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch service := sigv4.Service(r); {
	case service == "sqs":
		rt.sqs.ServeHTTP(w, r)
	case service == "sns":
		rt.sns.ServeHTTP(w, r)
	case service == "" && strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS."):
		rt.sqs.ServeHTTP(w, r)
	default:
		rt.s3.ServeHTTP(w, r)
	}
}
