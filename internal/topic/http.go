package topic

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	httpTimeout  = 15 * time.Second // unverified
	jobQueueSize = 1000
	// deliveryWorkers is pail's choice: it bounds the posts in flight.
	deliveryWorkers = 100
	maxAttempts     = 4 // 1 + 3 retries, AWS's default healthyRetryPolicy
	retryDelay      = 20 * time.Second

	// httpDeliveryPolicy is the EffectiveDeliveryPolicy of an HTTP subscription (unverified).
	httpDeliveryPolicy = `{"healthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"sicklyRetryPolicy":null,"throttlePolicy":null,"requestPolicy":{"headerContentType":"text/plain; charset=UTF-8"},"guaranteed":false}`
)

// httpJob is one POST to an HTTP or HTTPS endpoint.
type httpJob struct {
	url, body string
	header    http.Header
}

// newJob returns a POST of body with the headers SNS sends. subscriptionARN is
// empty for a SubscriptionConfirmation.
func newJob(endpoint, body, msgType, messageID, topicARN, subscriptionARN string, raw bool) httpJob {
	h := http.Header{}
	h.Set("Content-Type", "text/plain; charset=UTF-8")
	h.Set("User-Agent", "Amazon Simple Notification Service Agent")
	h.Set("x-amz-sns-message-type", msgType)
	h.Set("x-amz-sns-message-id", messageID)
	h.Set("x-amz-sns-topic-arn", topicARN)
	if subscriptionARN != "" {
		h.Set("x-amz-sns-subscription-arn", subscriptionARN)
	}
	if raw {
		h.Set("x-amz-sns-rawdelivery", "true")
	}
	return httpJob{url: endpoint, body: body, header: h}
}

// enqueue hands a job to RunDeliveries without blocking. A full queue drops it.
func (e *Engine) enqueue(j httpJob) {
	select {
	case e.jobs <- j:
	default:
		clogTopic().Warn("http delivery queue full, dropping", "endpoint", redact(j.url), "message", j.header.Get("x-amz-sns-message-id"))
	}
}

// redact drops the query, which can hold a secret.
func redact(endpoint string) string {
	base, _, _ := strings.Cut(endpoint, "?")
	return base
}

// sendConfirmation signs a SubscriptionConfirmation for a pending subscription
// and queues it. A failure is logged: the subscription stays pending. The
// caller must not hold e.mu.
func (e *Engine) sendConfirmation(sub subDef, version, baseURL string) {
	c := confirmation{
		messageID: newUUID(), topicARN: sub.TopicARN, token: sub.Token,
		timestamp:        time.Now().UTC().Format(timestampFormat),
		signatureVersion: version, baseURL: baseURL,
	}
	sig, err := e.signString(version, c.stringToSign())
	if err != nil {
		clogTopic().Warn("confirmation not signed", "subscription", sub.ARN, "error", err)
		return
	}
	c.signature = sig
	e.enqueue(newJob(sub.Endpoint, c.envelope(), "SubscriptionConfirmation", c.messageID, sub.TopicARN, "", false))
}

// RunDeliveries posts queued jobs with deliveryWorkers workers until ctx is
// done. It returns after every worker has stopped.
func (e *Engine) RunDeliveries(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for range deliveryWorkers {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-e.jobs:
					e.runJob(ctx, j)
				}
			}
		})
	}
}

// runJob posts j and retries a failure that AWS's default policy retries.
func (e *Engine) runJob(ctx context.Context, j httpJob) {
	for attempt := 1; ; attempt++ {
		retry, err := e.post(ctx, j)
		if err == nil || ctx.Err() != nil {
			return
		}
		if !retry || attempt == maxAttempts {
			clogTopic().Warn("http delivery failed", "endpoint", redact(j.url), "message", j.header.Get("x-amz-sns-message-id"), "attempts", attempt, "error", err)
			return
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// post sends j once. A network error, a 5xx, and a 429 are retryable; which
// statuses AWS retries is unverified.
func (e *Engine) post(ctx context.Context, j httpJob) (retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.url, strings.NewReader(j.body))
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}
	req.Header = j.header.Clone()
	resp, err := e.client.Do(req)
	if err != nil {
		return true, fmt.Errorf("post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode/100 == 2:
		return false, nil
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return true, fmt.Errorf("endpoint answered %d", resp.StatusCode)
	}
	return false, fmt.Errorf("endpoint answered %d", resp.StatusCode)
}
