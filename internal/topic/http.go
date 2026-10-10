package topic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
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
	// tokenTTL is how long a confirmation token lives (AWS docs, unverified).
	tokenTTL = 3 * 24 * time.Hour
)

// restore is a subscription that an unsigned Unsubscribe removed. Its
// UnsubscribeConfirmation token brings it back until expires.
type restore struct {
	sub     subDef
	expires time.Time
}

// httpJob is one POST to an HTTP or HTTPS endpoint.
type httpJob struct {
	url, body string
	header    http.Header
	plan      deliveryPlan
	sub       string // subscription ARN, the throttle key; empty for a confirmation
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
	return httpJob{url: endpoint, body: body, header: h, plan: defaultPlan}
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
	e.sendSigned(sub, confirmation{topicARN: sub.TopicARN, token: sub.Token, signatureVersion: version, baseURL: baseURL})
}

// sendUnsubscribeConfirmation tells the endpoint of a removed subscription,
// which sub.Token can restore. The caller must not hold e.mu.
func (e *Engine) sendUnsubscribeConfirmation(sub subDef, version, baseURL string) {
	e.sendSigned(sub, confirmation{topicARN: sub.TopicARN, token: sub.Token, unsubscribe: sub.ARN, signatureVersion: version, baseURL: baseURL})
}

func (e *Engine) sendSigned(sub subDef, c confirmation) {
	c.messageID, c.timestamp = newUUID(), time.Now().UTC().Format(timestampFormat)
	sig, err := e.signString(c.signatureVersion, c.stringToSign())
	if err != nil {
		clogTopic().Warn("confirmation not signed", "subscription", sub.ARN, "type", c.typ(), "error", err)
		return
	}
	c.signature = sig
	e.enqueue(newJob(sub.Endpoint, c.envelope(), c.typ(), c.messageID, sub.TopicARN, "", false))
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

// runJob posts j and retries a failure on the schedule of its delivery policy.
func (e *Engine) runJob(ctx context.Context, j httpJob) {
	for attempt := 0; ; attempt++ {
		if !e.throttle(ctx, j.sub, j.plan.perSecond) {
			return
		}
		retry, err := e.post(ctx, j)
		if err == nil || ctx.Err() != nil {
			return
		}
		if !retry || attempt == len(j.plan.delays) {
			clogTopic().Warn("http delivery failed", "endpoint", redact(j.url), "message", j.header.Get("x-amz-sns-message-id"), "attempts", attempt+1, "error", err)
			return
		}
		if !sleep(ctx, j.plan.delays[attempt]) {
			return
		}
	}
}

// sleep waits d and reports false when ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// throttle waits for the next free slot of subscription sub, which allows
// perSecond posts per second. It reports false when ctx ends first.
func (e *Engine) throttle(ctx context.Context, sub string, perSecond int) bool {
	if sub == "" || perSecond <= 0 {
		return ctx.Err() == nil
	}
	e.limMu.Lock()
	now := time.Now()
	at := now
	if next := e.nextPost[sub]; next.After(now) {
		at = next
	}
	e.nextPost[sub] = at.Add(time.Second / time.Duration(perSecond))
	e.limMu.Unlock()
	return sleep(ctx, at.Sub(now))
}

// forgetPost drops the throttle state of a removed subscription.
func (e *Engine) forgetPost(sub string) {
	e.limMu.Lock()
	delete(e.nextPost, sub)
	e.limMu.Unlock()
}

// sweep removes the pending subscriptions and restore entries that outlived
// tokenTTL. The caller holds e.mu.
func (e *Engine) sweep() {
	now := time.Now()
	for arn, s := range e.subs {
		if !s.Pending || !now.After(time.Unix(s.Created, 0).Add(tokenTTL)) {
			continue
		}
		if err := e.fs.Remove(subFile(arn)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			clogTopic().Warn("expired subscription not removed", "subscription", arn, "error", err)
			continue
		}
		delete(e.subs, arn)
		e.forgetPost(arn)
	}
	maps.DeleteFunc(e.restores, func(_ string, r restore) bool { return now.After(r.expires) })
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
