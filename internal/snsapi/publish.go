package snsapi

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/yavosh/pail/internal/topic"
)

const (
	maxBatchEntries = 10
	maxBatchBytes   = 262144
)

var batchIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

// baseURL is the scheme and host the client used, so the URLs in an envelope
// work for whatever name the client used to reach pail.
func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (h *handler) publish(r *http.Request, p params) (string, error) {
	if p.has("TargetArn") || p.has("PhoneNumber") {
		return "", fmt.Errorf("publishing to TargetArn or PhoneNumber is not supported: %w", topic.ErrInvalidParameter)
	}
	if !p.has("Message") {
		return "", fmt.Errorf("parameter Message is required: %w", errValidation)
	}
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	attrs, err := p.messageAttributes("MessageAttributes")
	if err != nil {
		return "", err
	}
	id, err := h.topics.Publish(r.Context(), topic.PublishInput{
		TopicARN:         arn,
		Message:          p.get("Message"),
		Subject:          p.get("Subject"),
		MessageStructure: p.get("MessageStructure"),
		Attributes:       attrs,
		GroupID:          p.get("MessageGroupId"),
		DeduplicationID:  p.get("MessageDeduplicationId"),
		BaseURL:          baseURL(r),
	})
	if err != nil {
		return "", err
	}
	var x xmlBuf
	x.elem("MessageId", id)
	return x.String(), nil
}

// batchEntries decodes PublishBatchRequestEntries.member.N and checks the
// rules that fail a whole batch.
func (p params) batchEntries() ([]topic.PublishEntry, error) {
	const name = "PublishBatchRequestEntries"
	var entries []topic.PublishEntry
	for n := 1; p.hasPrefix(memberName(name, n) + "."); n++ {
		m := memberName(name, n)
		attrs, err := p.messageAttributes(m + ".MessageAttributes")
		if err != nil {
			return nil, err
		}
		entries = append(entries, topic.PublishEntry{
			ID:               p.get(m + ".Id"),
			Message:          p.get(m + ".Message"),
			Subject:          p.get(m + ".Subject"),
			MessageStructure: p.get(m + ".MessageStructure"),
			Attributes:       attrs,
			GroupID:          p.get(m + ".MessageGroupId"),
			DeduplicationID:  p.get(m + ".MessageDeduplicationId"),
		})
	}
	return entries, checkBatch(entries)
}

func checkBatch(entries []topic.PublishEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("the batch has no entries: %w", errValidation)
	}
	if len(entries) > maxBatchEntries {
		return fmt.Errorf("the batch has %d entries, at most %d: %w", len(entries), maxBatchEntries, errTooManyInBatch)
	}
	seen := map[string]bool{}
	size := 0
	for _, e := range entries {
		if !batchIDRE.MatchString(e.ID) {
			return fmt.Errorf("batch entry id %q must be 1 to 80 letters, digits, hyphens, or underscores: %w", e.ID, errBadBatchEntryID)
		}
		if seen[e.ID] {
			return fmt.Errorf("batch entry id %q is used twice: %w", e.ID, errBatchIDsNotUniq)
		}
		seen[e.ID] = true
		size += len(e.Message)
		for name, a := range e.Attributes {
			size += len(name) + len(a.DataType) + len(a.StringValue) + len(a.BinaryValue)
		}
	}
	if size > maxBatchBytes {
		return fmt.Errorf("the batch is %d bytes, at most %d: %w", size, maxBatchBytes, errBatchTooLong)
	}
	return nil
}

func (h *handler) publishBatch(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	entries, err := p.batchEntries()
	if err != nil {
		return "", err
	}
	results, err := h.topics.PublishBatch(r.Context(), arn, entries, baseURL(r))
	if err != nil {
		return "", err
	}
	var ok, failed xmlBuf
	for _, res := range results {
		if res.Err == nil {
			ok.open("member")
			ok.elem("MessageId", res.MessageID)
			ok.elem("Id", res.ID)
			ok.close("member")
			continue
		}
		code := entryCode(res.Err)
		failed.open("member")
		failed.elem("Code", code)
		failed.elem("Message", res.Err.Error()) // the position is unverified; test/diff drops the text
		failed.elem("SenderFault", fmt.Sprint(code != "InternalError"))
		failed.elem("Id", res.ID)
		failed.close("member")
	}
	// AWS always writes Failed, first, and empty when nothing failed. An empty
	// Successful is written the same way (unverified).
	var x xmlBuf
	for _, list := range []struct {
		name string
		body *xmlBuf
	}{{"Failed", &failed}, {"Successful", &ok}} {
		if list.body.Len() == 0 {
			x.WriteString("<" + list.name + "/>")
			continue
		}
		x.open(list.name)
		x.WriteString(list.body.String())
		x.close(list.name)
	}
	return x.String(), nil
}

// The API's own errors for batch rules. Handlers wrap them with %w.
var (
	errTooManyInBatch  = errors.New("too many entries in batch")
	errBatchIDsNotUniq = errors.New("batch entry ids not distinct")
	errBadBatchEntryID = errors.New("invalid batch entry id")
	errBatchTooLong    = errors.New("batch too long")
	errValidation      = errors.New("validation error")
)
