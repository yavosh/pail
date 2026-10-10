package sqsapi

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/yavosh/pail/internal/queue"
)

const (
	maxBatchEntries = 10
	maxBatchBytes   = 1048576
)

var batchIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

// checkBatchIDs applies the rules every batch operation shares.
func checkBatchIDs(ids []string) error {
	switch {
	case len(ids) == 0:
		return fmt.Errorf("the batch has no entries: %w", errEmptyBatch)
	case len(ids) > maxBatchEntries:
		return fmt.Errorf("the batch has %d entries, the maximum is %d: %w", len(ids), maxBatchEntries, errTooManyInBatch)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !batchIDRE.MatchString(id) {
			return fmt.Errorf("batch entry id %q must be 1 to 80 letters, digits, hyphens, or underscores: %w", id, errBadBatchEntryID)
		}
		if seen[id] {
			return fmt.Errorf("batch entry id %q is used twice: %w", id, errBatchIDsNotUniq)
		}
		seen[id] = true
	}
	return nil
}

type failedEntry struct {
	ID          string `json:"Id"`
	SenderFault bool
	Code        string
	Message     string
}

type batchResponse[S any] struct {
	Successful []S           `json:",omitempty"`
	Failed     []failedEntry `json:",omitempty"`
}

// collect sorts entries into Successful and Failed by errs. success builds the
// Successful item of entry i.
func collect[S any](ids []string, errs []error, success func(i int) S) batchResponse[S] {
	var out batchResponse[S]
	for i, err := range errs {
		if err != nil {
			out.Failed = append(out.Failed, failedEntry{ids[i], true, entryCode(err), err.Error()})
			continue
		}
		out.Successful = append(out.Successful, success(i))
	}
	return out
}

type idOnly struct {
	ID string `json:"Id"`
}

type sendBatchEntry struct {
	ID string `json:"Id"`
	sendEntry
}

type sendMessageBatchRequest struct {
	QueueURL string `json:"QueueUrl"`
	Entries  []sendBatchEntry
}

type sendBatchResult struct {
	ID                           string `json:"Id"`
	MessageID                    string `json:"MessageId"`
	MD5OfMessageBody             string
	MD5OfMessageAttributes       string `json:",omitempty"`
	MD5OfMessageSystemAttributes string `json:",omitempty"`
}

func (h *handler) sendMessageBatch(r *http.Request, in sendMessageBatchRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(in.Entries))
	size := 0
	for i, e := range in.Entries {
		ids[i] = e.ID
		size += e.size()
	}
	if err := checkBatchIDs(ids); err != nil {
		return nil, err
	}
	if size > maxBatchBytes {
		return nil, fmt.Errorf("the batch is %d bytes, the maximum is %d: %w", size, maxBatchBytes, errBatchTooLong)
	}
	// An entry that cannot convert fails alone; the rest are sent.
	errs := make([]error, len(in.Entries))
	var inputs []queue.SendInput
	var sentAt []int // entry index of each input
	for i, e := range in.Entries {
		input, err := e.toInput()
		if err != nil {
			errs[i] = err
			continue
		}
		inputs = append(inputs, input)
		sentAt = append(sentAt, i)
	}
	res, err := h.queues.Send(r.Context(), name, inputs)
	if err != nil {
		return nil, err
	}
	results := make([]queue.SendResult, len(in.Entries))
	for j, s := range res {
		results[sentAt[j]] = s
		errs[sentAt[j]] = s.Err
	}
	return collect(ids, errs, func(i int) sendBatchResult {
		return sendBatchResult{
			ID:                           ids[i],
			MessageID:                    results[i].MessageID,
			MD5OfMessageBody:             results[i].MD5OfBody,
			MD5OfMessageAttributes:       results[i].MD5OfAttributes,
			MD5OfMessageSystemAttributes: results[i].MD5OfSystemAttributes,
		}
	}), nil
}

type deleteBatchEntry struct {
	ID            string `json:"Id"`
	ReceiptHandle string
}

type deleteMessageBatchRequest struct {
	QueueURL string `json:"QueueUrl"`
	Entries  []deleteBatchEntry
}

func (h *handler) deleteMessageBatch(r *http.Request, in deleteMessageBatchRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(in.Entries))
	handles := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		ids[i], handles[i] = e.ID, e.ReceiptHandle
	}
	if err := checkBatchIDs(ids); err != nil {
		return nil, err
	}
	errs, err := h.queues.Delete(r.Context(), name, handles)
	if err != nil {
		return nil, err
	}
	return collect(ids, errs, func(i int) idOnly { return idOnly{ids[i]} }), nil
}

type visibilityBatchEntry struct {
	ID                string `json:"Id"`
	ReceiptHandle     string
	VisibilityTimeout *int
}

type changeMessageVisibilityBatchRequest struct {
	QueueURL string `json:"QueueUrl"`
	Entries  []visibilityBatchEntry
}

func (h *handler) changeMessageVisibilityBatch(r *http.Request, in changeMessageVisibilityBatchRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		ids[i] = e.ID
	}
	if err := checkBatchIDs(ids); err != nil {
		return nil, err
	}
	// An entry without a timeout fails alone with InvalidParameterValue; the
	// MissingParameter code is only for whole requests.
	errs := make([]error, len(in.Entries))
	var changes []queue.VisibilityChange
	var sentAt []int
	for i, e := range in.Entries {
		if e.VisibilityTimeout == nil {
			errs[i] = fmt.Errorf("VisibilityTimeout is required: %w", queue.ErrInvalidParameterValue)
			continue
		}
		changes = append(changes, queue.VisibilityChange{ReceiptHandle: e.ReceiptHandle, Timeout: *e.VisibilityTimeout})
		sentAt = append(sentAt, i)
	}
	got, err := h.queues.ChangeVisibility(r.Context(), name, changes)
	if err != nil {
		return nil, err
	}
	for j, e := range got {
		errs[sentAt[j]] = e
	}
	return collect(ids, errs, func(i int) idOnly { return idOnly{ids[i]} }), nil
}
