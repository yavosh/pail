package queue

import (
	"fmt"
	"slices"
	"time"
)

const maxReceiveAttempts = 10000

type receiveAttempt struct {
	expires    time.Time
	validUntil time.Time
	limit      int
	timeout    int
	refs       []receiveRef
}

// A reference keeps no message body alive after deletion or retention expiry.
type receiveRef struct {
	id            [16]byte
	seq           uint32
	visibilityRev uint64
}

func (e *Engine) rememberReceive(q *queue, id string, got []Message, start, now time.Time, limit, timeout int) {
	a := receiveAttempt{
		expires: start.Add(dedupWindow), validUntil: now.Add(time.Duration(timeout) * time.Second),
		limit: limit, timeout: timeout,
	}
	for _, received := range got {
		id, seq, _ := e.parseHandle(q.def.Name, received.ReceiptHandle)
		index := slices.IndexFunc(q.messages, func(m *message) bool { return m.id == id })
		a.refs = append(a.refs, receiveRef{id: id, seq: seq, visibilityRev: q.messages[index].visibilityRev})
	}
	if q.attempts == nil {
		q.attempts = map[string]receiveAttempt{}
	}
	q.attempts[id] = a
}

// retryReceive validates the whole selection before resetting any visibility.
// The caller holds the engine mutex, as for take.
func (e *Engine) retryReceive(q *queue, a receiveAttempt, now time.Time, limit, timeout int) ([]Message, error) {
	invalid := fmt.Errorf("receive request attempt id is no longer valid for this request: %w", ErrInvalidParameterValue)
	if a.limit != limit || a.timeout != timeout {
		return nil, invalid
	}
	// Empty selections keep their original visibility deadline across retries.
	if len(a.refs) == 0 && !now.Before(a.validUntil) {
		return nil, invalid
	}
	var selected []*message
	for _, ref := range a.refs {
		index := slices.IndexFunc(q.messages, func(m *message) bool { return m.id == ref.id })
		if index < 0 {
			return nil, invalid
		}
		m := q.messages[index]
		if m.seq != ref.seq || m.visibilityRev != ref.visibilityRev || !now.Before(m.visibleAt) {
			return nil, invalid
		}
		selected = append(selected, m)
	}
	var got []Message
	for _, m := range selected {
		m.visibleAt = now.Add(time.Duration(timeout) * time.Second)
		m.lastReceived = now
		got = append(got, e.receivedMessage(q, m))
	}
	if len(selected) > 0 {
		q.notify()
	}
	return got, nil
}

func (e *Engine) receivedMessage(q *queue, m *message) Message {
	return Message{
		ID:                  idString(m.id),
		ReceiptHandle:       e.newHandle(q.def.Name, m.id, m.seq),
		Body:                m.body,
		MD5OfBody:           MD5OfBody(m.body),
		Attributes:          cloneAttrs(m.attrs),
		SystemAttributes:    cloneAttrs(m.sysAttrs),
		SentAt:              m.sentAt,
		FirstReceivedAt:     m.firstReceived,
		ReceiveCount:        m.receives,
		GroupID:             m.group,
		DeduplicationID:     m.dedupID,
		SequenceNumber:      m.sequence,
		DeadLetterSourceARN: m.dlqSource,
	}
}
