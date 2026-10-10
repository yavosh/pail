package queue

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

var numberRE = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)

const (
	maxBatchAttributes = 10
	maxInflightExtend  = 12 * time.Hour
)

// MessageAttribute is a typed message attribute. String and Number types use
// StringValue; Binary types use BinaryValue.
type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// SendInput is one message to send. A nil DelaySeconds uses the queue's.
// GroupID and DeduplicationID are for FIFO queues; a standard queue stores GroupID.
type SendInput struct {
	Body             string
	DelaySeconds     *int
	Attributes       map[string]MessageAttribute
	SystemAttributes map[string]MessageAttribute
	GroupID          string
	DeduplicationID  string
}

// SendResult is the outcome of one SendInput. The MD5 fields are empty for an empty map.
type SendResult struct {
	MessageID             string
	MD5OfBody             string
	MD5OfAttributes       string
	MD5OfSystemAttributes string
	SequenceNumber        string // FIFO queues only
	Err                   error
}

// ReceiveInput selects messages. Max 0 means 1; nil pointers use the queue's values.
type ReceiveInput struct {
	Max               int
	VisibilityTimeout *int
	WaitTimeSeconds   *int
}

// Message is a received message. It holds copies, never engine state.
type Message struct {
	ID               string
	ReceiptHandle    string
	Body             string
	MD5OfBody        string
	Attributes       map[string]MessageAttribute
	SystemAttributes map[string]MessageAttribute
	SentAt           time.Time
	FirstReceivedAt  time.Time
	ReceiveCount     int
	GroupID          string
	DeduplicationID  string
	SequenceNumber   string
	// DeadLetterSourceARN names the queue that moved the message here.
	DeadLetterSourceARN string
}

// VisibilityChange sets one message's remaining visibility timeout.
type VisibilityChange struct {
	ReceiptHandle string
	Timeout       int
}

type message struct {
	id            [16]byte
	body          string
	attrs         map[string]MessageAttribute
	sysAttrs      map[string]MessageAttribute
	sentAt        time.Time
	visibleAt     time.Time
	firstReceived time.Time
	lastReceived  time.Time
	receives      int
	seq           uint32 // sequence of the newest receipt handle
	group         string
	dedupID       string
	sequence      string // FIFO sequence number
	dlqSource     string
}

// dedupWindow is how long a FIFO queue remembers a deduplication ID.
const dedupWindow = 5 * time.Minute

// dedupEntry is a FIFO send in the deduplication window.
type dedupEntry struct {
	at     time.Time
	result SendResult
}

func newID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return b
}

func idString(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Send adds messages. A bad entry gets an error in its result; the others are still sent.
func (e *Engine) Send(ctx context.Context, name string, in []SendInput) ([]SendResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	q.prune(now)
	maps.DeleteFunc(q.dedup, func(_ string, d dedupEntry) bool { return now.Sub(d.at) >= dedupWindow })
	maxSize := q.intAttr("MaximumMessageSize")
	queueDelay := q.intAttr("DelaySeconds")
	out := make([]SendResult, len(in))
	added := false
	for i, s := range in {
		if err := validateSend(s, maxSize); err != nil {
			out[i].Err = err
			continue
		}
		key, err := q.checkRouting(s)
		if err != nil {
			out[i].Err = err
			continue
		}
		if d, ok := q.dedup[key]; ok {
			out[i] = d.result
			continue
		}
		m := &message{
			id:       newID(),
			body:     s.Body,
			attrs:    cloneAttrs(s.Attributes),
			sysAttrs: cloneAttrs(s.SystemAttributes),
			sentAt:   now,
			group:    s.GroupID,
		}
		if q.fifo() {
			m.dedupID = cmp.Or(s.DeduplicationID, contentHash(s.Body))
			m.sequence = q.newSequence()
		}
		delay := queueDelay
		if s.DelaySeconds != nil {
			delay = *s.DelaySeconds
		}
		m.visibleAt = now.Add(time.Duration(delay) * time.Second)
		q.messages = append(q.messages, m)
		added = true
		out[i] = SendResult{
			MessageID:             idString(m.id),
			MD5OfBody:             MD5OfBody(s.Body),
			MD5OfAttributes:       MD5OfAttributes(s.Attributes),
			MD5OfSystemAttributes: MD5OfAttributes(s.SystemAttributes),
			SequenceNumber:        m.sequence,
		}
		if key != "" {
			q.dedup[key] = dedupEntry{now, out[i]}
		}
	}
	if added {
		q.notify()
	}
	return out, nil
}

// newSequence returns the next FIFO sequence number.
func (q *queue) newSequence() string {
	q.nextSeq++
	return fmt.Sprintf("%020d", q.nextSeq)
}

func contentHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// validID reports whether s is a FIFO group or deduplication ID: 1 to 128
// printable ASCII characters.
func validID(s string) bool {
	return len(s) >= 1 && len(s) <= 128 && !strings.ContainsFunc(s, func(r rune) bool { return r < 0x21 || r > 0x7e })
}

// checkRouting applies the group and deduplication rules of the queue type. On
// a FIFO queue it returns the deduplication key, or "" when the entry has none.
func (q *queue) checkRouting(s SendInput) (key string, err error) {
	if !q.fifo() {
		if s.DeduplicationID != "" {
			return "", fmt.Errorf("MessageDeduplicationId is not valid for a standard queue: %w", ErrInvalidParameterValue)
		}
		return "", nil
	}
	switch {
	case s.GroupID == "":
		return "", fmt.Errorf("MessageGroupId is required for a FIFO queue: %w", ErrMissingParameter)
	case !validID(s.GroupID):
		return "", fmt.Errorf("MessageGroupId must be 1 to 128 printable ASCII characters: %w", ErrInvalidParameterValue)
	case s.DelaySeconds != nil:
		return "", fmt.Errorf("DelaySeconds cannot be set per message on a FIFO queue: %w", ErrInvalidParameterValue)
	case s.DeduplicationID == "" && q.def.Attributes["ContentBasedDeduplication"] != "true":
		return "", fmt.Errorf("the queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly: %w", ErrInvalidParameterValue)
	case s.DeduplicationID != "" && !validID(s.DeduplicationID):
		return "", fmt.Errorf("MessageDeduplicationId must be 1 to 128 printable ASCII characters: %w", ErrInvalidParameterValue)
	}
	key = cmp.Or(s.DeduplicationID, contentHash(s.Body))
	if q.def.Attributes["DeduplicationScope"] == "messageGroup" {
		key = s.GroupID + "\x00" + key
	}
	return key, nil
}

func cloneAttrs(a map[string]MessageAttribute) map[string]MessageAttribute {
	out := maps.Clone(a)
	for k, v := range out {
		v.BinaryValue = slices.Clone(v.BinaryValue)
		out[k] = v
	}
	return out
}

func validateSend(s SendInput, maxSize int) error {
	if s.Body == "" {
		return fmt.Errorf("empty message body: %w", ErrInvalidParameterValue)
	}
	if !validBody(s.Body) {
		return fmt.Errorf("message body has a character outside the allowed set: %w", ErrInvalidMessageContents)
	}
	if s.DelaySeconds != nil && (*s.DelaySeconds < 0 || *s.DelaySeconds > 900) {
		return fmt.Errorf("delay seconds %d: %w", *s.DelaySeconds, ErrInvalidParameterValue)
	}
	if len(s.Attributes) > maxBatchAttributes {
		return fmt.Errorf("%d message attributes: %w", len(s.Attributes), ErrInvalidParameterValue)
	}
	size := len(s.Body)
	for _, name := range slices.Sorted(maps.Keys(s.Attributes)) {
		a := s.Attributes[name]
		if err := validateAttribute(name, a); err != nil {
			return err
		}
		size += len(name) + len(a.DataType) + len(a.StringValue) + len(a.BinaryValue)
	}
	for name, a := range s.SystemAttributes {
		if name != "AWSTraceHeader" || a.DataType != "String" || a.StringValue == "" {
			return fmt.Errorf("system attribute %q: %w", name, ErrInvalidParameterValue)
		}
	}
	if size > maxSize {
		return fmt.Errorf("message size %d exceeds %d: %w", size, maxSize, ErrMessageTooLong)
	}
	return nil
}

// validBody applies the SQS character set: #x9 | #xA | #xD | #x20 to #xD7FF |
// #xE000 to #xFFFD | #x10000 to #x10FFFF.
func validBody(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		switch {
		case r == 0x9 || r == 0xA || r == 0xD:
		case r >= 0x20 && r <= 0xD7FF:
		case r >= 0xE000 && r <= 0xFFFD:
		case r >= 0x10000 && r <= 0x10FFFF:
		default:
			return false
		}
	}
	return true
}

func validateAttribute(name string, a MessageAttribute) error {
	bad := func(why string) error {
		return fmt.Errorf("message attribute %q: %s: %w", name, why, ErrInvalidParameterValue)
	}
	lower := strings.ToLower(name)
	switch {
	case name == "" || len(name) > 256:
		return bad("name must be 1 to 256 characters")
	case strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon."):
		return bad("name uses a reserved prefix")
	case strings.Contains(name, "..") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, "."):
		return bad("name has a misplaced period")
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
		default:
			return bad("name has an invalid character")
		}
	}
	base, label, hasLabel := strings.Cut(a.DataType, ".")
	if hasLabel && label == "" {
		return bad("data type has an empty custom label")
	}
	switch base {
	case "String", "Number":
		if a.StringValue == "" {
			return bad("string value is empty")
		}
		if len(a.BinaryValue) != 0 {
			return bad("string and number attributes cannot have a binary value")
		}
		if !validBody(a.StringValue) {
			return bad("value has a character outside the allowed set")
		}
		if base == "Number" && !numberRE.MatchString(a.StringValue) {
			return bad("number value is not a decimal number")
		}
	case "Binary":
		if len(a.BinaryValue) == 0 {
			return bad("binary value is empty")
		}
		if a.StringValue != "" {
			return bad("binary attributes cannot have a string value")
		}
	default:
		return bad("data type must be String, Number, or Binary")
	}
	return nil
}

// Receive returns up to in.Max visible messages and makes them invisible. It
// long-polls up to the wait time and never holds the engine lock while waiting.
func (e *Engine) Receive(ctx context.Context, name string, in ReceiveInput) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := cmp.Or(in.Max, 1)
	if limit < 1 || limit > 10 {
		return nil, fmt.Errorf("max number of messages %d: %w", limit, ErrInvalidParameterValue)
	}
	if w := in.WaitTimeSeconds; w != nil && (*w < 0 || *w > 20) {
		return nil, fmt.Errorf("wait time seconds %d: %w", *w, ErrInvalidParameterValue)
	}
	if v := in.VisibilityTimeout; v != nil && (*v < 0 || *v > 43200) {
		return nil, fmt.Errorf("visibility timeout %d: %w", *v, ErrInvalidParameterValue)
	}
	start := time.Now()
	var deadline time.Time
	for {
		e.mu.Lock()
		q, err := e.find(name)
		if err != nil {
			e.mu.Unlock()
			return nil, err
		}
		now := time.Now()
		q.prune(now)
		if deadline.IsZero() {
			wait := q.intAttr("ReceiveMessageWaitTimeSeconds")
			if in.WaitTimeSeconds != nil {
				wait = *in.WaitTimeSeconds
			}
			deadline = start.Add(time.Duration(wait) * time.Second)
		}
		timeout := q.intAttr("VisibilityTimeout")
		if in.VisibilityTimeout != nil {
			timeout = *in.VisibilityTimeout
		}
		got := e.take(q, now, limit, timeout)
		wake := q.wake
		next := q.nextVisible(now)
		e.mu.Unlock()

		if len(got) > 0 || !now.Before(deadline) {
			return got, nil
		}
		d := deadline.Sub(now)
		if !next.IsZero() {
			d = min(d, next.Sub(now))
		}
		stopped, err := e.wait(ctx, wake, d)
		if err != nil || stopped {
			return nil, err
		}
	}
}

// take receives up to limit visible messages. On a FIFO queue, a group with an
// in-flight message, or with an earlier message that is not visible, is skipped.
// A message past maxReceiveCount moves to the dead-letter queue instead. The
// caller holds e.mu.
func (e *Engine) take(q *queue, now time.Time, limit, timeout int) []Message {
	dlq, maxReceives := e.deadLetterTarget(q)
	var locked map[string]bool
	if q.fifo() {
		locked = map[string]bool{}
		for _, m := range q.messages {
			if m.receives > 0 && now.Before(m.visibleAt) {
				locked[m.group] = true
			}
		}
	}
	var got []Message
	var moved []*message
	for _, m := range q.messages {
		if len(got) == limit {
			break
		}
		if locked[m.group] {
			continue
		}
		if now.Before(m.visibleAt) {
			if locked != nil {
				locked[m.group] = true
			}
			continue
		}
		if dlq != nil && m.receives >= maxReceives {
			moved = append(moved, m)
			continue
		}
		m.receives++
		if m.firstReceived.IsZero() {
			m.firstReceived = now
		}
		m.lastReceived = now
		m.seq++
		m.visibleAt = now.Add(time.Duration(timeout) * time.Second)
		got = append(got, Message{
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
		})
	}
	if len(moved) > 0 {
		e.moveToDeadLetter(q, dlq, moved, now)
	}
	return got
}

// deadLetterTarget returns the queue that q redrives to and the receive count
// that triggers it, or nil when q has no policy or its target is gone.
func (e *Engine) deadLetterTarget(q *queue) (*queue, int) {
	p, ok := parseRedrivePolicy(q.def.Attributes["RedrivePolicy"])
	if !ok {
		return nil, 0
	}
	name, ok := strings.CutPrefix(p.TargetARN, e.ARN(""))
	if !ok {
		return nil, 0
	}
	return e.queues[name], p.MaxReceiveCount
}

// moveToDeadLetter moves messages from q to dlq. They keep their ID, body, and
// receive count, and are visible at once. A FIFO target numbers them again.
func (e *Engine) moveToDeadLetter(q, dlq *queue, moved []*message, now time.Time) {
	q.messages = slices.DeleteFunc(q.messages, func(m *message) bool { return slices.Contains(moved, m) })
	for _, m := range moved {
		m.visibleAt = now
		m.dlqSource = e.ARN(q.def.Name)
		if dlq.fifo() {
			m.sequence = dlq.newSequence()
		}
		dlq.messages = append(dlq.messages, m)
	}
	dlq.notify()
}

// nextVisible returns the earliest future visibleAt, or the zero time.
func (q *queue) nextVisible(now time.Time) time.Time {
	var next time.Time
	for _, m := range q.messages {
		if m.visibleAt.After(now) && (next.IsZero() || m.visibleAt.Before(next)) {
			next = m.visibleAt
		}
	}
	return next
}

// wait blocks until wake closes, d passes, ctx ends, or StopWaiters runs.
func (e *Engine) wait(ctx context.Context, wake <-chan struct{}, d time.Duration) (stopped bool, err error) {
	select {
	case <-e.stop:
		return true, nil
	default:
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-e.stop:
		return true, nil
	case <-wake:
	case <-t.C:
	}
	return false, nil
}

// Delete removes messages by receipt handle. It returns one error per handle.
// An old handle or a gone message succeeds without deleting, as AWS does.
func (e *Engine) Delete(ctx context.Context, name string, handles []string) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return nil, err
	}
	errs := make([]error, len(handles))
	for i, h := range handles {
		id, seq, ok := e.parseHandle(name, h)
		if !ok {
			errs[i] = ErrReceiptHandleInvalid
			continue
		}
		j := slices.IndexFunc(q.messages, func(m *message) bool { return m.id == id })
		if j >= 0 && q.messages[j].seq == seq {
			q.messages = slices.Delete(q.messages, j, j+1)
		}
	}
	return errs, nil
}

// ChangeVisibility sets the visibility timeout of a message by its latest
// receipt handle, also when the message is visible again, as AWS does. It
// returns one error per change.
func (e *Engine) ChangeVisibility(ctx context.Context, name string, changes []VisibilityChange) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	errs := make([]error, len(changes))
	for i, c := range changes {
		id, seq, ok := e.parseHandle(name, c.ReceiptHandle)
		if !ok {
			errs[i] = ErrReceiptHandleInvalid
			continue
		}
		if c.Timeout < 0 || c.Timeout > 43200 {
			errs[i] = fmt.Errorf("visibility timeout %d: %w", c.Timeout, ErrInvalidParameterValue)
			continue
		}
		j := slices.IndexFunc(q.messages, func(m *message) bool { return m.id == id })
		if j < 0 || q.messages[j].seq != seq {
			errs[i] = ErrMessageNotAvailable
			continue
		}
		m := q.messages[j]
		visibleAt := now.Add(time.Duration(c.Timeout) * time.Second)
		if visibleAt.After(m.lastReceived.Add(maxInflightExtend)) {
			errs[i] = fmt.Errorf("visibility timeout %d passes 12 hours from receive: %w", c.Timeout, ErrInvalidParameterValue)
			continue
		}
		m.visibleAt = visibleAt
		q.notify() // waiting receives size their timers from visibleAt
	}
	return errs, nil
}
