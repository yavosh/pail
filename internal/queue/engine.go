package queue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yavosh/pail/internal/vfs"
)

// Account is the account ID in every queue ARN and URL. pail serves one account.
const Account = "000000000000"

const (
	queuesDir     = "sqs/queues"
	maxTags       = 50
	maxListQueues = 1000
	purgeInterval = 60 * time.Second
)

var (
	queueNameRE     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	fifoQueueNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,75}\.fifo$`)
)

// Engine holds the queues. Definitions persist on a vfs.FS; messages do not.
type Engine struct {
	fs     vfs.FS
	region string
	key    []byte // signs receipt handles
	mu     sync.Mutex
	queues map[string]*queue

	stop     chan struct{}
	stopOnce sync.Once
}

// definition is the persisted part of a queue. Times are unix seconds.
type definition struct {
	Name         string            `json:"name"`
	Attributes   map[string]string `json:"attributes"`
	Tags         map[string]string `json:"tags,omitempty"`
	Created      int64             `json:"created"`
	LastModified int64             `json:"lastModified"`
}

type queue struct {
	def       definition
	messages  []*message // arrival order
	lastPurge time.Time
	wake      chan struct{}
	nextSeq   uint64                // last FIFO sequence number issued
	dedup     map[string]dedupEntry // FIFO sends in the deduplication window
}

func (q *queue) fifo() bool { return isFIFOName(q.def.Name) }

// notify wakes every long poll on q.
func (q *queue) notify() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// intAttr returns a validated integer attribute.
func (q *queue) intAttr(name string) int {
	n, _ := strconv.Atoi(q.def.Attributes[name])
	return n
}

// Open loads the queue definitions from fsys.
func Open(ctx context.Context, fsys vfs.FS, region string) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fsys.MkdirAll(queuesDir); err != nil {
		return nil, fmt.Errorf("create queues directory: %w", err)
	}
	entries, err := fsys.ReadDir(queuesDir)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	e := &Engine{fs: fsys, region: region, queues: map[string]*queue{}, stop: make(chan struct{})}
	e.key = make([]byte, 32)
	_, _ = rand.Read(e.key)
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		var def definition
		file := path.Join(queuesDir, ent.Name())
		if err := vfs.ReadJSON(fsys, file, &def); err != nil {
			return nil, fmt.Errorf("read queue %s: %w", ent.Name(), err)
		}
		// A file under another name would resurrect a queue deleted by name.
		if err := errors.Join(checkQueueName(def.Name), validateAttrs(def.Attributes, isFIFOName(def.Name))); err != nil {
			return nil, fmt.Errorf("queue file %s: %w", ent.Name(), err)
		}
		if file != queueFile(def.Name) {
			return nil, fmt.Errorf("queue file %s: name %q does not match the file name", ent.Name(), def.Name)
		}
		def.Attributes = defaultAttrs(canonicalAttrs(def.Attributes), isFIFOName(def.Name))
		e.queues[def.Name] = newQueue(def)
	}
	clogQueue().Info("queues loaded", "count", len(e.queues))
	return e, nil
}

// StopWaiters releases every long poll. Server shutdown calls it so polls
// return before http.Server.Shutdown times out; later Receive calls do not wait.
func (e *Engine) StopWaiters() {
	e.stopOnce.Do(func() { close(e.stop) })
}

// ARN returns the ARN of the queue called name.
func (e *Engine) ARN(name string) string {
	return "arn:aws:sqs:" + e.region + ":" + Account + ":" + name
}

// queueFile is hashed so case-sensitive names survive case-insensitive file systems.
func queueFile(name string) string {
	sum := sha256.Sum256([]byte(name))
	return path.Join(queuesDir, hex.EncodeToString(sum[:])+".json")
}

func (e *Engine) persist(def definition) error {
	if err := vfs.WriteJSON(e.fs, queueFile(def.Name), def); err != nil {
		return fmt.Errorf("write queue %s: %w", def.Name, err)
	}
	return nil
}

func newQueue(def definition) *queue {
	return &queue{def: def, wake: make(chan struct{}), dedup: map[string]dedupEntry{}}
}

func checkQueueName(name string) error {
	re := queueNameRE
	if isFIFOName(name) {
		re = fifoQueueNameRE
	}
	if !re.MatchString(name) {
		return fmt.Errorf("queue name %q: %w", name, ErrInvalidName)
	}
	return nil
}

// CreateQueue creates a queue. It succeeds without change when the queue
// exists and every given attribute matches.
func (e *Engine) CreateQueue(ctx context.Context, name string, attrs, tags map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkQueueName(name); err != nil {
		return err
	}
	fifo := isFIFOName(name)
	attrs, err := checkFifoAttribute(name, attrs)
	if err != nil {
		return err
	}
	if err := validateAttrs(attrs, fifo); err != nil {
		return err
	}
	attrs = canonicalAttrs(attrs)
	e.mu.Lock()
	defer e.mu.Unlock()
	if q, ok := e.queues[name]; ok {
		for k, v := range attrs {
			if q.def.Attributes[k] != v {
				return fmt.Errorf("queue %s attribute %s: %w", name, k, ErrQueueNameExists)
			}
		}
		return nil
	}
	// An empty policy means "unset" only for a new queue.
	maps.DeleteFunc(attrs, func(k, v string) bool { return v == "" && slices.Contains(clearable, k) })
	if len(tags) > maxTags {
		return fmt.Errorf("%d tags: %w", len(tags), ErrTooManyTags)
	}
	if err := e.checkRedrive(name, attrs); err != nil {
		return err
	}
	now := time.Now().Unix()
	def := definition{
		Name:         name,
		Attributes:   defaultAttrs(attrs, fifo),
		Tags:         maps.Clone(tags),
		Created:      now,
		LastModified: now,
	}
	if err := e.persist(def); err != nil {
		return err
	}
	e.queues[name] = newQueue(def)
	return nil
}

// checkFifoAttribute applies the FifoQueue rules of CreateQueue: a ".fifo"
// name needs FifoQueue "true", and FifoQueue "true" needs a ".fifo" name. It
// returns attrs without a FifoQueue "false", which a standard queue does not store.
func checkFifoAttribute(name string, attrs map[string]string) (map[string]string, error) {
	v, ok := attrs["FifoQueue"]
	switch {
	case ok && !isBool(v):
		return nil, fmt.Errorf("attribute FifoQueue value %q: %w", v, ErrInvalidAttributeValue)
	case isFIFOName(name) && v != "true":
		return nil, fmt.Errorf("queue %q ends in .fifo, so FifoQueue must be true: %w", name, ErrInvalidParameterValue)
	case !isFIFOName(name) && v == "true":
		return nil, fmt.Errorf("FifoQueue true needs a queue name that ends in .fifo: %w", ErrInvalidParameterValue)
	}
	if v == "false" {
		attrs = maps.Clone(attrs)
		delete(attrs, "FifoQueue")
	}
	return attrs, nil
}

// checkRedrive checks the RedrivePolicy in attrs against the other queues: the
// target must be an existing queue of this account and region, of the same
// type, and its RedriveAllowPolicy must permit name. The caller holds e.mu.
func (e *Engine) checkRedrive(name string, attrs map[string]string) error {
	v := attrs["RedrivePolicy"]
	if v == "" {
		return nil
	}
	p, _ := parseRedrivePolicy(v)
	target, ok := strings.CutPrefix(p.TargetARN, e.ARN(""))
	tq := e.queues[target]
	switch {
	case !ok || tq == nil:
		return fmt.Errorf("dead letter target %q does not exist: %w", p.TargetARN, ErrInvalidParameterValue)
	case target == name:
		return fmt.Errorf("a queue cannot be its own dead letter target: %w", ErrInvalidParameterValue)
	case tq.fifo() != isFIFOName(name):
		return fmt.Errorf("dead letter target %q is not the same queue type: %w", target, ErrInvalidParameterValue)
	}
	if allow, ok := parseRedriveAllowPolicy(tq.def.Attributes["RedriveAllowPolicy"]); ok && !allow.permits(e.ARN(name)) {
		return fmt.Errorf("dead letter target %q does not allow queue %q: %w", target, name, ErrInvalidParameterValue)
	}
	return nil
}

// Lookup returns ErrQueueDoesNotExist when no queue is called name.
func (e *Engine) Lookup(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, err := e.find(name)
	return err
}

// find returns the queue called name. The caller holds e.mu.
func (e *Engine) find(name string) (*queue, error) {
	q, ok := e.queues[name]
	if !ok {
		return nil, ErrQueueDoesNotExist
	}
	return q, nil
}

// DeleteQueue removes a queue and its messages. Waiting receives see it gone.
func (e *Engine) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return err
	}
	if err := e.fs.Remove(queueFile(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete queue %s: %w", name, err)
	}
	delete(e.queues, name)
	q.notify()
	return nil
}

// ListQueues returns queue names with prefix in sorted order, skipping names
// up to after. With limit 0 it returns up to 1000 names and no next. Otherwise it
// returns at most limit names and, when more remain, next as the last one.
func (e *Engine) ListQueues(ctx context.Context, prefix string, limit int, after string) (names []string, next string, err error) {
	if err = ctx.Err(); err != nil {
		return nil, "", err
	}
	e.mu.Lock()
	for n := range e.queues {
		if strings.HasPrefix(n, prefix) && (after == "" || n > after) {
			names = append(names, n)
		}
	}
	e.mu.Unlock()
	slices.Sort(names)
	names, next = paginate(names, limit)
	return names, next, nil
}

// paginate cuts sorted names to one page. With limit 0 it returns up to 1000
// names and no next. Otherwise next is the last name when more remain.
func paginate(names []string, limit int) (page []string, next string) {
	if limit == 0 {
		return names[:min(len(names), maxListQueues)], ""
	}
	if len(names) > limit {
		names = names[:limit]
		next = names[limit-1]
	}
	return names, next
}

// DeadLetterSourceQueues returns the names of the queues whose RedrivePolicy
// targets the queue called name, in sorted order. Paging works as in ListQueues.
func (e *Engine) DeadLetterSourceQueues(ctx context.Context, name string, limit int, after string) (names []string, next string, err error) {
	if err = ctx.Err(); err != nil {
		return nil, "", err
	}
	e.mu.Lock()
	if _, err = e.find(name); err != nil {
		e.mu.Unlock()
		return nil, "", err
	}
	arn := e.ARN(name)
	for n, q := range e.queues {
		if p, ok := parseRedrivePolicy(q.def.Attributes["RedrivePolicy"]); ok && p.TargetARN == arn && (after == "" || n > after) {
			names = append(names, n)
		}
	}
	e.mu.Unlock()
	slices.Sort(names)
	names, next = paginate(names, limit)
	return names, next, nil
}

// Attributes returns the named attributes, or all of them for "All".
func (e *Engine) Attributes(ctx context.Context, name string, names []string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	all := false
	for _, n := range names {
		if n == "All" {
			all = true
		} else if !knownAttr(n) {
			return nil, fmt.Errorf("attribute %s: %w", n, ErrInvalidAttributeName)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	q.prune(now)
	var visible, inflight, delayed int
	for _, m := range q.messages {
		switch {
		case !now.Before(m.visibleAt):
			visible++
		case m.receives > 0:
			inflight++
		default:
			delayed++
		}
	}
	full := maps.Clone(q.def.Attributes)
	full["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	full["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(inflight)
	full["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	full["CreatedTimestamp"] = strconv.FormatInt(q.def.Created, 10)
	full["LastModifiedTimestamp"] = strconv.FormatInt(q.def.LastModified, 10)
	full["QueueArn"] = e.ARN(name)
	if all {
		return full, nil
	}
	out := map[string]string{}
	for _, n := range names {
		if v, ok := full[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}

// SetAttributes changes settable attributes.
func (e *Engine) SetAttributes(ctx context.Context, name string, attrs map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := attrs["FifoQueue"]; ok {
		return fmt.Errorf("attribute FifoQueue cannot be changed: %w", ErrInvalidAttributeValue)
	}
	if err := validateAttrs(attrs, isFIFOName(name)); err != nil {
		return err
	}
	attrs = canonicalAttrs(attrs)
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return err
	}
	if err := e.checkRedrive(name, attrs); err != nil {
		return err
	}
	def := q.def
	def.Attributes = maps.Clone(def.Attributes)
	for k, v := range attrs {
		if v == "" && slices.Contains(clearable, k) {
			delete(def.Attributes, k)
			continue
		}
		def.Attributes[k] = v
	}
	def.LastModified = time.Now().Unix()
	return e.update(q, def)
}

// update persists def and then makes it the queue's definition.
func (e *Engine) update(q *queue, def definition) error {
	if err := e.persist(def); err != nil {
		return err
	}
	q.def = def
	return nil
}

// Tag adds or replaces tags.
func (e *Engine) Tag(ctx context.Context, name string, tags map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return err
	}
	def := q.def
	def.Tags = maps.Clone(def.Tags)
	if def.Tags == nil {
		def.Tags = map[string]string{}
	}
	maps.Copy(def.Tags, tags)
	if len(def.Tags) > maxTags {
		return fmt.Errorf("%d tags: %w", len(def.Tags), ErrTooManyTags)
	}
	return e.update(q, def)
}

// Untag removes tags by key. A missing key is not an error.
func (e *Engine) Untag(ctx context.Context, name string, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return err
	}
	def := q.def
	def.Tags = maps.Clone(def.Tags)
	for _, k := range keys {
		delete(def.Tags, k)
	}
	return e.update(q, def)
}

// Tags returns a copy of the queue's tags.
func (e *Engine) Tags(ctx context.Context, name string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return nil, err
	}
	out := maps.Clone(q.def.Tags)
	if out == nil {
		out = map[string]string{}
	}
	return out, nil
}

// Purge drops every message. SQS allows one purge per queue each 60 seconds.
func (e *Engine) Purge(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q, err := e.find(name)
	if err != nil {
		return err
	}
	now := time.Now()
	if !q.lastPurge.IsZero() && now.Sub(q.lastPurge) < purgeInterval {
		return fmt.Errorf("queue %s: %w", name, ErrPurgeInProgress)
	}
	q.messages = nil
	q.lastPurge = now
	return nil
}

// prune drops messages past the retention period.
func (q *queue) prune(now time.Time) {
	retention := time.Duration(q.intAttr("MessageRetentionPeriod")) * time.Second
	q.messages = slices.DeleteFunc(q.messages, func(m *message) bool {
		return !now.Before(m.sentAt.Add(retention))
	})
}
