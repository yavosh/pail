// Package topic implements SNS topics and subscriptions: definitions persist on a vfs.FS, and SQS deliveries go to the queue engine.
package topic

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/vfs"
)

const (
	topicsDir     = "sns/topics"
	subsDir       = "sns/subscriptions"
	maxTags       = 50
	pageSize      = 100
	protocolSQS   = "sqs"
	protocolHTTP  = "http"
	protocolHTTPS = "https"
	attrRaw       = "RawMessageDelivery"
	attrFilter    = "FilterPolicy"
	attrScope     = "FilterPolicyScope"
	sqsARNPrefix  = "arn:aws:sqs:"
	topicARNStart = "arn:aws:sns:"
)

var topicNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

// Queues is the queue engine that receives deliveries. *queue.Engine implements it.
type Queues interface {
	Send(ctx context.Context, name string, in []queue.SendInput) ([]queue.SendResult, error)
}

// Engine holds the topics and subscriptions. Definitions persist on a vfs.FS.
type Engine struct {
	fs     vfs.FS
	region string
	queues Queues
	mu     sync.Mutex
	topics map[string]*topicDef // by name
	subs   map[string]*subDef   // by subscription ARN
	sign   *signer              // nil until first needed
	client *http.Client         // posts to HTTP and HTTPS endpoints
	jobs   chan httpJob         // deliveries that RunDeliveries sends
	// restores holds subscriptions removed by an unsigned Unsubscribe, by
	// UnsubscribeConfirmation token. A restart loses them.
	restores map[string]restore
	limMu    sync.Mutex
	nextPost map[string]time.Time // earliest next post per subscription ARN, for throttling
}

// topicDef is the persisted part of a topic. Attributes holds only the ones a
// client set; the defaults come from topicAttrValue. Created is unix seconds.
type topicDef struct {
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Created    int64             `json:"created"`
}

// subDef is the persisted part of a subscription.
type subDef struct {
	ARN        string            `json:"arn"`
	TopicARN   string            `json:"topicArn"`
	Protocol   string            `json:"protocol"`
	Endpoint   string            `json:"endpoint"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Created    int64             `json:"created"`
	// Pending marks an HTTP or HTTPS subscription that awaits confirmation with
	// Token. A file without these fields is confirmed and authenticated.
	Pending         bool   `json:"pending,omitempty"`
	Token           string `json:"token,omitempty"`
	Unauthenticated bool   `json:"unauthenticated,omitempty"`
}

// Attribute is one entry of an attribute response. The order is part of the answer.
type Attribute struct{ Key, Value string }

// Subscription is one entry of a subscription list.
type Subscription struct {
	ARN, Owner, Protocol, Endpoint, TopicARN string
}

// Option changes how Open builds an Engine.
type Option func(*Engine)

// WithTLSSkipVerify makes the HTTP client accept any certificate from an
// HTTPS endpoint. Use it only for local testing.
func WithTLSSkipVerify(skip bool) Option {
	return func(e *Engine) {
		if !skip {
			return
		}
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in flag for local testing
		e.client.Transport = tr
		clogTopic().Warn("certificate checks are off for HTTPS subscription endpoints")
	}
}

// Open loads the topics and subscriptions from fsys.
func Open(ctx context.Context, fsys vfs.FS, region string, queues Queues, opts ...Option) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e := &Engine{fs: fsys, region: region, queues: queues, topics: map[string]*topicDef{}, subs: map[string]*subDef{},
		restores: map[string]restore{}, nextPost: map[string]time.Time{}}
	e.client = &http.Client{
		Timeout:       httpTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	e.jobs = make(chan httpJob, jobQueueSize)
	for _, opt := range opts {
		opt(e)
	}
	for _, dir := range []string{topicsDir, subsDir} {
		if err := fsys.MkdirAll(dir); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if err := e.loadTopics(); err != nil {
		return nil, err
	}
	if err := e.loadSubs(); err != nil {
		return nil, err
	}
	sg, err := loadSigner(fsys)
	if err != nil {
		return nil, err
	}
	e.sign = sg
	clogTopic().Info("topics loaded", "topics", len(e.topics), "subscriptions", len(e.subs))
	return e, nil
}

func (e *Engine) loadTopics() error {
	entries, err := e.fs.ReadDir(topicsDir)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		var def topicDef
		file := path.Join(topicsDir, ent.Name())
		if err := vfs.ReadJSON(e.fs, file, &def); err != nil {
			return fmt.Errorf("read topic %s: %w", ent.Name(), err)
		}
		if err := checkTopic(def); err != nil {
			return fmt.Errorf("topic file %s: %w", ent.Name(), err)
		}
		// A file under another name would resurrect a topic deleted by name.
		if file != topicFile(def.Name) {
			return fmt.Errorf("topic file %s: name %q does not match the file name", ent.Name(), def.Name)
		}
		e.topics[def.Name] = &def
	}
	return nil
}

func checkTopic(def topicDef) error {
	if err := checkTopicName(def.Name); err != nil {
		return err
	}
	for k, v := range def.Attributes {
		if err := checkTopicAttr(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) loadSubs() error {
	entries, err := e.fs.ReadDir(subsDir)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		var def subDef
		file := path.Join(subsDir, ent.Name())
		if err := vfs.ReadJSON(e.fs, file, &def); err != nil {
			return fmt.Errorf("read subscription %s: %w", ent.Name(), err)
		}
		if err := e.checkSub(def); err != nil {
			return fmt.Errorf("subscription file %s: %w", ent.Name(), err)
		}
		if file != subFile(def.ARN) {
			return fmt.Errorf("subscription file %s: ARN %q does not match the file name", ent.Name(), def.ARN)
		}
		e.subs[def.ARN] = &def
	}
	return nil
}

// checkSub checks a loaded subscription against its topic.
func (e *Engine) checkSub(def subDef) error {
	name, ok := strings.CutPrefix(def.TopicARN, e.topicARN(""))
	if _, found := e.topics[name]; !ok || !found {
		return fmt.Errorf("topic %q does not exist", def.TopicARN)
	}
	if id, ok := strings.CutPrefix(def.ARN, def.TopicARN+":"); !ok || !uuidRE.MatchString(id) {
		return fmt.Errorf("ARN %q does not extend topic ARN %q with a UUID", def.ARN, def.TopicARN)
	}
	if _, err := e.checkEndpoint(def.Protocol, def.Endpoint); err != nil {
		return err
	}
	if def.Pending && def.Token == "" {
		return fmt.Errorf("pending subscription %q has no token", def.ARN)
	}
	if err := checkProtocolAttrs(def.Protocol, def.Attributes); err != nil {
		return err
	}
	return checkSubAttrs(def.Attributes)
}

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (e *Engine) topicARN(name string) string {
	return topicARNStart + e.region + ":" + queue.Account + ":" + name
}

// topicFile is hashed so case-sensitive names survive case-insensitive file systems.
func topicFile(name string) string {
	sum := sha256.Sum256([]byte(name))
	return path.Join(topicsDir, hex.EncodeToString(sum[:])+".json")
}

// subFile is named by the UUID at the end of the subscription ARN.
func subFile(arn string) string {
	return path.Join(subsDir, arn[strings.LastIndex(arn, ":")+1:]+".json")
}

// newUUID returns a random lowercase UUID v4.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func checkTopicName(name string) error {
	if strings.HasSuffix(name, ".fifo") {
		return fmt.Errorf("topic name %q: FIFO topics are not supported: %w", name, ErrInvalidParameter)
	}
	if !topicNameRE.MatchString(name) {
		return fmt.Errorf("topic name %q must be 1 to 256 letters, digits, hyphens, or underscores: %w", name, ErrInvalidParameter)
	}
	return nil
}

// lock takes e.mu and drops the pending subscriptions whose token expired.
// Expiry is lazy, so the engine runs no goroutine for it.
func (e *Engine) lock() {
	e.mu.Lock()
	e.sweep()
}

// findTopic returns the topic that arn names. The caller holds e.mu.
func (e *Engine) findTopic(arn string) (*topicDef, error) {
	if !strings.HasPrefix(arn, "arn:") {
		return nil, fmt.Errorf("TopicArn %q is not an ARN: %w", arn, ErrInvalidParameter)
	}
	name, ok := strings.CutPrefix(arn, e.topicARN(""))
	t := e.topics[name]
	if !ok || t == nil {
		return nil, fmt.Errorf("topic %q: %w", arn, ErrNotFound)
	}
	return t, nil
}

func (e *Engine) findSub(arn string) (*subDef, error) {
	if !strings.HasPrefix(arn, "arn:") {
		return nil, fmt.Errorf("SubscriptionArn %q is not an ARN: %w", arn, ErrInvalidParameter)
	}
	s, ok := e.subs[arn]
	if !ok {
		return nil, fmt.Errorf("subscription %q: %w", arn, ErrNotFound)
	}
	return s, nil
}

func (e *Engine) persistTopic(def *topicDef) error {
	if err := vfs.WriteJSON(e.fs, topicFile(def.Name), def); err != nil {
		return fmt.Errorf("write topic %s: %w", def.Name, err)
	}
	return nil
}

func (e *Engine) persistSub(def *subDef) error {
	if err := vfs.WriteJSON(e.fs, subFile(def.ARN), def); err != nil {
		return fmt.Errorf("write subscription %s: %w", def.ARN, err)
	}
	return nil
}

// CreateTopic creates a topic and returns its ARN. It succeeds without change
// when the topic exists and every given attribute matches its value.
func (e *Engine) CreateTopic(ctx context.Context, name string, attrs, tags map[string]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := checkTopicName(name); err != nil {
		return "", err
	}
	for k, v := range attrs {
		if err := checkTopicAttr(k, v); err != nil {
			return "", err
		}
	}
	if err := checkTags(tags); err != nil {
		return "", err
	}
	attrs = maps.Clone(attrs)
	maps.DeleteFunc(attrs, noopAttr)
	for k, v := range attrs {
		attrs[k] = canonTopicAttr(k, v)
	}
	arn := e.topicARN(name)
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.topics[name]; ok {
		for _, k := range slices.Sorted(maps.Keys(attrs)) {
			if topicAttrValue(t, arn, k) != cmp.Or(attrs[k], topicAttrValue(&topicDef{}, arn, k)) {
				// The message text is unverified.
				return "", fmt.Errorf("topic %s already exists with different attributes: %w", name, ErrInvalidParameter)
			}
		}
		return arn, nil
	}
	if len(tags) > maxTags {
		return "", fmt.Errorf("%d tags: %w", len(tags), ErrTagLimitExceeded)
	}
	def := &topicDef{Name: name, Attributes: maps.Clone(attrs), Tags: maps.Clone(tags), Created: time.Now().Unix()}
	maps.DeleteFunc(def.Attributes, func(_, v string) bool { return v == "" })
	if err := e.persistTopic(def); err != nil {
		return "", err
	}
	e.topics[name] = def
	return arn, nil
}

// DeleteTopic removes a topic and its subscriptions. A missing topic is not an error.
func (e *Engine) DeleteTopic(ctx context.Context, arn string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.findTopic(arn)
	if errors.Is(err, ErrNotFound) {
		return nil // unverified: AWS is believed to answer success
	}
	if err != nil {
		return err
	}
	// Subscriptions go first, so a failure never leaves one without its topic.
	for _, s := range e.topicSubs(arn) {
		if err := e.fs.Remove(subFile(s.ARN)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete subscription %s: %w", s.ARN, err)
		}
		delete(e.subs, s.ARN)
		e.forgetPost(s.ARN)
	}
	if err := e.fs.Remove(topicFile(t.Name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete topic %s: %w", t.Name, err)
	}
	delete(e.topics, t.Name)
	return nil
}

// topicSubs returns the subscriptions of a topic, sorted by ARN. The caller holds e.mu.
func (e *Engine) topicSubs(topicARN string) []*subDef {
	var out []*subDef
	for _, s := range e.subs {
		if s.TopicARN == topicARN {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b *subDef) int { return strings.Compare(a.ARN, b.ARN) })
	return out
}

// page cuts the sorted keys after next to one page. next is the last key of
// the previous page; the returned next is empty on the last page.
func page(keys []string, next string) (out []string, after string) {
	if next != "" {
		i, _ := slices.BinarySearch(keys, next)
		for i < len(keys) && keys[i] <= next {
			i++
		}
		keys = keys[i:]
	}
	if len(keys) > pageSize {
		return keys[:pageSize], keys[pageSize-1]
	}
	return keys, ""
}

// ListTopics returns topic ARNs in sorted order, one page after next.
func (e *Engine) ListTopics(ctx context.Context, next string) ([]string, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	e.mu.Lock()
	arns := make([]string, 0, len(e.topics))
	for name := range e.topics {
		arns = append(arns, e.topicARN(name))
	}
	e.mu.Unlock()
	slices.Sort(arns)
	out, after := page(arns, next)
	return out, after, nil
}

// ListTagsForResource returns a copy of the tags of the topic arn.
func (e *Engine) ListTagsForResource(ctx context.Context, arn string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.resource(arn)
	if err != nil {
		return nil, err
	}
	return maps.Clone(t.Tags), nil
}

// resource finds a topic for a tag operation, which answers ErrResourceNotFound.
func (e *Engine) resource(arn string) (*topicDef, error) {
	t, err := e.findTopic(arn)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("resource %q: %w", arn, ErrResourceNotFound)
	}
	return t, err
}

// TagResource adds or replaces tags on a topic.
func (e *Engine) TagResource(ctx context.Context, arn string, tags map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkTags(tags); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.resource(arn)
	if err != nil {
		return err
	}
	merged := maps.Clone(t.Tags)
	if merged == nil {
		merged = map[string]string{}
	}
	maps.Copy(merged, tags)
	if len(merged) > maxTags {
		return fmt.Errorf("%d tags: %w", len(merged), ErrTagLimitExceeded)
	}
	updated := *t
	updated.Tags = merged
	if err := e.persistTopic(&updated); err != nil {
		return err
	}
	*t = updated
	return nil
}

// UntagResource removes tags from a topic. A key that is not set is ignored.
func (e *Engine) UntagResource(ctx context.Context, arn string, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t, err := e.resource(arn)
	if err != nil {
		return err
	}
	updated := *t
	updated.Tags = maps.Clone(t.Tags)
	for _, k := range keys {
		delete(updated.Tags, k)
	}
	if err := e.persistTopic(&updated); err != nil {
		return err
	}
	*t = updated
	return nil
}

// checkTags applies the model's limits: a key has 1 to 128 characters and a
// value 0 to 256. The error code is unverified.
func checkTags(tags map[string]string) error {
	for k, v := range tags {
		if n := utf8.RuneCountInString(k); n < 1 || n > 128 {
			return fmt.Errorf("tag key %q must be 1 to 128 characters: %w", k, ErrInvalidParameter)
		}
		if utf8.RuneCountInString(v) > 256 {
			return fmt.Errorf("value of tag %q is longer than 256 characters: %w", k, ErrInvalidParameter)
		}
	}
	return nil
}

// isJSONObject reports whether v is a JSON object.
func isJSONObject(v string) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal([]byte(v), &m) == nil && m != nil
}
