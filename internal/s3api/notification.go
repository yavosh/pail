package s3api

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/store"
)

// Notifier delivers bucket event notifications to SQS queues and SNS topics.
// The server implements it over the queue and topic engines, so s3api imports neither.
type Notifier interface {
	// Exists reports whether arn names a queue or topic that can receive messages.
	Exists(ctx context.Context, arn string) bool
	// Deliver sends message to the queue or topic. baseURL is the scheme and host
	// the client used; SNS puts it in the URLs of an envelope.
	Deliver(ctx context.Context, arn, message, baseURL string) error
}

var (
	errBadDestination = apiError{"InvalidArgument", http.StatusBadRequest, "Unable to validate the following destination configurations"}
	errOverlap        = apiError{"InvalidArgument", http.StatusBadRequest, "Configurations overlap. Configurations on the same bucket cannot share a common event type."}
	errBadEvent       = apiError{"InvalidArgument", http.StatusBadRequest, "The event is not supported for notifications"}
	errBadFilter      = apiError{"InvalidArgument", http.StatusBadRequest, "The filter rule is not valid"}
	errDuplicateID    = apiError{"InvalidArgument", http.StatusBadRequest, "Same ID used for multiple configurations"}
)

// notificationEvents are the configurable events pail raises.
var notificationEvents = []string{
	"s3:ObjectCreated:*", "s3:ObjectCreated:Put", "s3:ObjectCreated:Post", "s3:ObjectCreated:Copy",
	"s3:ObjectCreated:CompleteMultipartUpload", "s3:ObjectRemoved:*", "s3:ObjectRemoved:Delete",
	"s3:ObjectRemoved:DeleteMarkerCreated",
}

// unsupportedEventFamilies are AWS event types that pail never raises.
var unsupportedEventFamilies = []string{
	"s3:ObjectRestore:", "s3:Replication:", "s3:LifecycleExpiration:", "s3:LifecycleTransition",
	"s3:IntelligentTiering", "s3:ObjectTagging:", "s3:ObjectAcl:", "s3:ReducedRedundancyLostObject",
}

// notificationConfiguration follows the S3 NotificationConfiguration XML schema.
type notificationConfiguration struct {
	XMLName     xml.Name             `xml:"NotificationConfiguration"`
	Xmlns       string               `xml:"xmlns,attr,omitempty"`
	Topics      []notificationTarget `xml:"TopicConfiguration"`
	Queues      []notificationTarget `xml:"QueueConfiguration"`
	Lambdas     []xml.Name           `xml:"CloudFunctionConfiguration"`
	LambdaFuncs []xml.Name           `xml:"LambdaFunctionConfiguration"`
	EventBridge []xml.Name           `xml:"EventBridgeConfiguration"`
	Unknown     []xml.Name           `xml:",any"`
}

// notificationTarget is a TopicConfiguration or a QueueConfiguration.
type notificationTarget struct {
	ID      string              `xml:"Id,omitempty"`
	Topic   string              `xml:"Topic,omitempty"`
	Queue   string              `xml:"Queue,omitempty"`
	Events  []string            `xml:"Event"`
	Filter  *notificationFilter `xml:"Filter"`
	Unknown []xml.Name          `xml:",any"`
}

type notificationFilter struct {
	Key     *notificationKey `xml:"S3Key"`
	Unknown []xml.Name       `xml:",any"`
}

type notificationKey struct {
	Rules   []filterRule `xml:"FilterRule"`
	Unknown []xml.Name   `xml:",any"`
}

type filterRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

func (c *notificationTarget) arn() string { return cmp.Or(c.Queue, c.Topic) }

// affixes returns the prefix and suffix rules of the filter.
func (c *notificationTarget) affixes() (prefix, suffix string) {
	if c.Filter == nil || c.Filter.Key == nil {
		return "", ""
	}
	for _, r := range c.Filter.Key.Rules {
		if r.Name == "Prefix" {
			prefix = r.Value
		} else {
			suffix = r.Value
		}
	}
	return prefix, suffix
}

// matches reports whether the configuration covers event (such as
// "ObjectCreated:Put") on key.
func (c *notificationTarget) matches(event, key string) bool {
	full := "s3:" + event
	hit := slices.ContainsFunc(c.Events, func(e string) bool {
		return e == full || strings.HasSuffix(e, ":*") && strings.HasPrefix(full, strings.TrimSuffix(e, "*"))
	})
	prefix, suffix := c.affixes()
	return hit && strings.HasPrefix(key, prefix) && strings.HasSuffix(key, suffix)
}

// overlaps reports whether one object event can match both configurations.
func (c *notificationTarget) overlaps(o notificationTarget) bool {
	family := func(e string) string { return e[:strings.LastIndex(e, ":")] }
	shared := false
	for _, a := range c.Events {
		for _, b := range o.Events {
			shared = shared || a == b || family(a) == family(b) && (strings.HasSuffix(a, "*") || strings.HasSuffix(b, "*"))
		}
	}
	p1, s1 := c.affixes()
	p2, s2 := o.affixes()
	return shared && (strings.HasPrefix(p1, p2) || strings.HasPrefix(p2, p1)) && (strings.HasSuffix(s1, s2) || strings.HasSuffix(s2, s1))
}

// validate checks one configuration and canonicalizes its filter names. A nil
// notifier means no destination can be checked, so pail answers NotImplemented.
func (c *notificationTarget) validate(ctx context.Context, n Notifier, topic bool) apiError {
	if len(c.Unknown) != 0 || len(c.Events) == 0 || len(c.ID) > 255 {
		return errMalformedXML
	}
	if topic && (c.Topic == "" || c.Queue != "") || !topic && (c.Queue == "" || c.Topic != "") {
		return errMalformedXML
	}
	for _, e := range c.Events {
		if slices.Contains(notificationEvents, e) {
			continue
		}
		if slices.ContainsFunc(unsupportedEventFamilies, func(f string) bool { return strings.HasPrefix(e, f) }) {
			return errNotImplemented
		}
		return errBadEvent
	}
	if f := c.Filter; f != nil {
		if len(f.Unknown) != 0 || f.Key != nil && len(f.Key.Unknown) != 0 {
			return errMalformedXML
		}
		seen := map[string]bool{}
		for i, r := range f.rules() {
			name := map[string]string{"prefix": "Prefix", "suffix": "Suffix"}[strings.ToLower(r.Name)]
			if name == "" || seen[name] || len(r.Value) > 1024 {
				return errBadFilter
			}
			seen[name] = true
			f.Key.Rules[i].Name = name
		}
	}
	if n == nil {
		return errNotImplemented
	}
	service := "arn:aws:sqs:"
	if topic {
		service = "arn:aws:sns:"
	}
	if !strings.HasPrefix(c.arn(), service) || !n.Exists(ctx, c.arn()) {
		return errBadDestination
	}
	return apiError{}
}

func (f *notificationFilter) rules() []filterRule {
	if f.Key == nil {
		return nil
	}
	return f.Key.Rules
}

// parseNotification decodes and validates a PutBucketNotificationConfiguration body.
func parseNotification(ctx context.Context, body []byte, n Notifier) (notificationConfiguration, apiError) {
	var c notificationConfiguration
	if err := decodeXMLDocument(body, &c); err != nil || len(c.Unknown) != 0 {
		return c, errMalformedXML
	}
	if len(c.Lambdas) != 0 || len(c.LambdaFuncs) != 0 || len(c.EventBridge) != 0 {
		return c, errNotImplemented
	}
	var all []notificationTarget
	ids := map[string]bool{}
	for i, list := range [][]notificationTarget{c.Topics, c.Queues} {
		for j := range list {
			t := &list[j]
			if e := t.validate(ctx, n, i == 0); e != (apiError{}) {
				return c, e
			}
			if t.ID == "" {
				t.ID = base64.RawURLEncoding.EncodeToString(randomBytes(16))
			}
			if ids[t.ID] {
				return c, errDuplicateID
			}
			ids[t.ID] = true
			if slices.ContainsFunc(all, t.overlaps) {
				return c, errOverlap
			}
			all = append(all, *t)
		}
	}
	c.Xmlns = s3Namespace
	return c, apiError{}
}

func (h *handler) handleBucketNotification(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	if _, err := h.opts.Store.HeadBucket(r.Context(), t.bucket); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if r.Method == http.MethodGet {
		cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, "notification")
		if errors.Is(err, store.ErrNoSuchConfiguration) {
			cfg.XML, _ = xml.Marshal(notificationConfiguration{Xmlns: s3Namespace})
		} else if err != nil {
			writeError(w, r, toAPIError(err))
			return
		}
		writeTagging(w, cfg.XML)
		return
	}
	body, e, ok := readConfiguration(w, r, false)
	if !ok {
		writeError(w, r, e)
		return
	}
	c, e := parseNotification(r.Context(), body, h.opts.Notifier)
	if e != (apiError{}) {
		writeError(w, r, e)
		return
	}
	previous := h.notificationConfig(r.Context(), t.bucket)
	var put *store.BucketConfiguration
	if len(c.Topics)+len(c.Queues) > 0 {
		put = &store.BucketConfiguration{}
		put.XML, _ = xml.Marshal(c)
	}
	if err := h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "notification", put); err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	// AWS checks every destination with a test event. pail sends one only to a
	// destination the bucket did not have before.
	old := map[string]bool{}
	for _, d := range append(slices.Clone(previous.Topics), previous.Queues...) {
		old[d.arn()] = true
	}
	for _, d := range append(slices.Clone(c.Topics), c.Queues...) {
		if !old[d.arn()] {
			h.deliver(context.WithoutCancel(r.Context()), r, t, d, h.testEventJSON(w, t.bucket))
		}
	}
	w.WriteHeader(http.StatusOK)
}

// notificationConfig reads the stored configuration. A missing or unreadable
// one is empty; an unreadable one is logged.
func (h *handler) notificationConfig(ctx context.Context, bucket string) notificationConfiguration {
	var c notificationConfiguration
	cfg, err := h.opts.Store.GetBucketConfiguration(ctx, bucket, "notification")
	if errors.Is(err, store.ErrNoSuchConfiguration) {
		return c
	}
	if err == nil {
		err = xml.Unmarshal(cfg.XML, &c)
	}
	if err != nil {
		clogS3api().Error("read notification configuration", "bucket", bucket, "error", err)
	}
	return c
}

// objectEvent is what happened to one object.
type objectEvent struct {
	name, key, eTag string
	size            *int64 // nil on a removal
	versionID       string // empty when the bucket has no version to name
}

func createdEvent(name, key string, info store.ObjectInfo) objectEvent {
	return objectEvent{name: name, key: key, eTag: info.ETag, size: &info.Size, versionID: info.VersionID}
}

// removedEvent describes a DeleteObject that asked for requested (empty for
// none) and got res. In a versioned bucket, a delete without a version adds a
// delete marker, and a delete with one removes it for good.
func removedEvent(key, requested string, res store.DeleteResult) objectEvent {
	ev := objectEvent{name: "ObjectRemoved:Delete", key: key}
	if res.Versioned {
		ev.versionID = versionLabel(res.VersionID)
		if requested == "" {
			ev.name = "ObjectRemoved:DeleteMarkerCreated"
		}
	}
	return ev
}

type (
	eventPrincipal struct {
		PrincipalID string `json:"principalId"`
	}
	eventRecord struct {
		EventVersion      string            `json:"eventVersion"`
		EventSource       string            `json:"eventSource"`
		AWSRegion         string            `json:"awsRegion"`
		EventTime         string            `json:"eventTime"`
		EventName         string            `json:"eventName"`
		UserIdentity      eventPrincipal    `json:"userIdentity"`
		RequestParameters map[string]string `json:"requestParameters"`
		ResponseElements  map[string]string `json:"responseElements"`
		S3                eventS3           `json:"s3"`
	}
	eventS3 struct {
		SchemaVersion   string      `json:"s3SchemaVersion"`
		ConfigurationID string      `json:"configurationId"`
		Bucket          eventBucket `json:"bucket"`
		Object          eventObject `json:"object"`
	}
	eventBucket struct {
		Name          string         `json:"name"`
		OwnerIdentity eventPrincipal `json:"ownerIdentity"`
		ARN           string         `json:"arn"`
	}
	eventObject struct {
		Key        string `json:"key"`
		Size       *int64 `json:"size,omitempty"`
		ETag       string `json:"eTag,omitempty"`
		VersionID  string `json:"versionId,omitempty"`
		Annotation *bool  `json:"hasObjectAnnotation,omitempty"`
		Sequencer  string `json:"sequencer"`
	}
)

// sequencer returns a hex string that grows with every call, as AWS's does per key.
func (h *handler) sequencer() string {
	for {
		last := h.sequence.Load()
		next := max(last+1, uint64(time.Now().UnixNano()))
		if h.sequence.CompareAndSwap(last, next) {
			return fmt.Sprintf("%018X", next)
		}
	}
}

// eventJSON renders the S3 event message for one configuration.
func (h *handler) eventJSON(w http.ResponseWriter, r *http.Request, bucket, configID string, ev objectEvent) string {
	ip := hostOnly(r.RemoteAddr)
	owner := h.bucketOwner().ID
	obj := eventObject{
		// AWS URL-encodes the key but keeps its slashes.
		Key:  strings.ReplaceAll(url.QueryEscape(ev.key), "%2F", "/"),
		Size: ev.size, ETag: ev.eTag, VersionID: ev.versionID, Sequencer: h.sequencer(),
	}
	if ev.name == "ObjectCreated:Copy" {
		obj.Annotation = new(false)
	}
	rec := eventRecord{
		EventVersion: "2.6", EventSource: "aws:s3", AWSRegion: h.opts.Region,
		EventTime: time.Now().UTC().Format(timeFormat), EventName: ev.name,
		UserIdentity:      eventPrincipal{"AWS:" + owner},
		RequestParameters: map[string]string{"sourceIPAddress": ip},
		ResponseElements:  map[string]string{"x-amz-request-id": w.Header().Get("x-amz-request-id"), "x-amz-id-2": w.Header().Get("x-amz-id-2")},
		S3: eventS3{
			SchemaVersion: "1.0", ConfigurationID: configID,
			Bucket: eventBucket{Name: bucket, OwnerIdentity: eventPrincipal{owner}, ARN: "arn:aws:s3:::" + bucket},
			Object: obj,
		},
	}
	out, _ := json.Marshal(struct {
		Records []eventRecord
	}{[]eventRecord{rec}})
	return string(out)
}

// testEventJSON renders the message AWS sends to check a new destination.
func (h *handler) testEventJSON(w http.ResponseWriter, bucket string) string {
	out, _ := json.Marshal(struct {
		Service   string
		Event     string
		Time      string
		Bucket    string
		RequestID string `json:"RequestId"`
		HostID    string `json:"HostId"`
	}{"Amazon S3", "s3:TestEvent", time.Now().UTC().Format(timeFormat), bucket,
		w.Header().Get("x-amz-request-id"), w.Header().Get("x-amz-id-2")})
	return string(out)
}

// notify sends ev to every destination whose configuration matches. It runs
// after the write commits, holds no store lock, and logs a failure instead of
// failing the request. pail delivers before it answers; AWS delivers later.
func (h *handler) notify(w http.ResponseWriter, r *http.Request, t target, ev objectEvent) {
	if h.opts.Notifier == nil {
		return
	}
	// The request may be canceled once the write has committed.
	ctx := context.WithoutCancel(r.Context())
	c := h.notificationConfig(ctx, t.bucket)
	for _, d := range slices.Concat(c.Topics, c.Queues) {
		if d.matches(ev.name, ev.key) {
			h.deliver(ctx, r, t, d, h.eventJSON(w, r, t.bucket, d.ID, ev))
		}
	}
}

// deliver sends message to the destination of d and logs a failure.
func (h *handler) deliver(ctx context.Context, r *http.Request, t target, d notificationTarget, message string) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if t.virtualHost {
		host = host[len(t.bucket)+1:] // parseTarget matched "<bucket>." case-insensitively
	}
	if err := h.opts.Notifier.Deliver(ctx, d.arn(), message, scheme+"://"+host); err != nil {
		clogS3api().Error("deliver notification", "bucket", t.bucket, "destination", d.arn(), "error", err)
	}
}
