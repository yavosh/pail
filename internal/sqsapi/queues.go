package sqsapi

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/yavosh/pail/internal/queue"
)

const maxListResults = 1000

// queueURL builds the URL of a queue from the request's own scheme and host,
// so the URL works for whatever name the client used to reach pail.
func queueURL(r *http.Request, name string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/" + queue.Account + "/" + name
}

// queueName returns the queue name in a queue URL. The host is ignored. A URL
// that does not name a queue of pail's account means the queue does not exist.
func queueName(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("QueueUrl is required: %w", errMissingParam)
	}
	notFound := fmt.Errorf("queue URL %q: %w", rawURL, queue.ErrQueueDoesNotExist)
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", notFound
	}
	account, name, ok := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	if !ok || account != queue.Account || name == "" || strings.Contains(name, "/") {
		return "", notFound
	}
	return name, nil
}

type createQueueRequest struct {
	QueueName  string
	Attributes map[string]string
	Tags       map[string]string `json:"tags"`
}

type queueURLResponse struct {
	QueueURL string `json:"QueueUrl"`
}

func (h *handler) createQueue(r *http.Request, in createQueueRequest) (any, error) {
	if in.QueueName == "" {
		return nil, fmt.Errorf("QueueName is required: %w", queue.ErrInvalidParameterValue)
	}
	if err := h.queues.CreateQueue(r.Context(), in.QueueName, in.Attributes, in.Tags); err != nil {
		return nil, err
	}
	return queueURLResponse{queueURL(r, in.QueueName)}, nil
}

type getQueueURLRequest struct {
	QueueName string
	// QueueOwnerAWSAccountId is ignored: pail serves one account.
}

func (h *handler) getQueueURL(r *http.Request, in getQueueURLRequest) (any, error) {
	if in.QueueName == "" {
		return nil, fmt.Errorf("QueueName is required: %w", queue.ErrInvalidParameterValue)
	}
	if err := h.queues.Lookup(r.Context(), in.QueueName); err != nil {
		return nil, err
	}
	return queueURLResponse{queueURL(r, in.QueueName)}, nil
}

// queueRequest is the input of the operations that take only a queue URL.
type queueRequest struct {
	QueueURL string `json:"QueueUrl"`
}

func (h *handler) deleteQueue(r *http.Request, in queueRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	return nil, h.queues.DeleteQueue(r.Context(), name)
}

func (h *handler) purgeQueue(r *http.Request, in queueRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	return nil, h.queues.Purge(r.Context(), name)
}

type listQueuesRequest struct {
	QueueNamePrefix string
	MaxResults      *int
	NextToken       string
}

type listQueuesResponse struct {
	QueueURLs []string `json:"QueueUrls,omitempty"`
	NextToken string   `json:",omitempty"`
}

// The token is the last name of the previous page, so it needs no state.
func (h *handler) listQueues(r *http.Request, in listQueuesRequest) (any, error) {
	limit := 0
	if in.MaxResults != nil {
		limit = *in.MaxResults
		if limit < 1 || limit > maxListResults {
			return nil, fmt.Errorf("MaxResults %d must be from 1 to %d: %w", limit, maxListResults, queue.ErrInvalidParameterValue)
		}
	}
	var after string
	if in.NextToken != "" {
		b, err := base64.RawURLEncoding.DecodeString(in.NextToken)
		if err != nil {
			return nil, fmt.Errorf("NextToken is not valid: %w", queue.ErrInvalidParameterValue)
		}
		after = string(b)
	}
	names, next, err := h.queues.ListQueues(r.Context(), in.QueueNamePrefix, limit, after)
	if err != nil {
		return nil, err
	}
	out := listQueuesResponse{}
	for _, n := range names {
		out.QueueURLs = append(out.QueueURLs, queueURL(r, n))
	}
	if next != "" {
		out.NextToken = base64.RawURLEncoding.EncodeToString([]byte(next))
	}
	return out, nil
}

type getQueueAttributesRequest struct {
	QueueURL       string `json:"QueueUrl"`
	AttributeNames []string
}

type getQueueAttributesResponse struct {
	Attributes map[string]string `json:",omitempty"`
}

func (h *handler) getQueueAttributes(r *http.Request, in getQueueAttributesRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	attrs, err := h.queues.Attributes(r.Context(), name, in.AttributeNames)
	if err != nil {
		return nil, err
	}
	return getQueueAttributesResponse{attrs}, nil
}

type setQueueAttributesRequest struct {
	QueueURL   string `json:"QueueUrl"`
	Attributes map[string]string
}

func (h *handler) setQueueAttributes(r *http.Request, in setQueueAttributesRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	return nil, h.queues.SetAttributes(r.Context(), name, in.Attributes)
}

type tagQueueRequest struct {
	QueueURL string `json:"QueueUrl"`
	Tags     map[string]string
}

func (h *handler) tagQueue(r *http.Request, in tagQueueRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	return nil, h.queues.Tag(r.Context(), name, in.Tags)
}

type untagQueueRequest struct {
	QueueURL string `json:"QueueUrl"`
	TagKeys  []string
}

func (h *handler) untagQueue(r *http.Request, in untagQueueRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	return nil, h.queues.Untag(r.Context(), name, in.TagKeys)
}

type listQueueTagsResponse struct {
	Tags map[string]string `json:",omitempty"`
}

func (h *handler) listQueueTags(r *http.Request, in queueRequest) (any, error) {
	name, err := queueName(in.QueueURL)
	if err != nil {
		return nil, err
	}
	tags, err := h.queues.Tags(r.Context(), name)
	if err != nil {
		return nil, err
	}
	return listQueueTagsResponse{tags}, nil
}
