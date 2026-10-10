package s3api

import (
	"encoding/hex"
	"encoding/xml"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/store"
)

func (h *handler) resourceACL(r *http.Request, t target) (acl.Policy, error) {
	if t.key != "" {
		info, err := h.opts.Store.HeadObjectVersion(r.Context(), t.bucket, t.key, r.URL.Query().Get("versionId"))
		if err != nil {
			return acl.Policy{}, err
		}
		if info.ACL != nil {
			return *info.ACL, nil
		}
		return acl.Private(h.bucketOwner().ID), nil
	}
	cfg, err := h.opts.Store.GetBucketConfiguration(r.Context(), t.bucket, "acl")
	if errors.Is(err, store.ErrNoSuchConfiguration) {
		return acl.Private(h.bucketOwner().ID), nil
	}
	if err != nil {
		return acl.Policy{}, err
	}
	var policy acl.Policy
	if err := xml.Unmarshal(cfg.XML, &policy); err != nil {
		return acl.Policy{}, err
	}
	return policy, nil
}

func (h *handler) handleACL(w http.ResponseWriter, r *http.Request, t target) {
	if !validBucketName(t.bucket) {
		writeError(w, r, errInvalidBucketName)
		return
	}
	current, err := h.resourceACL(r, t)
	if err != nil {
		writeReadError(w, r, err)
		return
	}
	enforced, err := h.ownerEnforced(r.Context(), t.bucket)
	if err != nil {
		writeError(w, r, toAPIError(err))
		return
	}
	if enforced && r.Method == http.MethodPut {
		writeError(w, r, errACLNotSupported)
		return
	}
	if enforced {
		current = acl.Private(h.bucketOwner().ID)
	}
	if r.Method == http.MethodGet {
		current.Xmlns = s3Namespace
		writeXML(w, r, http.StatusOK, current)
		return
	}
	policy, e, ok := h.requestACL(r.Header, t.key == "", current.Owner.ID)
	if !ok {
		writeError(w, r, e)
		return
	}
	body, e, ok := readConfiguration(w, r, false)
	if !ok {
		writeError(w, r, e)
		return
	}
	if len(body) != 0 {
		if hasACLHeaders(r.Header) {
			writeError(w, r, errInvalidRequestACL)
			return
		}
		if err := decodeXMLDocument(body, &policy); err != nil {
			writeError(w, r, errMalformedXML)
			return
		}
		if e := validateACL(policy, current.Owner.ID); e != (apiError{}) {
			writeError(w, r, e)
			return
		}
	} else if !hasACLHeaders(r.Header) {
		writeError(w, r, errMalformedXML)
		return
	}
	if t.key == "" {
		body, _ := xml.Marshal(policy)
		err = h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "acl", &store.BucketConfiguration{XML: body})
	} else {
		err = h.opts.Store.PutObjectACL(r.Context(), t.bucket, t.key, r.URL.Query().Get("versionId"), policy)
	}
	if err != nil {
		writeReadError(w, r, err)
		return
	}
	setRequestedVersion(w, r.URL.Query().Get("versionId"))
	w.WriteHeader(http.StatusOK)
}

var errInvalidRequestACL = apiError{"InvalidRequest", http.StatusBadRequest, "Specifying both Canned ACLs and Header Grants is not allowed"}

var grantHeaders = []struct{ header, permission string }{
	{"x-amz-grant-read", "READ"}, {"x-amz-grant-write", "WRITE"},
	{"x-amz-grant-read-acp", "READ_ACP"}, {"x-amz-grant-write-acp", "WRITE_ACP"},
	{"x-amz-grant-full-control", "FULL_CONTROL"},
}

func hasACLHeaders(headers http.Header) bool {
	if headers.Get("x-amz-acl") != "" {
		return true
	}
	return slices.ContainsFunc(grantHeaders, func(g struct{ header, permission string }) bool { return headers.Get(g.header) != "" })
}

func (h *handler) requestACL(headers http.Header, bucket bool, ownerID string) (acl.Policy, apiError, bool) {
	policy := acl.Private(ownerID)
	canned := headers.Get("x-amz-acl")
	hasGrants := slices.ContainsFunc(grantHeaders, func(g struct{ header, permission string }) bool { return headers.Get(g.header) != "" })
	if canned != "" && hasGrants {
		return policy, errInvalidRequestACL, false
	}
	addGroup := func(uri, permission string) {
		policy.Grants = append(policy.Grants, acl.Grant{Grantee: acl.Grantee{Type: "Group", URI: uri}, Permission: permission})
	}
	switch canned {
	case "", "private":
	case "bucket-owner-read", "bucket-owner-full-control":
		if !bucket && ownerID != h.bucketOwner().ID {
			permission := "READ"
			if canned == "bucket-owner-full-control" {
				permission = "FULL_CONTROL"
			}
			policy.Grants = append(policy.Grants, acl.Grant{Grantee: acl.Grantee{Type: "CanonicalUser", ID: h.bucketOwner().ID}, Permission: permission})
		}
	case "public-read":
		addGroup(acl.AllUsers, "READ")
	case "public-read-write":
		addGroup(acl.AllUsers, "READ")
		addGroup(acl.AllUsers, "WRITE")
	case "authenticated-read":
		addGroup(acl.AuthenticatedUsers, "READ")
	case "log-delivery-write":
		if !bucket {
			return policy, errInvalidArgument, false
		}
		addGroup(acl.LogDelivery, "WRITE")
		addGroup(acl.LogDelivery, "READ_ACP")
	default:
		return policy, errInvalidArgument, false
	}
	if hasGrants {
		policy.Grants = nil
		for _, g := range grantHeaders {
			for _, raw := range headers.Values(g.header) {
				for value := range strings.SplitSeq(raw, ",") {
					kind, quoted, ok := strings.Cut(strings.TrimSpace(value), "=")
					grantee, err := strconv.Unquote(strings.TrimSpace(quoted))
					if !ok || err != nil {
						return policy, errInvalidArgument, false
					}
					grant := acl.Grant{Permission: g.permission}
					switch kind {
					case "id":
						grant.Grantee = acl.Grantee{Type: "CanonicalUser", ID: grantee}
					case "uri":
						grant.Grantee = acl.Grantee{Type: "Group", URI: grantee}
					case "emailAddress":
						return policy, errNotImplemented, false
					default:
						return policy, errInvalidArgument, false
					}
					policy.Grants = append(policy.Grants, grant)
				}
			}
		}
	}
	if e := validateACL(policy, ownerID); e != (apiError{}) {
		return policy, e, false
	}
	return policy, apiError{}, true
}

func validateACL(policy acl.Policy, ownerID string) apiError {
	if policy.Owner.ID != ownerID {
		return errInvalidArgument
	}
	if len(policy.Grants) > 100 {
		return errInvalidArgument
	}
	for _, grant := range policy.Grants {
		if !slices.Contains([]string{"READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"}, grant.Permission) {
			return errInvalidArgument
		}
		g := grant.Grantee
		switch g.Type {
		case "CanonicalUser":
			raw, err := hex.DecodeString(g.ID)
			if err != nil || len(raw) != 32 && g.ID != acl.AnonymousID || g.URI != "" || g.Email != "" {
				return errInvalidArgument
			}
		case "Group":
			if !slices.Contains([]string{acl.AllUsers, acl.AuthenticatedUsers, acl.LogDelivery}, g.URI) || g.ID != "" || g.Email != "" {
				return errInvalidArgument
			}
		case "AmazonCustomerByEmail":
			return errNotImplemented
		default:
			return errInvalidArgument
		}
	}
	return apiError{}
}

func (h *handler) anonymousAllowed(r *http.Request, t target, op operation) bool {
	if r.Header.Get("Authorization") != "" {
		return false
	}
	for _, query := range []string{"X-Amz-Algorithm", "X-Amz-Signature", "X-Amz-Credential", "X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Expires", "X-Amz-Security-Token", "AWSAccessKeyId", "Signature"} {
		if r.URL.Query().Has(query) {
			return false
		}
	}
	permission := ""
	resource := t
	switch op {
	case opGetObject, opHeadObject:
		for query := range responseOverrides {
			if r.URL.Query().Has(query) {
				return false
			}
		}
		permission = "READ"
	case opGetObjectACL, opGetBucketACL:
		permission = "READ_ACP"
	case opPutObjectACL, opPutBucketACL:
		permission = "WRITE_ACP"
	case opListObjects, opListObjectsV2, opListMultipartUploads, opHeadBucket:
		resource.key = ""
		permission = "READ"
	case opPutObject:
		resource.key = ""
		permission = "WRITE"
	default:
		return false
	}
	// Under BucketOwnerEnforced, stored grants grant nothing.
	if enforced, err := h.ownerEnforced(r.Context(), t.bucket); err != nil || enforced {
		return false
	}
	policy, err := h.resourceACL(r, resource)
	if err != nil || !policy.Public(permission) {
		return false
	}
	if op == opPutObject {
		current, err := h.resourceACL(r, t)
		return errors.Is(err, store.ErrNoSuchKey) || err == nil && current.Owner.ID == acl.AnonymousID
	}
	return true
}

func (h *handler) writeACL(r *http.Request, t target, bucket bool) (acl.Policy, apiError, bool) {
	if e, ok := h.checkACLsEnabled(r.Context(), t.bucket, r.Header); !ok {
		return acl.Policy{}, e, false
	}
	return h.requestPolicy(r, bucket)
}

// requestPolicy builds the ACL a write request asks for, owned by its caller.
func (h *handler) requestPolicy(r *http.Request, bucket bool) (acl.Policy, apiError, bool) {
	ownerID := h.bucketOwner().ID
	if anonymousRequest(r) {
		ownerID = acl.AnonymousID
	}
	return h.requestACL(r.Header, bucket, ownerID)
}

// anonymousRequest is used after routing has rejected invalid authentication.
func anonymousRequest(r *http.Request) bool {
	return r.Header.Get("Authorization") == "" && !r.URL.Query().Has("X-Amz-Signature") && !r.URL.Query().Has("Signature")
}

// storeBucketACL persists the ACL accepted during bucket creation.
func (h *handler) storeBucketACL(w http.ResponseWriter, r *http.Request, t target, policy acl.Policy) bool {
	body, err := xml.Marshal(policy)
	if err == nil {
		err = h.opts.Store.PutBucketConfiguration(r.Context(), t.bucket, "acl", &store.BucketConfiguration{XML: body})
	}
	if err != nil {
		writeError(w, r, toAPIError(err))
		return false
	}
	return true
}
