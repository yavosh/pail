// Package acl defines S3 ACL grants shared by storage and the HTTP API.
package acl

import "encoding/xml"

const (
	// AllUsers includes anonymous requests.
	AllUsers = "http://acs.amazonaws.com/groups/global/AllUsers"
	// AuthenticatedUsers includes signed requests.
	AuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	// LogDelivery identifies S3's log delivery service.
	LogDelivery = "http://acs.amazonaws.com/groups/s3/LogDelivery"
	// AnonymousID is S3's canonical owner ID for anonymous uploads.
	AnonymousID = "65a011a29cdf8ec533ec3d1ccaae921c"
)

// Policy is the S3 ACL document attached to a bucket or object.
type Policy struct {
	XMLName xml.Name `xml:"AccessControlPolicy" json:"-"`
	Xmlns   string   `xml:"xmlns,attr,omitempty" json:"-"`
	Owner   Owner    `xml:"Owner" json:"owner"`
	Grants  []Grant  `xml:"AccessControlList>Grant" json:"grants"`
}

// Owner identifies the account that owns a resource.
type Owner struct {
	ID string `xml:"ID" json:"id"`
}

// Grant associates a grantee with one permission.
type Grant struct {
	Grantee    Grantee `xml:"Grantee" json:"grantee"`
	Permission string  `xml:"Permission" json:"permission"`
}

// Grantee identifies a canonical account or predefined group.
type Grantee struct {
	Type  string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr" json:"type"`
	ID    string `xml:"ID,omitempty" json:"id,omitempty"`
	URI   string `xml:"URI,omitempty" json:"uri,omitempty"`
	Email string `xml:"EmailAddress,omitempty" json:"email,omitempty"`
}

// Private grants the owner full control.
func Private(id string) Policy {
	return Policy{Owner: Owner{ID: id}, Grants: []Grant{{Grantee: Grantee{Type: "CanonicalUser", ID: id}, Permission: "FULL_CONTROL"}}}
}

// Public reports whether AllUsers has the requested permission.
func (p *Policy) Public(permission string) bool {
	for _, grant := range p.Grants {
		if grant.Grantee.Type == "Group" && grant.Grantee.URI == AllUsers && (grant.Permission == permission || grant.Permission == "FULL_CONTROL") {
			return true
		}
	}
	return false
}

// MarshalXML preserves the xsi prefix expected by S3 SDK decoders.
func (g Grantee) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xmlns:xsi"}, Value: "http://www.w3.org/2001/XMLSchema-instance"}, xml.Attr{Name: xml.Name{Local: "xsi:type"}, Value: g.Type})
	type fields struct {
		ID    string `xml:"ID,omitempty"`
		URI   string `xml:"URI,omitempty"`
		Email string `xml:"EmailAddress,omitempty"`
	}
	return enc.EncodeElement(fields{ID: g.ID, URI: g.URI, Email: g.Email}, start)
}
