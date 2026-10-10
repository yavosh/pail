package snsapi

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/yavosh/pail/internal/topic"
)

// params is a decoded query-protocol form. Locations follow the service
// model: maps are "<Name>.entry.N.key" and "<Name>.entry.N.value", and lists
// are "<Name>.member.N". N counts from 1, and a list ends at the first gap.
type params url.Values

func (p params) get(name string) string { return url.Values(p).Get(name) }

func (p params) has(name string) bool { return url.Values(p).Has(name) }

// required returns a parameter that must be present and not empty.
func (p params) required(name string) (string, error) {
	v := p.get(name)
	if v == "" {
		return "", fmt.Errorf("%s is required: %w", name, topic.ErrInvalidParameter)
	}
	return v, nil
}

// hasPrefix reports whether any parameter name starts with prefix.
func (p params) hasPrefix(prefix string) bool {
	for k := range p {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

// attributes decodes a map of strings, such as Attributes.entry.1.key.
func (p params) attributes(name string) map[string]string {
	var out map[string]string
	for n := 1; p.has(entryName(name, n, "key")); n++ {
		if out == nil {
			out = map[string]string{}
		}
		out[p.get(entryName(name, n, "key"))] = p.get(entryName(name, n, "value"))
	}
	return out
}

func entryName(name string, n int, field string) string {
	return name + ".entry." + strconv.Itoa(n) + "." + field
}

func memberName(name string, n int) string { return name + ".member." + strconv.Itoa(n) }

// tags decodes Tags.member.N.Key and Tags.member.N.Value.
func (p params) tags(name string) map[string]string {
	var out map[string]string
	for n := 1; p.has(memberName(name, n)+".Key") || p.has(memberName(name, n)+".Value"); n++ {
		if out == nil {
			out = map[string]string{}
		}
		out[p.get(memberName(name, n)+".Key")] = p.get(memberName(name, n) + ".Value")
	}
	return out
}

// list decodes a list of strings, such as TagKeys.member.N.
func (p params) list(name string) []string {
	var out []string
	for n := 1; p.has(memberName(name, n)); n++ {
		out = append(out, p.get(memberName(name, n)))
	}
	return out
}

// messageAttributes decodes MessageAttributes.entry.N.Name and .Value.*.
func (p params) messageAttributes(name string) (map[string]topic.MessageAttribute, error) {
	var out map[string]topic.MessageAttribute
	for n := 1; p.has(entryName(name, n, "Name")); n++ {
		value := entryName(name, n, "Value")
		a := topic.MessageAttribute{DataType: p.get(value + ".DataType"), StringValue: p.get(value + ".StringValue")}
		if enc := p.get(value + ".BinaryValue"); enc != "" {
			b, err := base64.StdEncoding.DecodeString(enc)
			if err != nil {
				return nil, fmt.Errorf("message attribute %q: BinaryValue is not base64: %w", p.get(entryName(name, n, "Name")), topic.ErrInvalidParameter)
			}
			a.BinaryValue = b
		}
		if out == nil {
			out = map[string]topic.MessageAttribute{}
		}
		out[p.get(entryName(name, n, "Name"))] = a
	}
	return out, nil
}

// xmlBuf writes the XML inside a Result element.
type xmlBuf struct{ strings.Builder }

func (x *xmlBuf) open(name string)  { x.WriteString("<" + name + ">") }
func (x *xmlBuf) close(name string) { x.WriteString("</" + name + ">") }

func (x *xmlBuf) elem(name, text string) {
	x.open(name)
	_ = xml.EscapeText(x, []byte(text)) // a Builder never fails to write
	x.close(name)
}

// attributeMap writes <Attributes><entry><key/><value/></entry>...</Attributes> in order.
func (x *xmlBuf) attributeMap(attrs []topic.Attribute) {
	x.open("Attributes")
	for _, a := range attrs {
		x.open("entry")
		x.elem("key", a.Key)
		x.elem("value", a.Value)
		x.close("entry")
	}
	x.close("Attributes")
}
