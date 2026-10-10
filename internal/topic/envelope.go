package topic

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/yavosh/pail/internal/queue"
)

const timestampFormat = "2006-01-02T15:04:05.000Z"

// notification is the data of one SNS Notification envelope.
type notification struct {
	messageID, topicARN, subject, message, timestamp string
	signatureVersion, signature                      string
	baseURL, subscriptionARN                         string
	attrs                                            map[string]MessageAttribute
}

// stringToSign is the canonical text that SNS signs for a Notification.
func (n notification) stringToSign() string {
	var b strings.Builder
	b.WriteString("Message\n" + n.message + "\nMessageId\n" + n.messageID + "\n")
	if n.subject != "" {
		b.WriteString("Subject\n" + n.subject + "\n")
	}
	b.WriteString("Timestamp\n" + n.timestamp + "\nTopicArn\n" + n.topicARN + "\nType\nNotification\n")
	return b.String()
}

// quote returns s as a JSON string without HTML escaping.
func quote(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // a string always encodes
	return strings.TrimSuffix(b.String(), "\n")
}

// envelope returns the JSON body of an envelope delivery. A map would sort
// the keys, so it writes them in AWS's order.
func (n notification) envelope() string {
	var b strings.Builder
	field := func(key, value string) { b.WriteString(`,"` + key + `":` + quote(value)) }
	b.WriteString(`{"Type":"Notification"`)
	field("MessageId", n.messageID)
	field("TopicArn", n.topicARN)
	if n.subject != "" {
		field("Subject", n.subject)
	}
	field("Message", n.message)
	field("Timestamp", n.timestamp)
	field("SignatureVersion", n.signatureVersion)
	field("Signature", n.signature)
	field("SigningCertURL", n.baseURL+"/_pail/sns/signing-cert.pem")
	field("UnsubscribeURL", n.baseURL+"/?Action=Unsubscribe&SubscriptionArn="+n.subscriptionARN)
	if len(n.attrs) > 0 {
		b.WriteString(`,"MessageAttributes":{`)
		for i, name := range slices.Sorted(maps.Keys(n.attrs)) {
			a := n.attrs[name]
			value := a.StringValue
			if len(a.BinaryValue) > 0 {
				value = base64.StdEncoding.EncodeToString(a.BinaryValue)
			}
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(quote(name) + `:{"Type":` + quote(a.DataType) + `,"Value":` + quote(value) + `}`)
		}
		b.WriteByte('}')
	}
	b.WriteByte('}')
	return b.String()
}

// confirmation is the data of one SNS SubscriptionConfirmation envelope.
type confirmation struct {
	messageID, topicARN, token, timestamp string
	signatureVersion, signature, baseURL  string
	unsubscribe                           string // the removed subscription's ARN, for an UnsubscribeConfirmation
}

func (c confirmation) typ() string {
	if c.unsubscribe != "" {
		return "UnsubscribeConfirmation"
	}
	return "SubscriptionConfirmation"
}

func (c confirmation) message() string {
	if c.unsubscribe != "" {
		return "You have chosen to deactivate subscription " + c.unsubscribe + ".\nTo cancel this operation and restore the subscription, visit the SubscribeURL included in this message."
	}
	return "You have chosen to subscribe to the topic " + c.topicARN + ".\nTo confirm the subscription, visit the SubscribeURL included in this message."
}

func (c confirmation) subscribeURL() string {
	return c.baseURL + "/?Action=ConfirmSubscription&TopicArn=" + c.topicARN + "&Token=" + c.token
}

// stringToSign is the canonical text that SNS signs for a SubscriptionConfirmation or UnsubscribeConfirmation.
func (c confirmation) stringToSign() string {
	return "Message\n" + c.message() + "\nMessageId\n" + c.messageID + "\nSubscribeURL\n" + c.subscribeURL() +
		"\nTimestamp\n" + c.timestamp + "\nToken\n" + c.token + "\nTopicArn\n" + c.topicARN + "\nType\n" + c.typ() + "\n"
}

// envelope returns the JSON body of the confirmation, with the keys in AWS's order.
func (c confirmation) envelope() string {
	var b strings.Builder
	b.WriteString(`{"Type":` + quote(c.typ()))
	for _, f := range [][2]string{
		{"MessageId", c.messageID}, {"Token", c.token}, {"TopicArn", c.topicARN}, {"Message", c.message()},
		{"SubscribeURL", c.subscribeURL()}, {"Timestamp", c.timestamp}, {"SignatureVersion", c.signatureVersion},
		{"Signature", c.signature}, {"SigningCertURL", c.baseURL + "/_pail/sns/signing-cert.pem"},
	} {
		b.WriteString(`,"` + f[0] + `":` + quote(f[1]))
	}
	b.WriteByte('}')
	return b.String()
}

// rawAttributes maps SNS message attributes to SQS message attributes for raw delivery.
func rawAttributes(attrs map[string]MessageAttribute) map[string]queue.MessageAttribute {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]queue.MessageAttribute, len(attrs))
	for name, a := range attrs {
		out[name] = queue.MessageAttribute{DataType: a.DataType, StringValue: a.StringValue, BinaryValue: a.BinaryValue}
	}
	return out
}
