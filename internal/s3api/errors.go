package s3api

import (
	"encoding/xml"
	"net/http"
	"strconv"
)

// apiError is an S3 error: the code clients match on, and its HTTP status.
type apiError struct {
	Code    string
	Status  int
	Message string
}

var errNotImplemented = apiError{"NotImplemented", http.StatusNotImplemented, "A header or query you provided implies functionality that is not implemented."}

type errorBody struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// writeError sends e as an S3 XML error. A HEAD response gets the status only.
func writeError(w http.ResponseWriter, r *http.Request, e apiError) {
	body, err := xml.Marshal(errorBody{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		RequestID: w.Header().Get("x-amz-request-id"),
	})
	if err != nil {
		clogS3api().Error("marshal error body", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	body = append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(e.Status)
	_, _ = w.Write(body)
}
