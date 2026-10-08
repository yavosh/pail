package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
)

func TestDeleteObjectRejectsAmbiguousCondition(t *testing.T) {
	srv, st := storeServer(t, "")
	if err := st.CreateBucket(t.Context(), "bucket"); err != nil {
		t.Fatal(err)
	}
	info, err := st.PutObject(t.Context(), "bucket", "key", strings.NewReader("original"), store.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, srv.URL+"/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Add("If-Match", quoteETag(info.ETag))
	r.Header.Add("If-Match", `"wrong"`)
	signRequest(t, r, time.Now())
	resp, err := srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result errorBody
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || result.Code != "InvalidArgument" {
		t.Errorf("duplicate If-Match = %d %q, want 400 InvalidArgument", resp.StatusCode, result.Code)
	}
	f, after, err := st.GetObject(t.Context(), "bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil || string(body) != "original" || after.ETag != info.ETag {
		t.Errorf("object after ambiguous delete = %q, ETag %q, error %v, want original, %q, nil", body, after.ETag, err, info.ETag)
	}
}
