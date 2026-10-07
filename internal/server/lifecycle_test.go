package server

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func TestLifecycleWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys, err := localdisk.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer fsys.Close()
		st, err := store.Open(t.Context(), fsys)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.CreateBucket(t.Context(), "expired"); err != nil {
			t.Fatal(err)
		}
		s := &Server{store: st}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); s.runLifecycle(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		if _, err := st.PutObject(t.Context(), "expired", "key", strings.NewReader("body"), store.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		prefix := ""
		body, err := xml.Marshal(lifecycle.Configuration{Rules: []lifecycle.Rule{{Status: "Enabled", Prefix: &prefix, Expiration: &lifecycle.Expiration{Date: "2000-01-01T00:00:00Z"}}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.PutBucketConfiguration(t.Context(), "expired", "lifecycle", &store.BucketConfiguration{XML: body}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if _, err := st.HeadObject(t.Context(), "expired", "key"); !errors.Is(err, store.ErrNoSuchKey) {
			t.Fatalf("worker expiration = %v, want ErrNoSuchKey", err)
		}
	})
}
