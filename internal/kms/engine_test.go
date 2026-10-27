package kms

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/vfs"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func openEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	fsys, err := localdisk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	e, err := Open(t.Context(), fsys, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func mustCreate(t *testing.T, e *Engine) Metadata {
	t.Helper()
	k, err := e.Create(t.Context(), CreateInput{})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func wantCode(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" && err == nil {
		return
	}
	api, ok := errors.AsType[*Error](err)
	if !ok || api.Code != want {
		t.Fatalf("error = %v, want code %q", err, want)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	k := mustCreate(t, e)
	ctx := t.Context()
	if err := e.ChangeAlias(ctx, "alias/test", k.KeyID, "CreateAlias"); err != nil {
		t.Fatal(err)
	}
	if err := e.ChangeTags(ctx, k.KeyID, []Tag{{"env", "test"}}, nil); err != nil {
		t.Fatal(err)
	}
	policy := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"kms:*","Resource":"*"}]}`
	if _, err := e.Policy(ctx, k.KeyID, "default", &policy); err != nil {
		t.Fatal(err)
	}
	enc, err := e.Encrypt(ctx, CryptoInput{KeyID: "alias/test", Data: []byte("test payload")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Change(ctx, k.KeyID, "DisableKey", "", 0); err != nil {
		t.Fatal(err)
	}
	reopened := openEngine(t, dir)
	m, err := reopened.Describe(ctx, "alias/test")
	if err != nil || m.KeyID != k.KeyID || m.Enabled || m.KeyState != "Disabled" {
		t.Fatalf("reopened metadata = %v, %v, want disabled original key", m, err)
	}
	tags, _, err := reopened.ListTags(ctx, k.KeyID, 50, "")
	if err != nil || !reflect.DeepEqual(tags, []Tag{{"env", "test"}}) {
		t.Fatalf("reopened tags = %v, %v", tags, err)
	}
	gotPolicy, err := reopened.Policy(ctx, k.KeyID, "default", nil)
	if err != nil || gotPolicy != policy {
		t.Fatalf("reopened policy differs, error = %v", err)
	}
	if _, err := reopened.Change(ctx, k.KeyID, "EnableKey", "", 0); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Decrypt(ctx, CryptoInput{Data: enc.CiphertextBlob})
	if err != nil || !bytes.Equal(got.Plaintext, []byte("test payload")) {
		t.Fatalf("decrypt after restart error = %v, plaintext matches = %v", err, bytes.Equal(got.Plaintext, []byte("test payload")))
	}
	info, err := os.Stat(filepath.Join(dir, stateFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state file permissions = %v, %v; want 0600", info, err)
	}
}

func TestEncryption(t *testing.T) {
	e := openEngine(t, t.TempDir())
	k, other := mustCreate(t, e), mustCreate(t, e)
	ctx := t.Context()
	context := map[string]string{"purpose": "test", "tenant": "one", "empty": ""}
	data := bytes.Repeat([]byte{0, 1, 255, 127}, 1024)
	enc, err := e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Context: context, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Context: context, Data: data})
	if err != nil || bytes.Equal(enc.CiphertextBlob, second.CiphertextBlob) {
		t.Fatalf("independent encryptions must differ, error = %v", err)
	}
	bad := bytes.Clone(enc.CiphertextBlob)
	bad[len(bad)-1] ^= 1
	badHeader := bytes.Clone(enc.CiphertextBlob)
	copy(badHeader[len(blobPrefix):], other.KeyID)
	tests := []struct {
		name string
		in   CryptoInput
		code string
	}{
		{"implicit key", CryptoInput{Data: enc.CiphertextBlob, Context: context}, ""},
		{"key arn", CryptoInput{KeyID: k.ARN, Data: enc.CiphertextBlob, Context: context}, ""},
		{"context ordering", CryptoInput{Data: enc.CiphertextBlob, Context: map[string]string{"empty": "", "tenant": "one", "purpose": "test"}}, ""},
		{"missing context", CryptoInput{Data: enc.CiphertextBlob}, "InvalidCiphertextException"},
		{"different context", CryptoInput{Data: enc.CiphertextBlob, Context: map[string]string{"purpose": "TEST", "tenant": "one"}}, "InvalidCiphertextException"},
		{"wrong key", CryptoInput{KeyID: other.KeyID, Data: enc.CiphertextBlob, Context: context}, "IncorrectKeyException"},
		{"tampered body", CryptoInput{Data: bad, Context: context}, "InvalidCiphertextException"},
		{"tampered key header", CryptoInput{Data: badHeader, Context: context}, "InvalidCiphertextException"},
		{"truncated", CryptoInput{Data: enc.CiphertextBlob[:20], Context: context}, "InvalidCiphertextException"},
		{"unknown format", CryptoInput{Data: bytes.Repeat([]byte("x"), 128)}, "InvalidCiphertextException"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := e.Decrypt(t.Context(), tt.in)
			wantCode(t, err, tt.code)
			if tt.code == "" && !bytes.Equal(got.Plaintext, data) {
				t.Error("decrypted payload differs")
			}
		})
	}
	for _, size := range []int{0, 4097} {
		_, err := e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Data: make([]byte, size)})
		wantCode(t, err, "ValidationException")
	}
	for _, context := range []map[string]string{{"": "value"}, {"key": strings.Repeat("x", 8193)}} {
		_, err := e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Data: []byte("x"), Context: context})
		wantCode(t, err, "ValidationException")
	}
	_, err = e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Data: []byte("x"), Algorithm: "RSAES_OAEP_SHA_256"})
	wantCode(t, err, "InvalidKeyUsageException")
}

func TestReEncryptAndDataKeys(t *testing.T) {
	e := openEngine(t, t.TempDir())
	source, destination := mustCreate(t, e), mustCreate(t, e)
	ctx := t.Context()
	for _, size := range []int{1, 16, 32, 1024} {
		for _, plaintext := range []bool{false, true} {
			generated, err := e.GenerateDataKey(ctx, CryptoInput{KeyID: source.KeyID}, size, plaintext)
			if err != nil {
				t.Fatal(err)
			}
			if (len(generated.Plaintext) == size) != plaintext {
				t.Fatalf("data key plaintext length = %d, size = %d, requested = %v", len(generated.Plaintext), size, plaintext)
			}
			decrypted, err := e.Decrypt(ctx, CryptoInput{Data: generated.CiphertextBlob})
			if err != nil || len(decrypted.Plaintext) != size || (plaintext && !bytes.Equal(generated.Plaintext, decrypted.Plaintext)) {
				t.Fatalf("data key decrypt, size = %d, error = %v", size, err)
			}
			context := map[string]string{"purpose": "new"}
			reencrypted, id, err := e.ReEncrypt(ctx, CryptoInput{Data: generated.CiphertextBlob}, CryptoInput{KeyID: destination.KeyID, Context: context})
			if err != nil || id != source.ARN || reencrypted.KeyID != destination.ARN {
				t.Fatalf("ReEncrypt identifiers = %q, %q; error = %v", id, reencrypted.KeyID, err)
			}
			got, err := e.Decrypt(ctx, CryptoInput{Data: reencrypted.CiphertextBlob, Context: context})
			if err != nil || !bytes.Equal(got.Plaintext, decrypted.Plaintext) {
				t.Fatalf("ReEncrypt payload differs, error = %v", err)
			}
		}
	}
}

func TestKeyDeletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		e := openEngine(t, dir)
		k := mustCreate(t, e)
		ctx := t.Context()
		if err := e.ChangeAlias(ctx, "alias/expire", k.KeyID, "CreateAlias"); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Change(ctx, k.KeyID, "ScheduleKeyDeletion", "", 7); err != nil {
			t.Fatal(err)
		}
		_, err := e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Data: []byte("x")})
		wantCode(t, err, "KMSInvalidStateException")
		if _, err := e.Policy(ctx, k.KeyID, "default", nil); err != nil {
			t.Fatalf("GetKeyPolicy while pending deletion: %v", err)
		}
		if _, _, err := e.ListTags(ctx, k.KeyID, 50, ""); err != nil {
			t.Fatalf("ListResourceTags while pending deletion: %v", err)
		}
		m, err := e.Change(ctx, k.KeyID, "CancelKeyDeletion", "", 0)
		if err != nil || m.KeyState != "Disabled" || m.DeletionDate != 0 {
			t.Fatalf("CancelKeyDeletion metadata = %v, error = %v", m, err)
		}
		_, err = e.Encrypt(ctx, CryptoInput{KeyID: k.KeyID, Data: []byte("x")})
		wantCode(t, err, "DisabledException")
		if _, err := e.Change(ctx, k.KeyID, "EnableKey", "", 0); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Change(ctx, k.KeyID, "ScheduleKeyDeletion", "", 7); err != nil {
			t.Fatal(err)
		}
		time.Sleep(7*24*time.Hour - time.Second)
		if _, err := e.Describe(ctx, k.KeyID); err != nil {
			t.Fatalf("key expired before its deadline: %v", err)
		}
		time.Sleep(time.Second)
		_, err = e.Describe(ctx, k.KeyID)
		wantCode(t, err, "NotFoundException")
		aliases, _, err := e.ListAliases(ctx, "", 100, "")
		if err != nil || len(aliases) != 0 {
			t.Fatalf("aliases after expiry = %v, %v", aliases, err)
		}
		_, err = openEngine(t, dir).Describe(ctx, k.KeyID)
		wantCode(t, err, "NotFoundException")
	})
}

type failingFS struct {
	FS
	fail bool
}

func (f *failingFS) CreatePrivateTemp() (vfs.TempFile, error) {
	if f.fail {
		return nil, errors.New("injected storage failure")
	}
	return f.FS.CreatePrivateTemp()
}

func TestFailedCommitAndCancellation(t *testing.T) {
	e := openEngine(t, t.TempDir())
	k := mustCreate(t, e)
	failing := &failingFS{FS: e.fs, fail: true}
	e.fs = failing
	if _, err := e.Change(t.Context(), k.KeyID, "DisableKey", "", 0); err == nil {
		t.Fatal("DisableKey with failed persistence succeeded")
	}
	m, err := e.Describe(t.Context(), k.KeyID)
	if err != nil || !m.Enabled {
		t.Fatalf("failed commit changed state: metadata = %v, error = %v", m, err)
	}
	if err := e.ChangeAlias(t.Context(), "alias/failure", k.KeyID, "CreateAlias"); err == nil {
		t.Fatal("CreateAlias with failed persistence succeeded")
	}
	_, err = e.Describe(t.Context(), "alias/failure")
	wantCode(t, err, "NotFoundException")
	failing.fail = false
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Create(ctx, CreateInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Create = %v, want context.Canceled", err)
	}
	if _, err := e.Change(ctx, k.KeyID, "DisableKey", "", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled DisableKey = %v, want context.Canceled", err)
	}
	keys, _, err := e.ListKeys(t.Context(), 100, "")
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys after canceled Create = %v, %v", keys, err)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	e := openEngine(t, t.TempDir())
	k := mustCreate(t, e)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			name := fmt.Sprintf("tag-%d", i)
			if err := e.ChangeTags(t.Context(), k.KeyID, []Tag{{name, "test"}}, nil); err != nil {
				t.Errorf("concurrent TagResource: %v", err)
			}
			if _, err := e.Encrypt(t.Context(), CryptoInput{KeyID: k.KeyID, Data: []byte("x")}); err != nil {
				t.Errorf("concurrent Encrypt: %v", err)
			}
		})
	}
	wg.Wait()
	tags, _, err := e.ListTags(t.Context(), k.KeyID, 50, "")
	if err != nil || len(tags) != 20 {
		t.Fatalf("concurrent tag count = %d, %v; want 20", len(tags), err)
	}
}
