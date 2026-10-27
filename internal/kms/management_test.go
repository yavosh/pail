package kms

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/vfs"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func TestAliasesAndPagination(t *testing.T) {
	e := openEngine(t, t.TempDir())
	first, second := mustCreate(t, e), mustCreate(t, e)
	ctx := t.Context()
	for _, name := range []string{"alias/c", "alias/a", "alias/b"} {
		if err := e.ChangeAlias(ctx, name, first.KeyID, "CreateAlias"); err != nil {
			t.Fatal(err)
		}
	}
	wantCode(t, e.ChangeAlias(ctx, "alias/a", first.KeyID, "CreateAlias"), "AlreadyExistsException")
	aliases, marker, err := e.ListAliases(ctx, first.ARN, 2, "")
	if err != nil || len(aliases) != 2 || aliases[0].AliasName != "alias/a" || aliases[1].AliasName != "alias/b" || marker == "" {
		t.Fatalf("first alias page = %v, marker present = %v, error = %v", aliases, marker != "", err)
	}
	last, next, err := e.ListAliases(ctx, first.KeyID, 2, marker)
	if err != nil || len(last) != 1 || last[0].AliasName != "alias/c" || next != "" {
		t.Fatalf("last alias page = %v, marker = %q, error = %v", last, next, err)
	}
	_, _, err = e.ListKeys(ctx, 2, marker)
	wantCode(t, err, "InvalidMarkerException")
	_, _, err = e.ListAliases(ctx, second.KeyID, 2, marker)
	wantCode(t, err, "InvalidMarkerException")
	if _, err := e.Change(ctx, first.KeyID, "ScheduleKeyDeletion", "", 7); err != nil {
		t.Fatal(err)
	}
	if err := e.ChangeAlias(ctx, "alias/a", second.KeyID, "UpdateAlias"); err != nil {
		t.Fatalf("moving alias away from pending key: %v", err)
	}
	wantCode(t, e.ChangeAlias(ctx, "alias/a", first.KeyID, "UpdateAlias"), "KMSInvalidStateException")
	if err := e.ChangeAlias(ctx, "alias/b", "", "DeleteAlias"); err != nil {
		t.Fatalf("deleting pending key's alias: %v", err)
	}
	if _, err := e.Describe(ctx, first.KeyID); err != nil {
		t.Fatalf("deleting alias affected its key: %v", err)
	}
	for _, id := range []string{first.KeyID, first.ARN, "alias/c", e.arn("alias/c")} {
		m, err := e.Describe(ctx, id)
		if err != nil || m.KeyID != first.KeyID {
			t.Fatalf("Describe(%q) = %v, %v", id, m, err)
		}
	}
	_, err = e.Describe(ctx, e.arn(first.KeyID))
	wantCode(t, err, "InvalidArnException")
	_, err = e.Describe(ctx, e.arn("key/alias/c"))
	wantCode(t, err, "InvalidArnException")
	_, err = e.Describe(ctx, strings.Replace(first.ARN, "us-east-1", "us-west-2", 1))
	wantCode(t, err, "NotFoundException")
}

func TestPageAfterRemovedTail(t *testing.T) {
	names := []string{"a", "b", "c"}
	_, marker, err := page(names, 2, "", "test")
	if err != nil || marker == "" {
		t.Fatal("initial pagination failed")
	}
	got, next, err := page([]string{"a"}, 2, marker, "test")
	if err != nil || len(got) != 0 || next != "" {
		t.Fatalf("page after deleted tail = %v, %q, %v; want empty page", got, next, err)
	}
	for _, marker := range []string{"!", base64.RawURLEncoding.EncodeToString([]byte("test\x00"))} {
		_, _, err := page(names, 2, marker, "test")
		wantCode(t, err, "InvalidMarkerException")
	}
}

func TestKeyAndTagValidation(t *testing.T) {
	e := openEngine(t, t.TempDir())
	k := mustCreate(t, e)
	ctx := t.Context()
	for _, in := range []CreateInput{
		{KeySpec: "RSA_2048"}, {KeyUsage: "SIGN_VERIFY"}, {Origin: "EXTERNAL"},
		{MultiRegion: true}, {CustomKeyStoreID: "store"}, {XksKeyID: "external"},
	} {
		_, err := e.Create(ctx, in)
		wantCode(t, err, "UnsupportedOperationException")
	}
	for _, name := range []string{"", "test", "alias/", "alias/aws/test", "alias/space here", "alias/" + strings.Repeat("a", 251)} {
		wantCode(t, e.ChangeAlias(ctx, name, k.KeyID, "CreateAlias"), "InvalidAliasNameException")
	}
	tags := make([]Tag, 50)
	for i := range tags {
		tags[i] = Tag{TagKey: fmt.Sprintf("key-%02d", i), TagValue: "test"}
	}
	if err := e.ChangeTags(ctx, k.KeyID, tags, nil); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.ChangeTags(ctx, k.KeyID, []Tag{{"overflow", "test"}}, nil), "TagException")
	for _, tag := range []Tag{{"", "x"}, {"aws:test", "x"}, {"bad\nkey", "x"}, {strings.Repeat("x", 129), "x"}} {
		wantCode(t, e.ChangeTags(ctx, k.KeyID, []Tag{tag}, nil), "TagException")
	}
	if err := e.ChangeTags(ctx, k.KeyID, []Tag{{tags[0].TagKey, "updated"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := e.ChangeTags(ctx, k.KeyID, nil, []string{tags[1].TagKey, "absent"}); err != nil {
		t.Fatal(err)
	}
	got, _, err := e.ListTags(ctx, k.KeyID, 50, "")
	if err != nil || len(got) != 49 || got[0].TagValue != "updated" {
		t.Fatalf("tags after changes = %d, error = %v; want 49", len(got), err)
	}
	_, _, err = e.ListTags(ctx, k.KeyID, 51, "")
	wantCode(t, err, "ValidationException")
	_, _, err = e.ListAliases(ctx, "", 101, "")
	wantCode(t, err, "ValidationException")
}

type brokenTempFS struct {
	FS
	stage string
}

func (f *brokenTempFS) CreatePrivateTemp() (vfs.TempFile, error) {
	temp, err := f.FS.CreatePrivateTemp()
	if err != nil {
		return nil, err
	}
	return &brokenTemp{TempFile: temp, stage: f.stage}, nil
}

type brokenTemp struct {
	vfs.TempFile
	stage string
}

func (f *brokenTemp) Write(b []byte) (int, error) {
	if f.stage == "write" {
		return 0, errors.New("injected write failure")
	}
	return f.TempFile.Write(b)
}

func (f *brokenTemp) Commit(name string) error {
	if f.stage == "commit" {
		return errors.New("injected commit failure")
	}
	return f.TempFile.Commit(name)
}

func TestWriteAndCommitFailures(t *testing.T) {
	for _, stage := range []string{"write", "commit"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			e := openEngine(t, dir)
			k := mustCreate(t, e)
			e.fs = &brokenTempFS{FS: e.fs, stage: stage}
			if err := e.ChangeTags(t.Context(), k.KeyID, []Tag{{"failed", "test"}}, nil); err == nil {
				t.Fatal("TagResource with broken storage succeeded")
			}
			for _, engine := range []*Engine{e, openEngine(t, dir)} {
				tags, _, err := engine.ListTags(t.Context(), k.KeyID, 50, "")
				if err != nil || len(tags) != 0 {
					t.Fatalf("failed write changed tags: count = %d, error = %v", len(tags), err)
				}
			}
		})
	}
}

func TestOpenInvalidState(t *testing.T) {
	for _, body := range []string{`{`, `null`, `{"region":"other","keys":{},"aliases":{}}`, `{"region":"us-east-1","keys":{"invalid":{}},"aliases":{}}`} {
		t.Run(fmt.Sprintf("case-%d", len(body)), func(t *testing.T) {
			fsys, err := localdisk.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = fsys.Close() })
			if err := fsys.MkdirAll("kms"); err != nil {
				t.Fatal(err)
			}
			if err := vfs.WriteFile(fsys, stateFile, []byte(body)); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(t.Context(), fsys, "us-east-1"); err == nil {
				t.Error("Open accepted invalid persisted state")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Open(ctx, nil, "us-east-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("Open with canceled context = %v, want context.Canceled", err)
	}
}
