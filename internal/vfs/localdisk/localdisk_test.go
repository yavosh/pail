package localdisk

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yavosh/pail/internal/vfs"
)

func openFS(t *testing.T) (*FS, string) {
	t.Helper()
	dir := t.TempDir()
	f, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, dir
}

// commit writes data to name through a temp file.
func commit(t *testing.T, f *FS, name, data string) {
	t.Helper()
	tf, err := f.CreateTemp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tf, data); err != nil {
		t.Fatal(err)
	}
	if err := tf.Commit(name); err != nil {
		t.Fatalf("Commit(%q) error = %v", name, err)
	}
}

func readAll(t *testing.T, file vfs.File) string {
	t.Helper()
	b, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCommitAndOpen(t *testing.T) {
	f, _ := openFS(t)
	commit(t, f, "buckets/b/objects/x.json", "hello")

	file, err := f.Open("buckets/b/objects/x.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if got := readAll(t, file); got != "hello" {
		t.Errorf("read = %q, want %q", got, "hello")
	}
	if _, err := file.Seek(1, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, file); got != "ello" {
		t.Errorf("read after Seek(1) = %q, want %q", got, "ello")
	}
	info, err := f.Stat("buckets/b/objects/x.json")
	if err != nil || info.Size() != 5 {
		t.Errorf("Stat size = %v (err %v), want 5", info, err)
	}
	if entries, err := f.ReadDir("tmp"); err != nil || len(entries) != 0 {
		t.Errorf("tmp entries after Commit = %v (err %v), want none", entries, err)
	}
}

func TestCommitReplacesAtomically(t *testing.T) {
	f, _ := openFS(t)
	commit(t, f, "obj", "old")

	held, err := f.Open("obj")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	commit(t, f, "obj", "new")

	if got := readAll(t, held); got != "old" {
		t.Errorf("held handle reads %q after replace, want %q", got, "old")
	}
	fresh, err := f.Open("obj")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if got := readAll(t, fresh); got != "new" {
		t.Errorf("fresh handle reads %q, want %q", got, "new")
	}
}

func TestTempFileLifecycle(t *testing.T) {
	f, _ := openFS(t)

	tf, err := f.CreateTemp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tf, "discard"); err != nil {
		t.Fatal(err)
	}
	if err := tf.Abort(); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := tf.Abort(); err != nil {
		t.Errorf("second Abort() error = %v, want nil", err)
	}
	if entries, _ := f.ReadDir("tmp"); len(entries) != 0 {
		t.Errorf("tmp entries after Abort = %d, want 0", len(entries))
	}
	if err := tf.Commit("x"); err == nil {
		t.Error("Commit after Abort error = nil, want an error")
	}

	tf, err = f.CreateTemp()
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Commit("y"); err != nil {
		t.Fatal(err)
	}
	if err := tf.Abort(); err != nil {
		t.Errorf("Abort after Commit error = %v, want nil", err)
	}
	if _, err := f.Stat("y"); err != nil {
		t.Errorf("Abort after Commit removed the file: %v", err)
	}
	if _, err := tf.Write([]byte("late")); err == nil {
		t.Error("Write after Commit error = nil, want an error")
	}
	if err := tf.Commit("z"); err == nil {
		t.Error("second Commit error = nil, want an error")
	}
}

func TestInvalidNames(t *testing.T) {
	f, _ := openFS(t)
	names := []string{"../x", "/abs", "a/../b", "", "a/./b", "a//b", "a/"}
	for _, name := range names {
		ops := map[string]error{}
		_, ops["Open"] = f.Open(name)
		_, ops["Stat"] = f.Stat(name)
		ops["Remove"] = f.Remove(name)
		ops["RemoveAll"] = f.RemoveAll(name)
		_, ops["ReadDir"] = f.ReadDir(name)
		ops["MkdirAll"] = f.MkdirAll(name)
		tf, err := f.CreateTemp()
		if err != nil {
			t.Fatal(err)
		}
		ops["Commit"] = tf.Commit(name)
		_ = tf.Abort()
		for op, err := range ops {
			if !errors.Is(err, fs.ErrInvalid) {
				t.Errorf("%s(%q) error = %v, want fs.ErrInvalid", op, name, err)
			}
		}
	}
}

func TestSymlinkEscapeIsBlocked(t *testing.T) {
	f, dir := openFS(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if file, err := f.Open("link/secret"); err == nil {
		file.Close()
		t.Error("Open(link/secret) through a symlink out of the root succeeded, want an error")
	}
}

func TestNotExist(t *testing.T) {
	f, _ := openFS(t)
	_, openErr := f.Open("missing")
	_, statErr := f.Stat("missing")
	removeErr := f.Remove("missing")
	_, readDirErr := f.ReadDir("missing")
	for op, err := range map[string]error{"Open": openErr, "Stat": statErr, "Remove": removeErr, "ReadDir": readDirErr} {
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s(missing) error = %v, want fs.ErrNotExist", op, err)
		}
	}
}

func TestReadDirSortedAndRemoveAll(t *testing.T) {
	f, _ := openFS(t)
	for _, name := range []string{"d/c", "d/a", "d/b/x"} {
		commit(t, f, name, "")
	}
	entries, err := f.ReadDir("d")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("ReadDir(d) = %v, want %v", got, want)
	}
	if err := f.RemoveAll("d"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat("d"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat(d) after RemoveAll error = %v, want fs.ErrNotExist", err)
	}
}

func TestOpenCreatesDataDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	f, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if info, err := os.Stat(filepath.Join(dir, tmpDir)); err != nil || !info.IsDir() {
		t.Errorf("tmp directory missing after Open: %v", err)
	}
}
