// Package localdisk implements vfs.FS on a local directory.
package localdisk

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/yavosh/pail/internal/vfs"
)

// tmpDir holds files until Commit. It sits inside the root, so Commit is a
// rename within one filesystem and therefore atomic.
const tmpDir = "tmp"

// FS is a vfs.FS rooted at a directory. os.Root keeps every name, including
// symlink targets, inside that directory.
type FS struct {
	root *os.Root
}

var _ vfs.FS = (*FS)(nil)

// Open creates dir if needed and returns an FS rooted there.
func Open(dir string) (*FS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open data directory: %w", err)
	}
	if err := root.MkdirAll(tmpDir, 0o755); err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("create temp directory: %w", err)
	}
	return &FS{root: root}, nil
}

// Close releases the root directory.
func (f *FS) Close() error { return f.root.Close() }

// Open opens name for reading.
func (f *FS) Open(name string) (vfs.File, error) {
	if err := checkName("open", name); err != nil {
		return nil, err
	}
	file, err := f.root.Open(name)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// Stat describes name.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	if err := checkName("stat", name); err != nil {
		return nil, err
	}
	return f.root.Stat(name)
}

// CreateTemp creates an empty file under tmp/.
func (f *FS) CreateTemp() (vfs.TempFile, error) {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	name := path.Join(tmpDir, hex.EncodeToString(b))
	file, err := f.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &tempFile{fs: f, file: file, name: name}, nil
}

// Remove removes a file or an empty directory.
func (f *FS) Remove(name string) error {
	if err := checkName("remove", name); err != nil {
		return err
	}
	return f.root.Remove(name)
}

// RemoveAll removes name and everything under it.
func (f *FS) RemoveAll(name string) error {
	if err := checkName("removeall", name); err != nil {
		return err
	}
	return f.root.RemoveAll(name)
}

// ReadDir lists a directory sorted by name.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := checkName("readdir", name); err != nil {
		return nil, err
	}
	return fs.ReadDir(f.root.FS(), name)
}

// MkdirAll creates name and any missing parents.
func (f *FS) MkdirAll(name string) error {
	if err := checkName("mkdir", name); err != nil {
		return err
	}
	return f.root.MkdirAll(name, 0o755)
}

func checkName(op, name string) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return nil
}

type tempFile struct {
	fs   *FS
	file *os.File
	name string
	done bool
}

var errTempDone = errors.New("temp file already committed or aborted")

func (t *tempFile) Write(b []byte) (int, error) {
	if t.done {
		return 0, errTempDone
	}
	return t.file.Write(b)
}

// Commit renames the temp file to name. There is no fsync: crash durability
// is not a goal, and fsync slows test suites that write many small objects.
func (t *tempFile) Commit(name string) error {
	if t.done {
		return errTempDone
	}
	if err := checkName("commit", name); err != nil {
		return err
	}
	t.done = true
	if err := t.file.Close(); err != nil {
		_ = t.fs.root.Remove(t.name)
		return fmt.Errorf("close temp file: %w", err)
	}
	if dir := path.Dir(name); dir != "." {
		if err := t.fs.root.MkdirAll(dir, 0o755); err != nil {
			_ = t.fs.root.Remove(t.name)
			return fmt.Errorf("create parent directory: %w", err)
		}
	}
	if err := t.fs.root.Rename(t.name, name); err != nil {
		_ = t.fs.root.Remove(t.name)
		return fmt.Errorf("commit %s: %w", name, err)
	}
	return nil
}

// Abort discards the temp file. It does nothing after Commit or a prior Abort.
func (t *tempFile) Abort() error {
	if t.done {
		return nil
	}
	t.done = true
	closeErr := t.file.Close()
	if err := t.fs.root.Remove(t.name); err != nil {
		return fmt.Errorf("remove temp file: %w", err)
	}
	return closeErr
}
