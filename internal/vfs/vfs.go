// Package vfs defines the file operations pail's store needs from a backend.
// Names are slash-separated, relative to the backend's root, and valid per
// fs.ValidPath. Errors follow io/fs, so callers check errors.Is(err, fs.ErrNotExist).
package vfs

import (
	"io"
	"io/fs"
)

// FS is a backend that holds the store's files.
type FS interface {
	Open(name string) (File, error)
	Stat(name string) (fs.FileInfo, error)
	// CreateTemp returns a new file that becomes visible only on Commit.
	CreateTemp() (TempFile, error)
	Remove(name string) error
	RemoveAll(name string) error
	// ReadDir returns the entries of a directory sorted by name.
	ReadDir(name string) ([]fs.DirEntry, error)
	MkdirAll(name string) error
}

// File is an open file for reading.
type File interface {
	io.ReadSeekCloser
	Stat() (fs.FileInfo, error)
}

// TempFile is a file being written. Commit atomically replaces name with it
// and creates missing parent directories. Abort discards it; after Commit it
// does nothing, so `defer tf.Abort()` is always safe.
type TempFile interface {
	io.Writer
	Commit(name string) error
	Abort() error
}
