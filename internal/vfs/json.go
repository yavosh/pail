package vfs

import (
	"encoding/json"
	"fmt"
)

// WriteJSON commits v as JSON to name through a temp file.
func WriteJSON(fsys FS, name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return WriteFile(fsys, name, b)
}

// ReadJSON decodes the JSON file name into v.
func ReadJSON(fsys FS, name string, v any) error {
	b, err := ReadFile(fsys, name)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}
