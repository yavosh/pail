package main

import (
	"bytes"
	"strings"
	"testing"
)

func noEnv(string) string { return "" }

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(t.Context(), []string{"--version"}, noEnv, &out); err != nil {
		t.Fatalf("run(--version) error = %v, want nil", err)
	}
	if !strings.HasPrefix(out.String(), "pail ") {
		t.Errorf("run(--version) output = %q, want prefix %q", out.String(), "pail ")
	}
}

func TestRunMissingKeys(t *testing.T) {
	var out bytes.Buffer
	err := run(t.Context(), nil, noEnv, &out)
	if err == nil || !strings.Contains(err.Error(), "PAIL_ACCESS_KEY_ID") {
		t.Errorf("run() error = %v, want it to mention %q", err, "PAIL_ACCESS_KEY_ID")
	}
}
