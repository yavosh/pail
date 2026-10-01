package buildinfo

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func setVersion(t *testing.T, v string) {
	t.Helper()
	old := version
	version = v
	t.Cleanup(func() { version = old })
}

func TestVersionStampWins(t *testing.T) {
	setVersion(t, "v9.9.9")
	if got, want := Version(), "v9.9.9"; got != want {
		t.Errorf("Version() = %q, want %q", got, want)
	}
}

func TestVersionNeverEmpty(t *testing.T) {
	setVersion(t, "")
	if got := Version(); got == "" {
		t.Error("Version() = empty string, want a non-empty value")
	}
}

func TestString(t *testing.T) {
	setVersion(t, "v1.2.3")
	want := "pail v1.2.3 (" + runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH + ")"
	if got := String("pail"); got != want {
		t.Errorf("String(%q) = %q, want %q", "pail", got, want)
	}
}

func TestVCS(t *testing.T) {
	tests := []struct {
		name      string
		settings  []debug.BuildSetting
		wantRev   string
		wantDirty bool
	}{
		{"none", nil, "", false},
		{"clean", []debug.BuildSetting{{Key: "vcs.revision", Value: "abc1234def"}, {Key: "vcs.modified", Value: "false"}}, "abc1234def", false},
		{"dirty", []debug.BuildSetting{{Key: "vcs.revision", Value: "abc1234def"}, {Key: "vcs.modified", Value: "true"}}, "abc1234def", true},
		{"unrelated keys", []debug.BuildSetting{{Key: "GOOS", Value: "linux"}}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rev, dirty := vcs(&debug.BuildInfo{Settings: tt.settings})
			if rev != tt.wantRev || dirty != tt.wantDirty {
				t.Errorf("vcs(%v) = (%q, %v), want (%q, %v)", tt.settings, rev, dirty, tt.wantRev, tt.wantDirty)
			}
		})
	}
}
