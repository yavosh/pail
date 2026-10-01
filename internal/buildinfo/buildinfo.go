// Package buildinfo reports the identity of the running binary.
package buildinfo

import (
	"runtime"
	"runtime/debug"
)

// version is stamped at link time:
//
//	-ldflags "-X github.com/yavosh/pail/internal/buildinfo.version=v1.2.3"
var version string

// Version returns the stamped version, else the VCS revision, else the module
// version, else "dev".
func Version() string {
	if version != "" {
		return version
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if rev, dirty := vcs(bi); rev != "" {
		if len(rev) > 7 {
			rev = rev[:7]
		}
		if dirty {
			rev += "-dirty"
		}
		return rev
	}
	if v := bi.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	return "dev"
}

// String returns a one-line description such as "pail v1.2.3 (go1.27 linux/amd64)".
func String(name string) string {
	return name + " " + Version() + " (" + runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH + ")"
}

func vcs(bi *debug.BuildInfo) (rev string, dirty bool) {
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	return rev, dirty
}
