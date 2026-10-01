// Package config parses pail's flags and PAIL_* environment variables.
package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
)

// Config holds the settings for one pail process.
type Config struct {
	Addr            string
	DataDir         string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Domain          string
	LogLevel        slog.Level
	Version         bool
}

// Parse reads args, then fills every setting a flag did not set from getenv.
// Flag defaults are static, so --help never prints an environment value.
func Parse(args []string, getenv func(string) string) (Config, error) {
	var c Config
	var level string

	fs := flag.NewFlagSet("pail", flag.ContinueOnError)
	fs.StringVar(&c.Addr, "addr", "127.0.0.1:9000", "listen address (env PAIL_ADDR)")
	fs.StringVar(&c.DataDir, "data", "./data", "data directory (env PAIL_DATA)")
	fs.StringVar(&c.AccessKeyID, "access-key", "", "access key ID, required (env PAIL_ACCESS_KEY_ID)")
	fs.StringVar(&c.SecretAccessKey, "secret-key", "", "secret access key, required; prefer env PAIL_SECRET_ACCESS_KEY")
	fs.StringVar(&c.Region, "region", "us-east-1", "region (env PAIL_REGION)")
	fs.StringVar(&c.Domain, "domain", "", "base domain for virtual-hosted-style requests; empty turns it off (env PAIL_DOMAIN)")
	fs.StringVar(&level, "log-level", "info", "log level: debug, info, warn, or error (env PAIL_LOG_LEVEL)")
	fs.BoolVar(&c.Version, "version", false, "print the version and exit")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	envs := []struct {
		flag, env string
		dst       *string
	}{
		{"addr", "PAIL_ADDR", &c.Addr},
		{"data", "PAIL_DATA", &c.DataDir},
		{"access-key", "PAIL_ACCESS_KEY_ID", &c.AccessKeyID},
		{"secret-key", "PAIL_SECRET_ACCESS_KEY", &c.SecretAccessKey},
		{"region", "PAIL_REGION", &c.Region},
		{"domain", "PAIL_DOMAIN", &c.Domain},
		{"log-level", "PAIL_LOG_LEVEL", &level},
	}
	for _, e := range envs {
		if set[e.flag] {
			continue
		}
		if v := getenv(e.env); v != "" {
			*e.dst = v
		}
	}

	if err := c.LogLevel.UnmarshalText([]byte(level)); err != nil {
		return Config{}, fmt.Errorf("invalid log level %q: set --log-level or PAIL_LOG_LEVEL to debug, info, warn, or error: %w", level, err)
	}
	return c, nil
}

// Validate reports every required setting that is empty.
func (c Config) Validate() error {
	var errs []error
	if c.Addr == "" {
		errs = append(errs, errors.New("missing listen address: set --addr or PAIL_ADDR"))
	}
	if c.AccessKeyID == "" {
		errs = append(errs, errors.New("missing access key: set --access-key or PAIL_ACCESS_KEY_ID"))
	}
	if c.SecretAccessKey == "" {
		errs = append(errs, errors.New("missing secret key: set --secret-key or PAIL_SECRET_ACCESS_KEY"))
	}
	return errors.Join(errs...)
}
