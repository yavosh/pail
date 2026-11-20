package config

import (
	"log/slog"
	"strings"
	"testing"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParsePrecedence(t *testing.T) {
	defaults := Config{
		Addr:     "127.0.0.1:9000",
		DataDir:  "./data",
		Region:   "us-east-1",
		LogLevel: slog.LevelInfo,
	}
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want func(Config) Config
	}{
		{"defaults", nil, nil, func(c Config) Config { return c }},
		{"addr env", nil, map[string]string{"PAIL_ADDR": ":1"}, func(c Config) Config { c.Addr = ":1"; return c }},
		{"addr flag", []string{"--addr", ":2"}, nil, func(c Config) Config { c.Addr = ":2"; return c }},
		{"addr flag beats env", []string{"--addr", ":2"}, map[string]string{"PAIL_ADDR": ":1"}, func(c Config) Config { c.Addr = ":2"; return c }},
		{"data env", nil, map[string]string{"PAIL_DATA": "/e"}, func(c Config) Config { c.DataDir = "/e"; return c }},
		{"data flag beats env", []string{"--data", "/f"}, map[string]string{"PAIL_DATA": "/e"}, func(c Config) Config { c.DataDir = "/f"; return c }},
		{"access key env", nil, map[string]string{"PAIL_ACCESS_KEY_ID": "e"}, func(c Config) Config { c.AccessKeyID = "e"; return c }},
		{"access key flag beats env", []string{"--access-key", "f"}, map[string]string{"PAIL_ACCESS_KEY_ID": "e"}, func(c Config) Config { c.AccessKeyID = "f"; return c }},
		{"secret key env", nil, map[string]string{"PAIL_SECRET_ACCESS_KEY": "e"}, func(c Config) Config { c.SecretAccessKey = "e"; return c }},
		{"secret key flag beats env", []string{"--secret-key", "f"}, map[string]string{"PAIL_SECRET_ACCESS_KEY": "e"}, func(c Config) Config { c.SecretAccessKey = "f"; return c }},
		{"region env", nil, map[string]string{"PAIL_REGION": "eu-west-1"}, func(c Config) Config { c.Region = "eu-west-1"; return c }},
		{"region flag beats env", []string{"--region", "ap-south-1"}, map[string]string{"PAIL_REGION": "eu-west-1"}, func(c Config) Config { c.Region = "ap-south-1"; return c }},
		{"domain env", nil, map[string]string{"PAIL_DOMAIN": "e.test"}, func(c Config) Config { c.Domain = "e.test"; return c }},
		{"domain flag beats env", []string{"--domain", "f.test"}, map[string]string{"PAIL_DOMAIN": "e.test"}, func(c Config) Config { c.Domain = "f.test"; return c }},
		{"log level env", nil, map[string]string{"PAIL_LOG_LEVEL": "debug"}, func(c Config) Config { c.LogLevel = slog.LevelDebug; return c }},
		{"log level flag beats env", []string{"--log-level", "warn"}, map[string]string{"PAIL_LOG_LEVEL": "debug"}, func(c Config) Config { c.LogLevel = slog.LevelWarn; return c }},
		{"version flag", []string{"--version"}, nil, func(c Config) Config { c.Version = true; return c }},
		{"healthcheck flag", []string{"--healthcheck"}, nil, func(c Config) Config { c.Healthcheck = true; return c }},
		{"healthcheck reads addr env", []string{"--healthcheck"}, map[string]string{"PAIL_ADDR": ":1"}, func(c Config) Config { c.Healthcheck = true; c.Addr = ":1"; return c }},
		{"sns tls skip verify flag", []string{"--sns-tls-skip-verify"}, nil, func(c Config) Config { c.SNSTLSSkipVerify = true; return c }},
		{"sns tls skip verify env true", nil, map[string]string{"PAIL_SNS_TLS_SKIP_VERIFY": "true"}, func(c Config) Config { c.SNSTLSSkipVerify = true; return c }},
		{"sns tls skip verify env false", nil, map[string]string{"PAIL_SNS_TLS_SKIP_VERIFY": "false"}, func(c Config) Config { return c }},
		{"sns tls skip verify flag beats env", []string{"--sns-tls-skip-verify"}, map[string]string{"PAIL_SNS_TLS_SKIP_VERIFY": "false"}, func(c Config) Config { c.SNSTLSSkipVerify = true; return c }},
		{"sns tls skip verify flag false beats env", []string{"--sns-tls-skip-verify=false"}, map[string]string{"PAIL_SNS_TLS_SKIP_VERIFY": "true"}, func(c Config) Config { return c }},
		{"cognito config env", nil, map[string]string{"PAIL_COGNITO_CONFIG": "pools.json"}, func(c Config) Config { c.CognitoConfig = "pools.json"; return c }},
		{"cognito config flag beats env", []string{"--cognito-config=local.json"}, map[string]string{"PAIL_COGNITO_CONFIG": "pools.json"}, func(c Config) Config { c.CognitoConfig = "local.json"; return c }},
		{"empty flag value beats env", []string{"--domain="}, map[string]string{"PAIL_DOMAIN": "e.test"}, func(c Config) Config { return c }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args, envFrom(tt.env))
			if err != nil {
				t.Fatalf("Parse(%v, %v) error = %v", tt.args, tt.env, err)
			}
			if want := tt.want(defaults); got != want {
				t.Errorf("Parse(%v, %v) = %+v, want %+v", tt.args, tt.env, got, want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
	}{
		{"bad level flag", []string{"--log-level", "loud"}, nil, "PAIL_LOG_LEVEL"},
		{"bad level env", nil, map[string]string{"PAIL_LOG_LEVEL": "loud"}, "--log-level"},
		{"bad sns tls env", nil, map[string]string{"PAIL_SNS_TLS_SKIP_VERIFY": "maybe"}, "PAIL_SNS_TLS_SKIP_VERIFY"},
		{"positional argument", []string{"extra"}, nil, `unexpected argument "extra"`},
		{"unknown flag", []string{"--nope"}, nil, "nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args, envFrom(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Parse(%v, %v) error = %v, want it to contain %q", tt.args, tt.env, err, tt.wantErr)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		want    []string
		wantNot []string
	}{
		{"all set", Config{Addr: ":1", AccessKeyID: "a", SecretAccessKey: "s"}, nil, nil},
		{"addr missing", Config{AccessKeyID: "a", SecretAccessKey: "s"}, []string{"--addr", "PAIL_ADDR"}, []string{"PAIL_ACCESS_KEY_ID", "PAIL_SECRET_ACCESS_KEY"}},
		{"access key missing", Config{Addr: ":1", SecretAccessKey: "s"}, []string{"--access-key", "PAIL_ACCESS_KEY_ID"}, []string{"PAIL_SECRET_ACCESS_KEY"}},
		{"secret key missing", Config{Addr: ":1", AccessKeyID: "a"}, []string{"--secret-key", "PAIL_SECRET_ACCESS_KEY"}, []string{"PAIL_ACCESS_KEY_ID"}},
		{"all missing", Config{}, []string{"PAIL_ADDR", "PAIL_ACCESS_KEY_ID", "PAIL_SECRET_ACCESS_KEY"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				if err != nil {
					t.Errorf("Validate(%+v) = %v, want nil", tt.cfg, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate(%+v) = nil, want error containing %q", tt.cfg, tt.want)
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("Validate(%+v) = %q, want it to contain %q", tt.cfg, err, w)
				}
			}
			for _, w := range tt.wantNot {
				if strings.Contains(err.Error(), w) {
					t.Errorf("Validate(%+v) = %q, want it not to contain %q", tt.cfg, err, w)
				}
			}
		})
	}
}

func TestParseTLS(t *testing.T) {
	env := map[string]string{
		"PAIL_ADD_TLS_ADDR": ":9443",
		"PAIL_TLS_CERT":     "env.pem",
		"PAIL_TLS_KEY":      "env-key.pem",
	}
	tests := []struct {
		name string
		args []string
		want [3]string
	}{
		{"environment", nil, [3]string{":9443", "env.pem", "env-key.pem"}},
		{"flags override environment", []string{"--add-tls-addr=:8443", "--tls-cert=flag.pem", "--tls-key=flag-key.pem"}, [3]string{":8443", "flag.pem", "flag-key.pem"}},
		{"alias overrides environment", []string{"--add-tls-add=:8443"}, [3]string{":8443", "env.pem", "env-key.pem"}},
		{"last alias wins", []string{"--add-tls-addr=:8443", "--add-tls-add=:7443"}, [3]string{":7443", "env.pem", "env-key.pem"}},
		{"last canonical flag wins", []string{"--add-tls-add=:8443", "--add-tls-addr=:7443"}, [3]string{":7443", "env.pem", "env-key.pem"}},
		{"empty alias overrides environment", []string{"--add-tls-add=", "--tls-cert=", "--tls-key="}, [3]string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args, envFrom(env))
			if err != nil {
				t.Fatalf("Parse(%v) error = %v, want nil", tt.args, err)
			}
			if settings := [3]string{got.TLSAddr, got.TLSCert, got.TLSKey}; settings != tt.want {
				t.Errorf("Parse(%v) TLS settings = %q, want %q", tt.args, settings, tt.want)
			}
		})
	}
}

func TestValidateTLS(t *testing.T) {
	tests := []struct {
		name            string
		addr, cert, key string
		wantErr         bool
	}{
		{"disabled", "", "", "", false},
		{"enabled", ":8443", "server.pem", "server-key.pem", false},
		{"missing address", "", "server.pem", "server-key.pem", true},
		{"missing certificate", ":8443", "", "server-key.pem", true},
		{"missing key", ":8443", "server.pem", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Addr: ":9000", AccessKeyID: "a", SecretAccessKey: "s",
				TLSAddr: tt.addr, TLSCert: tt.cert, TLSKey: tt.key,
			}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%q, %q, %q) = %v, want error %t", tt.addr, tt.cert, tt.key, err, tt.wantErr)
			}
		})
	}
}
