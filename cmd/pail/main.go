// Command pail runs a small S3-compatible server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yavosh/pail/internal/buildinfo"
	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// After the first signal, restore default handling so a second one exits at once.
	context.AfterFunc(ctx, stop)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pail: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) error {
	cfg, err := config.Parse(args, getenv)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if cfg.Version {
		_, err := fmt.Fprintln(stdout, buildinfo.String("pail"))
		return err
	}
	if cfg.Healthcheck {
		return healthcheck(ctx, cfg.Addr)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	server.SetupLogging(cfg.LogLevel)
	return server.Run(ctx, cfg)
}

// healthURL returns the health endpoint for a listen address. A wildcard or
// empty host becomes loopback, because the container's own listener answers there.
func healthURL(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid address %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/_pail/health", nil
}

// healthcheck asks the pail at addr for /_pail/health, and fails unless it answers 200.
func healthcheck(ctx context.Context, addr string) error {
	url, err := healthURL(addr)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s answered %s", url, resp.Status)
	}
	return nil
}
