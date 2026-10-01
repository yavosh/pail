// Command pail runs a small S3-compatible server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

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
	if err := cfg.Validate(); err != nil {
		return err
	}
	server.SetupLogging(cfg.LogLevel)
	return server.Run(ctx, cfg)
}
