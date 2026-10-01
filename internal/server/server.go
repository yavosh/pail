// Package server runs pail's HTTP listener.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/s3api"
)

const (
	readHeaderTimeout = 30 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// Server owns the listener and the HTTP server.
type Server struct {
	cfg config.Config
	ln  net.Listener
}

// New returns a Server for cfg. Call Listen, then Serve.
func New(cfg config.Config) *Server {
	return &Server{cfg: cfg}
}

// Listen binds the configured address. A port conflict fails here, before serving.
func (s *Server) Listen(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	s.ln = ln
	return nil
}

// Addr returns the bound address, or nil before Listen succeeds.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve serves requests until ctx is done, then shuts down gracefully.
// It returns nil after a clean shutdown.
func (s *Server) Serve(ctx context.Context) error {
	if s.ln == nil {
		return errors.New("serve: not listening")
	}
	srv := &http.Server{Handler: s3api.New(s3api.Options{Domain: s.cfg.Domain}), ReadHeaderTimeout: readHeaderTimeout}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(s.ln) }()
	clogServer().Info("listening", "addr", s.ln.Addr().String())

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	// The shutdown deadline must outlive the canceled ctx.
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		_ = srv.Close() // force-close remaining connections; the shutdown error is the one to report
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	clogServer().Info("shut down")
	return nil
}

// Run binds the address and serves until ctx is done.
func Run(ctx context.Context, cfg config.Config) error {
	s := New(cfg)
	if err := s.Listen(ctx); err != nil {
		return err
	}
	return s.Serve(ctx)
}

// SetupLogging sends slog output to stderr at level.
func SetupLogging(level slog.Level) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
