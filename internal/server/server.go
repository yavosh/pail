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
	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	readHeaderTimeout = 30 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// Server owns the listener and the HTTP server.
type Server struct {
	cfg    config.Config
	ln     net.Listener
	fs     *localdisk.FS
	store  *store.Store
	queues *queue.Engine
}

// New returns a Server for cfg. Call Listen, then Serve.
func New(cfg config.Config) *Server {
	return &Server{cfg: cfg}
}

// Listen opens the data directory and binds the configured address, so a bad
// data directory or a port conflict fails here, before serving.
func (s *Server) Listen(ctx context.Context) error {
	if s.ln != nil {
		return errors.New("listen: already listening")
	}
	fsys, err := localdisk.Open(s.cfg.DataDir)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, fsys)
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("open store in %s: %w", s.cfg.DataDir, err)
	}
	queues, err := queue.Open(ctx, fsys, s.cfg.Region)
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("open queues in %s: %w", s.cfg.DataDir, err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	s.fs, s.store, s.queues, s.ln = fsys, st, queues, ln
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
	srv := &http.Server{Handler: NewHandler(s.cfg, s.store, s.queues), ReadHeaderTimeout: readHeaderTimeout}
	// Shutdown waits for handlers, and a long poll can wait 20 s, longer than shutdownTimeout.
	srv.RegisterOnShutdown(s.queues.StopWaiters)
	// After a shutdown timeout, handlers still running see a closed data
	// directory and fail; their temp files are cleared at the next start.
	defer func() { _ = s.fs.Close() }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(s.ln) }()
	lifecycleCtx, stopLifecycle := context.WithCancel(ctx)
	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		s.runLifecycle(lifecycleCtx)
	}()
	defer func() {
		stopLifecycle()
		<-lifecycleDone
	}()
	clogServer().Info("listening", "addr", s.ln.Addr().String())

	select {
	case err := <-serveErr:
		s.queues.StopWaiters()
		_ = srv.Close() // close established connections before releasing storage
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

func (s *Server) runLifecycle(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := s.store.SweepLifecycle(ctx, time.Now()); err != nil && ctx.Err() == nil {
			clogServer().Error("lifecycle sweep", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
