// Package server runs pail's HTTP and HTTPS listeners.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/yavosh/pail/internal/cognito"
	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/kms"
	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/topic"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	readHeaderTimeout = 30 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// Server owns the listeners and the HTTP server.
type Server struct {
	cfg      config.Config
	ln       net.Listener
	tlsLn    net.Listener
	tlsCfg   *tls.Config
	fs       *localdisk.FS
	store    *store.Store
	queues   *queue.Engine
	topics   *topic.Engine
	keys     *kms.Engine
	identity *cognito.Engine
}

// New returns a Server for cfg. Call Listen, then Serve.
func New(cfg config.Config) *Server {
	return &Server{cfg: cfg}
}

// Listen loads the TLS key pair, opens the data directory, and binds the
// configured addresses before serving.
func (s *Server) Listen(ctx context.Context) error {
	if s.ln != nil {
		return errors.New("listen: already listening")
	}
	var tlsCfg *tls.Config
	if s.cfg.TLSAddr != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("load tls key pair: %w", err)
		}
		// Serve may initialize HTTP/2 before ServeTLS starts.
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2", "http/1.1"},
		}
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
	topics, err := topic.Open(ctx, fsys, s.cfg.Region, queues, topic.WithTLSSkipVerify(s.cfg.SNSTLSSkipVerify))
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("open topics in %s: %w", s.cfg.DataDir, err)
	}
	keys, err := kms.Open(ctx, fsys, s.cfg.Region)
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("open kms in %s: %w", s.cfg.DataDir, err)
	}
	var identity *cognito.Engine
	if s.cfg.CognitoConfig != "" {
		identity, err = cognito.Open(ctx, fsys, s.cfg.CognitoConfig)
		if err != nil {
			_ = fsys.Close()
			return fmt.Errorf("open cognito: %w", err)
		}
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		_ = fsys.Close()
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	var tlsLn net.Listener
	if tlsCfg != nil {
		tlsLn, err = lc.Listen(ctx, "tcp", s.cfg.TLSAddr)
		if err != nil {
			_ = ln.Close()
			_ = fsys.Close()
			return fmt.Errorf("listen for tls on %s: %w", s.cfg.TLSAddr, err)
		}
	}
	s.fs, s.store, s.queues, s.topics, s.ln = fsys, st, queues, topics, ln
	s.tlsLn, s.tlsCfg = tlsLn, tlsCfg
	s.keys = keys
	s.identity = identity
	return nil
}

// Addr returns the bound HTTP address, or nil before Listen succeeds.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// TLSAddr returns the bound HTTPS address, or nil when it is unavailable.
func (s *Server) TLSAddr() net.Addr {
	if s.tlsLn == nil {
		return nil
	}
	return s.tlsLn.Addr()
}

// Serve serves requests until ctx is done, then shuts down gracefully.
// It returns nil after a clean shutdown.
func (s *Server) Serve(ctx context.Context) error {
	if s.ln == nil {
		return errors.New("serve: not listening")
	}
	srv := &http.Server{
		Handler:           NewHandler(s.cfg, s.store, s.queues, s.topics, s.keys, s.identity),
		ReadHeaderTimeout: readHeaderTimeout,
		TLSConfig:         s.tlsCfg,
	}
	// Shutdown waits for handlers, and a long poll can wait 20 s, longer than shutdownTimeout.
	srv.RegisterOnShutdown(s.queues.StopWaiters)
	// After a shutdown timeout, handlers still running see a closed data
	// directory and fail; their temp files are cleared at the next start.
	defer func() { _ = s.fs.Close() }()

	serveErr := make(chan error, 2)
	var serveLoops sync.WaitGroup
	defer serveLoops.Wait()
	listeners := 1
	serveLoops.Go(func() { serveErr <- srv.Serve(s.ln) })
	if s.tlsLn != nil {
		listeners++
		serveLoops.Go(func() {
			// ServeTLS can fail before http.Server takes ownership of the listener.
			defer func() { _ = s.tlsLn.Close() }()
			serveErr <- fmt.Errorf("tls: %w", srv.ServeTLS(s.tlsLn, "", ""))
		})
	}
	lifecycleCtx, stopLifecycle := context.WithCancel(ctx)
	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		s.runLifecycle(lifecycleCtx)
	}()
	deliveriesDone := make(chan struct{})
	go func() {
		defer close(deliveriesDone)
		s.topics.RunDeliveries(lifecycleCtx)
	}()
	defer func() {
		stopLifecycle()
		<-lifecycleDone
		<-deliveriesDone
	}()
	logger := clogServer().With("addr", s.Addr().String())
	if addr := s.TLSAddr(); addr != nil {
		logger = logger.With("tls_addr", addr.String())
	}
	logger.Info("listening")

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
	for range listeners {
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
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

// Run binds the addresses and serves until ctx is done.
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
