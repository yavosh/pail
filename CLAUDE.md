# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project overview

pail is a small S3-compatible server written in pure Go. It targets local testing and personal projects. The goal is S3 API compatibility, not scale.

## Build and development commands

```bash
make build           # CGO_ENABLED=0 local build
go run ./cmd/pail --access-key dev --secret-key devsecret   # run the server; flags fall back to PAIL_* variables
./pail --version                                       # build identity
make test            # go test -race ./...
make fmt             # go fmt and goimports
make lint            # golangci-lint
```

## Architecture

### Design principles

- **Pure Go, zero CGO**: The binary must compile with `CGO_ENABLED=0`.
- **API compatibility over scale**: Match S3 behavior that clients depend on. Do not add complexity for throughput or clustering.

## Go conventions

- Go 1.27. Use `log/slog` for logging, never `log.Printf`. Log through the package's `clog*` helper, never a package-level `slog.Info`: every line carries a `component` naming its subsystem, and `forbidigo` enforces it. The helper lives in the package's `component.go`, for example `func clogServer() *slog.Logger { return slog.With("component", "server") }`.
- Prefer the standard library. Put all packages under `internal/`.
- Wrap errors with `fmt.Errorf("context: %w", err)`. Error strings are lowercase with no trailing punctuation. Add sentinel errors only when a caller needs `errors.Is`.
- Pass `context.Context` as the first parameter.
- Import order: stdlib, third-party, then `github.com/yavosh/pail`. goimports enforces this.
- Write table-driven tests with the stdlib `testing` package only. Use `t.Helper()` and `t.Cleanup`. Failures report input, got, and want.
- Time-dependent tests use `testing/synctest`, not sleeps. HTTP endpoints inside a bubble use `httptest.NewTestServer`; its in-memory network is the only one `synctest` can see.
- Comments are 2 to 3 lines at most. State the one non-obvious thing and stop.
- Every long-lived goroutine takes a `context.Context` or a stop channel, and the starter stops it. Prefer synchronous functions.
- Accept interfaces, return structs. Define an interface in the package that consumes it.
- Indent error flow. No `else` after `return`. No naked returns. Don't panic outside `main` or `init`.
- Use one short receiver name per type. Never mix value and pointer receivers.
- Initialisms keep their case: `bucketID`, `URL`, `ETag`.
- `main()` is a few lines: it parses flags, sets up a signal context, and calls `run(ctx, ...) error`.
- HTTP handlers: register routes in one place, do setup at construction, and declare request and response types next to the handler. When middleware or auth is part of what you verify, test the route through `httptest.NewTestServer` and the real mux.
- S3 requests bypass `http.ServeMux`, because it redirects paths that are not clean, such as `a//b`, and those are valid keys. `internal/s3api` routes them from one operation table. `ServeMux` serves only pail's own `/_pail/` endpoints.
- `golangci-lint` enforces much of this (revive, nakedret, bodyclose, containedctx, usetesting, noctx, modernize, errorlint). For noctx, use `ExecContext`, `DialContext`, `HandshakeContext`, and `httptest.NewRequestWithContext`.
- Write Go 1.27 idioms, not their older equivalents. `modernize` and `errorlint` catch most of this in CI, and `go fix -stringsbuilder=false ./...` applies the mechanical half. The rest is on you: `errors.Is` and `errors.AsType[T]` over `==` and type assertions, `errors.Join` for accumulated errors, `cmp.Or` for fallback chains (every argument is evaluated, so no side effects), typed `atomic.Bool` and `atomic.Pointer[T]` over `atomic.Value`, `sync.OnceValue` over a `sync.Once` plus a result variable, `slices.Sorted(maps.Keys(m))` for deterministic map output, `new(v)` over a temporary variable taken by address, and method-aware `ServeMux` patterns with `r.PathValue`.
- Never use `time.Tick`. It cannot be stopped, which breaks the goroutine-lifetime rule above. Use `time.NewTicker` and `Stop` it.
- Before you write or review Go code, read [`docs/code-style.md`](docs/code-style.md). It lists the external style guides these rules come from (Effective Go, Google Go Style, modern Go idioms, Dave Cheney, Mat Ryer). Where this list is silent, follow those guides.

## Documentation

- Update `README.md` and `CLAUDE.md` in the same PR as the change. Stale docs are a bug.
- Plan docs go in `docs/plans/YYYY-MM-DD-<slug>.md`, one per effort. `tasks/` is scratch, not the plan.

## After opening a PR

When CI is green, review the diff adversarially across correctness, simplicity, code reuse, and security. Reuse existing helpers. Delete dead code that the change created. Fix real findings on the same branch. If there are none, say "no findings".

## Pre-push checklist

```bash
# Needs Go 1.27 and golangci-lint >= 2.13.1; older lint builds refuse a go1.27 module.
go fix -stringsbuilder=false ./...   # the one modernizer .golangci.yml rejects
goimports -w .
golangci-lint run ./...
go test ./... -race
govulncheck ./...    # not yet enforced in CI
```
