# AGENTS.md

Guidance for coding agents working in this repository.

## Project overview

pail is a small S3-compatible server written in pure Go. It targets local testing and personal projects. The goal is S3 API compatibility, not scale.

## Build and development commands

```bash
make build           # CGO_ENABLED=0 local build
go run ./cmd/pail --access-key dev --secret-key devsecret   # run the server; flags fall back to PAIL_* variables
./pail --version                                       # build identity
./pail --healthcheck                                   # exit 0 when pail at --addr answers /_pail/health; needs no keys
make docker          # docker build -t pail:dev; CI builds and runs the image in the docker job
make test            # go test -race ./...
make fmt             # go fmt and goimports
go test ./test/      # aws-sdk-go-v2 tests against an in-process pail, path-style and virtual-hosted
go test ./test/diff  # replay the AWS S3 golden files against pail; see test/diff/README.md
make lint            # golangci-lint
make build && PYTHON="uv run --with boto3==1.42.97 python" scripts/smoke.sh   # AWS CLI and boto3 against a real pail
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
- Listener failures close established HTTP connections before releasing storage.
- Accept interfaces, return structs. Define an interface in the package that consumes it. Shared backend contracts, such as `vfs.FS`, are the exception: every backend must return the same types, so they live in a neutral package, like `io/fs`.
- Indent error flow. No `else` after `return`. No naked returns. Don't panic outside `main` or `init`.
- Use one short receiver name per type. Never mix value and pointer receivers.
- Initialisms keep their case: `bucketID`, `URL`, `ETag`.
- `main()` is a few lines: it parses flags, sets up a signal context, and calls `run(ctx, ...) error`.
- HTTP handlers: register routes in one place, do setup at construction, and declare request and response types next to the handler. When middleware or auth is part of what you verify, test the route through `httptest.NewTestServer` and the real mux.
- S3 requests bypass `http.ServeMux`, because it redirects paths that are not clean, such as `a//b`, and those are valid keys. `internal/s3api` routes them from one operation table. `ServeMux` serves only pail's own `/_pail/` endpoints.
- `internal/server/router.go` routes each request by its SigV4 credential scope: `sqs` to `internal/sqsapi` and `sns` to `internal/snsapi`. An unsigned request with `X-Amz-Target: AmazonSQS.*` also goes to SQS. Everything else, including other unsigned requests, goes to `internal/s3api`.
- `internal/queue` holds SQS queues. Definitions persist under `sqs/queues/` in the data directory; messages live in memory. Visibility, delay, and retention are evaluated lazily, so the engine runs no goroutine. Long polls wait on a per-queue wake channel, and `StopWaiters` releases them at shutdown.
- `http.Server.RegisterOnShutdown` calls `queue.Engine.StopWaiters`, so long polls end before `Shutdown`'s deadline.
- `golangci-lint` enforces much of this (revive, nakedret, bodyclose, containedctx, usetesting, noctx, modernize, errorlint). For noctx, use `ExecContext`, `DialContext`, `HandshakeContext`, and `httptest.NewRequestWithContext`.
- Write Go 1.27 idioms, not their older equivalents. `modernize` and `errorlint` catch most of this in CI, and `go fix -stringsbuilder=false ./...` applies the mechanical half. The rest is on you: `errors.Is` and `errors.AsType[T]` over `==` and type assertions, `errors.Join` for accumulated errors, `cmp.Or` for fallback chains (every argument is evaluated, so no side effects), typed `atomic.Bool` and `atomic.Pointer[T]` over `atomic.Value`, `sync.OnceValue` over a `sync.Once` plus a result variable, `slices.Sorted(maps.Keys(m))` for deterministic map output, `new(v)` over a temporary variable taken by address, and method-aware `ServeMux` patterns with `r.PathValue`.
- Never use `time.Tick`. It cannot be stopped, which breaks the goroutine-lifetime rule above. Use `time.NewTicker` and `Stop` it.
- Before you write or review Go code, read [`docs/code-style.md`](docs/code-style.md). It lists the external style guides these rules come from (Effective Go, Google Go Style, modern Go idioms, Dave Cheney, Mat Ryer). Where this list is silent, follow those guides.

## Bucket settings and browser uploads

- `internal/s3api/configuration.go` validates CORS and lifecycle XML and checks request checksums.
- Bucket configurations persist atomically in `internal/store/configuration.go`.
- `internal/lifecycle` holds expiration and multipart cleanup rules. The server owns the cleanup worker and stops it before closing storage.
- Lifecycle cleanup holds the bucket lock exclusively. It reads current metadata before deleting eligible objects.
- `internal/acl` holds ACL documents. Object ACLs travel with metadata through uploads and multipart completion.
- ACL grants use the literal `xsi:type` prefix in responses because SDK decoders require it.
- Anonymous overwrites check ownership again under the object lock before committing.
- Anonymous GET and HEAD check the ACL from the metadata snapshot used for the response.
- Conditional deletes read current metadata under the object lock and hold it through deletion. `store.DeleteOptions.IfMatch` distinguishes an absent condition from an empty one. Single deletes use `If-Match`; batch deletes use each XML `ETag` and preserve errors in `Quiet` mode.
- `internal/sigv4/post.go` verifies signed form policies. The file arrives last, and size checks finish before storage commits.
- pail enables ACLs but still authenticates one account. Tag filters, storage transitions, version actions, and email grantees remain unsupported.

## Client tests

- SQS and SNS tests use `p.sqsClient()` and `p.snsClient()`, not `forEachStyle`: addressing styles are S3-only. Keep SDK defaults, including SQS MD5 validation.
- Every S3 feature adds aws-sdk-go-v2 tests in `test/`. Use `forEachStyle`, so each test runs path-style and virtual-hosted.
- Keep the SDK's default settings, such as checksums. The harness sets them explicitly, because `s3.New` skips the config loader that would. It turns retries off, so a test sees the first answer. Any other change names the issue that removes the need.
- Use lowercase bucket names without dots. The SDK silently falls back to path-style for other names, so the virtual-hosted run would not test virtual-hosted routing.
- Send raw or presigned requests through `pail.httpClient`. It routes every host to the test server, with no DNS or proxy.
- The SDK sends `aws-chunked` uploads only over HTTPS. Use `startPailTLS` to test them; it serves `example.com`, which the `httptest` certificate covers. `forEachStyle` starts a plain-HTTP pail, so such a test loops over `styles` itself.
- Keep user metadata names lowercase on the wire. The boto3 smoke test checks casing that Go HTTP clients normalize.
- `scripts/smoke.sh` runs the AWS CLI and boto3 (pinned) against a real pail, with their default settings. Both script files isolate themselves from real AWS credentials and point every client at pail's endpoint. They unset `AWS_ENDPOINT_URL_SQS` and `AWS_ENDPOINT_URL_SNS` as part of that isolation. Keep that when you edit them. boto3 presigns URLs with its default client. Browser POST policies use an explicit SigV4 client.
- `internal/sigv4/sigv2.go` verifies SigV2 query signatures. Routing supplies the virtual bucket; canonical resources preserve path escapes and sort S3 subresources. SigV2 Authorization headers remain unsupported.
- aws-sdk-go-v2 is a test-only dependency. CI checks that `go list -deps ./cmd/pail` names no `aws` or `smithy` package.

## Release

- A `v*` tag runs `.github/workflows/release.yml`. It refuses a commit without a green `ci.yml` run, attaches Linux and macOS binaries to a GitHub release, and pushes `ghcr.io/yavosh/pail`.
- Images support Linux amd64 and arm64. Stable releases publish version tags without `v`, a major.minor tag, and `latest`.
- After the first image publish, open [package settings](https://github.com/users/yavosh/packages/container/pail/settings) and change visibility to **Public**. GitHub creates packages as private; repository visibility does not make them public. Verify an anonymous pull of `ghcr.io/yavosh/pail:latest` with an empty Docker configuration directory.
- Before you tag, check that the CI run on the commit is not cancelled (`cancel-in-progress` cancels a superseded run). Re-run a cancelled run first.
- The Dockerfile builds only `./cmd/pail` with `CGO_ENABLED=0` into a distroless static image. The image has no shell or `curl`, so its healthcheck is `pail --healthcheck`.
- Pin every action by SHA, with the version in a comment. Docker base images use floating `golang:1.27` and `gcr.io/distroless/static-debian12:nonroot` tags.

## Documentation

- Update `README.md`, `docs/s3-compatibility.md`, `docs/sqs-sns-compatibility.md`, and `AGENTS.md` in the same PR as the change. Stale docs are a bug.
- `README.md` summarizes pail. Exact S3 behavior, limits, and AWS differences go in `docs/s3-compatibility.md`.
- Exact SQS and SNS behavior goes in `docs/sqs-sns-compatibility.md`.
- Follow the [Google developer documentation style guide](https://developers.google.com/style).
- Plan docs go in `docs/plans/YYYY-MM-DD-<slug>.md`, one per effort. `tasks/` is scratch, not the plan.

## Differential suite

- `test/diff` checks pail against golden files recorded from AWS S3, SQS, and SNS. A new S3, SQS, or SNS behavior adds a scenario there.
- Scenarios never call `ListBuckets`, `ListTopics`, or `ListSubscriptions`, and call `ListQueues` only with `QueueNamePrefix`. Golden files must not expose the account's other resources.
- Compare successful object bodies byte for byte, including XML objects. Normalize only protocol XML, never stored object content.
- Never edit a golden file by hand. Record it with `go test ./test/diff -record`, which needs AWS credentials, so a maintainer runs it.
- Fix a difference in pail, or list it in `test/diff/testdata/known-diffs.txt` with a reason and an issue link.
- A feature PR removes its steps from `test/diff/testdata/pending.txt`. The PR is done when they pass.

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
