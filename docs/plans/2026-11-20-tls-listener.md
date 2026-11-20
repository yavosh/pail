# Optional TLS listener

Add HTTPS on a second address in the same pail process.
The HTTP address still serves clients and the built-in health check.
Both listeners share the router, credentials, and storage.

## Interface

- Set `--add-tls-addr` and provide PEM files with `--tls-cert` and `--tls-key`.
- Accept `--add-tls-add` as an alias for the HTTPS address.
- Use `PAIL_ADD_TLS_ADDR`, `PAIL_TLS_CERT`, and `PAIL_TLS_KEY` for environment configuration.
- Require the address, certificate, and key together.

## Implementation

- Load the key pair and bind both addresses before serving requests.
- Close the first listener and storage if the second address fails to bind.
- Use a single HTTP server and shutdown deadline. A listener failure closes connections on both addresses before storage closes.
- Update CLI, Docker, service compatibility, and Cognito setup documentation.

## Validation

- Verify settings, aliases, flag precedence, and startup failures.
- Exercise both transports through the real router and check that they share service state.
- Verify shutdown and connection cleanup after either listener fails.
- Run goimports, lint, the race suite, and a CGO-disabled build.
