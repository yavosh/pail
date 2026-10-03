# Build on the host's platform and cross-compile, so a multi-platform build
# does not run the Go compiler under emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X github.com/yavosh/pail/internal/buildinfo.version=$VERSION" \
    -o /out/pail ./cmd/pail \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/pail /pail
# The image has no shell, so the build stage creates /data for the nonroot user.
COPY --from=build --chown=65532:65532 /out/data /data
ENV PAIL_ADDR=0.0.0.0:9000 PAIL_DATA=/data
VOLUME ["/data"]
EXPOSE 9000
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 CMD ["/pail", "--healthcheck"]
ENTRYPOINT ["/pail"]
