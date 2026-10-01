.PHONY: build test fmt lint clean

BINARY := pail
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/yavosh/pail/internal/buildinfo.version=$(VERSION)

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o . ./cmd/...

test:
	go test ./... -race

fmt:
	go fmt ./...
	goimports -w .

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
