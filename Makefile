.PHONY: build test fmt lint clean

BINARY := pail

build:
	CGO_ENABLED=0 go build -o . ./cmd/...

test:
	go test ./... -race

fmt:
	go fmt ./...
	goimports -w .

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
