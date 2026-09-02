BINARY   := remount
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.PHONY: all build test race cover vet fmt lint clean dist install demo conformance

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/remount

install:
	CGO_ENABLED=0 go install -trimpath -ldflags="$(LDFLAGS)" ./cmd/remount

test:
	go test -count=1 -timeout 300s ./...

race:
	go test -race -count=1 -timeout 900s ./...

cover:
	go test -count=1 -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -func=coverage.txt | tail -1

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint: vet
	@gofmt -l . | grep -v '^$$' && { echo "gofmt needed on the files above"; exit 1; } || echo "gofmt clean"
	@./scripts/lint-locks.sh .

# Cross-compile a static binary for every supported platform. No CGO anywhere,
# so these are plain files you can scp onto a box and run.
dist:
	@mkdir -p dist
	@for p in $(PLATFORMS); do \
	  os=$${p%/*}; arch=$${p#*/}; \
	  echo "building dist/$(BINARY)-$$os-$$arch"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags="$(LDFLAGS)" \
	    -o dist/$(BINARY)-$$os-$$arch ./cmd/remount || exit 1; \
	done
	@ls -lh dist/

# Bring up a server and node in one process, for poking at.
demo: build
	./$(BINARY) standalone --data ./remount-data

clean:
	rm -rf $(BINARY) dist coverage.txt remount-data remount-node
