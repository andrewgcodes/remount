BINARY   := remount
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

.PHONY: all build test race fuzz cover vet fmt lint docs acpgen clean dist install demo conformance public-api modal-binary modal-deploy modal-smoke

FUZZTIME ?= 5s

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/remount

install:
	CGO_ENABLED=0 go install -trimpath -ldflags="$(LDFLAGS)" ./cmd/remount

test:
	go test -count=1 -timeout 300s ./...
	$(MAKE) public-api

race:
	go test -race -count=1 -timeout 900s ./...

fuzz:
	go test ./internal/proto -run='^$$' -fuzz=FuzzDecodeFrame -fuzztime=$(FUZZTIME)
	go test ./internal/artifact -run='^$$' -fuzz=FuzzRestore -fuzztime=$(FUZZTIME)
	go test ./internal/fsops -run='^$$' -fuzz=FuzzResolve -fuzztime=$(FUZZTIME)
	go test ./internal/broker -run='^$$' -fuzz=FuzzDestinationParsing -fuzztime=$(FUZZTIME)
	go test ./internal/control -run='^$$' -fuzz=FuzzVerifyGrant -fuzztime=$(FUZZTIME)
	go test ./internal/control -run='^$$' -fuzz=FuzzWorkspaceLifecycle -fuzztime=$(FUZZTIME)
	go test ./internal/ids -run='^$$' -fuzz=FuzzPrefix -fuzztime=$(FUZZTIME)
	go test ./internal/session -run='^$$' -fuzz=FuzzLogCursorRanges -fuzztime=$(FUZZTIME)

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
	@./scripts/gen-llms.sh >/dev/null && git diff --quiet -- llms.txt llms-full.txt || { echo "llms.txt is stale: run make docs and commit"; exit 1; }
	@go run ./cmd/acpgen -check

# Regenerate internal/acp/*_gen.go from the vendored ACP schema in spec/acp.
acpgen:
	@go run ./cmd/acpgen

# Regenerate llms.txt and llms-full.txt from README.md and docs/.
docs:
	@./scripts/gen-llms.sh

# Cross-compile a static binary for every supported platform. No CGO anywhere,
# so these are plain files you can scp onto a box and run.
dist:
	@mkdir -p dist
	@for p in $(PLATFORMS); do \
	  os=$${p%/*}; arch=$${p#*/}; \
	  ext=""; test "$$os" != windows || ext=.exe; \
	  echo "building dist/$(BINARY)-$$os-$$arch$$ext"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags="$(LDFLAGS)" \
	    -o dist/$(BINARY)-$$os-$$arch$$ext ./cmd/remount || exit 1; \
	done
	@ls -lh dist/

# Modal consumes the same static linux/amd64 artifact as a release. Keeping the
# path under dist/ makes a clean checkout and CI behave identically.
modal-binary:
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" \
		-o dist/remount-linux-amd64 ./cmd/remount

modal-deploy: modal-binary
	@test -n "$(MODAL_ENVIRONMENT)" || { echo "MODAL_ENVIRONMENT is required"; exit 1; }
	modal deploy deploy/modal_app.py

modal-smoke:
	@test -n "$(MODAL_ENVIRONMENT)" || { echo "MODAL_ENVIRONMENT is required"; exit 1; }
	modal run deploy/modal_app.py::smoke

# Hostile inputs and compromised-workspace behavior are covered in these
# packages. Run them under the race detector so this target cannot become a
# documentation-only success gate again.
conformance:
	go test -race -count=1 -timeout 1200s \
		./internal/artifact ./internal/broker ./internal/client ./internal/control \
		./internal/fsops ./internal/node ./internal/proto ./internal/relay \
		./internal/server ./internal/session ./internal/sim ./internal/transport \
		./internal/workspace

# Compile and test the SDK from a module outside remount.dev/remount. This
# catches accidental exposure of internal-only types that an in-module test
# cannot detect because of Go's internal package visibility rule.
public-api:
	cd integration/publicsdk && go test -count=1 ./...

# Bring up a server and node in one process, for poking at.
demo: build
	./$(BINARY) standalone --data ./remount-data

clean:
	rm -rf $(BINARY) dist coverage.txt remount-data remount-node
