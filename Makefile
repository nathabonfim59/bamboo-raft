# bamboo-raft build entry points. Heavy runs (full test suite, race, QEMU
# tests) go to a throwaway VM: `bbraft run --push . -- make check`.
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/nathabonfim59/bamboo-raft/internal/version.Version=$(VERSION)
CMDS    := bbraft bbraft-server bbraft-node bbraft-guest

.PHONY: all build test race lint fmt fmt-check tidy-check proto proto-check check clean

all: check build

# Builds every command that exists so far into bin/.
build:
	@for c in $(CMDS); do if [ -d cmd/$$c ]; then \
		echo "go build $$c"; CGO_ENABLED=0 $(GO) build -ldflags '$(LDFLAGS)' -o bin/$$c ./cmd/$$c || exit 1; fi; done

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...
	$(GO) tool staticcheck ./...

# Only the repository's own Go files: agent worktrees live under .tty59/, and
# images/.cache and images/out hold built guest trees (Go stdlib included).
# find, not git ls-files: test VMs get git archives without .git.
GOFILES = $(shell find . \( -path ./.tty59 -o -path ./.git -o -path ./.cache -o -path ./node_modules -o -path ./images/.cache -o -path ./images/out \) -prune -o -name '*.go' -print)
GOFMT ?= $(shell $(GO) env GOROOT)/bin/gofmt

fmt:
	"$(GOFMT)" -w $(GOFILES)

fmt-check:
	@out="$$("$(GOFMT)" -l $(GOFILES))" || exit $$?; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

tidy-check:
	$(GO) mod tidy -diff

# Generated code is committed, so building needs no protoc. Regenerate with
# `make proto` (protoc from mise.toml, plugins from go.mod tools).
PROTOS = $(shell find proto -name '*.proto')
proto:
	PATH="$$PWD/bin/tools:$$PATH" sh -c '$(GO) build -o bin/tools/ google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc && \
	protoc -I proto --go_out=. --go_opt=module=github.com/nathabonfim59/bamboo-raft \
		--go-grpc_out=. --go-grpc_opt=module=github.com/nathabonfim59/bamboo-raft $(PROTOS)'

# What CI and every milestone review run.
check: fmt-check tidy-check lint race

clean:
	rm -rf bin
