.PHONY: help proto proto-go proto-py orchestrator worker-deps test test-py lint-py test-integration ci run clean tools install-protoc

# Project root.
ROOT := $(shell pwd)
PROTO_DIR := proto
PROTO_FILE := $(PROTO_DIR)/minimodal.proto

# Output dirs for generated stubs.
GO_PB_OUT := orchestrator/pb
SDK_PB_OUT := sdk/minimodal/pb
WORKER_PB_OUT := worker/pb

PYTHON ?= python3

help:
	@echo "minimodal — make targets"
	@echo "  make tools         Install protoc + Go gRPC plugins (one-time setup)"
	@echo "  make proto         Generate Go + Python gRPC stubs from $(PROTO_FILE)"
	@echo "  make orchestrator  Build the Go orchestrator binary"
	@echo "  make worker-deps   pip install Python worker + SDK dependencies"
	@echo "  make run           docker-compose up orchestrator + workers"
	@echo "  make test          Run Go unit tests (with race detector)"
	@echo "  make test-py       Run Python unit tests (tests/unit)"
	@echo "  make lint-py       ruff-lint the Python sources"
	@echo "  make test-integration  Run Python end-to-end tests (boots orchestrator + worker)"
	@echo "  make ci            Run the same checks CI runs: ruff, pytest, gofmt, vet, go test, build"
	@echo "  make clean         Remove build artifacts and generated stubs"

# One-time toolchain setup. Idempotent.
tools:
	@command -v protoc >/dev/null 2>&1 || $(MAKE) install-protoc
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(PYTHON) -m pip install --upgrade grpcio grpcio-tools cloudpickle pytest ruff

install-protoc:
	@echo "Installing protoc..."
	@case "$$(uname -s)" in \
		Darwin) brew install protobuf ;; \
		Linux)  sudo apt-get update && sudo apt-get install -y protobuf-compiler ;; \
		*) echo "install protoc manually: https://grpc.io/docs/protoc-installation/"; exit 1 ;; \
	esac

proto: proto-go proto-py

proto-go:
	@mkdir -p $(GO_PB_OUT)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GO_PB_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_PB_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

proto-py:
	@mkdir -p $(SDK_PB_OUT) $(WORKER_PB_OUT)
	$(PYTHON) -m grpc_tools.protoc \
		--proto_path=$(PROTO_DIR) \
		--python_out=$(SDK_PB_OUT) \
		--grpc_python_out=$(SDK_PB_OUT) \
		--pyi_out=$(SDK_PB_OUT) \
		$(PROTO_FILE)
	$(PYTHON) -m grpc_tools.protoc \
		--proto_path=$(PROTO_DIR) \
		--python_out=$(WORKER_PB_OUT) \
		--grpc_python_out=$(WORKER_PB_OUT) \
		--pyi_out=$(WORKER_PB_OUT) \
		$(PROTO_FILE)
	@touch $(SDK_PB_OUT)/__init__.py $(WORKER_PB_OUT)/__init__.py
	# grpcio-tools emits `import minimodal_pb2` (absolute) which breaks
	# package imports. Rewrite to relative `from . import minimodal_pb2`.
	@for f in $(SDK_PB_OUT)/minimodal_pb2_grpc.py $(WORKER_PB_OUT)/minimodal_pb2_grpc.py; do \
		sed -i.bak 's/^import minimodal_pb2 as /from . import minimodal_pb2 as /' $$f && rm $$f.bak; \
	done

orchestrator:
	cd orchestrator && go build -o orchestrator .

worker-deps:
	$(PYTHON) -m pip install -r worker/requirements.txt
	$(PYTHON) -m pip install -e sdk

test:
	cd orchestrator && go test -race ./...

# Python unit tests. Fast + hermetic: no orchestrator, no docker. Needs the
# generated Python stubs (`make proto-py`) because the SDK imports them.
test-py:
	$(PYTHON) -m pytest

lint-py:
	$(PYTHON) -m ruff check .

# Integration tests boot the actual orchestrator binary + a worker, so the
# binary must already be built and proto stubs generated.
test-integration: orchestrator
	$(PYTHON) tests/test_idempotency.py
	$(PYTHON) tests/test_fault_tolerance.py
	$(PYTHON) tests/test_streaming.py

# Mirror what .github/workflows/ci.yml runs — useful for catching CI failures
# before pushing.
ci: lint-py test-py
	@unformatted=$$(cd orchestrator && gofmt -l .); \
		if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	cd orchestrator && go vet ./...
	cd orchestrator && go test -race ./...
	cd orchestrator && go build -o orchestrator .

run:
	docker-compose up --build

clean:
	rm -f orchestrator/orchestrator
	rm -f $(GO_PB_OUT)/*.pb.go
	rm -f $(SDK_PB_OUT)/*_pb2*.py $(SDK_PB_OUT)/*.pyi
	rm -f $(WORKER_PB_OUT)/*_pb2*.py $(WORKER_PB_OUT)/*.pyi
	rm -f *.db orchestrator/*.db
