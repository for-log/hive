GOPATH   := $(shell go env GOPATH)
PROTOC   := protoc
GEN_GO   := $(GOPATH)/bin/protoc-gen-go
GEN_GRPC := $(GOPATH)/bin/protoc-gen-go-grpc

PROTO_SRC := proto/hive.proto
PROTO_OUT := gen/hivepb

CONFIG ?= configs/master-1.yaml

.PHONY: proto-gen build lint test test-race run-router run-master run-master-1 run-master-2 clean help

proto-gen: $(PROTO_SRC)
	@mkdir -p $(PROTO_OUT)
	PATH="$(PATH):$(GOPATH)/bin" $(PROTOC) \
		--proto_path=proto \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_SRC)

build:
	@mkdir -p bin
	go build -o bin/hive-router ./cmd/router
	go build -o bin/hive-master ./cmd/master

lint:
	golangci-lint run ./...

test:
	go test ./...

test-race:
	go test -race ./...

run-router:
	go run ./cmd/router --config configs/router.yaml

run-master:
	go run ./cmd/master --config $(CONFIG)

run-master-1:
	go run ./cmd/master --config configs/master-1.yaml

run-master-2:
	go run ./cmd/master --config configs/master-2.yaml

clean:
	rm -rf $(PROTO_OUT)/*.go bin/

help:
	@echo "Targets:"
	@echo "  proto-gen      generate Go code from $(PROTO_SRC)"
	@echo "  build          compile hive-router and hive-master into bin/"
	@echo "  lint           run golangci-lint"
	@echo "  test           run all tests"
	@echo "  test-race      run all tests with race detector"
	@echo "  run-router     start router   (configs/router.yaml)"
	@echo "  run-master-1   start master-1 (configs/master-1.yaml)"
	@echo "  run-master-2   start master-2 (configs/master-2.yaml)"
	@echo "  run-master     start master with CONFIG=<path> (default: $(CONFIG))"
	@echo "  clean          remove generated proto files and bin/"
