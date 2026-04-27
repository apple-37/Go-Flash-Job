GO ?= go

.PHONY: help tidy fmt vet test build run-scheduler run-executor run-logger

help:
	@echo "Available targets:"
	@echo "  make tidy          - Sync and clean go module dependencies"
	@echo "  make fmt           - Format all Go files"
	@echo "  make vet           - Run go vet on all packages"
	@echo "  make test          - Run tests for all packages"
	@echo "  make build         - Build all packages"
	@echo "  make run-scheduler - Start scheduler service"
	@echo "  make run-executor  - Start executor service"
	@echo "  make run-logger    - Start logger service"

tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

build:
	$(GO) build ./...

run-scheduler:
	$(GO) run ./scheduler/cmd

run-executor:
	$(GO) run ./executor/cmd

run-logger:
	$(GO) run ./logger/cmd
