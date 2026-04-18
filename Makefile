APP_NAME := cursus
CLI_NAME := cursus-cli

GO := go
GOLINT := golangci-lint
TEST_FLAGS := -v -race
BUILD_FLAGS := -ldflags="-s -w"

E2E_COMPOSE_FILE := test/e2e/docker-compose.yml
COMPOSE_PROJECT := e2e
BROKER_CONTAINER := broker

ifeq (, $(shell docker compose version 2>/dev/null))
	DOCKER_COMPOSE := docker-compose
else
	DOCKER_COMPOSE := docker compose
endif

.PHONY: all
all: build

.PHONY: lint
lint: tools
	@echo "Running linter..."
	$(GOLINT) run ./...

.PHONY: test
test:
	@echo "Running unit tests..."
	$(GO) test $(TEST_FLAGS) $(shell go list ./... | grep -v /test/e2e)

.PHONY: e2e
e2e: e2e-build e2e-up
	@echo "Running E2E tests..."
	$(call run_e2e_test,$(GO) test -v -timeout 10m ./test/e2e/...)

.PHONY: e2e-verbose
e2e-verbose: e2e-build e2e-up
	@echo "Running E2E tests (verbose)..."
	$(call run_e2e_test,$(GO) test -v -race -timeout 10m ./test/e2e/...)

.PHONY: e2e-coverage
e2e-coverage: e2e-build e2e-up
	@echo "Running E2E tests with coverage..."
	$(call run_e2e_test,$(GO) test -v -timeout 10m -coverprofile=e2e-coverage.out ./test/e2e/...)
	@echo "E2E coverage report saved to e2e-coverage.out"

define run_e2e_test
	@bash -c '\
	set -e; \
	$1 || EXIT=$$?; \
	if [ -n "$$EXIT" ]; then \
		echo "Tests failed with exit code $$EXIT, showing logs..."; \
		$(MAKE) e2e-logs; \
		$(MAKE) e2e-clean; \
		exit $$EXIT; \
	fi; \
	$(MAKE) e2e-clean; \
	'
endef

.PHONY: e2e-build
e2e-build:
	@echo "Building E2E test images..."
	$(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT) -f $(E2E_COMPOSE_FILE) build

.PHONY: e2e-up
e2e-up:
	@echo "Starting E2E test containers..."
	$(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT) -f $(E2E_COMPOSE_FILE) up -d
	@echo "Waiting for broker container to become healthy..."
	@bash -c '\
	for i in $$(seq 1 60); do \
		STATUS=$$(docker inspect --format="{{.State.Health.Status}}" $(BROKER_CONTAINER) 2>/dev/null || echo "starting"); \
		if [ "$$STATUS" = "healthy" ]; then \
			echo "Broker is healthy"; \
			exit 0; \
		fi; \
		echo "Broker status=$$STATUS (retry $$i)"; \
		sleep 2; \
	done; \
	echo "Broker failed to become healthy"; \
	$(MAKE) e2e-logs; \
	exit 1 \
	'

.PHONY: e2e-clean
e2e-clean:
	@echo "Cleaning E2E test environment..."
	$(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT) -f $(E2E_COMPOSE_FILE) down -v
	-docker run --rm -v $(PWD)/test/logs:/logs alpine:3.20 rm -rf /logs/* || true

.PHONY: e2e-logs
e2e-logs:
	@echo "Showing E2E test logs..."
	@echo "=== Broker logs ==="
	-docker logs $(BROKER_CONTAINER) 2>&1 || echo "Broker container not found"

.PHONY: bench
bench:
	@bash -c '\
	set +e; \
	echo "[MAKE] Running benchmark with docker-compose..."; \
	timeout 180s docker compose -f test/docker-compose.yml up --build --remove-orphans; \
	echo "[MAKE] Containers finished or timed out"; \
	docker compose -f test/docker-compose.yml logs; \
	docker compose -f test/docker-compose.yml down -v; \
	'

.PHONY: build
build: build-api build-cli

.PHONY: build-api
build-api:
	@echo "Building API server..."
	CGO_ENABLED=0 GOOS=linux $(GO) build $(BUILD_FLAGS) -o bin/$(APP_NAME) ./cmd/broker/main.go

.PHONY: build-cli
build-cli:
	@echo "Building CLI..."
	CGO_ENABLED=0 GOOS=linux $(GO) build $(BUILD_FLAGS) -o bin/$(CLI_NAME) ./cmd/cli/main.go

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/*
	rm -f coverage.out e2e-coverage.out

.PHONY: run
run:
	@echo "Running broker..."
	$(GO) run ./cmd/broker/main.go

.PHONY: cli
cli:
	@echo "Running CLI..."
	$(GO) run ./cmd/cli/main.go

.PHONY: tools
tools:
	@echo "Installing/updating tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
  
.PHONY: fmt  
fmt:  
	@echo "Formatting code..."  
	$(GO) fmt ./...  
  
.PHONY: coverage
coverage:
	@echo "Running tests with coverage..."
	$(GO) test $(TEST_FLAGS) -coverprofile=coverage.out $(shell go list ./... | grep -v -E '/test/|/examples/|/cmd/|/bench$$')
	@echo "Coverage report saved to coverage.out"
  
.PHONY: help  
help:  
	@echo "Makefile commands:"  
	@echo "  make lint            Run linter"  
	@echo "  make test            Run unit tests"  
	@echo "  make e2e             Run E2E tests (builds images first)"  
	@echo "  make e2e-verbose     Run E2E tests with race detection" 
	@echo "  make e2e-logs        Show E2E test container logs" 
	@echo "  make bench           Run benchmarks"  
	@echo "  make build           Build all binaries (api, cli)"  
	@echo "  make clean           Remove build artifacts"  
	@echo "  make run             Run broker in dev mode" 
	@echo "  make tools           Install or update development tools"  
	@echo "  make fmt             Format code according to Go standards"  
	@echo "  make coverage        Run tests with coverage report"