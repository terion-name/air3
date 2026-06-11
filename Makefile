BIN_DIR := bin
COMMANDS := edge-gateway private-connector signurl
GO_PACKAGES := ./...
COMPOSE_FILE := deploy/compose.yaml
COMPOSE_PERF_FILE := deploy/compose.perf.yaml
COMPOSE_MULTISERVER_FILE := deploy/compose.multiserver.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
COMPOSE_MULTISERVER := $(COMPOSE) -f $(COMPOSE_MULTISERVER_FILE)
COMPOSE_MULTISERVER_FILES := $(COMPOSE_FILE):$(COMPOSE_MULTISERVER_FILE)
AIR3_PERF_MULTI_CONNECTORS ?= 3

.PHONY: fmt test ts-test python-test build validate compose-config compose-perf-config compose-multiserver-config compose-up compose-multiserver-up compose-down compose-multiserver-down certs seed seed-multiserver smoke smoke-multiserver e2e e2e-multiserver perf perf-multi readme-benchmark clean

fmt:
	gofmt -w cmd internal packages/go

test:
	go test $(GO_PACKAGES)

ts-test:
	npm --prefix packages/ts ci
	npm --prefix packages/ts test
	npm --prefix packages/ts run build

python-test:
	PYTHONPATH=packages/python python3 -m unittest discover -s packages/python/tests

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/edge-gateway ./cmd/edge-gateway
	go build -o $(BIN_DIR)/private-connector ./cmd/private-connector
	go build -o $(BIN_DIR)/signurl ./cmd/signurl

compose-config:
	$(COMPOSE) config >/dev/null

compose-perf-config:
	$(COMPOSE) -f $(COMPOSE_PERF_FILE) config >/dev/null

validate: fmt test ts-test python-test build compose-config

certs:
	./deploy/scripts/certs.sh

compose-multiserver-config:
	$(COMPOSE_MULTISERVER) config >/dev/null

compose-up:
	$(COMPOSE) up -d --build

compose-multiserver-up:
	$(COMPOSE_MULTISERVER) up -d --build

compose-down:
	$(COMPOSE) down --remove-orphans

compose-multiserver-down:
	$(COMPOSE_MULTISERVER) down --remove-orphans

seed:
	./deploy/scripts/seed-s3.sh

seed-multiserver:
	COMPOSE_FILE="$(COMPOSE_MULTISERVER_FILES)" ./deploy/scripts/seed-multiserver.sh

smoke:
	./deploy/scripts/smoke.sh

smoke-multiserver:
	COMPOSE_FILE="$(COMPOSE_MULTISERVER_FILES)" ./deploy/scripts/smoke-multiserver.sh

e2e: certs compose-up seed smoke compose-down

e2e-multiserver: compose-multiserver-config certs
	set -e; \
	trap '$(COMPOSE_MULTISERVER) down --remove-orphans' EXIT; \
	$(COMPOSE_MULTISERVER) up -d --build; \
	COMPOSE_FILE="$(COMPOSE_MULTISERVER_FILES)" ./deploy/scripts/seed-multiserver.sh; \
	COMPOSE_FILE="$(COMPOSE_MULTISERVER_FILES)" ./deploy/scripts/smoke-multiserver.sh

perf: compose-perf-config
	./deploy/scripts/perf-compose.sh

perf-multi: compose-perf-config
	AIR3_PERF_CONNECTORS=$(AIR3_PERF_MULTI_CONNECTORS) ./deploy/scripts/perf-compose.sh

readme-benchmark:
	./deploy/scripts/readme-benchmark.sh

clean:
	rm -rf $(BIN_DIR)
