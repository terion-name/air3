BIN_DIR := bin
COMMANDS := edge-gateway private-connector signurl
GO_PACKAGES := ./...
COMPOSE_FILE := deploy/compose.yaml
COMPOSE_PERF_FILE := deploy/compose.perf.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
AIR3_PERF_MULTI_CONNECTORS ?= 3

.PHONY: fmt test ts-test python-test build validate compose-config compose-perf-config compose-up compose-down certs seed smoke e2e perf perf-multi clean

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

compose-up:
	$(COMPOSE) up -d --build

compose-down:
	$(COMPOSE) down --remove-orphans

seed:
	./deploy/scripts/seed-s3.sh

smoke:
	./deploy/scripts/smoke.sh

e2e: certs compose-up seed smoke compose-down

perf: compose-perf-config
	./deploy/scripts/perf-compose.sh

perf-multi: compose-perf-config
	AIR3_PERF_CONNECTORS=$(AIR3_PERF_MULTI_CONNECTORS) ./deploy/scripts/perf-compose.sh

clean:
	rm -rf $(BIN_DIR)
