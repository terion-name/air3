BIN_DIR := bin
COMMANDS := edge-gateway private-connector signurl
GO_PACKAGES := ./...
COMPOSE_FILE := deploy/compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)

.PHONY: fmt test ts-test python-test build validate compose-config compose-up compose-down certs seed smoke e2e clean

fmt:
	gofmt -w cmd internal packages/go

test:
	go test $(GO_PACKAGES)

ts-test:
	npm --prefix packages/ts test

python-test:
	PYTHONPATH=packages/python python3 -m unittest discover -s packages/python/tests

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/edge-gateway ./cmd/edge-gateway
	go build -o $(BIN_DIR)/private-connector ./cmd/private-connector
	go build -o $(BIN_DIR)/signurl ./cmd/signurl

compose-config:
	$(COMPOSE) config >/dev/null

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

clean:
	rm -rf $(BIN_DIR)
