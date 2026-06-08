BIN_DIR := bin
COMMANDS := edge-gateway private-connector signurl
GO_PACKAGES := ./...

.PHONY: fmt test build validate compose-up compose-down certs seed smoke clean

fmt:
	gofmt -w cmd internal

test:
	go test $(GO_PACKAGES)

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/edge-gateway ./cmd/edge-gateway
	go build -o $(BIN_DIR)/private-connector ./cmd/private-connector
	go build -o $(BIN_DIR)/signurl ./cmd/signurl

validate: fmt test build

compose-up:
	@echo "Docker Compose environment is not implemented yet."

compose-down:
	@echo "Docker Compose environment is not implemented yet."

certs:
	@echo "Certificate generation is not implemented yet."

seed:
	@echo "S3 seed data setup is not implemented yet."

smoke:
	@echo "Smoke tests are not implemented yet."

clean:
	rm -rf $(BIN_DIR)
