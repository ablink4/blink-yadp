BIN_DIR := bin
SENSOR_BIN := $(BIN_DIR)/sensor
INGESTOR_BIN := $(BIN_DIR)/ingestor

SENSOR_DIR := ./cmd/sensor
INGESTOR_DIR := ./cmd/ingestor

PROTO_BASE_DIR := ./internal/proto
PROTO_FILES := $(shell find $(PROTO_BASE_DIR) -name "*.proto")

GO_BUILD := go build

.PHONY: build clean sensor ingestor proto generate all

build: sensor ingestor

sensor: 
	@echo "Building sensor..."
	@$(GO_BUILD) -o $(SENSOR_BIN) $(SENSOR_DIR)

ingestor:
	@echo "Building ingestor..."
	@$(GO_BUILD) -o $(INGESTOR_BIN) $(INGESTOR_DIR)

proto:
	@echo "Generating protobuf Go code..."
	@for file in $(PROTO_FILES); do \
		protoc --proto_path=$(PROTO_BASE_DIR) \
		       --go_out=$(PROTO_BASE_DIR) --go_opt=paths=source_relative \
		       --go-grpc_out=$(PROTO_BASE_DIR) --go-grpc_opt=paths=source_relative \
		       $$file; \
	done

# build protobuf and binaries together
all: proto build

clean:
	@echo "Cleaning up binaries..."
	@rm -rf $(BIN_DIR)
	@find $(PROTO_BASE_DIR) -name "*.pb.go" -delete
