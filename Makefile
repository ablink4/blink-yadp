BIN_DIR := bin
SENSOR_BIN := $(BIN_DIR)/sensor
INGESTOR_BIN := $(BIN_DIR)/ingestor

SENSOR_DIR := ./cmd/sensor
INGESTOR_DIR := ./cmd/ingestor

PROTO_DIR := ./internal/proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

GO_BUILD := go build

.PHONY: build clean sensor ingestor proto generate

build: sensor ingestor

sensor: 
	@echo "Building sensor..."
	@$(GO_BUILD) -o $(SENSOR_BIN) $(SENSOR_DIR)

ingestor:
	@echo "Building ingestor..."
	@$(GO_BUILD) -o $(INGESTOR_BIN) $(INGESTOR_DIR)

proto:
	@echo "Generating protobuf Go code..."
	@protoc --go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
			--go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
			$(PROTO_FILES)

# build protobuf and binaries together
all: proto build

clean:
	@echo "Cleaning up binaries..."
	@rm -rf $(BIN_DIR)
	@find $(PROTO_DIR) -name "*.pb.go" -delete