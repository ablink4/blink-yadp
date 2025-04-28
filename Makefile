BIN_DIR := bin
SENSOR_BIN := $(BIN_DIR)/sensor
INGESTOR_BIN := $(BIN_DIR)/ingestor

SENSOR_DIR := ./cmd/sensor
INGESTOR_DIR := ./cmd/ingestor

GO_BUILD := go build

.PHONY: build clean sensor ingestor

build: sensor ingestor

sensor: 
	@echo "Building sensor..."
	@$(GO_BUILD) -o $(SENSOR_BIN) $(SENSOR_DIR)

ingestor:
	@echo "Building ingestor..."
	@$(GO_BUILD) -o $(INGESTOR_BIN) $(INGESTOR_DIR)

clean:
	@echo "Cleaning up binaries..."
	@rm -rf $(BIN_DIR)