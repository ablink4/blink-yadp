package main

import (
	"context"
	"fmt"
	"log"
	"runtime"

	"blink-yadp/internal/ingest"
	"blink-yadp/internal/sensor"
)

func main() {
	ctx := context.Background()

	numWorkers := runtime.NumCPU() // one gorountine per core for performance
	log.Printf("Starting with %d workers\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {

			conn, err := ingest.NewClickHouseConn()
			if err != nil {
				log.Fatalf("[Worker %d] failed to connect: %v", workerID, err)
			}
			defer conn.Close()

			if err := conn.Ping(ctx); err != nil {
				log.Fatalf("[Worker %d] ClickHouse ping failed: %v", workerID, err)
			}
			fmt.Println("Connected to ClickHouse.")

			if err := ingest.EnsureTable(conn, ctx); err != nil {
				log.Fatalf("[Worker %d] EnsureTable failed: %v", workerID, err)
			}

			log.Printf("[Worker %d] Ingestion started", workerID)
			for {
				// TODO: ingest data from remote sensors instead of just simulate it from the sensor module
				ingest.InsertBatch(conn, ctx, 100000, sensor.GenerateSensorData)
			}
		}(i)
	}

	select {} // let the goroutines run forever
}
