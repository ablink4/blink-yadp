package main

import (
	"context"
	"log"
	"runtime"
	"time"

	sensordata "blink-yadp/internal/proto"
	"blink-yadp/internal/sensor"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	serverAddr = "192.168.24.128:50051"
	batchSize  = 10000
)

func main() {
	numWorkers := runtime.NumCPU()
	log.Printf("Spawning %d sensor workers", numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("failed to dial: %v", err)
			}
			defer conn.Close()

			client := sensordata.NewSensorIngestorClient(conn)
			ctx := context.Background()

			var buffer []*sensordata.SensorData
			ticker := time.NewTicker(10 * time.Microsecond)
			defer ticker.Stop()

			// replace for/select with `for range ticker.C`
			for {
				select {
				case <-ticker.C:
					d := sensor.GenerateSensorData()
					buffer = append(buffer, &sensordata.SensorData{
						Id:        d.ID.String(),
						Timestamp: timestamppb.New(d.Timestamp),
						SensorId:  d.SensorId,
						Value:     d.Value,
						Metadata:  d.Metadata,
					})

					if len(buffer) >= batchSize {
						_, err := client.SendSensorBatch(ctx, &sensordata.SensorDataBatch{Items: buffer})
						if err != nil {
							log.Printf("[Worker %d] SendSensorBatch error: %v", workerID, err)
						}
						buffer = buffer[:0]
					}
				}
			}
		}(i)
	}

	select {} // let the goroutines run forever
}
