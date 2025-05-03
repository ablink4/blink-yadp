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

			stream, err := client.SendSensorStream(ctx)
			if err != nil {
				log.Fatalf("[Worker %d] failed to open stream: %v", workerID, err)
			}

			ticker := time.NewTicker(10 * time.Microsecond)
			defer ticker.Stop()

			// replace for/select with `for range ticker.C`
			for range ticker.C {
				d := sensor.GenerateSensorData()
				msg := &sensordata.SensorData{
					Id:        d.ID.String(),
					Timestamp: timestamppb.New(d.Timestamp),
					SensorId:  d.SensorId,
					Value:     d.Value,
					Metadata:  d.Metadata,
				}

				if err := stream.Send(msg); err != nil {
					log.Printf("[Worker %d] stream send error %v", workerID, err)
					return
				}
			}
		}(i)
	}

	select {} // let the goroutines run forever
}
