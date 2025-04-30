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

func main() {
	conn, err := grpc.Dial("192.168.24.128:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := sensordata.NewSensorIngestorClient(conn)

	numWorkers := runtime.NumCPU()
	log.Printf("Starting with %d workers\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerId int) {
			for {
				d := sensor.GenerateSensorData()

				_, err := client.SendSensorData(context.Background(), &sensordata.SensorData{
					Id:        d.ID.String(),
					Timestamp: timestamppb.New(d.Timestamp),
					SensorId:  d.SensorId,
					Value:     d.Value,
					Metadata:  d.Metadata,
				})

				if err != nil {
					log.Printf("[Worker %d] send error: %v", workerId, err)
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	select {} // let the goroutines run forever
}
