package main

import (
	"context"
	"flag"
	"log"
	"runtime"

	sensordata "blink-yadp/internal/proto/sensordata"
	"blink-yadp/internal/sensor"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	serverAddr = "192.168.24.128:50051"
	batchSize  = 10000
)

func main() {
	sensorId := flag.Int("sensor-id", -1, "Optional sensor ID.  If not provided, will generate random sensor IDs.")
	flag.Parse()

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

			for {
				var batch sensordata.SensorDataBatch

				for range batchSize {
					d := sensor.GenerateSensorData(*sensorId)
					msg := &sensordata.SensorData{
						Timestamp: timestamppb.New(d.Timestamp),
						SensorId:  d.SensorId,
						Value:     d.Value,
						Metadata:  d.Metadata,
					}

					batch.Items = append(batch.Items, msg)
				}

				if err := stream.Send(&batch); err != nil {
					log.Printf("[Worker %d] stream send error %v", workerID, err)
					return
				}
			}
		}(i)
	}

	select {} // let the sensor send data forever
}
