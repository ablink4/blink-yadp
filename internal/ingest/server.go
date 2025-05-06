package ingest

import (
	"context"
	"io"
	"log"
	"runtime"

	"blink-yadp/internal/data"
	sensordata "blink-yadp/internal/proto"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// InsertJob is used to separate and parallelize inserting to the database from receiving from the network
type InsertJob struct {
	Records []data.SensorData
}

// Server is the gRPC server
type Server struct {
	sensordata.UnimplementedSensorIngestorServer
	DB clickhouse.Conn
}

// global channel to add jobs to insert into the database
var insertJobs = make(chan InsertJob, 1000) // 1000 is queue size of jobs to insert

func StartInsertWorkers(db clickhouse.Conn, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for job := range insertJobs {
				batch, err := db.PrepareBatch(context.Background(), "INSERT INTO sensor_data")
				if err != nil {
					log.Printf("[Worker %d] PrepareBatch error: %v", workerID, err)
					continue
				}

				for _, record := range job.Records {
					if err := batch.AppendStruct(&record); err != nil {
						log.Printf("[Worker %d] AppendStruct error: %v", workerID, err)
					}
				}

				if err := batch.Send(); err != nil {
					log.Printf("[Worker %d] Batch send error: %v", workerID, err)
				}
			}
		}(i)
	}
}

func (s *Server) SendSensorData(ctx context.Context, req *sensordata.SensorData) (*sensordata.Ack, error) {
	data := data.SensorData{
		Timestamp: req.Timestamp.AsTime(),
		SensorId:  req.SensorId,
		Value:     req.Value,
		Metadata:  req.Metadata,
	}

	batch, err := s.DB.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		return nil, err
	}

	if err := batch.AppendStruct(&data); err != nil {
		return nil, err
	}

	if err := batch.Send(); err != nil {
		return nil, err
	}

	return &sensordata.Ack{Message: "OK"}, nil
}

func (s *Server) SendSensorBatch(ctx context.Context, batchReq *sensordata.SensorDataBatch) (*sensordata.Ack, error) {
	if len(batchReq.Items) == 0 {
		return &sensordata.Ack{Message: "Empty batch"}, nil
	}

	batch, err := s.DB.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		log.Printf("PrepareBatch error: %v", err)
		return nil, err
	}

	for _, req := range batchReq.Items {
		record := data.SensorData{
			Timestamp: req.Timestamp.AsTime(),
			SensorId:  req.SensorId,
			Value:     req.Value,
			Metadata:  req.Metadata,
		}

		if err := batch.AppendStruct(&record); err != nil {
			log.Printf("AppendStruct error: %v", err)
			return nil, err
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("Batch send error: %v", err)
		return nil, err
	}

	return &sensordata.Ack{Message: "OK"}, nil
}

func (s *Server) SendSensorStream(stream sensordata.SensorIngestor_SendSensorStreamServer) error {
	numWorkers := runtime.NumCPU()
	StartInsertWorkers(s.DB, numWorkers)

	const batchSize = 10000
	var buffer []data.SensorData

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			log.Println("Client closed stream, flushing remaining data.")

			if len(buffer) > 0 {
				insertJobs <- InsertJob{Records: buffer}
			}

			return stream.SendAndClose(&sensordata.Ack{Message: "OK"})
		}

		if err != nil {
			log.Printf("Stream recv error: %v", err)
			return err
		}

		record := data.SensorData{
			Timestamp: req.Timestamp.AsTime(),
			SensorId:  req.SensorId,
			Value:     req.Value,
			Metadata:  req.Metadata,
		}

		buffer = append(buffer, record)

		if len(buffer) >= batchSize {
			insertJobs <- InsertJob{Records: buffer}
			buffer = make([]data.SensorData, 0, batchSize)
		}
	}
}
