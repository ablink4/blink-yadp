package ingest

import (
	"context"
	"io"
	"log"

	"blink-yadp/internal/data"
	sensordata "blink-yadp/internal/proto"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Server struct {
	sensordata.UnimplementedSensorIngestorServer
	DB clickhouse.Conn
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
	ctx := stream.Context()

	batch, err := s.DB.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		log.Printf("PrepareBatch error: %v", err)
		return err
	}

	const batchSize = 10000
	count := 0

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			log.Println("Client closed stream, flushing remaining data.")

			if count > 0 {
				if err := batch.Send(); err != nil {
					log.Printf("Final batch send error: %v", err)
					return err
				}
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

		if err := batch.AppendStruct(&record); err != nil {
			log.Printf("AppendStruct error: %v", err)
			return err
		}

		count++

		// flush batches periodically
		if count >= batchSize {
			if err := batch.Send(); err != nil {
				log.Printf("Batch send error: %v", err)
				return err
			}

			if err := batch.Send(); err != nil {
				log.Printf("Batch send error: %v", err)
				return err
			}

			// reset the batch
			batch, err = s.DB.PrepareBatch(ctx, "INSERT INTO sensor_data")
			if err != nil {
				log.Printf("PrepareBatch error after send: %v", err)
				return err
			}

			count = 0
		}
	}
}
