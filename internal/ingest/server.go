package ingest

import (
	"context"
	"log"

	"blink-yadp/internal/data"
	sensordata "blink-yadp/internal/proto"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type Server struct {
	sensordata.UnimplementedSensorIngestorServer
	DB clickhouse.Conn
}

func (s *Server) SendSensorData(ctx context.Context, req *sensordata.SensorData) (*sensordata.Ack, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		log.Printf("UUID parse error: %v", err)
		return nil, err
	}

	data := data.SensorData{
		ID:        id,
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
		id, err := uuid.Parse(req.Id)
		if err != nil {
			log.Printf("UUID parse error: %v", err)
			return nil, err
		}

		record := data.SensorData{
			ID:        id,
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
