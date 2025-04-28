package ingest

import (
	"context"
	"log"
	"time"

	"blink-yadp/internal/data"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func NewClickHouseConn() (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{"192.168.24.128:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "aaron",
		},
		Protocol: clickhouse.Native,
	})
}

func EnsureTable(conn clickhouse.Conn, ctx context.Context) error {
	return conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS sensor_data (
		id UUID,
		timestamp DateTime64,
		sensor_id UInt32,
		value Float32,
		metadata String
		) ENGINE = MergeTree()
		 ORDER BY (timestamp, value)
		 `)
}

func InsertBatch(conn clickhouse.Conn, ctx context.Context, batchSize int, generator func() data.SensorData) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		// if the server is overwhelmed you might have an error here, log it and pause for a short time, but don't crash
		log.Printf("Pausing briefly and then retrying, error is: %s", err)
		time.Sleep(10 * time.Millisecond) // very brief pause, don't want to wait too long to avoid performance impact

		return
	}

	for i := 0; i < batchSize; i++ {
		d := generator()
		if err := batch.AppendStruct(&d); err != nil {
			log.Printf("AppendStruct error: %s", err)

			return
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("Send error: %s", err)
	}
}
