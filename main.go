package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type SensorData struct {
	ID        uuid.UUID `ch:"id"`
	Timestamp time.Time `ch:"timestamp"`
	SensorId  uint32    `ch:"sensor_id"`
	Value     float32   `ch:"value"`
	Metadata  string    `ch:"metadata"`
}

func main() {
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "aaron",
		},
		Protocol: clickhouse.Native,
	})

	if err != nil {
		panic(err)
	}

	if err := conn.Ping(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Connected to ClickHouse.")

	err = conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS sensor_data (
	id UUID,
	timestamp DateTime,
	sensor_id UInt32,
	value Float32,
	metadata String
	) ENGINE = MergeTree()
	ORDER BY (timestamp)
	`)
	if err != nil {
		panic(err)
	}

	rand.Seed(int64(time.Now().UnixNano()))

	for {
		data := SensorData{
			uuid.New(),
			time.Now(),
			123456, // just have data from one sensor for now
			rand.Float32() * 10000,
			"", // no metadata for now
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO sensor_data")
		if err != nil {
			panic(err)
		}

		if err := batch.AppendStruct(&data); err != nil {
			panic(err)
		}

		if err := batch.Send(); err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}
