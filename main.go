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

	rand.Seed(int64(time.Now().UnixNano()))

	const workers = 4

	for w := 0; w < workers; w++ {
		go func() {
			// use a unique conn per goroutine to improve performance
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

			// insert data forever
			for {
				insertBatch(conn, ctx)
			}
		}()
	}

	select {} // let the goroutines run forever
}

func insertBatch(conn clickhouse.Conn, ctx context.Context) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		panic(err)
	}

	batchSize := 100000

	for range batchSize {
		data := SensorData{
			uuid.New(),
			time.Now(),
			rand.Uint32() % 10000, // generate random sensor IDs to mimic multiple sensors
			rand.Float32() * 10000,
			"", // no metadata for now
		}

		if err := batch.AppendStruct(&data); err != nil {
			panic(err)
		}
	}

	if err := batch.Send(); err != nil {
		panic(err)
	}
}
