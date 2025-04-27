package main

import (
	"context"
	"fmt"
	"log"
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

	const workers = 8 // need to tailor this to the resources of the server, otherwise you get CPU or RAM limitation errors

	for w := 0; w < workers; w++ {
		go func() {
			// use a unique conn per goroutine to improve performance
			conn, err := clickhouse.Open(&clickhouse.Options{
				Addr: []string{"192.168.24.128:9000"},
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
			timestamp DateTime64,  
			sensor_id UInt32,
			value Float32,
			metadata String
			) ENGINE = MergeTree()
			ORDER BY (timestamp, value)
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

// insertBatch inserts a batch of sensor data into the database
func insertBatch(conn clickhouse.Conn, ctx context.Context) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO sensor_data")
	if err != nil {
		// if the server is overwhelmed you might have an error here, log it and pause for a short time, but don't crash
		log.Printf("Pausing briefly and then retrying, error is: %s", err)
		time.Sleep(10 * time.Millisecond) // very brief pause, don't want to wait too long to avoid performance impact

		return // don't continue since the batch isn't valid because of the error
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
