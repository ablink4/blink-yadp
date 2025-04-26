package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type Event struct {
	ID        uuid.UUID
	Timestamp time.Time
	UserID    uint32
	EventType string
	Metadata  string
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
	CREATE TABLE IF NOT EXISTS events (
	id UUID,
	timestamp DateTime,
	user_id UInt32,
	event_type String,
	metadata String
	) ENGINE = MergeTree()
	ORDER BY (timestamp)
	`)
	if err != nil {
		panic(err)
	}

	events, err := readCSV("data.csv")
	if err != nil {
		panic(err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		panic(err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.ID, e.Timestamp, e.UserID, e.EventType, e.Metadata,
		); err != nil {
			panic(err)
		}
	}

	if err := batch.Send(); err != nil {
		panic(err)
	}

	fmt.Printf("Insert %d events.\n", len(events))
}

func readCSV(filename string) ([]Event, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close() // defer means this will happen at the end of the function regardless to clean up resources

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var events []Event
	for i, row := range records {
		if i == 0 {
			continue // skip header
		}

		id, _ := uuid.Parse(row[0])
		timestamp, err := time.Parse(time.RFC3339, row[1])

		if err != nil {
			fmt.Printf("Error parsing timestamp '%s': %v\n", row[1], err)
		}

		userID, _ := strconv.ParseUint(row[2], 10, 32)

		event := Event{
			ID:        id,
			Timestamp: timestamp,
			UserID:    uint32(userID),
			EventType: row[3],
			Metadata:  row[4],
		}
		events = append(events, event)
	}

	return events, nil
}
