package ingest

import (
	"context"

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
		timestamp DateTime64,
		sensor_id UInt32,
		value Float32,
		metadata String
		) ENGINE = MergeTree()
		 ORDER BY (timestamp, value)
		 `)
}
