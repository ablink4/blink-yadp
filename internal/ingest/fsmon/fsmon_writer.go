package ingest_fsmon

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

func EnsureFsmonTable(conn clickhouse.Conn, ctx context.Context) error {
	return conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS fsmon_data (		
		timestamp DateTime64,
		event_type String,
		name String,
		pid Int32,
		file String,
		cmd String,
		proc_name String,
		path String,
		ppid Int32,
		uid Int32,
		gid Int32,
		groups Array(Int32),
		cap_eff String,
		cap_prm String,
		cap_bnd String,
		seccomp Int32,
		no_new_privs Int32,
		threads Int32,
		vm_size Int32,
		vm_rss Int32,
		vm_data Int32,
		voluntary_ctx_switches Int64,
		nonvoluntary_ctx_switches Int64,
		computer_id String
	) ENGINE = MergeTree()
	ORDER BY (timestamp, pid)
	`)
}
