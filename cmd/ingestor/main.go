package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	ingest_fsmon "blink-yadp/internal/ingest/fsmon"
	ingest_sensordata "blink-yadp/internal/ingest/sensordata"
	"blink-yadp/internal/proto/fsmon"
	sensordata "blink-yadp/internal/proto/sensordata"

	"google.golang.org/grpc"
)

// endpoint for pprof
func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func main() {
	ctx := context.Background()

	conn, err := ingest_sensordata.NewClickHouseConn()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ClickHouse ping failed: %v", err)
	}
	fmt.Println("Connected to ClickHouse.")

	if err := ingest_sensordata.EnsureTable(conn, ctx); err != nil {
		log.Fatalf("EnsureTable for SensorData failed: %v", err)
	}

	if err := ingest_fsmon.EnsureFsmonTable(conn, ctx); err != nil {
		log.Fatalf("EnsureTable for FsMon failed: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	sensordata.RegisterSensorIngestorServer(grpcServer, &ingest_sensordata.Server{DB: conn})
	fsmon.RegisterFsMonIngestorServer(grpcServer, &ingest_fsmon.Server{DB: conn})

	fmt.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
