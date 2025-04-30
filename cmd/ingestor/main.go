package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"blink-yadp/internal/ingest"
	sensordata "blink-yadp/internal/proto"

	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()

	conn, err := ingest.NewClickHouseConn()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ClickHouse ping failed: %v", err)
	}
	fmt.Println("Connected to ClickHouse.")

	if err := ingest.EnsureTable(conn, ctx); err != nil {
		log.Fatalf("EnsureTable failed: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	sensordata.RegisterSensorIngestorServer(grpcServer, &ingest.Server{DB: conn})

	fmt.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
