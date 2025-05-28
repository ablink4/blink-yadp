package ingest_fsmon

import (
	"context"
	"io"
	"log"
	"runtime"

	"blink-yadp/internal/data"
	fsmon "blink-yadp/internal/proto/fsmon"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Server is the gRPC server
type Server struct {
	fsmon.UnimplementedFsMonIngestorServer
	DB clickhouse.Conn
}

// InsertJob is used to separate and parallelize inserting to the database from receiving from the network
type InsertJob struct {
	Records []data.FsMonData
}

// global channel to add jobs to insert into the database
var insertJobs = make(chan InsertJob, 1000) // 1000 is queue size of jobs to insert

func StartInsertWorkers(db clickhouse.Conn, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for job := range insertJobs {
				batch, err := db.PrepareBatch(context.Background(), "INSERT INTO fsmon_data")
				if err != nil {
					log.Printf("[Worker %d] PrepareBatch error: %v", workerID, err)
					continue
				}

				for _, record := range job.Records {
					if err := batch.AppendStruct(&record); err != nil {
						log.Printf("[Worker %d] AppendStruct error: %v", workerID, err)
					}
				}

				if err := batch.Send(); err != nil {
					log.Printf("[Worker %d] Batch send error: %v", workerID, err)
				}
			}
		}(i)
	}
}

func (s *Server) SendFsMonData(ctx context.Context, req *fsmon.FsMon) (*fsmon.Ack, error) {
	data := data.FsMonData{
		Timestamp:               req.Timestamp.AsTime(),
		EventType:               req.EventType,
		Name:                    req.Name,
		PID:                     req.Pid,
		File:                    req.File,
		Cmd:                     req.Cmd,
		ProcName:                req.ProcName,
		Path:                    req.Path,
		PPID:                    req.Ppid,
		UID:                     req.Uid,
		GID:                     req.Gid,
		Groups:                  req.Groups,
		CapEff:                  req.CapEff,
		CapPrm:                  req.CapPrm,
		CapBnd:                  req.CapBnd,
		Seccomp:                 req.Seccomp,
		NoNewPrivs:              req.NoNewPrivs,
		Threads:                 req.Threads,
		VMSize:                  req.VmSize,
		VMRSS:                   req.VmRss,
		VMData:                  req.VmData,
		VoluntaryCtxSwitches:    req.VoluntaryCtxSwitches,
		NonvoluntaryCtxSwitches: req.NonvoluntaryCtxSwitches,
		ComputerId:              req.ComputerId,
	}

	batch, err := s.DB.PrepareBatch(ctx, "INSERT INTO fsmon_data")
	if err != nil {
		return nil, err
	}

	if err := batch.AppendStruct(&data); err != nil {
		return nil, err
	}

	if err := batch.Send(); err != nil {
		return nil, err
	}

	return &fsmon.Ack{Message: "OK"}, nil
}

func (s *Server) SendFsMonBatch(ctx context.Context, batchReq *fsmon.FsMonBatch) (*fsmon.Ack, error) {
	if len(batchReq.Items) == 0 {
		return &fsmon.Ack{Message: "Empty batch"}, nil
	}

	batch, err := s.DB.PrepareBatch(ctx, "INSERT INTO fsmon_data")
	if err != nil {
		log.Printf("PrepareBatch error: %v", err)
		return nil, err
	}

	for _, req := range batchReq.Items {
		record := data.FsMonData{
			Timestamp:               req.Timestamp.AsTime(),
			EventType:               req.EventType,
			Name:                    req.Name,
			PID:                     req.Pid,
			File:                    req.File,
			Cmd:                     req.Cmd,
			ProcName:                req.ProcName,
			Path:                    req.Path,
			PPID:                    req.Ppid,
			UID:                     req.Uid,
			GID:                     req.Gid,
			Groups:                  req.Groups,
			CapEff:                  req.CapEff,
			CapPrm:                  req.CapPrm,
			CapBnd:                  req.CapBnd,
			Seccomp:                 req.Seccomp,
			NoNewPrivs:              req.NoNewPrivs,
			Threads:                 req.Threads,
			VMSize:                  req.VmSize,
			VMRSS:                   req.VmRss,
			VMData:                  req.VmData,
			VoluntaryCtxSwitches:    req.VoluntaryCtxSwitches,
			NonvoluntaryCtxSwitches: req.NonvoluntaryCtxSwitches,
			ComputerId:              req.ComputerId,
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

	return &fsmon.Ack{Message: "OK"}, nil
}

func (s *Server) SendFsMonStream(stream fsmon.FsMonIngestor_SendFsMonStreamServer) error {
	numWorkers := runtime.NumCPU()
	StartInsertWorkers(s.DB, numWorkers) // must support FsMonData

	const batchSize = 1000
	var buffer []data.FsMonData

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			log.Println("Client closed stream, flushing remaining data.")

			if len(buffer) > 0 {
				insertJobs <- InsertJob{Records: buffer}
			}

			return stream.SendAndClose(&fsmon.Ack{Message: "OK"})
		}

		if err != nil {
			log.Printf("Stream recv error: %v", err)
			return err
		}

		for _, item := range req.Items {
			record := data.FsMonData{
				Timestamp:               item.Timestamp.AsTime(),
				EventType:               item.EventType,
				Name:                    item.Name,
				PID:                     item.Pid,
				File:                    item.File,
				Cmd:                     item.Cmd,
				ProcName:                item.ProcName,
				Path:                    item.Path,
				PPID:                    item.Ppid,
				UID:                     item.Uid,
				GID:                     item.Gid,
				Groups:                  item.Groups,
				CapEff:                  item.CapEff,
				CapPrm:                  item.CapPrm,
				CapBnd:                  item.CapBnd,
				Seccomp:                 item.Seccomp,
				NoNewPrivs:              item.NoNewPrivs,
				Threads:                 item.Threads,
				VMSize:                  item.VmSize,
				VMRSS:                   item.VmRss,
				VMData:                  item.VmData,
				VoluntaryCtxSwitches:    item.VoluntaryCtxSwitches,
				NonvoluntaryCtxSwitches: item.NonvoluntaryCtxSwitches,
				ComputerId:              item.ComputerId,
			}

			buffer = append(buffer, record)

			if len(buffer) >= batchSize {
				insertJobs <- InsertJob{Records: buffer}
				buffer = make([]data.FsMonData, 0, batchSize)
			}
		}
	}
}
