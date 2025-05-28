package data

import (
	"time"
)

type SensorData struct {
	Timestamp time.Time `ch:"timestamp"`
	SensorId  uint32    `ch:"sensor_id"`
	Value     float32   `ch:"value"`
	Metadata  string    `ch:"metadata"`
}

type FsMonData struct {
	Timestamp               time.Time `ch:"timestamp"`
	EventType               string    `ch:"event_type"`
	Name                    string    `ch:"name"`
	PID                     int32     `ch:"pid"`
	File                    string    `ch:"file"`
	Cmd                     string    `ch:"cmd"`
	ProcName                string    `ch:"proc_name"`
	Path                    string    `ch:"path"`
	PPID                    int32     `ch:"ppid"`
	UID                     int32     `ch:"uid"`
	GID                     int32     `ch:"gid"`
	Groups                  []int32   `ch:"groups"`
	CapEff                  string    `ch:"cap_eff"`
	CapPrm                  string    `ch:"cap_prm"`
	CapBnd                  string    `ch:"cap_bnd"`
	Seccomp                 int32     `ch:"seccomp"`
	NoNewPrivs              int32     `ch:"no_new_privs"`
	Threads                 int32     `ch:"threads"`
	VMSize                  int32     `ch:"vm_size"`
	VMRSS                   int32     `ch:"vm_rss"`
	VMData                  int32     `ch:"vm_data"`
	VoluntaryCtxSwitches    int64     `ch:"voluntary_ctx_switches"`
	NonvoluntaryCtxSwitches int64     `ch:"nonvoluntary_ctx_switches"`
	ComputerId              string    `ch:"computer_id"`
}
