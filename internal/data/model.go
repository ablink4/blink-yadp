package data

import (
	"time"

	"github.com/google/uuid"
)

type SensorData struct {
	ID        uuid.UUID `ch:"id"`
	Timestamp time.Time `ch:"timestamp"`
	SensorId  uint32    `ch:"sensor_id"`
	Value     float32   `ch:"value"`
	Metadata  string    `ch:"metadata"`
}
