package sensor

import (
	"math/rand"
	"time"

	"blink-yadp/internal/data"

	"github.com/google/uuid"
)

func GenerateSensorData() data.SensorData {
	return data.SensorData{
		ID:        uuid.New(),
		Timestamp: time.Now(),
		SensorId:  rand.Uint32() % 10000,
		Value:     rand.Float32() * 10000,
		Metadata:  "",
	}
}
