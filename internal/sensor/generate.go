package sensor

import (
	"math/rand"
	"time"

	"blink-yadp/internal/data"
)

// GenerateSensorData generates a sample sensor data for ingestion
func GenerateSensorData(sensorId int) data.SensorData {
	// if we don't have a specific ID, we generate random numbers to simulate multiple different sensors
	var finalSensorId uint32
	if sensorId == -1 {
		finalSensorId = rand.Uint32() % 10000
	} else {
		finalSensorId = uint32(sensorId)
	}

	return data.SensorData{
		Timestamp: time.Now(),
		SensorId:  finalSensorId,
		Value:     rand.Float32() * 10000,
		Metadata:  "",
	}
}
