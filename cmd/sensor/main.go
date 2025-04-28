package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"blink-yadp/internal/sensor"
)

func main() {
	numWorkers := runtime.NumCPU()
	log.Printf("Starting with %d workers\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerId int) {
			for {
				data := sensor.GenerateSensorData()
				b, _ := json.Marshal(data)
				fmt.Printf("[Worker %d] %s\n", workerId, string(b)) // TODO: send to ingestor
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	select {} // let the goroutines run forever
}
