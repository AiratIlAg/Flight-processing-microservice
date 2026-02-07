package service

import (
	"flight_processing/internal/kafka"
	"log"
)

func StartKafkaWorker(ch <-chan KafkaJob, producer *kafka.Producer) {
	go func() {
		for job := range ch {
			err := producer.SendFlightMessage(job.MetaID, &job.Request)
			if err != nil {
				log.Println("kafka send error:", err)
			}
		}
	}()
}
