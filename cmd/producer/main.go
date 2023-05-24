package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	// Create new producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start sarama producer: %v\n", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("failed to stop sarama producer: %v\n", err)
		}
	}()

	msg := sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("testing"),
	}

	partition, offset, err := producer.SendMessage(&msg)
	if err != nil {
		fmt.Printf("failed to send message: %v\n", err)
	} else {
		fmt.Printf("message sent, partition: %v, offset: %v\n", partition, offset)
	}
}
