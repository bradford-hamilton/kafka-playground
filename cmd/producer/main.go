package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

const (
	topic = "neat_topic"
	addr  = "localhost:9092"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		log.Fatalf("failed to start sarama producer: %v\n", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("failed to stop sarama producer: %v\n", err)
		}
	}()

	messages := []string{"neat_data", "is really neat", "and fast"}

	for _, m := range messages {
		msg := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(m),
		}

		partition, offset, err := producer.SendMessage(&msg)
		if err != nil {
			fmt.Printf("failed to send message: %v\n", err)
		} else {
			fmt.Printf("message sent, partition: %v, offset: %v\n", partition, offset)
		}
	}
}
