package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

const (
	topic = "neat_topic"
	addr  = "localhost:9092"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{addr}, config)
	if err != nil {
		log.Fatalf("failed to create new consumer: %v", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Printf("failed to close consumer: %v", err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("failed to create new consumer: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

run:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf(
				"\nvalue: %s, offset: %d, partition: %d, topic: %s\n",
				string(msg.Value),
				msg.Offset,
				msg.Partition,
				msg.Topic,
			)
		case <-sigChan:
			fmt.Println("finishing process and cleaning up")
			break run
		default:
			fmt.Print(".")
			time.Sleep(time.Millisecond * 250)
		}
	}
}
