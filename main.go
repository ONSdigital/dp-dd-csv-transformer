package main

import (
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-dd-csv-transformer/config"
	"github.com/ONSdigital/dp-dd-csv-transformer/handlers"
	"github.com/ONSdigital/dp-dd-csv-transformer/message"
	"github.com/ONSdigital/go-ns/log"
	"github.com/bsm/sarama-cluster"
)

func main() {
	config.Load()

	consumerConfig := cluster.NewConfig()
	consumer, err := cluster.NewConsumer([]string{config.KafkaAddr}, config.KafkaConsumerGroup, []string{config.KafkaConsumerTopic}, consumerConfig)
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go func() {
		<-signals

		consumer.Close()
		log.Debug("Graceful shutdown was successful.", nil)
		os.Exit(0)
	}()

	message.ConsumerLoop(consumer, handlers.HandleRequest)

}
