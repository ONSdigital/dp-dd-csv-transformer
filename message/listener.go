package message

import (
	"encoding/json"

	"fmt"

	"github.com/ONSdigital/dp-dd-csv-transformer/handlers"
	"github.com/ONSdigital/dp-dd-csv-transformer/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
)

func ConsumerLoop(listener Listener, transformerer handlers.TransformFunc) {
	for message := range listener.Messages() {
		log.Debug("Message received from Kafka: "+string(message.Value), nil)
		processMessage(message, transformerer)
	}
}

func processMessage(message *sarama.ConsumerMessage, transformer handlers.TransformFunc) error {

	var transformRequest event.TransformRequest
	if err := json.Unmarshal(message.Value, &transformRequest); err != nil {
		log.Error(err, nil)
		return err
	}

	log.Debug(fmt.Sprintf("About to process:%s", transformRequest.String()), nil)
	transformer(transformRequest)
	log.Debug(fmt.Sprintf("Finished processing:%s", transformRequest.String()), nil)

	return nil
}

type Listener interface {
	Messages() <-chan *sarama.ConsumerMessage
}
