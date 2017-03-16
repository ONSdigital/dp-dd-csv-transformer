package config

import (
	"os"

	"github.com/ONSdigital/go-ns/log"
	"strconv"
)

const bindAddrKey = "BIND_ADDR"
const kafkaAddrKey = "KAFKA_ADDR"
const kafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
const kafkaConsumerTopic = "KAFKA_CONSUMER_TOPIC"
const awsRegionKey = "AWS_REGION"
const hierarchyEndpoint = "HIERARCHY_ENDPOINT"
const useGzipCompression = "USE_GZIP"

const HIERACHY_ID_PLACEHOLDER = "{hierarchy_id}"

// BindAddr the address to bind to.
var BindAddr = ":21200"

// KafkaAddr the Kafka address to send messages to.
var KafkaAddr = "localhost:9092"

// AWSRegion the AWS region to use.
var AWSRegion = "eu-west-1"

// KafkaConsumerGroup the consumer group to consume messages from.
var KafkaConsumerGroup = "transform-request"

// KafkaConsumerTopic the name of the topic to consume messages from.
var KafkaConsumerTopic = "transform-request"

// HierarchyEndpoint the url of the metadata api hierarchy endpoint.
var HierarchyEndpoint = "http://localhost:20099/hierarchies/" + HIERACHY_ID_PLACEHOLDER

// UseGzipCompression determines whether files should be compressed when uploaded to S3 and served with `Content-Encoding: gzip` header.
var UseGzipCompression = false

func init() {
	if bindAddrEnv := os.Getenv(bindAddrKey); len(bindAddrEnv) > 0 {
		BindAddr = bindAddrEnv
	}

	if kafkaAddrEnv := os.Getenv(kafkaAddrKey); len(kafkaAddrEnv) > 0 {
		KafkaAddr = kafkaAddrEnv
	}

	if awsRegionEnv := os.Getenv(awsRegionKey); len(awsRegionEnv) > 0 {
		AWSRegion = awsRegionEnv
	}

	if consumerGroupEnv := os.Getenv(kafkaConsumerGroup); len(consumerGroupEnv) > 0 {
		KafkaConsumerGroup = consumerGroupEnv
	}

	if consumerTopicEnv := os.Getenv(kafkaConsumerTopic); len(consumerTopicEnv) > 0 {
		KafkaConsumerTopic = consumerTopicEnv
	}

	if hierarchyEndpointEnv := os.Getenv(hierarchyEndpoint); len(hierarchyEndpointEnv) > 0 {
		HierarchyEndpoint = hierarchyEndpointEnv
	}

	if useGzipCompressionEnv := os.Getenv(useGzipCompression); len(useGzipCompressionEnv) > 0 {
		var err error
		UseGzipCompression, err = strconv.ParseBool(useGzipCompressionEnv)
		if err != nil {
			panic("Invalid boolean value for " + useGzipCompression + ": " + useGzipCompressionEnv)
		}
	}

}

func Load() {
	// Will call init().
	log.Debug("dp-csv-transformer Configuration", log.Data{
		bindAddrKey:        BindAddr,
		kafkaAddrKey:       KafkaAddr,
		awsRegionKey:       AWSRegion,
		kafkaConsumerGroup: KafkaConsumerGroup,
		kafkaConsumerTopic: KafkaConsumerTopic,
		hierarchyEndpoint:  HierarchyEndpoint,
		useGzipCompression: UseGzipCompression,
	})
}
