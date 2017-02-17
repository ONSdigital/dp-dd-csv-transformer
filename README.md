dp-csv-transformer
================

Application retrieves a specified CSV file from AWS s3 bucket, and transforms it by adding values to dimensions with hierarchies.  The output is then written to a new file in an AWS s3 bucket.

The ```/transformer``` endpoint accepts HTTP POST request with a FilterRequest body ```{"filePath": "$PATH_TO_FILE$"}```

### Getting started

First grab the code

`go get github.com/ONSdigital/dp-csv-transformer`

You will need to have Kafka set up locally. Set the following env variables (the example here uses the default ports)

```
ZOOKEEPER=localhost:2181
KAFKA=localhost:9092
```

Install Kafka:

```
brew install kafka
brew services start kafka
brew services start zookeeper
```

Run the Kafka console consumer
```
kafka-console-consumer --zookeeper $ZOOKEEPER --topic transform-request
```

Run the Kafka console producer
```
kafka-console-producer --broker-list $KAFKA --topic transform-request
```

Start the metadata api
```
git clone git@github.com:ONSdigital/dp-dd-dimensional-metadata-api.git
cd dp-dd-dimensional-metadata-api
mvn -U spring-boot:run
```
(Please see [dp-dd-dimensional-metadata-api](https://github.com/ONSdigital/dp-dd-dimensional-metadata-api) 
for more details about the requirements of the metadata api)

Run the transformer
```
make debug
```

Paste the following line into the kafka console producer mentioned above:
```
{ "inputUrl": "s3://dp-csv-filter/Open-Data-v3-filtered.csv", "outputUrl": "s3://dp-dd-csv-filter/Open-Data-v3-transformed.csv" }
```

The project includes a small data set in the `sample_csv` directory for test usage.

### Configuration

| Environment variable | Default                                                 | Description
| -------------------- | ------------------------------------------------------- | ----------------------------------------------------
| KAFKA_ADDR           | "http://localhost:9092"                                 | The Kafka address to request messages from.
| HIEARARCHY_ENDPOINT  | "http://localhost:20099/hierarchies/{hierarchy_id}"     | The endpoint to call to get hierarchy information.
| AWS_REGION           | "eu-west-1"                                             | The AWS region to use.
| KAFKA_CONSUMER_GROUP | "transform-request"                                     | The name of the Kafka group to read messages from.
| KAFKA_CONSUMER_TOPIC | "transform-request"                                     | The name of the Kafka topic to read messages from.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
