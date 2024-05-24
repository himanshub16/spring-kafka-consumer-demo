# Getting Started

1. Download Kafka, and start Kafka server and Zookeeper. https://kafka.apache.org/quickstart
2. Create topic `consumer-events` with partitions using command below.
3. Tune parameters in `application.properties`.
4. Start `./gradlew bootRun`

in `<kakfa_download_dir>/config/server.properties`, add `delete.topic.enable=true`.
This helps delete the topic immediately if we want to change number of partitions and experiment again.

To delete topic
```shell
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic consumer-demo --delete
```

To create topic with partitions
```shell
bin/kafka-topics.sh --create --topic consumer-demo --bootstrap-server localhost:9092 --partitions 3
```


To produce messages,
```
curl -X POST http://localhost:8080/publish \
-H "Content-Type: application/json" \
--data '{"message": "hello", "times": 12}'
```
The times parameter submits <times> messages to the topic.

The consumer should start processing the messages in background.
With the log lines, we can see what time it takes for the processing to complete.

For each message, we can also check the time difference between when the message was published and when the processing was complete.