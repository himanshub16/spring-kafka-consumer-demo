# Getting Started

1. Download Kafka, and start Kafka server and Zookeeper. https://kafka.apache.org/quickstart
2. Create topic `consumer-events` with 3 partitions - `bin/kafka-topics.sh --create --topic consumer-demo --bootstrap-server localhost:9092 --partitions 3`
3. Tune parameters in `application.properties`.
4. Start `./gradlew bootRun`


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