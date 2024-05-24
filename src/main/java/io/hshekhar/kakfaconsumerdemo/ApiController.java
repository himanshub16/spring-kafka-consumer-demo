package io.hshekhar.kakfaconsumerdemo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController("/")
public class ApiController {
    private static final Logger logger = LogManager.getLogger(ApiController.class);

    public record PublishRequest(String message, int times) {}

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    @Qualifier("topicName")
    String topicName;

    @PostMapping("/publish")
    public ResponseEntity<List<Long>> publishToKafka(@RequestBody PublishRequest request) {
        logger.info("publishing {} messages to topic {}. message={}", request.message, request.times);
        var offsets = IntStream.range(1, request.times+1)
                .boxed()
                .map(idx -> {
                    try {
                        return kafkaTemplate.send(topicName, idx, String.format("%d-%s", idx, request.message)).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(sendResult -> sendResult.getRecordMetadata().offset())
                .collect(Collectors.toList());
        return ResponseEntity.ofNullable(offsets);
    }
}
