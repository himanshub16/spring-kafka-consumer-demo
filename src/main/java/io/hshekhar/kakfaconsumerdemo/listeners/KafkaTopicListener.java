package io.hshekhar.kakfaconsumerdemo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaTopicListener {
    private static final Logger log = LogManager.getLogger(KafkaTopicListener.class);
    private final ExecutorService executorService;

    public KafkaTopicListener(@Value("${kafka.batch.processing.concurrency}") int processingConcurrency) {
        log.info("creating batch listener with thread pool size of " + processingConcurrency);
        this.executorService = Executors.newFixedThreadPool(processingConcurrency);
    }


    @KafkaListener(
            id = "${spring.kafka.consumer.group-id}",
            topics = "${kafka.topic.name}",
            autoStartup = "true",
            concurrency = "${spring.kafka.listener.concurrency}" // number of topics in case everything is assigned to single partition
    )
    public void listenBatch(List<ConsumerRecord<Integer, String>> records, Acknowledgment ack) {
        Instant startTime = Instant.now();
        log.info("processing " + records.size() + " records");
        var futures = records.stream()
                .map(r -> new Task(r.key(), r.value(), r.timestamp()))
                .map(t -> CompletableFuture.runAsync(t, executorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        ack.acknowledge();
        log.info("processed " + records.size() + " records in " + (Instant.now().toEpochMilli() - startTime.toEpochMilli()) + " ms");
    }
}
