package io.hshekhar.kakfaconsumerdemo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchMessageListenerWithAck implements BatchAcknowledgingMessageListener<Integer, String> {
    private static final Logger log = LogManager.getLogger(BatchMessageListenerWithAck.class);

    private final ExecutorService executorService;

    public BatchMessageListenerWithAck(int processingConcurrency) {
        this.executorService = Executors.newFixedThreadPool(processingConcurrency);
    }

    @Override
    public void onMessage(List<ConsumerRecord<Integer, String>> records, Acknowledgment acknowledgment) {
        log.info("got {} records", records.size());
        var startTime = Instant.now();
        var futures = records.stream()
                .map(record -> new Task(record.key(), record.value(), record.timestamp()))
                .map(task -> CompletableFuture.runAsync(task, executorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();

        acknowledgment.acknowledge();
        log.info("processed {} records in {} ms", records.size(), startTime.until(Instant.now(), ChronoUnit.MILLIS));
    }
}
