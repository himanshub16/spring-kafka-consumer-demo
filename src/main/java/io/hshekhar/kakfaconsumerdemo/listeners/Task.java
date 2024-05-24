package io.hshekhar.kakfaconsumerdemo.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class Task implements Runnable {
    private static final Logger log = LogManager.getLogger(Task.class);
    private final String value;
    private final Integer key;
    private final long timestamp;

    public Task(Integer key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(500);
            log.info("task completed: key={}, value={}, time_taken={}", key, value, Instant.now().toEpochMilli() - timestamp);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
