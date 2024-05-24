package io.hshekhar.kakfaconsumerdemo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;

public class SingleMessageListenerWithAck implements AcknowledgingMessageListener<Integer, String> {
    private final static Logger log = LogManager.getLogger(SingleMessageListenerWithAck.class);

    @Override
    public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
        log.info("received message (key={}, value={}) with offset {}", data.value(), data.key(), data.offset());
        try {
            new Task(data.key(), data.value(), data.timestamp()).run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        acknowledgment.acknowledge();
    }
}
