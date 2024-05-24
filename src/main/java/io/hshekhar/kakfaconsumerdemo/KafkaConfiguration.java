package io.hshekhar.kakfaconsumerdemo;

import io.hshekhar.kakfaconsumerdemo.listeners.BatchMessageListenerWithAck;
import io.hshekhar.kakfaconsumerdemo.listeners.SingleMessageListenerWithAck;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.listener.MessageListener;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.listener.type}")
    private String listenerType;

    @Value("${spring.kafka.listener.concurrency}")
    private int listenerConcurrency;

    @Value("${kafka.batchlistener.concurrency}")
    private int batchListenerConcurrency;

    @Bean
    public Map<String, Object> producerConfigs() {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    @Qualifier("topicName")
    public String getTopicName() {
        return this.topicName;
    }

    @Bean
    GenericMessageListener<? extends Object> kafkaMessageListener() {
        if (listenerType.equals("SingleMessageListenerWithAck")) {
            return new SingleMessageListenerWithAck();
        } else if (listenerType.equals("BatchMessageListenerWithAck")) {
            return new BatchMessageListenerWithAck(batchListenerConcurrency);
        } else {
            throw new RuntimeException("cannot find listener of type " + listenerType);
        }
    }

    @Bean
    @Qualifier("consumerConfigs")
    public Map<String, Object> consumerConfigs() {
        var conf = new HashMap<String, Object>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        if (listenerType.equals("BatchMessageListenerWithAck")) {
            conf.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchListenerConcurrency);
        }
        return conf;
    }

    @Bean
    public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    ConcurrentMessageListenerContainer<Integer, String> concurrentMessageListenerContainer() {
        var containerProps = new ContainerProperties(topicName);
        containerProps.setMessageListener(kafkaMessageListener());
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);

        var listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory(), containerProps);
        listenerContainer.setConcurrency(listenerConcurrency);

        return listenerContainer;
    }
}
