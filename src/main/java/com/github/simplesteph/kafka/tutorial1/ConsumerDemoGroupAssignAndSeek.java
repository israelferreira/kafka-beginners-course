package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoGroupAssignAndSeek {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupAssignAndSeek.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        Consumer<String, String> kafkaConsumer = new KafkaConsumer<>(getProperties());

        var partitionToReadFrom = new TopicPartition("first_topic", 0);

        kafkaConsumer.assign(List.of(partitionToReadFrom));
        long offsetToReadFrom = 1L;

        kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (var consumerRecord : records) {
                numberOfMessageReadSoFar++;

                logger.info("""
                            Key: {}
                            Value: {}
                            Partition: {}
                            Offset: {}
                            Timestamp: {}
                            """,
                            consumerRecord.key(),
                            consumerRecord.value(),
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            consumerRecord.timestamp()
                );

                if (numberOfMessageReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");
        kafkaConsumer.close();
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
