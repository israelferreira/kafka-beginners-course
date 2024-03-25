package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "hello world " + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            logger.info("Key: {}", key);

            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("""
                            Received new metadata.
                            Topic: {}
                            Partition: {}
                            Offset: {}
                            Timestamp: {}
                            """,
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", exception);
                }
            }).get(); //block the .send() to make it synchronously - don't do this in production
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
