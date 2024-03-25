package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        var latch = new CountDownLatch(1);
        var consumerRunnable = new ConsumerRunnable(latch);
        var consumerThread = new Thread(consumerRunnable);

        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application has exited", e);
            } finally {
                logger.error("Application is closing");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.error("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {
        private static Properties getProperties() {
            String groupId = "my-sixth-application";
            var properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }

        private final CountDownLatch latch;
        private final Consumer<String, String> kafkaConsumer;
        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
            kafkaConsumer = new KafkaConsumer<>(getProperties());
            kafkaConsumer.subscribe(List.of("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord: records) {
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
                    }
                }
            }  catch(WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally  {
                kafkaConsumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }
    }

}
