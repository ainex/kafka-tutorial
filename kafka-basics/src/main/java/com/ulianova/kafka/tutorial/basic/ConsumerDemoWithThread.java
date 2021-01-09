package com.ulianova.kafka.tutorial.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String groupId = "my-sixth-application";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread wih latch: {}", latch.getCount());
        Runnable consumerRunnable = new ConsumerRunnable(TutorialConstants.BOOTSTRAP_SERVER, groupId, TutorialConstants.FIRST_TOPIC, latch);

        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application was interrupted");
            } finally {
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application was interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;


        public ConsumerRunnable(String bootstrapServer, String groupId, String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            // https://kafka.apache.org/documentation/#consumerconfigs
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer and subscribe
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(List.of(topic));
        }

        @Override
        public void run() {
            // poll a new piece of data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(MessageFormat.format("Key: {0} Value: {1}",
                                record.key(), record.value()));
                        logger.info(MessageFormat.format("Partition: {0,number,#} Offset: {1,number,#}",
                                record.partition(), record.offset()));
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // it is done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // special method to interrupt consumer.poll()
            // throw WakeUpException
            consumer.wakeup();
        }
    }
}
