package com.ulianova.kafka.tutorial.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // no subscribe
        // assign
        TopicPartition partition = new TopicPartition(TutorialConstants.FIRST_TOPIC, 0);
        long offsetFrom = 15L;
        consumer.assign(List.of(partition));
        //seek
        consumer.seek(partition, offsetFrom);


        int readMessagesCount = 0;
        int messageLimit = 5;
        while (readMessagesCount < messageLimit) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                readMessagesCount++;
                logger.info("Key: {} Value: {}", record.key(), record.value());
                logger.info("Partition: {} Offset: {}", record.partition(), record.offset());
                if (readMessagesCount >= messageLimit) {
                    break;
                }
            }
        }
    }
}
