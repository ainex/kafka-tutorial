package com.ulianova.kafka.tutorial.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoKeys {

    public static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        // create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs

        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        IntStream.rangeClosed(0, 9)
                .mapToObj(number -> new ProducerRecord<>(FIRST_TOPIC, "id_" + number, "Message with number " + number))
                .forEach(record ->
                        // send data - asynchronous - with callback
                        producer.send(record, (recordMetadata, e) -> {
                            // executes every time on record sending, returns information or exception
                            if (e == null) {
                                logger.info(MessageFormat.format("New metadata. Topic: {0} Partition: {1} Offset: {2} Timestamp: {3,number,#}",
                                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
                            } else {
                                logger.error("Error while producing", e);
                            }
                        }));


        producer.flush();
        producer.close();
    }
}
