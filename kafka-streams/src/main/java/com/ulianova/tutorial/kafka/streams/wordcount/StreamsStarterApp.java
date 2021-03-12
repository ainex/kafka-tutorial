package com.ulianova.tutorial.kafka.streams.wordcount;

import com.ulianova.tutorial.kafka.streams.TutorialConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsStarterApp {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(StreamsStarterApp.class);
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, TutorialConstants.APPLICATION_ID);
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TutorialConstants.BOOTSTRAP_SERVER);
        streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // stream from Kafka
        KStream<String, String> wordCountInput = builder.stream(TutorialConstants.WORD_COUNT_TOPIC_IN,
                Consumed.with(Serdes.String(), Serdes.String()));

        // map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase())
                // flat map values
                .flatMapValues(value -> List.of(value.split(" ")))
                // select key to apply a key
                .selectKey((key, value) -> value)
                // group by key before aggregation
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                // count occurrences
                .count();

        wordCounts.toStream().to(TutorialConstants.WORD_COUNT_TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamProperties);
        //streams.start();

        // Add shutdown hook
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(TutorialConstants.APPLICATION_ID + "-shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutting down ...");
                streams.close();
                latch.countDown();
                logger.info("Shutting down done");
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            logger.info("TOPOLOGY:\n {}", topology.describe());
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
