package com.ulianova.tutorial.kafka.streams.favcolour;

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

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class FavColourStarterApp {
    private static final Set<String> ALLOWED_COLOURS = Set.of("red", "green", "blue");

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(FavColourStarterApp.class);
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, TutorialConstants.FAV_COLOUR_APPLICATION_ID);
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TutorialConstants.BOOTSTRAP_SERVER);
        streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // stream from Kafka
        KStream<String, String> favColourInput = builder.stream(TutorialConstants.FAV_COLOUR_TOPIC_IN,
                Consumed.with(Serdes.String(), Serdes.String()));
        // alice,red   bob,green, alice,blue  mike,white
        KTable<String, Long> favColourCounts = favColourInput
                .mapValues(value -> value.toLowerCase())
                .filter((name, colour) -> ALLOWED_COLOURS.contains(colour))
                // select key to apply a key
                //.selectKey((key, value) -> value)
                // group by key before aggregation
                .groupBy((name, colour) -> colour, Grouped.with(Serdes.String(), Serdes.String()))
                // count occurrences
                .count();

        favColourCounts.toStream().to(TutorialConstants.FAV_COLOUR_TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamProperties);

        // Add shutdown hook
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(TutorialConstants.FAV_COLOUR_APPLICATION_ID + "-shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutting down {} ...", TutorialConstants.FAV_COLOUR_APPLICATION_ID);
                streams.close();
                latch.countDown();
                logger.info("Shutting down of {} is done", TutorialConstants.FAV_COLOUR_APPLICATION_ID);
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            logger.info("TOPOLOGY of {}:\n {}", TutorialConstants.FAV_COLOUR_APPLICATION_ID, topology.describe());
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
