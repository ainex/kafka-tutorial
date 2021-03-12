package com.ulianova.tutorial.kafka.streams;

public class TutorialConstants {
    private TutorialConstants() {
    }

    public static final String APPLICATION_ID = "kafka-streams-app";
    public static final String WORD_COUNT_TOPIC_IN = "word-count-input";
    public static final String WORD_COUNT_TOPIC_OUT = "word-count-output";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static final String FAV_COLOUR_APPLICATION_ID = "fav-colours-app";

    public static final String FAV_COLOUR_TOPIC_IN = "fav-colour-input";
    public static final String FAV_COLOUR_TOPIC_OUT = "fav-colour-output";
    public static final String FAV_COLOUR_FILTERED_TABLE = "fav-colour-filtered-table";
}
