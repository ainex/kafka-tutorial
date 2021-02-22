package com.ulianova.tutorial.kafka.streams;

public class TutorialConstants {
    private TutorialConstants() {
    }

    public static final String APPLICATION_ID = "kafka-streams-app";
    public static final String WORD_COUNT_TOPIC_IN = "word-count-input";
    public static final String WORD_COUNT_TOPIC_OUT = "word-count-output";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
}
