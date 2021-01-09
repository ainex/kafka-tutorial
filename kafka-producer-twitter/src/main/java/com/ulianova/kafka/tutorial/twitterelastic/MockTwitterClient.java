package com.ulianova.kafka.tutorial.twitterelastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class MockTwitterClient implements Client, Runnable {

    private volatile boolean isDone = false;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlockingQueue<String> messageQueue;

    public MockTwitterClient(BlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void connect() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void reconnect() {

    }

    @Override
    public void stop() {
        isDone = true;
    }

    @Override
    public void stop(int i) {

    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public StreamingEndpoint getEndpoint() {
        return null;
    }

    @Override
    public StatsReporter.StatsTracker getStatsTracker() {
        return null;
    }

    @Override
    public void run() {
        // todo
        while (!isDone) {
            try {
                messageQueue.add(objectMapper.writeValueAsString(createTweet()));
                Thread.sleep(500);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                isDone = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                isDone = true;
            }
        }
    }

    private MockTweetDto createTweet() {
        return new MockTweetDto(
                UUID.randomUUID().toString(),
                RandomStringUtils.randomAlphabetic(50),
                RandomStringUtils.randomNumeric(10),
                Instant.now()
        );
    }
}
