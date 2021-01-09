package com.ulianova.kafka.tutorial.twitterelastic;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class MockTwitterClient implements Client, Runnable {

    private BlockingQueue<String> messageQueue;
    private volatile boolean isDone = false;

    public MockTwitterClient(BlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;
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
            messageQueue.add("New tweet: " + UUID.randomUUID().toString());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                isDone = true;
            }
        }
    }
}
