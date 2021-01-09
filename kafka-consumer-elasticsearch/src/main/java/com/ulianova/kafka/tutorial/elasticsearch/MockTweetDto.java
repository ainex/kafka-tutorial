package com.ulianova.kafka.tutorial.elasticsearch;

import java.time.Instant;

public class MockTweetDto {

    private String id;
    private String text;
    private String authorId;
    private Instant timestamp;

    public MockTweetDto() {
    }

    public MockTweetDto(String id, String text, String authorId, Instant timestamp) {
        this.id = id;
        this.text = text;
        this.authorId = authorId;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public String getAuthorId() {
        return authorId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setAuthorId(String authorId) {
        this.authorId = authorId;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
