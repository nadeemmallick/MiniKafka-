package com.minikafka.model;

/**
 * Represents a message in the MiniKafka system.
 * Each message has an offset, value, and timestamp.
 */
public class Message {
    private long offset;
    private String value;
    private long timestamp;

    public Message(long offset, String value) {
        this.offset = offset;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(long offset, String value, long timestamp) {
        this.offset = offset;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "offset=" + offset +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
