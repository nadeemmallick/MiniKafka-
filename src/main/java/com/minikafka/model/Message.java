package com.minikafka.model;

/**
 * Represents a message in the MiniKafka system.
 * Each message has an offset and a value.
 */
public class Message {
    private long offset;
    private String value;

    public Message(long offset, String value) {
        this.offset = offset;
        this.value = value;
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

    @Override
    public String toString() {
        return "Message{" +
                "offset=" + offset +
                ", value='" + value + '\'' +
                '}';
    }
}
