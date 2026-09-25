package com.minikafka.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a partition within a topic.
 * A partition is an ordered sequence of messages.
 */
public class Partition {
    private int partitionId;
    private List<Message> messages;
    private long currentOffset;

    public Partition(int partitionId) {
        this.partitionId = partitionId;
        this.messages = new ArrayList<>();
        this.currentOffset = 0;
    }

    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Appends a message to this partition and assigns it an offset.
     * Validates message value before appending.
     */
    public void appendMessage(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Message value cannot be null or empty");
        }
        Message message = new Message(currentOffset, value);
        messages.add(message);
        currentOffset++;
    }

    /**
     * Gets a message at a specific offset.
     */
    public Message getMessage(long offset) {
        if (offset < 0 || offset >= messages.size()) {
            throw new IllegalArgumentException("Invalid offset: " + offset);
        }
        return messages.get((int) offset);
    }

    /**
     * Gets all messages in this partition.
     */
    public List<Message> getMessages() {
        return new ArrayList<>(messages);
    }

    /**
     * Gets the current offset (next available offset).
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Gets the number of messages in this partition.
     */
    public int getMessageCount() {
        return messages.size();
    }
}
