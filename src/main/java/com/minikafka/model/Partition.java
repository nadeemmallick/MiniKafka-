package com.minikafka.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a partition within a topic.
 * A partition is an ordered sequence of messages.
 * Thread-safe for concurrent message appending and reading.
 */
public class Partition {
    private final int partitionId;
    private final List<Message> messages;
    private long currentOffset;
    private final Object lock = new Object(); // Lock for thread safety

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
     * Thread-safe using synchronized block.
     * Validates message value before appending.
     */
    public void appendMessage(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Message value cannot be null or empty");
        }
        
        synchronized (lock) {
            Message message = new Message(currentOffset, value);
            messages.add(message);
            currentOffset++;
        }
    }

    /**
     * Gets a message at a specific offset.
     * Thread-safe - reads from synchronized list but doesn't modify.
     */
    public Message getMessage(long offset) {
        synchronized (lock) {
            if (offset < 0 || offset >= messages.size()) {
                throw new IllegalArgumentException("Invalid offset: " + offset);
            }
            return messages.get((int) offset);
        }
    }

    /**
     * Gets all messages in this partition.
     * Thread-safe - returns a copy to prevent external modification.
     */
    public List<Message> getMessages() {
        synchronized (lock) {
            return new ArrayList<>(messages);
        }
    }

    /**
     * Gets the current offset (next available offset).
     * Thread-safe read operation.
     */
    public long getCurrentOffset() {
        synchronized (lock) {
            return currentOffset;
        }
    }

    /**
     * Gets the number of messages in this partition.
     * Thread-safe read operation.
     */
    public int getMessageCount() {
        synchronized (lock) {
            return messages.size();
        }
    }
}
