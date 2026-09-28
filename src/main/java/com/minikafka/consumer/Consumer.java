package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.model.Message;
import com.minikafka.model.Partition;
import com.minikafka.model.Topic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer that reads messages from the broker.
 * Supports reading from specific partitions with offset tracking per partition.
 * Thread-safe for concurrent consumption.
 */
public class Consumer {
    private final Broker broker;
    private volatile String subscribedTopic;
    private final ConcurrentMap<Integer, AtomicLong> partitionOffsets; // Thread-safe offset tracking

    public Consumer(Broker broker) {
        this.broker = broker;
        this.subscribedTopic = null;
        this.partitionOffsets = new ConcurrentHashMap<>();
    }

    /**
     * Subscribes to a specific topic.
     * Validates topic name and existence before subscription.
     * Initializes offset tracking for all partitions in the topic.
     * Thread-safe using synchronized block for topic change.
     */
    public synchronized void subscribe(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (!broker.topicExists(topicName)) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        this.subscribedTopic = topicName;
        
        // Initialize offset tracking for all partitions with AtomicLong
        Topic topic = broker.getTopic(topicName);
        partitionOffsets.clear();
        for (int i = 0; i < topic.getNumPartitions(); i++) {
            partitionOffsets.put(i, new AtomicLong(0));
        }
        
        System.out.println("Consumer subscribed to topic: " + topicName + " with " + topic.getNumPartitions() + " partitions");
    }

    /**
     * Polls for the next message from partition 0 of the subscribed topic.
     * For backward compatibility with Day 2.
     */
    public Message poll() {
        return poll(0);
    }

    /**
     * Polls for the next message from a specific partition of the subscribed topic.
     * Uses thread-safe offset tracking per partition with AtomicLong.
     */
    public Message poll(int partitionId) {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed to any topic");
        }

        Topic topic = broker.getTopic(subscribedTopic);
        Partition partition = topic.getPartition(partitionId);
        
        // Get current offset for this partition using AtomicLong
        AtomicLong offsetCounter = partitionOffsets.get(partitionId);
        if (offsetCounter == null) {
            offsetCounter = new AtomicLong(0);
            partitionOffsets.put(partitionId, offsetCounter);
        }
        
        long currentOffset = offsetCounter.get();

        if (currentOffset >= partition.getMessageCount()) {
            return null; // No more messages available in this partition
        }

        Message message = partition.getMessage(currentOffset);
        
        // Atomically increment offset for this partition
        offsetCounter.incrementAndGet();
        
        return message;
    }

    /**
     * Gets the current offset for a specific partition.
     * Thread-safe read using AtomicLong.
     */
    public long getCurrentOffset(int partitionId) {
        AtomicLong offsetCounter = partitionOffsets.get(partitionId);
        return offsetCounter != null ? offsetCounter.get() : 0L;
    }

    /**
     * Gets the current offset for partition 0 (for backward compatibility).
     */
    public long getCurrentOffset() {
        return getCurrentOffset(0);
    }

    /**
     * Resets the offset for a specific partition (for testing or re-reading).
     * Thread-safe using AtomicLong.
     */
    public void seekToBeginning(int partitionId) {
        AtomicLong offsetCounter = partitionOffsets.get(partitionId);
        if (offsetCounter != null) {
            offsetCounter.set(0);
        } else {
            partitionOffsets.put(partitionId, new AtomicLong(0));
        }
    }

    /**
     * Resets all offsets to beginning (for testing or re-reading).
     * Thread-safe operation.
     */
    public void seekToBeginningAll() {
        for (AtomicLong offsetCounter : partitionOffsets.values()) {
            offsetCounter.set(0);
        }
    }
}
