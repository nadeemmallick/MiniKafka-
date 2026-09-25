package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.model.Message;
import com.minikafka.model.Partition;
import com.minikafka.model.Topic;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer that reads messages from the broker.
 * Supports reading from specific partitions with offset tracking per partition.
 */
public class Consumer {
    private Broker broker;
    private String subscribedTopic;
    private Map<Integer, Long> partitionOffsets; // Track offset per partition

    public Consumer(Broker broker) {
        this.broker = broker;
        this.subscribedTopic = null;
        this.partitionOffsets = new HashMap<>();
    }

    /**
     * Subscribes to a specific topic.
     * Validates topic name and existence before subscription.
     * Initializes offset tracking for all partitions in the topic.
     */
    public void subscribe(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (!broker.topicExists(topicName)) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        this.subscribedTopic = topicName;
        
        // Initialize offset tracking for all partitions
        Topic topic = broker.getTopic(topicName);
        partitionOffsets.clear();
        for (int i = 0; i < topic.getNumPartitions(); i++) {
            partitionOffsets.put(i, 0L);
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
     * Uses offset tracking per partition.
     */
    public Message poll(int partitionId) {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed to any topic");
        }

        Topic topic = broker.getTopic(subscribedTopic);
        Partition partition = topic.getPartition(partitionId);
        
        // Get current offset for this partition
        long currentOffset = partitionOffsets.getOrDefault(partitionId, 0L);

        if (currentOffset >= partition.getMessageCount()) {
            return null; // No more messages available in this partition
        }

        Message message = partition.getMessage(currentOffset);
        
        // Update offset for this partition
        partitionOffsets.put(partitionId, currentOffset + 1);
        
        return message;
    }

    /**
     * Gets the current offset for a specific partition.
     */
    public long getCurrentOffset(int partitionId) {
        return partitionOffsets.getOrDefault(partitionId, 0L);
    }

    /**
     * Gets the current offset for partition 0 (for backward compatibility).
     */
    public long getCurrentOffset() {
        return getCurrentOffset(0);
    }

    /**
     * Resets the offset for a specific partition (for testing or re-reading).
     */
    public void seekToBeginning(int partitionId) {
        partitionOffsets.put(partitionId, 0L);
    }

    /**
     * Resets all offsets to beginning (for testing or re-reading).
     */
    public void seekToBeginningAll() {
        for (Integer partitionId : partitionOffsets.keySet()) {
            partitionOffsets.put(partitionId, 0L);
        }
    }
}
