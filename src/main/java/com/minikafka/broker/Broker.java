package com.minikafka.broker;

import com.minikafka.model.Partition;
import com.minikafka.model.Topic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The central broker that manages topics and their partitions.
 * Producers send messages to the broker, consumers read from it.
 * Thread-safe for concurrent operations.
 */
public class Broker {
    private ConcurrentMap<String, Topic> topics;
    private ConcurrentMap<String, AtomicInteger> topicPartitionCounters; // Thread-safe partition counters

    public Broker() {
        this.topics = new ConcurrentHashMap<>();
        this.topicPartitionCounters = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new topic with the specified number of partitions.
     * Validates topic name and partition count.
     * Thread-safe using ConcurrentHashMap atomic operations.
     */
    public void createTopic(String topicName, int numPartitions) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }
        
        // Use putIfAbsent for atomic check-and-create operation
        Topic existingTopic = topics.putIfAbsent(topicName, new Topic(topicName, numPartitions));
        if (existingTopic != null) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
        
        // Initialize partition counter atomically
        topicPartitionCounters.putIfAbsent(topicName, new AtomicInteger(0));
        
        System.out.println("Created topic: " + topicName + " with " + numPartitions + " partitions");
    }

    /**
     * Gets a topic by name.
     */
    public Topic getTopic(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        return topic;
    }

    /**
     * Checks if a topic exists.
     */
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
    }

    /**
     * Gets all topics.
     * Returns a copy to prevent external modification of internal state.
     */
    public ConcurrentMap<String, Topic> getTopics() {
        return new ConcurrentHashMap<>(topics);
    }

    /**
     * Sends a message to a specific topic.
     * Uses round-robin to select a partition for message distribution.
     * Thread-safe using AtomicInteger for partition counter.
     * Validates topic name and message before sending.
     */
    public void send(String topicName, String message) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (message == null || message.trim().isEmpty()) {
            throw new IllegalArgumentException("Message cannot be null or empty");
        }
        
        Topic topic = getTopic(topicName);
        
        // Thread-safe round-robin partition selection using AtomicInteger
        AtomicInteger partitionCounter = topicPartitionCounters.get(topicName);
        int numPartitions = topic.getNumPartitions();
        
        // getAndIncrement() is atomic - returns current value then increments
        int nextPartitionId = partitionCounter.getAndIncrement() % numPartitions;
        
        Partition partition = topic.getPartition(nextPartitionId);
        partition.appendMessage(message);
        
        System.out.println("Message sent to topic '" + topicName + "', partition " + nextPartitionId + ": " + message);
    }
}
