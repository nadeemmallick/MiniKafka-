package com.minikafka.broker;

import com.minikafka.model.Partition;
import com.minikafka.model.Topic;

import java.util.HashMap;
import java.util.Map;

/**
 * The central broker that manages topics and their partitions.
 * Producers send messages to the broker, consumers read from it.
 */
public class Broker {
    private Map<String, Topic> topics;
    private Map<String, Integer> topicPartitionCounters; // Tracks next partition for round-robin

    public Broker() {
        this.topics = new HashMap<>();
        this.topicPartitionCounters = new HashMap<>();
    }

    /**
     * Creates a new topic with the specified number of partitions.
     * Validates topic name and partition count.
     */
    public void createTopic(String topicName, int numPartitions) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (topics.containsKey(topicName)) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }
        
        Topic topic = new Topic(topicName, numPartitions);
        topics.put(topicName, topic);
        topicPartitionCounters.put(topicName, 0); // Initialize partition counter to 0
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
     */
    public Map<String, Topic> getTopics() {
        return new HashMap<>(topics);
    }

    /**
     * Sends a message to a specific topic.
     * Uses round-robin to select a partition for message distribution.
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
        
        // Round-robin partition selection
        int nextPartitionId = topicPartitionCounters.get(topicName);
        Partition partition = topic.getPartition(nextPartitionId);
        partition.appendMessage(message);
        
        // Update partition counter for next message (round-robin)
        int numPartitions = topic.getNumPartitions();
        topicPartitionCounters.put(topicName, (nextPartitionId + 1) % numPartitions);
        
        System.out.println("Message sent to topic '" + topicName + "', partition " + nextPartitionId + ": " + message);
    }
}
