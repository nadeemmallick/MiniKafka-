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

    public Broker() {
        this.topics = new HashMap<>();
    }

    /**
     * Creates a new topic with the specified number of partitions.
     */
    public void createTopic(String topicName, int numPartitions) {
        if (topics.containsKey(topicName)) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }
        
        Topic topic = new Topic(topicName, numPartitions);
        topics.put(topicName, topic);
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
     * Uses round-robin to select a partition.
     */
    public void send(String topicName, String message) {
        Topic topic = getTopic(topicName);
        
        // Simple round-robin partition selection (always use partition 0 for now)
        // Will be improved in Day 3
        Partition partition = topic.getPartition(0);
        partition.appendMessage(message);
        
        System.out.println("Message sent to topic '" + topicName + "': " + message);
    }
}
