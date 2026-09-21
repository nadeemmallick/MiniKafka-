package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.model.Message;
import com.minikafka.model.Partition;
import com.minikafka.model.Topic;

/**
 * Consumer that reads messages from the broker.
 */
public class Consumer {
    private Broker broker;
    private String subscribedTopic;
    private long currentOffset;

    public Consumer(Broker broker) {
        this.broker = broker;
        this.subscribedTopic = null;
        this.currentOffset = 0;
    }

    /**
     * Subscribes to a specific topic.
     */
    public void subscribe(String topicName) {
        if (!broker.topicExists(topicName)) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        this.subscribedTopic = topicName;
        this.currentOffset = 0;
        System.out.println("Consumer subscribed to topic: " + topicName);
    }

    /**
     * Polls for the next message from the subscribed topic.
     */
    public Message poll() {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed to any topic");
        }

        Topic topic = broker.getTopic(subscribedTopic);
        Partition partition = topic.getPartition(0); // Always use partition 0 for now

        if (currentOffset >= partition.getMessageCount()) {
            return null; // No more messages available
        }

        Message message = partition.getMessage(currentOffset);
        currentOffset++;
        return message;
    }

    /**
     * Gets the current offset of this consumer.
     */
    public long getCurrentOffset() {
        return currentOffset;
    }
}
