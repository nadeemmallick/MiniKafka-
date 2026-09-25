package com.minikafka.producer;

import com.minikafka.broker.Broker;

/**
 * Producer that sends messages to the broker.
 */
public class Producer {
    private Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    /**
     * Sends a message to a specific topic.
     * Validates topic name and message before sending.
     */
    public void send(String topicName, String message) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (message == null || message.trim().isEmpty()) {
            throw new IllegalArgumentException("Message cannot be null or empty");
        }
        broker.send(topicName, message);
    }
}
