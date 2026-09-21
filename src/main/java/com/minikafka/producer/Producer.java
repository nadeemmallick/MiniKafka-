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
     */
    public void send(String topicName, String message) {
        broker.send(topicName, message);
    }
}
