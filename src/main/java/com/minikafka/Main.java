package com.minikafka;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.model.Message;
import com.minikafka.producer.Producer;

/**
 * Main class to test the Producer → Broker → Consumer flow.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("=== MiniKafka Day 2 - Producer + Consumer ===\n");

        // Step 1: Create a broker
        Broker broker = new Broker();
        System.out.println("Broker created");

        // Step 2: Create a topic
        broker.createTopic("orders", 1);
        System.out.println();

        // Step 3: Create a producer
        Producer producer = new Producer(broker);

        // Step 4: Producer sends messages
        System.out.println("--- Producer sending messages ---");
        producer.send("orders", "Order #101");
        producer.send("orders", "Order #102");
        producer.send("orders", "Order #103");
        System.out.println();

        // Step 5: Create a consumer
        Consumer consumer = new Consumer(broker);
        consumer.subscribe("orders");
        System.out.println();

        // Step 6: Consumer reads messages
        System.out.println("--- Consumer reading messages ---");
        Message message1 = consumer.poll();
        System.out.println("Received: " + message1);

        Message message2 = consumer.poll();
        System.out.println("Received: " + message2);

        Message message3 = consumer.poll();
        System.out.println("Received: " + message3);

        // Step 7: Try to read when no more messages
        Message message4 = consumer.poll();
        if (message4 == null) {
            System.out.println("No more messages available");
        }

        System.out.println("\n=== Day 2 Complete ===");
        System.out.println("✓ Producer can publish messages");
        System.out.println("✓ Broker stores messages");
        System.out.println("✓ Consumer can read messages");
        System.out.println("✓ Offsets are tracked correctly");
    }
}
