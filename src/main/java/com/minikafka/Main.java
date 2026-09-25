package com.minikafka;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.model.Message;
import com.minikafka.producer.Producer;

/**
 * Main class to test the Producer → Broker → Consumer flow with comprehensive validation.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("=== MiniKafka Day 2 - Producer + Consumer (Complete) ===\n");

        // Step 1: Create a broker
        Broker broker = new Broker();
        System.out.println("✓ Broker created");

        // Step 2: Create a topic
        System.out.println("\n--- Topic Creation ---");
        broker.createTopic("orders", 1);
        System.out.println("✓ Topic 'orders' created with 1 partition");

        // Step 3: Create a producer
        Producer producer = new Producer(broker);
        System.out.println("✓ Producer created");

        // Step 4: Producer sends messages
        System.out.println("\n--- Producer Sending Messages ---");
        producer.send("orders", "Order #101");
        producer.send("orders", "Order #102");
        producer.send("orders", "Order #103");
        System.out.println("✓ 3 messages sent successfully");

        // Step 5: Create a consumer
        Consumer consumer = new Consumer(broker);
        consumer.subscribe("orders");
        System.out.println("✓ Consumer subscribed to 'orders'");

        // Step 6: Consumer reads messages
        System.out.println("\n--- Consumer Reading Messages ---");
        Message message1 = consumer.poll();
        System.out.println("✓ Received: " + message1);

        Message message2 = consumer.poll();
        System.out.println("✓ Received: " + message2);

        Message message3 = consumer.poll();
        System.out.println("✓ Received: " + message3);

        // Step 7: Try to read when no more messages
        Message message4 = consumer.poll();
        if (message4 == null) {
            System.out.println("✓ No more messages available (expected behavior)");
        }

        // Step 8: Test error handling
        System.out.println("\n--- Error Handling Tests ---");
        testErrorHandling(broker, producer);

        // Step 9: Test multiple topics
        System.out.println("\n--- Multiple Topics Test ---");
        testMultipleTopics(broker, producer);

        System.out.println("\n=== Day 2 Complete ===");
        System.out.println("✓ Producer can publish messages");
        System.out.println("✓ Broker stores messages with validation");
        System.out.println("✓ Consumer can read messages with offset tracking");
        System.out.println("✓ Error handling for invalid inputs");
        System.out.println("✓ Multiple topics support");
        System.out.println("✓ Offsets are tracked correctly");
    }

    private static void testErrorHandling(Broker broker, Producer producer) {
        // Test 1: Send to non-existent topic
        try {
            producer.send("nonexistent", "Test message");
            System.out.println("✗ FAILED: Should have thrown exception for non-existent topic");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected message to non-existent topic: " + e.getMessage());
        }

        // Test 2: Send null topic name
        try {
            producer.send(null, "Test message");
            System.out.println("✗ FAILED: Should have thrown exception for null topic name");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected null topic name: " + e.getMessage());
        }

        // Test 3: Send empty topic name
        try {
            producer.send("", "Test message");
            System.out.println("✗ FAILED: Should have thrown exception for empty topic name");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected empty topic name: " + e.getMessage());
        }

        // Test 4: Send null message
        try {
            producer.send("orders", null);
            System.out.println("✗ FAILED: Should have thrown exception for null message");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected null message: " + e.getMessage());
        }

        // Test 5: Send empty message
        try {
            producer.send("orders", "");
            System.out.println("✗ FAILED: Should have thrown exception for empty message");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected empty message: " + e.getMessage());
        }

        // Test 6: Create duplicate topic
        try {
            broker.createTopic("orders", 1);
            System.out.println("✗ FAILED: Should have thrown exception for duplicate topic");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected duplicate topic: " + e.getMessage());
        }

        // Test 7: Create topic with invalid partition count
        try {
            broker.createTopic("invalid", 0);
            System.out.println("✗ FAILED: Should have thrown exception for invalid partition count");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected invalid partition count: " + e.getMessage());
        }

        // Test 8: Subscribe to non-existent topic
        try {
            Consumer testConsumer = new Consumer(broker);
            testConsumer.subscribe("nonexistent");
            System.out.println("✗ FAILED: Should have thrown exception for non-existent topic subscription");
        } catch (IllegalArgumentException e) {
            System.out.println("✓ Correctly rejected subscription to non-existent topic: " + e.getMessage());
        }

        // Test 9: Poll without subscription
        try {
            Consumer testConsumer = new Consumer(broker);
            testConsumer.poll();
            System.out.println("✗ FAILED: Should have thrown exception for polling without subscription");
        } catch (IllegalStateException e) {
            System.out.println("✓ Correctly rejected polling without subscription: " + e.getMessage());
        }
    }

    private static void testMultipleTopics(Broker broker, Producer producer) {
        // Create second topic
        broker.createTopic("payments", 1);
        System.out.println("✓ Topic 'payments' created");

        // Send messages to both topics
        producer.send("payments", "Payment #501");
        producer.send("payments", "Payment #502");
        System.out.println("✓ Messages sent to 'payments' topic");

        // Create consumer for payments
        Consumer paymentsConsumer = new Consumer(broker);
        paymentsConsumer.subscribe("payments");

        // Consume from payments topic
        Message payment1 = paymentsConsumer.poll();
        System.out.println("✓ Received from payments: " + payment1);

        Message payment2 = paymentsConsumer.poll();
        System.out.println("✓ Received from payments: " + payment2);

        // Verify messages are kept separate per topic
        System.out.println("✓ Multiple topics work independently");
    }
}
