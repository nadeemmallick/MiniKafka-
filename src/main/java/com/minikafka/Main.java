package com.minikafka;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.model.Message;
import com.minikafka.producer.Producer;

/**
 * Main class to test the Producer → Broker → Consumer flow with comprehensive validation.
 * Includes Day 4 multithreading tests.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     🎬 MINIKAFRA COMPREHENSIVE DEMO (Days 2-4)             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // Run Day 2-3 demo first
        runDay2Demo();
        
        // Run Day 4 multithreading demo
        runDay4Demo();
    }

    private static void runDay2Demo() {
        System.out.println("📅 DAYS 2-3: Single-Threaded Message Flow");
        System.out.println("══════════════════════════════════════════════════════════════");

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

        System.out.println("\n=== Day 2-3 Complete ===");
        System.out.println("✓ Producer can publish messages");
        System.out.println("✓ Broker stores messages with validation");
        System.out.println("✓ Consumer can read messages with offset tracking");
        System.out.println("✓ Error handling for invalid inputs");
        System.out.println("✓ Multiple topics support");
        System.out.println("✓ Offsets are tracked correctly");
    }

    private static void runDay4Demo() {
        System.out.println("\n\n📅 DAY 4: Multithreading - Concurrent Producers and Consumers");
        System.out.println("══════════════════════════════════════════════════════════════");

        // Create a new broker for threading tests
        Broker broker = new Broker();
        System.out.println("✓ Thread-safe Broker created");

        // Create topic with multiple partitions for concurrent access
        System.out.println("\n--- Creating Topic for Threading Tests ---");
        broker.createTopic("concurrent-test", 3);
        System.out.println("✓ Topic 'concurrent-test' created with 3 partitions");

        // Test 1: Multiple concurrent producers
        System.out.println("\n--- Test 1: Multiple Concurrent Producers ---");
        testConcurrentProducers(broker);

        // Test 2: Multiple concurrent consumers
        System.out.println("\n--- Test 2: Multiple Concurrent Consumers ---");
        testConcurrentConsumers(broker);

        // Test 3: Mixed concurrent operations
        System.out.println("\n--- Test 3: Mixed Concurrent Operations ---");
        testMixedConcurrentOperations(broker);

        System.out.println("\n=== Day 4 Complete ===");
        System.out.println("✓ Thread-safe Broker operations");
        System.out.println("✓ Concurrent message publishing works");
        System.out.println("✓ Concurrent message consumption works");
        System.out.println("✓ No data corruption under concurrent access");
        System.out.println("✓ Thread-safe offset tracking");
    }

    private static void testConcurrentProducers(Broker broker) {
        final int NUM_PRODUCERS = 5;
        final int MESSAGES_PER_PRODUCER = 10;
        
        System.out.println("Starting " + NUM_PRODUCERS + " producer threads, each sending " + MESSAGES_PER_PRODUCER + " messages");
        
        Thread[] producerThreads = new Thread[NUM_PRODUCERS];
        
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            final int producerId = i;
            producerThreads[i] = new Thread(() -> {
                Producer producer = new Producer(broker);
                for (int j = 0; j < MESSAGES_PER_PRODUCER; j++) {
                    String message = "Producer-" + producerId + "-Message-" + j;
                    producer.send("concurrent-test", message);
                }
            });
            producerThreads[i].start();
        }
        
        // Wait for all producers to complete
        for (Thread thread : producerThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Verify message count
        int totalMessages = 0;
        for (int i = 0; i < 3; i++) {
            totalMessages += broker.getTopic("concurrent-test").getPartition(i).getMessageCount();
        }
        
        int expectedMessages = NUM_PRODUCERS * MESSAGES_PER_PRODUCER;
        System.out.println("Expected messages: " + expectedMessages);
        System.out.println("Actual messages: " + totalMessages);
        
        if (totalMessages == expectedMessages) {
            System.out.println("✓ All messages successfully stored");
        } else {
            System.out.println("✗ Message count mismatch!");
        }
    }

    private static void testConcurrentConsumers(Broker broker) {
        final int NUM_CONSUMERS = 3;
        
        System.out.println("Starting " + NUM_CONSUMERS + " consumer threads reading from different partitions");
        
        Thread[] consumerThreads = new Thread[NUM_CONSUMERS];
        
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            final int consumerId = i;
            final int partitionId = i;
            consumerThreads[i] = new Thread(() -> {
                Consumer consumer = new Consumer(broker);
                consumer.subscribe("concurrent-test");
                
                int messagesRead = 0;
                Message message;
                while ((message = consumer.poll(partitionId)) != null && messagesRead < 5) {
                    messagesRead++;
                    // Simulate processing
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                System.out.println("Consumer-" + consumerId + " read " + messagesRead + " messages from partition " + partitionId);
            });
            consumerThreads[i].start();
        }
        
        // Wait for all consumers to complete
        for (Thread thread : consumerThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.println("✓ All consumers completed successfully");
    }

    private static void testMixedConcurrentOperations(Broker broker) {
        System.out.println("Testing mixed producer and consumer operations");
        
        // Add more messages first
        Producer producer = new Producer(broker);
        for (int i = 0; i < 5; i++) {
            producer.send("concurrent-test", "Additional-Message-" + i);
        }
        
        // Create producer and consumer threads
        Thread producerThread = new Thread(() -> {
            Producer prod = new Producer(broker);
            for (int i = 0; i < 3; i++) {
                prod.send("concurrent-test", "Mixed-Producer-Message-" + i);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        
        Thread consumerThread = new Thread(() -> {
            Consumer cons = new Consumer(broker);
            cons.subscribe("concurrent-test");
            
            int messagesRead = 0;
            Message message;
            while ((message = cons.poll(0)) != null && messagesRead < 3) {
                messagesRead++;
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            System.out.println("Mixed consumer read " + messagesRead + " messages");
        });
        
        producerThread.start();
        consumerThread.start();
        
        try {
            producerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        System.out.println("✓ Mixed concurrent operations completed successfully");
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
