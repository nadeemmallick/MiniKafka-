# MiniKafka — Product Requirements Document (PRD)

**Version:** 1.0  
**Duration:** 7 Days  
**Project Type:** Kafka-inspired educational message broker  
**Primary Language:** Java  
**Difficulty:** Beginner → Intermediate

---

## 1. Project Overview

MiniKafka is a small message-broker system inspired by the core ideas of Apache Kafka.

The purpose is **not** to recreate Apache Kafka. The purpose is to understand how a message broker works by building a simplified version from scratch using Core Java.

The project will be developed in small steps during one week. Each day adds one meaningful concept and produces a working result that can be tested.

### Simple Project Flow

```text
Producer → Broker → Topic → Partition → Message
                              ↓
                           Consumer
```

---

## 2. Problem Statement

When applications communicate directly with many other applications, they become tightly coupled.

For example:

```text
Order Service
     |
     ├──→ Payment Service
     ├──→ Email Service
     ├──→ Notification Service
     └──→ Analytics Service
```

A message broker provides an intermediate system where messages can be published and later consumed.

MiniKafka will demonstrate this idea using a simple Java implementation:

```text
Producer
   ↓
MiniKafka Broker
   ↓
Consumer
```

---

## 3. Project Goal

The project should allow us to:

- Understand the basic architecture of Apache Kafka.
- Implement a simplified producer-broker-consumer flow.
- Understand topics, partitions and offsets through code.
- Practice Core Java and OOP.
- Use Java Collections for message management.
- Use multithreading for concurrent producers and consumers.
- Implement basic file-based message persistence.
- Implement simple TCP communication.
- Create a project that can be clearly explained in a technical interview.

---

# 4. Scope

## 4.1 Included

The 1-week project will include:

- Producer
- Consumer
- Broker
- Topic
- Message
- Partition
- Offset
- In-memory storage
- Multithreading
- File-based persistence
- Basic TCP socket communication
- Basic error handling
- JUnit tests
- README and architecture documentation

## 4.2 Not Included

The first version will **not** include:

- Full Apache Kafka compatibility
- Multiple brokers
- Replication
- Leader election
- KRaft
- ZooKeeper
- Exactly-once semantics
- Production-grade fault tolerance
- Kubernetes
- Complex frontend

These can be considered future enhancements.

---

# 5. Technology Stack

| Technology | Purpose |
|---|---|
| Java | Main programming language |
| Core Java / OOP | Project design and domain classes |
| Java Collections | Topics, partitions and message storage |
| Java Concurrency | Producer/consumer threads |
| Java File I/O | Persistent message logs |
| Java Socket / ServerSocket | Client-server communication |
| Maven | Build and project management |
| JUnit | Testing |
| Git / GitHub | Version control and portfolio |

### Important Rule

We will **not use the Apache Kafka library** to implement the core system.

The purpose is to understand the concepts ourselves.

---

# 6. Core Concepts

| Concept | Simple Meaning |
|---|---|
| Producer | Program that sends/publishes messages |
| Broker | Central system that receives, stores and serves messages |
| Topic | Named category/stream of messages |
| Message | Actual data sent by a producer |
| Partition | Ordered section of a topic containing messages |
| Offset | Position of a message inside a partition |
| Consumer | Program that reads/consumes messages |
| Persistence | Saving messages so they survive a restart |

---

# 7. Final Architecture

```text
                  MiniKafka Server
                        |
                +-------+-------+
                |     Broker    |
                +-------+-------+
                        |
                     Topics
                        |
                    Partitions
                        |
                  Messages/Offsets
                        |
                   File Storage


Producer Client --TCP--> Broker <--TCP-- Consumer Client
```

---

# 8. Core Message Flow

Suppose a producer wants to send:

```text
"Order #101 created"
```

The flow is:

```text
Producer
   |
   | publish("orders", "Order #101 created")
   ↓
Broker
   |
   ↓
orders Topic
   |
   ↓
Partition
   |
   ↓
Offset 0 → "Order #101 created"
   |
   ↓
Consumer
```

The main idea is:

> **Producer sends → Broker stores → Consumer reads**

---

# 9. One-Week Development Plan

## Day 1 — Understand + Project Setup

### Goal

Understand the problem and create the basic project structure.

### Learn

- What is Apache Kafka?
- Why do we need a message broker?
- What is a Producer?
- What is a Consumer?
- What is a Broker?
- What is a Topic?
- What is a Partition?
- What is an Offset?

### Build

Create the Maven project.

Create:

```text
Message.java
Partition.java
Topic.java
Broker.java
```

### Tasks

- Create Maven Java project.
- Create package structure.
- Create `Message` class.
- Create `Partition` class.
- Create `Topic` class.
- Create basic `Broker` class.
- Create a simple `main()` method.
- Create a topic from the broker.

### Expected Result

You should be able to explain:

```text
Producer
   ↓
Broker
   ↓
Topic
   ↓
Partition
```

---

# Day 2 — Producer + Consumer

### Goal

Build the first complete in-memory message flow.

### Build

Producer:

```text
Producer → Broker
```

Consumer:

```text
Broker → Consumer
```

### Tasks

- Implement `broker.send(topic, message)`.
- Store messages in partitions.
- Create `Consumer`.
- Implement `consumer.poll()`.
- Test Producer → Broker → Consumer.
- Handle invalid topic errors.

### Example

```java
Broker broker = new Broker();

broker.createTopic("orders", 1);

broker.send("orders", "Order #101");

Consumer consumer = new Consumer(broker);

consumer.subscribe("orders");

Message message = consumer.poll();

System.out.println(message);
```

### Expected Result

```text
Producer:
Order #101

Broker:
Message stored

Consumer:
Order #101
```

---

# Day 3 — Topics + Partitions + Offsets

### Goal

Make the system behave more like Kafka.

### Learn

Why do we need partitions?

Because a topic can contain many messages.

```text
orders

Partition 0
-----------
Order 1
Order 3
Order 5

Partition 1
-----------
Order 2
Order 4
Order 6
```

### Learn Offset

```text
Partition 0

Offset 0 → Order 1
Offset 1 → Order 3
Offset 2 → Order 5
```

### Tasks

- Support multiple partitions per topic.
- Implement simple round-robin partition selection.
- Assign increasing offsets.
- Read messages using offsets.
- Test message ordering inside a partition.
- Document why partitions and offsets exist.

### Expected Result

Messages are distributed across partitions and each partition has ordered offsets.

---

# Day 4 — Multithreading

### Goal

Allow producers and consumers to work concurrently.

### Example

```text
Producer Thread 1
        ↓
Producer Thread 2
        ↓
      Broker
        ↑
Consumer Thread 1
        ↑
Consumer Thread 2
```

### Tasks

- Create producer threads.
- Create consumer threads.
- Use thread-safe collections or synchronization.
- Test multiple producers.
- Test multiple consumers.
- Check for race conditions.
- Verify offset consistency.

### Expected Result

Multiple producers and consumers can operate without corrupting message state.

---

# Day 5 — Persistence

### Goal

Make messages survive application restart.

Currently:

```text
Program running
      ↓
Messages in memory
      ↓
Program stops
      ↓
Messages lost
```

We want:

```text
Producer
   ↓
Broker
   ↓
Partition
   ↓
File
```

Example:

```text
data/
└── orders/
    ├── partition-0.log
    └── partition-1.log
```

### Tasks

- Design a simple partition log format.
- Write messages to files.
- Store topic/partition/offset information.
- Load messages when the broker starts.
- Test shutdown → restart → recovery.
- Handle basic file errors.

### Expected Result

Messages are recovered after restarting the broker.

---

# Day 6 — TCP Networking

### Goal

Separate the producer and consumer from the broker.

Before networking:

```text
Producer Object
      ↓
Broker Object
      ↓
Consumer Object
```

After networking:

```text
Producer Application
        |
        | TCP
        ↓
  MiniKafka Server
        |
        | TCP
        ↓
Consumer Application
```

### Tasks

- Create `MiniKafkaServer` using `ServerSocket`.
- Create a simple request format.
- Create `ProducerClient` using `Socket`.
- Create `ConsumerClient` using `Socket`.
- Send publish requests over TCP.
- Send consume requests over TCP.
- Test broker, producer and consumer as separate processes.

### Expected Result

A separate Producer Client and Consumer Client can communicate with the MiniKafka Server.

---

# Day 7 — Testing + Documentation + Interview Preparation

### Goal

Make the project clean, stable and explainable.

### Tasks

- Write JUnit tests.
- Test `Message`.
- Test `Topic`.
- Test `Partition`.
- Test `Broker`.
- Test invalid topics.
- Test invalid offsets.
- Test concurrent producers/consumers.
- Test persistence and recovery.
- Clean code and package names.
- Write README.
- Add architecture diagram.
- Prepare 2-minute interview explanation.
- Prepare 5-minute interview explanation.
- Commit final project to GitHub.

### Expected Result

A working, documented and interview-explainable MiniKafka project.

---

# 10. Proposed Project Structure

```text
mini-kafka/
│
├── pom.xml
├── README.md
│
├── data/
│
└── src/
    │
    ├── main/
    │   └── java/
    │       └── com/
    │           └── minikafka/
    │
    │               ├── model/
    │               │   ├── Message.java
    │               │   ├── Partition.java
    │               │   └── Topic.java
    │               │
    │               ├── broker/
    │               │   └── Broker.java
    │               │
    │               ├── producer/
    │               │   └── Producer.java
    │               │
    │               ├── consumer/
    │               │   └── Consumer.java
    │               │
    │               ├── storage/
    │               │   └── LogStorage.java
    │               │
    │               └── network/
    │                   ├── MiniKafkaServer.java
    │                   ├── ProducerClient.java
    │                   └── ConsumerClient.java
    │
    └── test/
        └── java/
            └── com/
                └── minikafka/
```

---

# 11. Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-01 | Create a topic with a chosen number of partitions | High |
| FR-02 | Publish a message to a topic | High |
| FR-03 | Assign messages to partitions | High |
| FR-04 | Assign sequential offsets within each partition | High |
| FR-05 | Consume messages from a topic | High |
| FR-06 | Track consumer position/offset | High |
| FR-07 | Support concurrent producers and consumers | Medium |
| FR-08 | Persist messages to local files | Medium |
| FR-09 | Recover messages after restart | Medium |
| FR-10 | Communicate through TCP sockets | Medium |
| FR-11 | Provide tests and documentation | High |

---

# 12. Example End-to-End Flow

```text
1. Start MiniKafka Server

2. Create topic:
   orders

3. Topic has:
   2 partitions

4. Producer sends:
   Order #101

5. Broker selects:
   Partition 0

6. Broker assigns:
   Offset 0

7. Message is stored

8. Consumer requests:
   orders

9. Broker returns:
   Order #101

10. Consumer processes it

11. Consumer moves to:
    next offset
```

---

# 13. Initial Internal API

The exact API may change during development, but the first version should be simple.

```java
Broker broker = new Broker();

broker.createTopic("orders", 2);

broker.send("orders", "Order #101");
broker.send("orders", "Order #102");

Consumer consumer = new Consumer(broker);

consumer.subscribe("orders");

Message message = consumer.poll();
```

---

# 14. Testing Strategy

The project should test:

### Basic Functionality

- Create topic.
- Create partitions.
- Publish message.
- Consume message.
- Verify offsets.

### Error Handling

- Invalid topic.
- Invalid partition.
- Invalid offset.
- Empty topic.

### Concurrency

- Multiple producers.
- Multiple consumers.
- Concurrent publishing.
- Concurrent consumption.

### Persistence

```text
Start
 ↓
Publish
 ↓
Save
 ↓
Stop
 ↓
Restart
 ↓
Recover
 ↓
Consume
```

### Networking

```text
Producer Client
      ↓ TCP
MiniKafka Server
      ↑ TCP
Consumer Client
```

---

# 15. Interview Explanation

## 2-Minute Version

> "I built a simplified Kafka-inspired message broker using Core Java. The system has producers that publish messages to topics, a broker that stores messages in partitions with offsets, and consumers that read those messages. I initially implemented the system in memory and then added multithreading, file-based persistence and TCP socket communication. The main purpose was to understand how message brokers and asynchronous communication work internally rather than simply using the Kafka library."

---

## Important Interview Questions

1. Why do we need a message broker?
2. What is a Producer?
3. What is a Consumer?
4. What is a Broker?
5. What is a Topic?
6. Why are partitions used?
7. What is an Offset?
8. Why did you use a queue?
9. How did you handle concurrent access?
10. What happens when the broker restarts?
11. Why did you use TCP sockets?
12. How is MiniKafka different from Apache Kafka?
13. What would you add to make it distributed?
14. How would replication work?
15. What happens if a consumer crashes?

---

# 16. Definition of Done

The 1-week project is considered complete when:

- [ ] Producer can publish messages.
- [ ] Broker can manage multiple topics.
- [ ] Topics can contain multiple partitions.
- [ ] Messages receive sequential offsets.
- [ ] Consumer can read messages.
- [ ] Multiple producers/consumers can run concurrently.
- [ ] Messages can be saved to local files.
- [ ] Messages can be recovered after restart.
- [ ] Producer and consumer can communicate with the broker over TCP.
- [ ] Core components have unit tests.
- [ ] README explains setup and architecture.
- [ ] Architecture diagram is included.
- [ ] Developer can explain the complete system in an interview.

---

# 17. Important Project Rule

Do **not** add advanced Kafka features just to make the project look bigger.

The goal is:

```text
Understand
    ↓
Design
    ↓
Implement
    ↓
Test
    ↓
Explain
```

Every feature should be something you understand well enough to explain to an interviewer.

The 1-week version should prioritize a **working, clean and understandable system** over production-level complexity.

---

# 18. Future Enhancements

After the 1-week version is stable, possible enhancements are:

- Message keys
- Hash-based partitioning
- Consumer groups
- Partition rebalancing
- Broker replication
- Leader/follower architecture
- Failure detection
- Retention policies
- Compression
- Authentication
- Metrics and monitoring
- Docker deployment

These are **not required for the first version**.
