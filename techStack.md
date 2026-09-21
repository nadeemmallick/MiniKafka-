MiniKafka — Tech Stack

1. Overview

The MiniKafka project will use a simple Java-based technology stack.

The goal is to understand the core concepts behind Apache Kafka by implementing a small Kafka-inspired message broker from scratch.

We will avoid unnecessary frameworks and technologies so that every part of the project remains easy to understand and explain in an interview.

2. Core Technology Stack

Technology

Purpose

Java 17+

Main programming language

Core Java / OOP

Build Producer, Consumer, Broker, Topic, Partition and Message

Java Collections

Store topics, partitions and messages

Java Concurrency

Handle multiple producers and consumers

Java File I/O

Persist messages to local files

Java Socket / ServerSocket

Producer/Consumer ↔ Broker communication

Maven

Build and project/dependency management

JUnit 5

Unit testing

Git

Version control

GitHub

Repository and portfolio

3. Architecture

              ┌─────────────────┐
              │    Producer     │
              │   Java Client   │
              └────────┬────────┘
                       │
                    TCP Socket
                       │
                       ▼
              ┌─────────────────┐
              │   MiniKafka     │
              │     Broker      │
              └────────┬────────┘
                       │
              ┌────────┴────────┐
              │                 │
         Topic: orders     Topic: payments
              │
         ┌────┴────┐
         ▼         ▼
       P0          P1
         │         │
         └────┬────┘
              │
       File Persistence
              │
              ▼
       ┌─────────────────┐
       │    Consumer     │
       │   Java Client   │
       └─────────────────┘

4. Where Each Technology Is Used

Java

Everything is implemented in Java.

Main components:

Broker
Producer
Consumer
Topic
Partition
Message
LogStorage
MiniKafkaServer

Core Java / OOP

We will use classes and objects to model the Kafka components.

Example:

class Message {
    private long offset;
    private String value;
}

class Topic {
    private String name;
    private List<Partition> partitions;
}

This helps us understand how the real system can be modeled as separate components.

Java Collections

Collections will be used for the initial in-memory implementation.

Examples:

HashMap
ArrayList
LinkedList
Queue
ConcurrentHashMap

Possible structure:

Broker
  ↓
Map<String, Topic>

Topic
  ↓
List<Partition>

Partition
  ↓
List<Message>

Java Concurrency

Used when multiple producers and consumers work at the same time.

Example:

Producer Thread 1 ──┐
Producer Thread 2 ──┼──→ Broker
Producer Thread 3 ──┘

Consumer Thread 1 ──┐
Consumer Thread 2 ──┘

Concepts we will learn:

Thread

Runnable

ExecutorService

Synchronization

Thread-safe collections

Race conditions

Java File I/O

Used for basic message persistence.

Example:

data/
└── orders/
    ├── partition-0.log
    └── partition-1.log

Example log:

0|Order #101
1|Order #102
2|Order #103

When MiniKafka restarts:

File
 ↓
Read messages
 ↓
Rebuild partitions
 ↓
Continue consuming

Java Socket / ServerSocket

Used in the final stage to allow the Producer and Consumer to communicate with the Broker as separate applications.

Architecture:

Producer Client
      |
      | Socket
      ↓
MiniKafka Server
      |
      | Socket
      ↓
Consumer Client

We will use:

ServerSocket
Socket
InputStream
OutputStream

5. Build Tool — Maven

Maven will manage the Java project.

Basic structure:

mini-kafka/
├── pom.xml
└── src/
    ├── main/
    │   └── java/
    └── test/
        └── java/

Maven will be used to:

Compile the project

Run tests

Manage dependencies

Package the application

6. Testing — JUnit 5

JUnit will test individual components.

Examples:

MessageTest
PartitionTest
TopicTest
BrokerTest
ConsumerTest
StorageTest

Tests will verify:

Messages are stored correctly.

Offsets increase correctly.

Topics are created correctly.

Consumers receive the correct messages.

Invalid operations are handled.

Persistence works after restart.

7. Version Control — Git & GitHub

Git will be used throughout development.

Example workflow:

git init

git add .

git commit -m "Initial MiniKafka project"

git commit -m "Add producer and consumer"

git commit -m "Add partitions and offsets"

git commit -m "Add persistence"

git commit -m "Add TCP communication"

The final project will be pushed to GitHub as a portfolio project.

8. Technologies We Will NOT Use Initially

The first version will intentionally avoid:

❌ Spring Boot
❌ Apache Kafka library
❌ Redis
❌ MySQL/PostgreSQL
❌ Docker
❌ Kubernetes
❌ ZooKeeper
❌ KRaft

Why?

Because the purpose is to understand the internal concepts.

For example, instead of:

Spring Boot
    ↓
Kafka dependency
    ↓
send(message)

we want to understand:

Producer
    ↓
Socket
    ↓
Broker
    ↓
Topic
    ↓
Partition
    ↓
Message

9. Final Technology Stack

The final 1-week project stack is:

Language:
Java 17+

Core:
Core Java
OOP
Collections
Concurrency

Storage:
Java File I/O

Networking:
Java Socket / ServerSocket

Build:
Maven

Testing:
JUnit 5

Version Control:
Git
GitHub

10. Resume Version

Tech Stack

Java, Core Java, OOP, Collections, Multithreading, File I/O, Socket Programming, Maven, JUnit 5, Git, GitHub

Project Description

MiniKafka — Kafka-Inspired Message Broker
Built a lightweight message-broker system using Core Java that implements producers, consumers, topics, partitions, offsets, concurrent message processing, file-based persistence, and TCP client-server communication. The project was developed from scratch to understand the core architecture and message flow of Apache Kafka.