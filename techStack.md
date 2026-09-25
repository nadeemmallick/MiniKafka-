# MiniKafka — Technology Stack Document

**Version:** 1.0  
**Project:** MiniKafka - Kafka-inspired educational message broker  
**Date:** September 2026  
**Status:** Approved

---

## 1. Overview

The MiniKafka project will use a simple Java-based technology stack focused on core Java concepts and standard libraries.

The goal is to understand the core concepts behind Apache Kafka by implementing a small Kafka-inspired message broker from scratch.

We will avoid unnecessary frameworks and technologies so that every part of the project remains easy to understand and explain in an interview.

### Design Philosophy

- **Simplicity First**: Use standard Java libraries over frameworks
- **Educational Value**: Prioritize learning over production features
- **Interview Ready**: Every component should be explainable in 2-5 minutes
- **Incremental Complexity**: Start simple, add complexity gradually

## 2. Core Technology Stack

| Technology | Version | Purpose | Rationale |
|------------|---------|---------|-----------|
| **Java** | 17+ | Main programming language | Modern Java features, LTS support, widely used in industry |
| **Core Java / OOP** | - | Build Producer, Consumer, Broker, Topic, Partition and Message | Demonstrates OOP principles, interview-friendly |
| **Java Collections** | - | Store topics, partitions and messages | Efficient in-memory data structures, fundamental Java skill |
| **Java Concurrency** | - | Handle multiple producers and consumers | Critical for distributed systems, common interview topic |
| **Java File I/O** | - | Persist messages to local files | Basic persistence, demonstrates file handling |
| **Java Socket / ServerSocket** | - | Producer/Consumer ↔ Broker communication | Fundamental networking, demonstrates client-server architecture |
| **Maven** | 3.8+ | Build and project/dependency management | Industry standard, dependency management |
| **JUnit 5** | 5.8+ | Unit testing | Modern testing framework, essential for quality code |
| **Git** | 2.30+ | Version control | Industry standard version control |
| **GitHub** | - | Repository and portfolio | Portfolio hosting, collaboration |

## 3. System Architecture

### 3.1 High-Level Architecture

```
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
```

### 3.2 Component Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    MiniKafka Broker                      │
├─────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │ Producer │  │ Consumer │  │  Topic   │  │ Message  │ │
│  │ Component│  │ Component│  │ Component│  │ Component│ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │Partition │  │ Storage  │  │ Network  │              │
│  │ Component│  │ Component│  │ Component│              │
│  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────────────────────────────────────────┘
```

### 3.3 Data Flow Architecture

**In-Memory Flow (Days 1-4):**
```
Producer → Broker → Topic → Partition → Message → Consumer
```

**Persistent Flow (Day 5+):**
```
Producer → Broker → Topic → Partition → Message → File Storage
                                                    ↓
                                              Recovery on Startup
```

**Network Flow (Day 6+):**
```
Producer Client → TCP → MiniKafka Server → TCP → Consumer Client
```

### 3.4 Package Structure

```
com.minikafka/
├── model/              # Domain models
│   ├── Message.java
│   ├── Partition.java
│   └── Topic.java
├── broker/             # Core broker logic
│   └── Broker.java
├── producer/           # Producer implementation
│   └── Producer.java
├── consumer/           # Consumer implementation
│   └── Consumer.java
├── storage/            # Persistence layer
│   └── LogStorage.java
└── network/            # Networking layer
    ├── MiniKafkaServer.java
    ├── ProducerClient.java
    └── ConsumerClient.java
```

## 4. Technology Implementation Details

### 4.1 Java & Core Java/OOP

**Usage**: Everything is implemented in Java using object-oriented principles.

**Main Components:**
- Broker.java - Central message broker
- Producer.java - Message publisher
- Consumer.java - Message subscriber
- Topic.java - Topic management
- Partition.java - Partition management
- Message.java - Message data model
- LogStorage.java - Persistence layer
- MiniKafkaServer.java - Network server

**OOP Concepts Demonstrated:**
- **Encapsulation**: Private fields with getters/setters
- **Inheritance**: (if needed for base classes)
- **Polymorphism**: Interface-based design where appropriate
- **Abstraction**: Hiding implementation details

**Example Code Structure:**
```java
public class Message {
    private long offset;
    private String value;
    private long timestamp;

    public Message(long offset, String value) {
        this.offset = offset;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and setters
}
```

### 4.2 Java Collections

**Usage**: Store topics, partitions, and messages in memory.

**Collections Used:**
- **HashMap<String, Topic>** - Broker topic storage
- **ArrayList<Partition>** - Topic partition storage
- **ArrayList<Message>** - Partition message storage
- **ConcurrentHashMap** - Thread-safe topic storage (concurrency phase)
- **LinkedList** - Message queue (alternative implementation)

**Data Structure Hierarchy:**
```
Broker
  ↓
Map<String, Topic>              // HashMap
  ↓
Topic
  ↓
List<Partition>                 // ArrayList
  ↓
Partition
  ↓
List<Message>                   // ArrayList
```

**Rationale:**
- HashMap provides O(1) topic lookup
- ArrayList provides efficient indexed access
- Collections are fundamental Java skills

### 4.3 Java Concurrency

**Usage**: Handle multiple producers and consumers working simultaneously.

**Concurrency Concepts:**
- **Thread** - Basic thread creation and management
- **Runnable** - Task execution interface
- **ExecutorService** - Thread pool management
- **synchronized** - Method/block synchronization
- **ConcurrentHashMap** - Thread-safe map operations
- **AtomicLong** - Thread-safe counter for offsets
- **Lock/ReentrantLock** - Advanced synchronization (optional)

**Concurrency Architecture:**
```
Producer Thread 1 ──┐
Producer Thread 2 ──┼──→ Broker (Thread-Safe)
Producer Thread 3 ──┘

Consumer Thread 1 ──┐
Consumer Thread 2 ──┘
```

**Implementation Strategy:**
- Phase 1-2: Single-threaded (no concurrency)
- Phase 3: Add thread-safe collections
- Phase 4: Add synchronization for critical sections
- Phase 5+: Test concurrent scenarios

**Example Synchronization:**
```java
public class Partition {
    private final List<Message> messages = new ArrayList<>();
    private final Object lock = new Object();

    public void append(Message message) {
        synchronized (lock) {
            messages.add(message);
        }
    }
}
```

### 4.4 Java File I/O

**Usage**: Persist messages to local files for durability.

**File I/O Components:**
- **FileWriter / BufferedWriter** - Writing messages to files
- **FileReader / BufferedReader** - Reading messages from files
- **Files API** - Directory and file operations
- **Path** - File path handling

**Directory Structure:**
```
data/
└── orders/
    ├── partition-0.log
    └── partition-1.log
```

**Log File Format:**
```
offset|value|timestamp
0|Order #101|1727356800000
1|Order #102|1727356801000
2|Order #103|1727356802000
```

**Persistence Flow:**
```
Message Publish
    ↓
Append to Partition
    ↓
Write to Log File (append-only)
    ↓
Broker Shutdown
    ↓
Broker Restart
    ↓
Read Log Files
    ↓
Rebuild Partitions
    ↓
Resume Operations
```

**Error Handling:**
- Handle missing directories (create on demand)
- Handle file permission errors
- Handle corrupted log files (skip or warn)
- Use try-with-resources for proper resource management

### 4.5 Java Socket / ServerSocket

**Usage**: Enable TCP communication between clients and broker.

**Networking Components:**
- **ServerSocket** - Server-side socket for accepting connections
- **Socket** - Client-side socket for connecting to server
- **InputStream / OutputStream** - Stream-based communication
- **BufferedReader / PrintWriter** - Text-based protocol

**Network Architecture:**
```
Producer Client
      |
      | Socket (localhost:9092)
      ↓
MiniKafka Server (ServerSocket)
      |
      | Socket (localhost:9092)
      ↓
Consumer Client
```

**Protocol Format:**
- **Request**: `COMMAND|PARAM1|PARAM2|...`
- **Response**: `SUCCESS|DATA` or `ERROR|message`

**Example Protocol:**
```
Client: CREATE_TOPIC|orders|2
Server: SUCCESS|Topic created

Client: PUBLISH|orders|Order #101
Server: SUCCESS|Message published

Client: CONSUME|orders|0|0
Server: SUCCESS|Order #101
```

**Implementation Phases:**
- Phase 1-5: In-memory only (no networking)
- Phase 6: Add TCP server and clients
- Phase 7: Test end-to-end communication

### 4.6 Maven

**Usage**: Build automation and dependency management.

**Maven Structure:**
```
mini-kafka/
├── pom.xml
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/minikafka/
│   └── test/
│       └── java/
│           └── com/minikafka/
└── target/
```

**Maven Commands:**
```bash
mvn clean compile      # Compile the project
mvn test              # Run tests
mvn package           # Create JAR file
mvn clean install     # Build and install to local repo
```

**Dependencies:**
```xml
<dependencies>
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter-api</artifactId>
        <version>5.8.2</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

**Build Lifecycle:**
1. **validate** - Validate project structure
2. **compile** - Compile source code
3. **test** - Run unit tests
4. **package** - Create JAR file
5. **install** - Install to local repository

### 4.7 JUnit 5

**Usage**: Unit testing and integration testing.

**Testing Strategy:**
- **Unit Tests** - Test individual components in isolation
- **Integration Tests** - Test component interactions
- **Concurrency Tests** - Test thread safety
- **End-to-End Tests** - Test complete flows

**Test Components:**
- MessageTest.java
- PartitionTest.java
- TopicTest.java
- BrokerTest.java
- ProducerTest.java
- ConsumerTest.java
- StorageTest.java
- NetworkTest.java

**Example Test:**
```java
@Test
void testMessageCreation() {
    Message message = new Message(0, "Test message");
    assertEquals(0, message.getOffset());
    assertEquals("Test message", message.getValue());
    assertNotNull(message.getTimestamp());
}
```

**Test Coverage Goals:**
- Overall coverage: ≥ 80%
- Core components: ≥ 90%
- Critical paths: 100%

### 4.8 Git & GitHub

**Usage**: Version control and portfolio hosting.

**Git Workflow:**
```bash
git init                    # Initialize repository
git add .                   # Stage all changes
git commit -m "message"     # Commit changes
git branch feature-name     # Create feature branch
git checkout feature-name    # Switch to branch
git merge feature-name      # Merge branch
git push origin main        # Push to GitHub
```

**Commit Strategy:**
- Feature branches for each day's work
- Descriptive commit messages
- Regular commits (at least daily)
- Clear commit history

**GitHub Usage:**
- Repository hosting
- Issue tracking (optional)
- README documentation
- Portfolio presentation

## 5. Development Environment

### 5.1 Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| JDK (Java Development Kit) | 17+ | Java compilation and runtime |
| Maven | 3.8+ | Build automation |
| IDE (IntelliJ IDEA/Eclipse/VS Code) | Latest | Code development |
| Git | 2.30+ | Version control |
| Terminal/Command Prompt | - | Command execution |

### 5.2 IDE Configuration

**Recommended IDE:** IntelliJ IDEA Community Edition

**Required Plugins:**
- Maven Integration (usually built-in)
- Git Integration (usually built-in)

**Configuration Steps:**
1. Install JDK 17+
2. Configure JAVA_HOME environment variable
3. Install Maven
4. Configure Maven in IDE
5. Import project as Maven project
6. Configure code style to Java standards

### 5.3 Environment Variables

```bash
JAVA_HOME=C:\Program Files\Java\jdk-17
MAVEN_HOME=C:\Program Files\Apache\maven-3.8.6
PATH=%JAVA_HOME%\bin;%MAVEN_HOME%\bin;%PATH%
```

### 5.4 Project Initialization

```bash
# Create project directory
mkdir mini-kafka
cd mini-kafka

# Initialize Git repository
git init

# Create Maven project
mvn archetype:generate -DgroupId=com.minikafka -DartifactId=mini-kafka -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

# Navigate to project
cd mini-kafka

# Build project
mvn clean install
```

## 6. Technologies Not Used (and Why)

### 6.1 Intentionally Excluded Technologies

| Technology | Reason for Exclusion |
|------------|---------------------|
| **Spring Boot** | Adds framework complexity; goal is to learn core Java |
| **Apache Kafka Library** | Defeats educational purpose; we want to understand internals |
| **Redis** | Unnecessary for basic message broker |
| **MySQL/PostgreSQL** | File-based persistence is sufficient for learning |
| **Docker** | Adds deployment complexity; not needed for local development |
| **Kubernetes** | Overkill for single-node educational project |
| **ZooKeeper** | Not needed for single-broker implementation |
| **KRaft** | Advanced Kafka feature; beyond scope |
| **Message Queues (RabbitMQ, ActiveMQ)** | We're building our own broker |
| **Complex Frontend** | Focus is on backend message processing |

### 6.2 Rationale for Exclusions

**Educational Value Over Production Features:**

Instead of:
```
Spring Boot → Kafka Dependency → send(message)
```

We implement:
```
Producer → Socket → Broker → Topic → Partition → Message
```

**Benefits of This Approach:**
- Deep understanding of message broker internals
- Interview-friendly implementation
- No framework magic to hide behind
- Complete control over architecture
- Portable skills (core Java vs framework-specific)

**When to Consider These Technologies:**
- Future production deployment
- Scaling beyond single node
- Adding advanced features (replication, HA)
- Integration with existing systems

## 7. Final Technology Stack Summary

### 7.1 Complete Stack

| Category | Technology | Version |
|----------|------------|---------|
| **Language** | Java | 17+ |
| **Core** | Core Java, OOP, Collections, Concurrency | - |
| **Storage** | Java File I/O | - |
| **Networking** | Java Socket / ServerSocket | - |
| **Build** | Maven | 3.8+ |
| **Testing** | JUnit 5 | 5.8+ |
| **Version Control** | Git | 2.30+ |
| **Repository** | GitHub | - |

### 7.2 Technology Mapping to Components

| Component | Primary Technologies |
|-----------|---------------------|
| Message | Java, OOP |
| Partition | Java, Collections, Concurrency |
| Topic | Java, Collections |
| Broker | Java, Collections, Concurrency |
| Producer | Java, Socket |
| Consumer | Java, Socket |
| Storage | Java, File I/O |
| Network | Java, Socket, ServerSocket |
| Build | Maven |
| Testing | JUnit 5 |
| Version Control | Git, GitHub |

### 7.3 Technology Learning Outcomes

By completing this project, you will gain expertise in:

**Java Fundamentals:**
- Object-oriented programming principles
- Java Collections Framework
- Exception handling
- File I/O operations

**Advanced Java:**
- Multithreading and concurrency
- Synchronization and thread safety
- Socket programming
- Stream-based I/O

**Software Engineering:**
- Build automation with Maven
- Unit testing with JUnit 5
- Version control with Git
- Test-driven development

**System Design:**
- Message broker architecture
- Client-server communication
- Data persistence strategies
- Concurrent system design

## 8. Resume Version

### 8.1 Tech Stack (for Resume)

```
Java 17, Core Java, OOP, Java Collections, Multithreading, 
File I/O, Socket Programming, Maven, JUnit 5, Git, GitHub
```

### 8.2 Project Description (for Resume)

```
MiniKafka — Kafka-Inspired Message Broker

Built a lightweight message-broker system from scratch using Core Java that 
implements producers, consumers, topics, partitions, offsets, concurrent 
message processing, file-based persistence, and TCP client-server communication. 
The project was developed to understand the core architecture and message flow 
of Apache Kafka without using external frameworks or the Kafka library.

Key Achievements:
• Designed and implemented producer-broker-consumer architecture
• Implemented thread-safe concurrent message processing
• Built file-based persistence system for message durability
• Created TCP socket-based client-server communication
• Achieved 80%+ test coverage with JUnit 5
• Documented architecture and design decisions
```

### 8.3 Interview Talking Points

**Technical Skills Demonstrated:**
- Core Java and OOP design
- Multithreading and concurrency
- Network programming with sockets
- File I/O and persistence
- Test-driven development
- Build automation with Maven

**Problem-Solving Skills:**
- System design and architecture
- Trade-off analysis (simplicity vs. features)
- Incremental development approach
- Debugging and testing strategies

**Communication Skills:**
- Technical documentation
- Code comments and structure
- Architecture diagrams
- Clear explanations of complex concepts

## 9. Technology Risks and Mitigations

### 9.1 Identified Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Thread safety bugs | High | Extensive concurrency testing, use thread-safe collections |
| File corruption | Medium | Append-only writes, error handling, recovery logic |
| Network failures | Medium | Connection retry logic, graceful error handling |
| Memory leaks | Low | Proper resource management, testing |
| Performance issues | Low | Profiling, optimization only if needed |

### 9.2 Mitigation Strategies

**Thread Safety:**
- Use ConcurrentHashMap for shared maps
- Synchronize critical sections
- Test with multiple concurrent producers/consumers
- Use AtomicLong for offset counters

**File I/O:**
- Use append-only writes
- Implement proper error handling
- Use try-with-resources
- Validate file integrity on recovery

**Networking:**
- Implement connection retry logic
- Handle connection timeouts
- Validate protocol messages
- Graceful shutdown handling

## 10. Future Technology Enhancements

### 10.1 Potential Future Technologies

| Technology | Potential Use Case | Priority |
|------------|-------------------|----------|
| **Lombok** | Reduce boilerplate code | Low |
| **SLF4J/Logback** | Structured logging | Low |
| **Jackson** | JSON serialization | Medium |
| **Docker** | Containerized deployment | Low |
| **Spring Boot** | Production REST API | Low |
| **Message Keys** | Hash-based partitioning | Medium |
| **Consumer Groups** | Advanced consumption | Medium |

### 10.2 Technology Migration Path

**Phase 1 (Current):** Core Java only
**Phase 2 (Optional):** Add logging framework
**Phase 3 (Optional):** Add JSON serialization
**Phase 4 (Optional):** Add Docker for deployment
**Phase 5 (Optional):** Consider Spring Boot for production

## 11. Technology Decision Matrix

### 11.1 Core Java vs. Frameworks

| Factor | Core Java | Frameworks |
|--------|-----------|------------|
| Learning Value | High | Low |
| Interview Value | High | Medium |
| Development Speed | Medium | High |
| Flexibility | High | Medium |
| Dependency Complexity | Low | High |
| **Decision** | **Selected** | **Not Selected** |

### 11.2 In-Memory vs. Database

| Factor | In-Memory + File I/O | Database |
|--------|---------------------|----------|
| Learning Value | High | Low |
| Complexity | Low | Medium |
| Performance | High | Medium |
| Scalability | Low | High |
| **Decision** | **Selected** | **Not Selected** |

### 11.3 Sockets vs. HTTP/REST

| Factor | TCP Sockets | HTTP/REST |
|--------|-------------|-----------|
| Learning Value | High | Medium |
| Complexity | Medium | Low |
| Performance | High | Medium |
| Industry Relevance | High | High |
| **Decision** | **Selected** | **Future** |

## 12. Technology Compliance

### 12.1 Java Version Compliance

- **Minimum Version**: Java 17 (LTS)
- **Recommended Version**: Java 17 or Java 21 (LTS)
- **Features Used**: Records (optional), Switch expressions, Text blocks (optional)

### 12.2 Coding Standards

- **Naming Conventions**: Standard Java naming (camelCase, PascalCase)
- **Code Style**: Clean Code principles
- **Documentation**: Javadoc for public APIs
- **Testing**: JUnit 5 best practices

### 12.3 Build Standards

- **Maven Version**: 3.8+
- **Dependency Management**: Maven Central only
- **Build Lifecycle**: Standard Maven phases
- **Packaging**: JAR file

## 13. Technology Resources

### 13.1 Learning Resources

**Java:**
- Oracle Java Documentation
- Effective Java by Joshua Bloch
- Java Concurrency in Practice

**Maven:**
- Apache Maven Documentation
- Maven: The Complete Reference

**Testing:**
- JUnit 5 User Guide
- Test-Driven Development by Kent Beck

**Networking:**
- Java Socket Programming tutorials
- TCP/IP Illustrated

### 13.2 Reference Documentation

- [Java 17 API Documentation](https://docs.oracle.com/en/java/javase/17/docs/api/)
- [Maven Documentation](https://maven.apache.org/guides/)
- [JUnit 5 User Guide](https://junit.org/junit5/docs/current/user-guide/)
- [Git Documentation](https://git-scm.com/doc)

## 14. Technology Maintenance

### 14.1 Dependency Updates

**Current Dependencies:**
- JUnit 5: 5.8.2 (check for updates quarterly)

**Update Policy:**
- Review dependencies every 3 months
- Update only if security vulnerabilities found
- Test thoroughly after updates

### 14.2 Java Version Updates

**Current Version:** Java 17

**Update Policy:**
- Stay on LTS versions (17, 21, 25, ...)
- Update when current LTS approaches end-of-life
- Test compatibility before upgrading

### 14.3 Tool Updates

**Maven:** Update when new LTS version released  
**Git:** Update when new version includes security fixes  
**IDE:** Update regularly for stability and features

## 15. Conclusion

This technology stack is designed to maximize educational value while maintaining interview readiness. The focus on core Java concepts ensures that the skills learned are portable and fundamental, rather than framework-specific.

The stack is intentionally simple to ensure that every component can be explained clearly in a technical interview, while still covering all the essential concepts of a message broker system.

**Key Principles:**
1. **Learn fundamentals, not frameworks**
2. **Build from scratch to understand internals**
3. **Keep it simple and explainable**
4. **Focus on interview-ready skills**
5. **Add complexity only when needed**

---

## Appendix A: Technology Quick Reference

### Java Collections Quick Reference

```java
// HashMap - O(1) get/put
Map<String, Topic> topics = new HashMap<>();

// ArrayList - O(1) indexed access
List<Message> messages = new ArrayList<>();

// ConcurrentHashMap - Thread-safe map
Map<String, Topic> topics = new ConcurrentHashMap<>();

// LinkedList - Efficient add/remove
Queue<Message> queue = new LinkedList<>();
```

### Java Concurrency Quick Reference

```java
// Thread-safe counter
AtomicLong offset = new AtomicLong(0);
long nextOffset = offset.incrementAndGet();

// Synchronized block
synchronized (lock) {
    // Critical section
}

// Thread pool
ExecutorService executor = Executors.newFixedThreadPool(10);
executor.submit(task);
```

### File I/O Quick Reference

```java
// Write to file
try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
    writer.write("offset|value|timestamp");
}

// Read from file
try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
    String line = reader.readLine();
}
```

### Socket Quick Reference

```java
// Server
ServerSocket serverSocket = new ServerSocket(9092);
Socket clientSocket = serverSocket.accept();

// Client
Socket socket = new Socket("localhost", 9092);
PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
```

---

**Document Status:** Complete  
**Last Updated:** September 2026  
**Next Review:** Upon technology stack changes