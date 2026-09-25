# MiniKafka — Functional Requirements Document (FRD)

**Version:** 1.0  
**Project:** MiniKafka - Kafka-inspired educational message broker  
**Date:** September 2026  
**Status:** Draft

---

## 1. Document Purpose

This Functional Requirements Document (FRD) specifies the detailed functional requirements for the MiniKafka project. It complements the Product Requirements Document (PRD) by providing:

- Detailed functional specifications
- User stories and acceptance criteria
- Input/output specifications
- Error handling requirements
- Performance and scalability requirements

---

## 2. User Personas

### 2.1 Primary Users

| Persona | Role | Goals |
|---------|------|-------|
| Developer | Software developer building message-driven applications | Send messages to topics, consume messages from topics, understand message broker concepts |
| Student | Learner studying distributed systems | Understand Kafka architecture, implement message broker from scratch |
| Interview Candidate | Job seeker preparing for technical interviews | Demonstrate understanding of message brokers, OOP, and multithreading |

### 2.2 Secondary Users

| Persona | Role | Goals |
|---------|------|-------|
| System Administrator | Manages MiniKafka deployment | Start/stop broker, monitor topics, manage storage |

---

## 3. Functional Requirements by Component

### 3.1 Message Component (FR-01)

**Requirement ID:** FR-MESSAGE-001  
**Priority:** High  
**Status:** Pending

#### Description
The Message component represents a single unit of data that can be published to a topic and consumed by consumers.

#### Functional Specifications

**FR-MESSAGE-001.1:** Message Structure
- Each message must contain:
  - `offset`: Long value representing the message's position in a partition
  - `value`: String value containing the actual message content
  - `timestamp`: Long value representing when the message was created (optional)
  - `key`: String value for message routing (optional, future enhancement)

**FR-MESSAGE-001.2:** Message Validation
- Message value must not be null or empty
- Message offset must be non-negative
- Message value length should not exceed 1MB (configurable)

**FR-MESSAGE-001.3:** Message Serialization
- Messages must be serializable for file storage
- Messages must be deserializable for recovery

#### Acceptance Criteria
- [ ] Can create a message with valid value and offset
- [ ] Cannot create a message with null or empty value
- [ ] Cannot create a message with negative offset
- [ ] Message can be serialized to string format
- [ ] Message can be deserialized from string format
- [ ] Message properties are accessible via getters

#### User Story
> As a developer, I want to create messages with content and metadata so that I can publish structured data to topics.

---

### 3.2 Partition Component (FR-02)

**Requirement ID:** FR-PARTITION-001  
**Priority:** High  
**Status:** Pending

#### Description
The Partition component represents an ordered sequence of messages within a topic.

#### Functional Specifications

**FR-PARTITION-001.1:** Partition Structure
- Each partition must contain:
  - `partitionId`: Integer identifier
  - `messages`: Ordered collection of messages
  - `currentOffset`: Long value tracking the next available offset

**FR-PARTITION-001.2:** Message Storage
- Messages must be stored in order of arrival
- Each message must receive a sequential offset starting from 0
- Offsets must be monotonically increasing

**FR-PARTITION-001.3:** Message Retrieval
- Must support retrieval by specific offset
- Must support retrieval from a given offset onwards
- Must return null or empty result for non-existent offsets

**FR-PARTITION-001.4:** Thread Safety
- Partition must handle concurrent message appends
- Partition must handle concurrent message reads
- Must prevent race conditions during offset assignment

#### Acceptance Criteria
- [ ] Can create a partition with a valid ID
- [ ] Can append messages to partition
- [ ] Messages receive sequential offsets (0, 1, 2, ...)
- [ ] Can retrieve message by specific offset
- [ ] Can retrieve messages from offset onwards
- [ ] Multiple threads can append messages concurrently without corruption
- [ ] Multiple threads can read messages concurrently without corruption
- [ ] Offset increments correctly after each append

#### User Story
> As a developer, I want to store messages in ordered partitions so that consumers can read them in sequence.

---

### 3.3 Topic Component (FR-03)

**Requirement ID:** FR-TOPIC-001  
**Priority:** High  
**Status:** Pending

#### Description
The Topic component represents a named category or stream of messages.

#### Functional Specifications

**FR-TOPIC-001.1:** Topic Structure
- Each topic must contain:
  - `name`: String name (unique within broker)
  - `partitions`: Collection of partition objects
  - `partitionCount`: Number of partitions

**FR-TOPIC-001.2:** Topic Creation
- Topic name must be non-null and non-empty
- Topic name must be unique within broker
- Partition count must be positive (>= 1)
- Topic must initialize specified number of partitions on creation

**FR-TOPIC-001.3:** Message Distribution
- Must support round-robin partition selection for message distribution
- Must support retrieving a specific partition by ID
- Must validate partition ID is within valid range

**FR-TOPIC-001.4:** Topic Metadata
- Must provide topic name
- Must provide partition count
- Must provide list of partition IDs

#### Acceptance Criteria
- [ ] Can create a topic with valid name and partition count
- [ ] Cannot create a topic with null or empty name
- [ ] Cannot create a topic with partition count < 1
- [ ] Topic initializes correct number of partitions
- [ ] Messages are distributed across partitions using round-robin
- [ ] Can retrieve specific partition by ID
- [ ] Returns error for invalid partition ID
- [ ] Topic metadata is accessible

#### User Story
> As a developer, I want to create topics with multiple partitions so that I can distribute messages across different consumers.

---

### 3.4 Broker Component (FR-04)

**Requirement ID:** FR-BROKER-001  
**Priority:** High  
**Status:** Pending

#### Description
The Broker component is the central system that manages topics and facilitates message publishing and consumption.

#### Functional Specifications

**FR-BROKER-001.1:** Broker Structure
- Broker must maintain:
  - `topics`: Map of topic name to Topic objects
  - `storage`: Reference to storage component

**FR-BROKER-001.2:** Topic Management
- Must support creating new topics
- Must support checking if topic exists
- Must support retrieving existing topic
- Must prevent duplicate topic creation
- Must return error for operations on non-existent topics

**FR-BROKER-001.3:** Message Publishing
- Must support publishing messages to topics
- Must validate topic exists before publishing
- Must assign message to appropriate partition
- Must assign offset to message
- Must return confirmation of successful publish
- Must return error for failed publish

**FR-BROKER-001.4:** Message Consumption
- Must support retrieving messages from topics
- Must validate topic exists before consumption
- Must support offset-based consumption
- Must track consumer positions (optional, for consumer groups)

**FR-BROKER-001.5:** Thread Safety
- Broker must handle concurrent topic creation
- Broker must handle concurrent message publishing
- Broker must handle concurrent message consumption
- Must prevent race conditions in topic management

#### Acceptance Criteria
- [ ] Can create a new topic with name and partition count
- [ ] Cannot create duplicate topic with same name
- [ ] Can check if topic exists
- [ ] Can retrieve existing topic
- [ ] Returns error for operations on non-existent topic
- [ ] Can publish message to existing topic
- [ ] Published message receives valid offset
- [ ] Can consume message from topic by offset
- [ ] Multiple threads can create topics concurrently
- [ ] Multiple threads can publish messages concurrently
- [ ] Multiple threads can consume messages concurrently

#### User Story
> As a developer, I want a central broker to manage topics and messages so that producers and consumers can communicate through a common system.

---

### 3.5 Producer Component (FR-05)

**Requirement ID:** FR-PRODUCER-001  
**Priority:** High  
**Status:** Pending

#### Description
The Producer component is responsible for publishing messages to the broker.

#### Functional Specifications

**FR-PRODUCER-001.1:** Producer Structure
- Producer must maintain:
  - `broker`: Reference to broker instance
  - `connected`: Boolean connection status

**FR-PRODUCER-001.2:** Message Publishing
- Must support publishing to specific topic
- Must validate topic name is not null or empty
- Must validate message value is not null or empty
- Must handle broker connection errors
- Must handle topic not found errors
- Must retry on transient failures (optional)

**FR-PRODUCER-001.3:** Connection Management
- Must support connecting to broker
- Must support disconnecting from broker
- Must validate connection status before operations

**FR-PRODUCER-001.4:** Batch Publishing (Optional)
- Must support publishing multiple messages in a batch
- Must maintain order within batch
- Must handle partial batch failures

#### Acceptance Criteria
- [ ] Can create producer with broker reference
- [ ] Can publish message to topic
- [ ] Returns error for null or empty topic name
- [ ] Returns error for null or empty message value
- [ ] Returns error when topic does not exist
- [ ] Returns error when not connected to broker
- [ ] Can connect to broker
- [ ] Can disconnect from broker
- [ ] Can publish batch of messages (optional)

#### User Story
> As a developer, I want to publish messages to topics so that other applications can consume them asynchronously.

---

### 3.6 Consumer Component (FR-06)

**Requirement ID:** FR-CONSUMER-001  
**Priority:** High  
**Status:** Pending

#### Description
The Consumer component is responsible for reading messages from the broker.

#### Functional Specifications

**FR-CONSUMER-001.1:** Consumer Structure
- Consumer must maintain:
  - `broker`: Reference to broker instance
  - `subscriptions`: Set of subscribed topics
  - `currentOffsets`: Map of topic/partition to current offset
  - `connected`: Boolean connection status

**FR-CONSUMER-001.2:** Subscription Management
- Must support subscribing to topics
- Must support unsubscribing from topics
- Must validate topic exists before subscription
- Must track active subscriptions

**FR-CONSUMER-001.3:** Message Polling
- Must support polling for new messages
- Must return messages from subscribed topics
- Must start from last committed offset or beginning
- Must return empty result when no new messages available
- Must support timeout for polling (optional)

**FR-CONSUMER-001.4:** Offset Management
- Must track current position for each partition
- Must support manual offset commit (optional)
- Must support auto-commit (optional)
- Must reset offset to beginning or end (optional)

**FR-CONSUMER-001.5:** Connection Management
- Must support connecting to broker
- Must support disconnecting from broker
- Must validate connection status before operations

#### Acceptance Criteria
- [ ] Can create consumer with broker reference
- [ ] Can subscribe to existing topic
- [ ] Cannot subscribe to non-existent topic
- [ ] Can unsubscribe from topic
- [ ] Can poll for messages from subscribed topics
- [ ] Returns messages in order within partition
- [ ] Returns empty result when no new messages
- [ ] Tracks current offset for each partition
- [ ] Can manually commit offset (optional)
- [ ] Can connect to broker
- [ ] Can disconnect from broker

#### User Story
> As a developer, I want to consume messages from topics so that I can process them in my application.

---

### 3.7 Storage Component (FR-07)

**Requirement ID:** FR-STORAGE-001  
**Priority:** Medium  
**Status:** Pending

#### Description
The Storage component handles persistent storage of messages to local files.

#### Functional Specifications

**FR-STORAGE-001.1:** Storage Structure
- Storage must maintain:
  - `dataDirectory`: Root directory for storing data
  - `fileFormat`: Format for storing messages (e.g., "offset|value")

**FR-STORAGE-001.2:** Directory Structure
- Must create topic-specific subdirectories
- Must create partition-specific log files
- Structure: `data/<topicName>/partition-<id>.log`

**FR-STORAGE-001.3:** Message Persistence
- Must append messages to partition log files
- Must use append-only write strategy
- Must format messages as "offset|value"
- Must handle file I/O errors gracefully

**FR-STORAGE-001.4:** Message Recovery
- Must read all messages from log files on startup
- Must parse messages from "offset|value" format
- Must rebuild partition state from log files
- Must handle corrupted log files gracefully

**FR-STORAGE-001.5:** File Management
- Must create directories if they don't exist
- Must handle permission errors
- Must validate file integrity (optional)

#### Acceptance Criteria
- [ ] Can create storage with valid data directory
- [ ] Creates topic subdirectory on first write
- [ ] Creates partition log file on first write
- [ ] Appends messages to log file in correct format
- [ ] Can read messages from log file
- [ ] Can recover partition state from log file
- [ ] Handles file I/O errors without crashing
- [ ] Rebuilds correct partition state after restart

#### User Story
> As a system administrator, I want messages to be persisted to disk so that they survive broker restarts.

---

### 3.8 Network Component (FR-08)

**Requirement ID:** FR-NETWORK-001  
**Priority:** Medium  
**Status:** Pending

#### Description
The Network component handles TCP socket communication between clients and the broker.

#### Functional Specifications

**FR-NETWORK-001.1:** Server Structure
- Server must maintain:
  - `port`: Integer port number
  - `broker`: Reference to broker instance
  - `running`: Boolean server status

**FR-NETWORK-001.2:** Server Operations
- Must start on specified port
- Must accept incoming client connections
- Must handle multiple clients concurrently
- Must process client requests
- Must send responses to clients
- Must gracefully shutdown

**FR-NETWORK-001.3:** Client Structure
- Client must maintain:
  - `host`: String server host
  - `port`: Integer server port
  - `socket`: Socket connection

**FR-NETWORK-001.4:** Client Operations
- Must connect to server
- Must send requests to server
- Must receive responses from server
- Must handle connection errors
- Must disconnect from server

**FR-NETWORK-001.5:** Request/Response Protocol
- Must define simple request format
- Must define simple response format
- Supported request types:
  - CREATE_TOPIC
  - PUBLISH_MESSAGE
  - CONSUME_MESSAGE
  - SUBSCRIBE_TOPIC
- Must handle malformed requests

#### Acceptance Criteria
- [ ] Server can start on specified port
- [ ] Server accepts client connections
- [ ] Server handles multiple clients concurrently
- [ ] Client can connect to server
- [ ] Client can send CREATE_TOPIC request
- [ ] Client can send PUBLISH_MESSAGE request
- [ ] Client can send CONSUME_MESSAGE request
- [ ] Client can send SUBSCRIBE_TOPIC request
- [ ] Server returns appropriate responses
- [ ] Server handles malformed requests
- [ ] Server can shutdown gracefully
- [ ] Client can disconnect gracefully

#### User Story
> As a developer, I want to communicate with the broker over TCP so that producer and consumer can run as separate applications.

---

## 4. Cross-Functional Requirements

### 4.1 Error Handling (FR-EH-001)

**Priority:** High  
**Status:** Pending

#### Functional Specifications

**FR-EH-001.1:** Input Validation
- Must validate all input parameters
- Must return meaningful error messages
- Must not crash on invalid input

**FR-EH-001.2:** Exception Handling
- Must catch and handle expected exceptions
- Must log errors appropriately
- Must provide recovery mechanisms where possible

**FR-EH-001.3:** Error Messages
- Error messages must be descriptive
- Error messages must include context
- Error messages must suggest resolution (optional)

#### Acceptance Criteria
- [ ] All public methods validate input parameters
- [ ] Invalid inputs throw appropriate exceptions
- [ ] Exception handlers prevent application crashes
- [ ] Error messages are descriptive and helpful

---

### 4.2 Concurrency (FR-CONC-001)

**Priority:** Medium  
**Status:** Pending

#### Functional Specifications

**FR-CONC-001.1:** Thread Safety
- Shared data structures must be thread-safe
- Critical sections must be synchronized
- Must avoid deadlocks

**FR-CONC-001.2:** Concurrent Operations
- Must support multiple producers publishing concurrently
- Must support multiple consumers consuming concurrently
- Must support mixed producer/consumer operations

**FR-CONC-001.3:** Performance
- Concurrent operations must not significantly degrade performance
- Must minimize lock contention

#### Acceptance Criteria
- [ ] Multiple producers can publish without data corruption
- [ ] Multiple consumers can consume without data corruption
- [ ] No deadlocks occur under normal operations
- [ ] Concurrent operations complete in reasonable time

---

### 4.3 Testing (FR-TEST-001)

**Priority:** High  
**Status:** Pending

#### Functional Specifications

**FR-TEST-001.1:** Unit Tests
- Must have unit tests for all core components
- Must test positive cases
- Must test negative cases
- Must test edge cases

**FR-TEST-001.2:** Integration Tests
- Must test component interactions
- Must test end-to-end flows
- Must test error scenarios

**FR-TEST-001.3:** Test Coverage
- Target test coverage: >= 80%
- Critical components: >= 90%

#### Acceptance Criteria
- [ ] Message component has unit tests
- [ ] Partition component has unit tests
- [ ] Topic component has unit tests
- [ ] Broker component has unit tests
- [ ] Producer component has unit tests
- [ ] Consumer component has unit tests
- [ ] Storage component has unit tests
- [ ] Network component has unit tests
- [ ] Integration tests cover main flows
- [ ] Overall test coverage >= 80%

---

## 5. Non-Functional Requirements

### 5.1 Performance

**FR-NFR-PERF-001:** Message Latency
- Message publish latency: < 100ms (in-memory)
- Message consume latency: < 100ms (in-memory)

**FR-NFR-PERF-002:** Throughput
- Support at least 1000 messages/second (in-memory)
- Support at least 100 messages/second (with persistence)

**FR-NFR-PERF-003:** Capacity
- Support at least 10 topics
- Support at least 100 partitions per topic
- Support at least 10,000 messages per partition

### 5.2 Reliability

**FR-NFR-REL-001:** Data Durability
- Messages must survive broker restart (with persistence)
- No message loss under normal operations

**FR-NFR-REL-002:** Error Recovery
- System must recover from I/O errors
- System must recover from network errors

### 5.3 Usability

**FR-NFR-USAB-001:** API Simplicity
- Core API should have < 10 methods per component
- Method names should be self-explanatory

**FR-NFR-USAB-002:** Documentation
- All public methods must have Javadoc
- README must explain setup and usage

### 5.4 Maintainability

**FR-NFR-MAINT-001:** Code Quality
- Follow Java naming conventions
- Use meaningful variable names
- Keep methods focused and short (< 50 lines)

**FR-NFR-MAINT-002:** Modularity
- Components should be loosely coupled
- Components should be independently testable

---

## 6. Data Models

### 6.1 Message Data Model

```java
class Message {
    private long offset;
    private String value;
    private long timestamp; // optional
    private String key;     // optional, future
    
    // Getters and setters
}
```

### 6.2 Partition Data Model

```java
class Partition {
    private int partitionId;
    private List<Message> messages;
    private long currentOffset;
    
    // Methods: append, get, getAll
}
```

### 6.3 Topic Data Model

```java
class Topic {
    private String name;
    private List<Partition> partitions;
    
    // Methods: getPartition, distributeMessage
}
```

### 6.4 Broker Data Model

```java
class Broker {
    private Map<String, Topic> topics;
    private LogStorage storage;
    
    // Methods: createTopic, send, get
}
```

---

## 7. API Specifications

### 7.1 Broker API

```java
// Topic Management
void createTopic(String name, int partitionCount);
boolean topicExists(String name);
Topic getTopic(String name);

// Message Operations
void send(String topicName, String messageValue);
List<Message> consume(String topicName, int partitionId, long offset);
```

### 7.2 Producer API

```java
// Connection
void connect(String host, int port);
void disconnect();

// Publishing
void publish(String topicName, String messageValue);
void publishBatch(String topicName, List<String> messages);
```

### 7.3 Consumer API

```java
// Connection
void connect(String host, int port);
void disconnect();

// Subscription
void subscribe(String topicName);
void unsubscribe(String topicName);

// Consumption
Message poll();
Message poll(long timeout);
void commitOffset(String topicName, int partitionId, long offset);
```

---

## 8. File Format Specifications

### 8.1 Log File Format

**Structure:** `offset|value|timestamp`

**Example:**
```
0|Order #101|1727356800000
1|Order #102|1727356801000
2|Order #103|1727356802000
```

**Directory Structure:**
```
data/
├── orders/
│   ├── partition-0.log
│   └── partition-1.log
└── payments/
    ├── partition-0.log
    └── partition-1.log
```

---

## 9. Protocol Specifications

### 9.1 Request Format

**Structure:** `COMMAND|PARAM1|PARAM2|...`

**Commands:**
- `CREATE_TOPIC|topicName|partitionCount`
- `PUBLISH|topicName|messageValue`
- `CONSUME|topicName|partitionId|offset`
- `SUBSCRIBE|topicName`

### 9.2 Response Format

**Success:** `SUCCESS|DATA`

**Error:** `ERROR|errorMessage`

**Examples:**
```
SUCCESS|Topic created
ERROR|Topic already exists
SUCCESS|Order #101
ERROR|Topic not found
```

---

## 10. Test Scenarios

### 10.1 Happy Path Tests

1. **Basic Message Flow**
   - Create topic → Publish message → Consume message → Verify content

2. **Multiple Messages**
   - Create topic → Publish 10 messages → Consume all → Verify order

3. **Multiple Partitions**
   - Create topic with 3 partitions → Publish 10 messages → Verify distribution

4. **Persistence**
   - Publish messages → Stop broker → Start broker → Consume messages → Verify recovery

5. **Networking**
   - Start server → Connect client → Publish message → Consume message → Verify

### 10.2 Error Path Tests

1. **Invalid Topic**
   - Publish to non-existent topic → Verify error

2. **Invalid Message**
   - Publish null/empty message → Verify error

3. **Invalid Offset**
   - Consume from invalid offset → Verify error

4. **Connection Failure**
   - Client operations without connection → Verify error

### 10.3 Concurrency Tests

1. **Multiple Producers**
   - 5 producers publish 100 messages each → Verify all messages stored

2. **Multiple Consumers**
   - 5 consumers consume from same topic → Verify no duplicates

3. **Mixed Operations**
   - Producers and consumers operate concurrently → Verify no corruption

---

## 11. Traceability Matrix

| Requirement ID | Component | Test Case | Priority |
|----------------|-----------|-----------|----------|
| FR-MESSAGE-001 | Message | MessageTest | High |
| FR-PARTITION-001 | Partition | PartitionTest | High |
| FR-TOPIC-001 | Topic | TopicTest | High |
| FR-BROKER-001 | Broker | BrokerTest | High |
| FR-PRODUCER-001 | Producer | ProducerTest | High |
| FR-CONSUMER-001 | Consumer | ConsumerTest | High |
| FR-STORAGE-001 | Storage | StorageTest | Medium |
| FR-NETWORK-001 | Network | NetworkTest | Medium |
| FR-EH-001 | All | ErrorHandlingTest | High |
| FR-CONC-001 | All | ConcurrencyTest | Medium |
| FR-TEST-001 | All | CoverageTest | High |

---

## 12. Implementation Phases

### Phase 1: Core Components (Days 1-2)
- Message, Partition, Topic, Broker
- Basic in-memory message flow
- Unit tests for core components

### Phase 2: Producer/Consumer (Day 2-3)
- Producer implementation
- Consumer implementation
- Offset management
- Integration tests

### Phase 3: Concurrency (Day 4)
- Thread-safe collections
- Synchronization
- Concurrent testing

### Phase 4: Persistence (Day 5)
- Storage implementation
- File I/O
- Recovery testing

### Phase 5: Networking (Day 6)
- Server implementation
- Client implementation
- Protocol implementation
- End-to-end testing

### Phase 6: Testing & Documentation (Day 7)
- Comprehensive testing
- Documentation
- Code review
- Final verification

---

## 13. Sign-off

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Product Owner | | | |
| Technical Lead | | | |
| Developer | | | |

---

## 14. Change History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Sept 2026 | Initial | Initial FRD creation |

---

## Appendix A: Glossary

- **Broker**: Central system that manages topics and facilitates message publishing/consumption
- **Consumer**: Application that reads messages from topics
- **Message**: Unit of data containing value, offset, and optional metadata
- **Offset**: Sequential position of a message within a partition
- **Partition**: Ordered sequence of messages within a topic
- **Producer**: Application that publishes messages to topics
- **Topic**: Named category or stream of messages

---

## Appendix B: References

- MiniKafka PRD (Product Requirements Document)
- MiniKafka Tech Stack Document
- Apache Kafka Documentation (for reference only)
