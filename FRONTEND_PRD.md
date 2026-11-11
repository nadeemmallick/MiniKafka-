# MiniKafka Frontend - Product Requirements Document

## 1. Project Overview

### 1.1 Purpose
To create an interactive web-based frontend for MiniKafka that visualizes and demonstrates how a message broker works in real-time. The frontend will make MiniKafka educational, intuitive, and visually engaging.

### 1.2 Target Audience
- Students learning about message brokers
- Developers understanding Kafka internals
- Technical interview preparation
- Educational demonstrations

### 1.3 Project Goals
- Visualize MiniKafka's message flow in real-time
- Provide interactive controls for topic and message management
- Show partition distribution and offset tracking
- Demonstrate round-robin message routing
- Make message broker concepts easy to understand

---

## 2. Scope

### 2.1 In Scope
- Web-based dashboard interface
- Real-time message flow visualization
- Topic and partition management
- Message production and consumption
- Consumer offset tracking
- Statistics and metrics display
- Interactive architecture diagram

### 2.2 Out of Scope
- Multi-broker visualization (future enhancement)
- Consumer group management (future enhancement)
- Advanced security features
- Production deployment optimizations
- Mobile responsive design (initially desktop-focused)

---

## 3. Functional Requirements

### 3.1 Topic Management
- **FR-1**: User can create new topics with specified partition count
- **FR-2**: User can view list of all existing topics
- **FR-3**: User can view partition count for each topic
- **FR-4**: User can delete topics (optional)

### 3.2 Message Production
- **FR-5**: User can send messages to specific topics
- **FR-6**: User can view messages being sent in real-time
- **FR-7**: System shows which partition receives each message
- **FR-8**: User can configure auto-generation of messages
- **FR-9**: User can adjust message send rate

### 3.3 Message Consumption
- **FR-10**: User can create consumers for specific topics
- **FR-11**: User can specify which partition to consume from
- **FR-12**: User can poll for messages manually
- **FR-13**: User can enable auto-poll mode
- **FR-14**: System displays consumed messages with offsets

### 3.4 Visualization
- **FR-15**: Display real-time message flow animation (Producer → Broker → Partition → Consumer)
- **FR-16**: Show partition distribution as visual chart
- **FR-17**: Display consumer offset positions
- **FR-18**: Show message timeline per partition
- **FR-19**: Display architecture diagram with component details

### 3.5 Statistics
- **FR-20**: Display total messages sent per topic
- **FR-21**: Display messages per partition
- **FR-22**: Display consumer consumption rate
- **FR-23**: Show system throughput (messages/second)
- **FR-24**: Display message latency statistics

### 3.6 Log File Viewer
- **FR-25**: User can browse persisted log files
- **FR-26**: User can view log file contents
- **FR-27**: System displays messages in parsed format
- **FR-28**: User can search messages in log files

---

## 4. Non-Functional Requirements

### 4.1 Performance
- **NFR-1**: UI updates within 100ms of message events
- **NFR-2**: Support up to 100 messages/second display rate
- **NFR-3**: Page load time under 2 seconds

### 4.2 Usability
- **NFR-4**: Intuitive interface requiring minimal learning
- **NFR-5**: Clear visual feedback for all actions
- **NFR-6**: Tooltips and help text for complex features

### 4.3 Reliability
- **NFR-7**: Handle network disconnections gracefully
- **NFR-8**: Display error messages clearly
- **NFR-9**: Prevent data loss on page refresh (optional)

### 4.4 Maintainability
- **NFR-10**: Clean, documented code
- **NFR-11**: Modular component structure
- **NFR-12**: Easy to add new features

---

## 5. User Stories

### 5.1 Topic Management
- **US-1**: As a learner, I want to create topics so I can understand how topics work
- **US-2**: As a learner, I want to see all topics so I can understand the data organization
- **US-3**: As a learner, I want to see partition counts so I can understand load distribution

### 5.2 Message Production
- **US-4**: As a learner, I want to send messages so I can see how producers work
- **US-5**: As a learner, I want to see which partition receives my message so I can understand round-robin
- **US-6**: As a learner, I want to auto-generate messages so I can test high throughput

### 5.3 Message Consumption
- **US-7**: As a learner, I want to consume messages so I can see how consumers work
- **US-8**: As a learner, I want to see offsets so I can understand position tracking
- **US-9**: As a learner, I want to consume from specific partitions so I can understand parallel consumption

### 5.4 Visualization
- **US-10**: As a learner, I want to see animated message flow so I can understand the data path
- **US-11**: As a learner, I want to see partition distribution so I can understand load balancing
- **US-12**: As a learner, I want to see the architecture so I can understand component relationships

---

## 6. User Interface Requirements

### 6.1 Layout
- **UIR-1**: Three-column layout: Controls (left), Visualization (center), Details (right)
- **UIR-2**: Responsive design for desktop (1920x1080 minimum resolution)
- **UIR-3**: Dark theme option for better visibility

### 6.2 Components
- **UIR-4**: Topic Management Panel with form controls
- **UIR-5**: Message Producer Console with input and send button
- **UIR-6**: Consumer Control Panel with subscription controls
- **UIR-7**: Visualization Canvas for animations
- **UIR-8**: Statistics Dashboard with charts
- **UIR-9**: Log File Browser with tree view

### 6.3 Visual Feedback
- **UIR-10**: Color-coded messages by topic
- **UIR-11**: Animated message flow indicators
- **UIR-12**: Progress bars for message throughput
- **UIR-13**: Tooltips on hover for component details

---

## 7. Technical Requirements

### 7.1 Frontend Technology
- **TR-1**: React 18+ for UI framework
- **TR-2**: TypeScript for type safety
- **TR-3**: WebSocket for real-time communication
- **TR-4**: Chart.js or Recharts for data visualization
- **TR-5**: Tailwind CSS for styling

### 7.2 Backend Bridge
- **TR-6**: Node.js for backend bridge server
- **TR-7**: Express.js for REST API
- **TR-8**: Socket.io for WebSocket communication
- **TR-9**: TCP client to connect to MiniKafka server

### 7.3 Integration
- **TR-10**: REST API for CRUD operations
- **TR-11**: WebSocket for real-time updates
- **TR-12**: TCP connection to MiniKafka on port 9092

---

## 8. Success Criteria

### 8.1 Functional Success
- **SC-1**: All 28 functional requirements implemented
- **SC-2**: Real-time message flow working smoothly
- **SC-3**: All user stories completed

### 8.2 Educational Success
- **SC-4**: Users can understand message broker concepts after using the tool
- **SC-5**: Visualizations clearly demonstrate key concepts
- **SC-6**: Tool is useful for technical interview preparation

### 8.3 Technical Success
- **SC-7**: Performance requirements met
- **SC-8**: Code is clean and maintainable
- **SC-9**: Integration with MiniKafka backend works reliably

---

## 9. Constraints

### 9.1 Time Constraints
- **C-1**: Development timeline: 1-2 weeks for MVP
- **C-2**: Phased approach: MVP first, enhancements later

### 9.2 Resource Constraints
- **C-3**: Single developer
- **C-4**: Limited server resources (local deployment)

### 9.3 Technical Constraints
- **C-5**: Must work with existing MiniKafka backend
- **C-6**: No modifications to MiniKafka Java code required
- **C-7**: Browser-based (no desktop app)

---

## 10. Assumptions

### 10.1 User Assumptions
- **A-1**: Users have basic understanding of web applications
- **A-2**: Users have modern web browser (Chrome, Firefox, Edge)
- **A-3**: Users have MiniKafka backend running locally

### 10.2 Technical Assumptions
- **A-4**: MiniKafka server runs on localhost:9092
- **A-5**: Node.js bridge runs on localhost:3000
- **A-6**: WebSocket connection is stable

---

## 11. Risks and Mitigations

### 11.1 Technical Risks
- **R-1**: WebSocket connection instability
  - **M-1**: Implement reconnection logic
  - **M-2**: Fallback to polling if WebSocket fails

- **R-2**: Performance issues with high message rate
  - **M-3**: Implement message throttling
  - **M-4**: Virtual scrolling for large message lists

### 11.2 Project Risks
- **R-3**: Scope creep
  - **M-5**: Strict MVP definition
  - **M-6**: Phase 2 for advanced features

- **R-4**: Integration complexity
  - **M-7**: Thorough testing of bridge layer
  - **M-8**: Clear error handling

---

## 12. Phase Planning

### Phase 1: MVP (Week 1)
- Basic topic management
- Simple message production/consumption
- Topic/partition list view
- Basic statistics

### Phase 2: Visualization (Week 2)
- Real-time message flow animation
- Partition distribution chart
- Offset tracking visualization
- Architecture diagram

### Phase 3: Advanced Features (Future)
- Log file viewer
- Advanced statistics
- Consumer groups
- Multi-broker visualization

---

## 13. Definition of Done

A feature is considered done when:
- [ ] Functional requirements met
- [ ] Code reviewed and documented
- [ ] Tested manually
- [ ] Integration with MiniKafka verified
- [ ] UI/UX approved
- [ ] No critical bugs

---

## 14. Glossary

- **Topic**: Logical channel for organizing messages
- **Partition**: Physical storage unit within a topic
- **Offset**: Position of a message within a partition
- **Producer**: Component that sends messages
- **Consumer**: Component that receives messages
- **Round-Robin**: Algorithm for distributing messages evenly
- **WebSocket**: Protocol for real-time bidirectional communication
- **Bridge**: Node.js server connecting frontend to MiniKafka

---

**Document Version**: 1.0  
**Last Updated**: 2026-11-12  
**Author**: MiniKafka Team
