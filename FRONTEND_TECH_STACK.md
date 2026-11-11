# MiniKafka Frontend - Technology Stack Document

## 1. Overview

This document defines the technology stack for the MiniKafka frontend, including the React UI, Node.js bridge server, and integration with the MiniKafka Java backend.

---

## 2. Frontend Technology Stack

### 2.1 Core Framework
- **React 18.2+**: UI framework with hooks and concurrent features
- **TypeScript 5.0+**: Type-safe JavaScript
- **Vite 4.0+**: Fast build tool and dev server

**Rationale**: React is widely used, well-documented, and has a large ecosystem. TypeScript adds type safety. Vite provides fast development experience.

### 2.2 State Management
- **React Context API**: For global state (topics, messages, settings)
- **React Hooks**: useState, useEffect, useContext for component state
- **Zustand (optional)**: For complex state management if needed

**Rationale**: Context API is sufficient for MVP. Can upgrade to Zustand if state becomes complex.

### 2.3 UI Components
- **Tailwind CSS 3.0+**: Utility-first CSS framework
- **Headless UI**: Accessible UI components (optional)
- **Lucide React**: Icon library

**Rationale**: Tailwind provides rapid UI development. Headless UI adds accessible components. Lucide has clean, modern icons.

### 2.4 Data Visualization
- **Recharts**: Chart library for React
- **React Flow**: For architecture diagrams and node graphs
- **Framer Motion**: For animations and transitions

**Rationale**: Recharts is React-native and easy to use. React Flow perfect for architecture diagrams. Framer Motion provides smooth animations.

### 2.5 Communication
- **Socket.io-client**: WebSocket client for real-time updates
- **Axios**: HTTP client for REST API calls

**Rationale**: Socket.io provides reliable WebSocket with fallbacks. Axios is popular and feature-rich.

### 2.6 Development Tools
- **ESLint**: Code linting
- **Prettier**: Code formatting
- **Vitest**: Unit testing (optional for MVP)

---

## 3. Backend Bridge Technology Stack

### 3.1 Core Framework
- **Node.js 18+**: JavaScript runtime
- **Express.js 4.18+**: Web framework
- **TypeScript 5.0+**: Type-safe backend code

**Rationale**: Node.js is ideal for bridging between frontend and MiniKafka. Express is simple and well-documented.

### 3.2 Communication
- **Socket.io**: WebSocket server for real-time updates
- **net module**: Built-in Node.js TCP client for MiniKafka connection

**Rationale**: Socket.io pairs with frontend. net module handles TCP connection to MiniKafka.

### 3.3 API Layer
- **Express Router**: REST API endpoints
- **CORS**: Cross-origin resource sharing
- **Helmet**: Security headers

**Rationale**: Express Router organizes API routes. CORS and Helmet add security.

### 3.4 State Management
- **In-memory state**: For topics, messages, connections
- **EventEmitter**: For internal event handling

**Rationale**: In-memory state sufficient for MVP. Can add Redis for persistence later.

---

## 4. Integration Architecture

### 4.1 System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Browser (React UI)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Topic Panel  │  │ Message View │  │ Statistics   │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                 │
│                    ┌───────▼────────┐                        │
│                    │  Socket.io    │                        │
│                    │   Client       │                        │
│                    └───────┬────────┘                        │
└────────────────────────────┼─────────────────────────────────┘
                             │ WebSocket
┌────────────────────────────┼─────────────────────────────────┐
│                    ┌───────▼────────┐                        │
│                    │  Node.js      │                        │
│                    │  Bridge       │                        │
│                    │  Server       │                        │
│                    └───────┬────────┘                        │
│                            │                                 │
│              ┌─────────────┼─────────────┐                  │
│              │             │             │                  │
│         ┌────▼────┐  ┌────▼────┐  ┌────▼────┐             │
│         │ REST    │  │ Socket  │  │  TCP    │             │
│         │ API     │  │ .io     │  │ Client  │             │
│         └─────────┘  └─────────┘  └────┬────┘             │
│                                         │                   │
└─────────────────────────────────────────┼───────────────────┘
                                          │ TCP
┌─────────────────────────────────────────┼───────────────────┐
│                            ┌───────────▼────────┐           │
│                            │  MiniKafka Server  │           │
│                            │  (Java - Port 9092)│           │
│                            └────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Communication Protocols

**Frontend ↔ Bridge**:
- REST API (HTTP): For CRUD operations
- WebSocket: For real-time updates

**Bridge ↔ MiniKafka**:
- TCP Socket: For MiniKafka protocol commands

---

## 5. Project Structure

### 5.1 Frontend Structure

```
frontend/
├── public/
│   └── index.html
├── src/
│   ├── components/
│   │   ├── TopicManager/
│   │   │   ├── TopicManager.tsx
│   │   │   ├── CreateTopicForm.tsx
│   │   │   └── TopicList.tsx
│   │   ├── MessageProducer/
│   │   │   ├── MessageProducer.tsx
│   │   │   ├── MessageInput.tsx
│   │   │   └── AutoGenerateControls.tsx
│   │   ├── MessageConsumer/
│   │   │   ├── MessageConsumer.tsx
│   │   │   ├── ConsumerControls.tsx
│   │   │   └── MessageList.tsx
│   │   ├── Visualization/
│   │   │   ├── MessageFlow.tsx
│   │   │   ├── PartitionChart.tsx
│   │   │   └── ArchitectureDiagram.tsx
│   │   ├── Statistics/
│   │   │   ├── StatisticsDashboard.tsx
│   │   │   ├── ThroughputChart.tsx
│   │   │   └── MessageCountCard.tsx
│   │   └── Shared/
│   │       ├── Button.tsx
│   │       ├── Input.tsx
│   │       └── Card.tsx
│   ├── hooks/
│   │   ├── useTopics.ts
│   │   ├── useMessages.ts
│   │   ├── useWebSocket.ts
│   │   └── useStatistics.ts
│   ├── services/
│   │   ├── api.ts
│   │   ├── socket.ts
│   │   └── types.ts
│   ├── store/
│   │   ├── topicStore.ts
│   │   └── messageStore.ts
│   ├── utils/
│   │   ├── formatters.ts
│   │   └── validators.ts
│   ├── App.tsx
│   ├── main.tsx
│   └── vite-env.d.ts
├── package.json
├── tsconfig.json
├── tailwind.config.js
└── vite.config.ts
```

### 5.2 Bridge Server Structure

```
bridge/
├── src/
│   ├── server.ts
│   ├── routes/
│   │   ├── topics.ts
│   │   ├── messages.ts
│   │   └── statistics.ts
│   ├── services/
│   │   ├── minikafka-client.ts
│   │   ├── topic-manager.ts
│   │   └── message-manager.ts
│   ├── socket/
│   │   ├── socket-handler.ts
│   │   └── events.ts
│   ├── types/
│   │   └── index.ts
│   └── utils/
│       ├── logger.ts
│       └── parser.ts
├── package.json
└── tsconfig.json
```

---

## 6. API Specification

### 6.1 REST API Endpoints

**Topics**
```
GET    /api/topics                    - List all topics
POST   /api/topics                    - Create topic
DELETE /api/topics/:name              - Delete topic
GET    /api/topics/:name              - Get topic details
GET    /api/topics/:name/partitions   - Get topic partitions
```

**Messages**
```
POST   /api/topics/:name/messages     - Send message
GET    /api/topics/:name/messages     - Get messages
GET    /api/topics/:name/partitions/:id/messages - Get partition messages
```

**Statistics**
```
GET    /api/statistics                - Get system statistics
GET    /api/statistics/throughput    - Get throughput data
```

### 6.2 WebSocket Events

**Client → Server**
```typescript
socket.emit('subscribe:topics')
socket.emit('subscribe:messages', { topicName })
socket.emit('subscribe:statistics')
```

**Server → Client**
```typescript
socket.emit('topic:created', topic)
socket.emit('message:sent', message)
socket.emit('message:consumed', message)
socket.emit('statistics:update', stats)
```

### 6.3 MiniKafka Protocol Commands

```typescript
CREATE_TOPIC <topicName> <partitionCount>
PRODUCE <topicName> <message>
CONSUME <topicName> <partitionId>
PING
BYE
```

---

## 7. Data Models

### 7.1 Frontend Types

```typescript
interface Topic {
  name: string;
  partitionCount: number;
  messageCount: number;
  partitions: Partition[];
}

interface Partition {
  id: number;
  messageCount: number;
  messages: Message[];
}

interface Message {
  offset: number;
  value: string;
  timestamp: number;
  topicName: string;
  partitionId: number;
}

interface Consumer {
  id: string;
  topicName: string;
  partitionId: number;
  currentOffset: number;
}

interface Statistics {
  totalMessages: number;
  messagesPerSecond: number;
  topicsCount: number;
  partitionsCount: number;
}
```

### 7.2 Bridge Types

```typescript
interface MiniKafkaResponse {
  status: 'OK' | 'ERROR';
  message: string;
  data?: any;
}

interface TopicData {
  name: string;
  partitions: number;
}
```

---

## 8. Development Environment

### 8.1 Prerequisites
- Node.js 18+ and npm
- Java 17+ (for MiniKafka backend)
- Git

### 8.2 Setup Commands

**Frontend**:
```bash
cd frontend
npm install
npm run dev
```

**Bridge Server**:
```bash
cd bridge
npm install
npm run dev
```

**MiniKafka Backend**:
```bash
cd ..
mvn clean compile
java -cp "target/classes;target/dependency/*" com.minikafka.network.MiniKafkaServer 9092
```

### 8.3 Development Workflow
1. Start MiniKafka server (port 9092)
2. Start bridge server (port 3000)
3. Start frontend dev server (port 5173)
4. Open browser to http://localhost:5173

---

## 9. Build and Deployment

### 9.1 Frontend Build
```bash
cd frontend
npm run build
```
Output: `frontend/dist/`

### 9.2 Bridge Build
```bash
cd bridge
npm run build
```
Output: `bridge/dist/`

### 9.3 Production Deployment
```bash
# Run MiniKafka server
java -jar mini-kafka.jar

# Run bridge server
node bridge/dist/server.js

# Serve frontend (using nginx or similar)
# Point to frontend/dist/
```

---

## 10. Testing Strategy

### 10.1 Frontend Testing
- **Vitest**: Unit tests for components and hooks
- **React Testing Library**: Component integration tests
- **Playwright**: E2E tests (optional for MVP)

### 10.2 Bridge Testing
- **Jest**: Unit tests for services
- **Supertest**: API endpoint tests
- **Manual testing**: Integration with MiniKafka

---

## 11. Performance Considerations

### 11.1 Frontend Optimization
- Code splitting with React.lazy()
- Memoization with React.memo()
- Virtual scrolling for large lists
- Debounce API calls

### 11.2 Bridge Optimization
- Connection pooling for MiniKafka
- Message batching for high throughput
- Rate limiting for API endpoints

---

## 12. Security Considerations

### 12.1 Frontend Security
- Input validation
- XSS prevention (React handles this)
- CSRF tokens (if needed)

### 12.2 Bridge Security
- CORS configuration
- Rate limiting
- Input sanitization
- Helmet.js for security headers

---

## 13. Monitoring and Logging

### 13.1 Frontend Monitoring
- Error boundaries
- Console logging (dev only)
- Performance monitoring (optional)

### 13.2 Bridge Monitoring
- Winston for logging
- Health check endpoint
- Error tracking

---

## 14. Future Enhancements

### 14.1 Potential Upgrades
- **State Management**: Upgrade to Redux Toolkit or Zustand
- **UI Library**: Add shadcn/ui components
- **Testing**: Add Playwright for E2E tests
- **Performance**: Add service worker for offline support
- **Deployment**: Docker containerization

### 14.2 Advanced Features
- Real-time collaboration (multiple users)
- Message search and filtering
- Export/import configurations
- Theme customization
- Keyboard shortcuts

---

**Document Version**: 1.0  
**Last Updated**: 2026-11-12  
**Author**: MiniKafka Team
