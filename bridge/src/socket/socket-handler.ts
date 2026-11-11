import { Server as SocketIOServer } from 'socket.io';
import { Topic, Message, Statistics } from '../types';

export function setupSocketHandlers(io: SocketIOServer) {
  io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    // Subscribe to topics
    socket.on('subscribe:topics', () => {
      socket.join('topics');
      console.log(`📡 Client ${socket.id} subscribed to topics`);
    });

    // Subscribe to messages for a specific topic
    socket.on('subscribe:messages', (data: { topicName: string }) => {
      const room = `messages:${data.topicName}`;
      socket.join(room);
      console.log(`📡 Client ${socket.id} subscribed to messages for ${data.topicName}`);
    });

    // Subscribe to statistics
    socket.on('subscribe:statistics', () => {
      socket.join('statistics');
      console.log(`📡 Client ${socket.id} subscribed to statistics`);
    });

    // Unsubscribe from topics
    socket.on('unsubscribe:topics', () => {
      socket.leave('topics');
      console.log(`📡 Client ${socket.id} unsubscribed from topics`);
    });

    // Unsubscribe from messages
    socket.on('unsubscribe:messages', (data: { topicName: string }) => {
      const room = `messages:${data.topicName}`;
      socket.leave(room);
      console.log(`📡 Client ${socket.id} unsubscribed from messages for ${data.topicName}`);
    });

    // Unsubscribe from statistics
    socket.on('unsubscribe:statistics', () => {
      socket.leave('statistics');
      console.log(`📡 Client ${socket.id} unsubscribed from statistics`);
    });

    // Disconnect
    socket.on('disconnect', () => {
      console.log(`🔌 Client disconnected: ${socket.id}`);
    });
  });
}

// Event emitters for broadcasting
export function emitTopicCreated(io: SocketIOServer, topic: Topic) {
  io.to('topics').emit('topic:created', topic);
}

export function emitMessageSent(io: SocketIOServer, message: Message) {
  const room = `messages:${message.topicName}`;
  io.to(room).emit('message:sent', message);
}

export function emitStatisticsUpdate(io: SocketIOServer, stats: Statistics) {
  io.to('statistics').emit('statistics:update', stats);
}
