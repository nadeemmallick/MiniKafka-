"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupSocketHandlers = setupSocketHandlers;
exports.emitTopicCreated = emitTopicCreated;
exports.emitMessageSent = emitMessageSent;
exports.emitStatisticsUpdate = emitStatisticsUpdate;
function setupSocketHandlers(io) {
    io.on('connection', (socket) => {
        console.log(`🔌 Client connected: ${socket.id}`);
        // Subscribe to topics
        socket.on('subscribe:topics', () => {
            socket.join('topics');
            console.log(`📡 Client ${socket.id} subscribed to topics`);
        });
        // Subscribe to messages for a specific topic
        socket.on('subscribe:messages', (data) => {
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
        socket.on('unsubscribe:messages', (data) => {
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
function emitTopicCreated(io, topic) {
    io.to('topics').emit('topic:created', topic);
}
function emitMessageSent(io, message) {
    const room = `messages:${message.topicName}`;
    io.to(room).emit('message:sent', message);
}
function emitStatisticsUpdate(io, stats) {
    io.to('statistics').emit('statistics:update', stats);
}
