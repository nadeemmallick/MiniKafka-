import express from 'express';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';

import topicRoutes from './routes/topics';
import messageRoutes from './routes/messages';
import statisticsRoutes from './routes/statistics';
import { setupSocketHandlers } from './socket/socket-handler';

dotenv.config();

const app = express();
const httpServer = createServer(app);
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: process.env.FRONTEND_URL || 'http://localhost:5173',
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 3000;
const MINIKAFKA_HOST = process.env.MINIKAFKA_HOST || 'localhost';
const MINIKAFKA_PORT = process.env.MINIKAFKA_PORT || 9092;

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/api/topics', topicRoutes);
app.use('/api/topics', messageRoutes);
app.use('/api/statistics', statisticsRoutes);

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Socket.io setup
setupSocketHandlers(io);

// Start server
httpServer.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════════════════════════════╗`);
  console.log(`║     MiniKafka Bridge Server Started                          ║`);
  console.log(`╚══════════════════════════════════════════════════════════════╝`);
  console.log(``);
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`🔗 MiniKafka: ${MINIKAFKA_HOST}:${MINIKAFKA_PORT}`);
  console.log(`🌐 Frontend: ${process.env.FRONTEND_URL || 'http://localhost:5173'}`);
  console.log(``);
  console.log(`✅ Ready to accept connections`);
});

export { app, io };
