"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.io = exports.app = void 0;
const express_1 = __importDefault(require("express"));
const http_1 = require("http");
const socket_io_1 = require("socket.io");
const cors_1 = __importDefault(require("cors"));
const helmet_1 = __importDefault(require("helmet"));
const dotenv_1 = __importDefault(require("dotenv"));
const topics_1 = __importDefault(require("./routes/topics"));
const messages_1 = __importDefault(require("./routes/messages"));
const statistics_1 = __importDefault(require("./routes/statistics"));
const socket_handler_1 = require("./socket/socket-handler");
dotenv_1.default.config();
const app = (0, express_1.default)();
exports.app = app;
const httpServer = (0, http_1.createServer)(app);
const io = new socket_io_1.Server(httpServer, {
    cors: {
        origin: process.env.FRONTEND_URL || 'http://localhost:5173',
        methods: ['GET', 'POST']
    }
});
exports.io = io;
const PORT = process.env.PORT || 3000;
const MINIKAFKA_HOST = process.env.MINIKAFKA_HOST || 'localhost';
const MINIKAFKA_PORT = process.env.MINIKAFKA_PORT || 9092;
// Middleware
app.use((0, helmet_1.default)());
app.use((0, cors_1.default)());
app.use(express_1.default.json());
app.use(express_1.default.urlencoded({ extended: true }));
// Routes
app.use('/api/topics', topics_1.default);
app.use('/api/topics', messages_1.default);
app.use('/api/statistics', statistics_1.default);
// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});
// Socket.io setup
(0, socket_handler_1.setupSocketHandlers)(io);
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
