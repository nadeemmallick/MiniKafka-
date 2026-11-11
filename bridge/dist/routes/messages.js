"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const minikafka_client_1 = require("../services/minikafka-client");
const statistics_1 = require("./statistics");
const router = (0, express_1.Router)();
// In-memory message storage
const messages = new Map();
// POST /api/topics/:topicName/messages - Send message
router.post('/:topicName/messages', async (req, res) => {
    try {
        const { topicName, message } = req.body;
        if (!topicName || !message) {
            return res.status(400).json({ error: 'topicName and message are required' });
        }
        const client = (0, minikafka_client_1.getMiniKafkaClient)();
        const response = await client.produceMessage(topicName, message);
        if (response.status === 'OK') {
            // Update statistics
            (0, statistics_1.incrementMessageCount)();
            // Store message in memory
            const topicMessages = messages.get(topicName) || [];
            const newMessage = {
                offset: topicMessages.length,
                value: message,
                timestamp: Date.now(),
                topicName,
                partitionId: 0 // Will be updated by actual MiniKafka
            };
            topicMessages.push(newMessage);
            messages.set(topicName, topicMessages);
            res.json({ message: newMessage, response: response.message });
        }
        else {
            res.status(400).json({ error: response.message });
        }
    }
    catch (error) {
        console.error('Error sending message:', error);
        res.status(500).json({ error: error.message });
    }
});
// GET /api/messages/:topicName - Get messages for topic
router.get('/:topicName', (req, res) => {
    const topicMessages = messages.get(req.params.topicName) || [];
    res.json({ messages: topicMessages });
});
// POST /api/messages/consume - Consume message
router.post('/consume', async (req, res) => {
    try {
        const { topicName, partitionId } = req.body;
        if (!topicName || partitionId === undefined) {
            return res.status(400).json({ error: 'topicName and partitionId are required' });
        }
        const client = (0, minikafka_client_1.getMiniKafkaClient)();
        const response = await client.consumeMessage(topicName, partitionId);
        if (response.status === 'MSG' && response.data) {
            res.json({ message: response.data });
        }
        else {
            res.status(400).json({ error: response.message });
        }
    }
    catch (error) {
        console.error('Error consuming message:', error);
        res.status(500).json({ error: error.message });
    }
});
exports.default = router;
