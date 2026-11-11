"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const minikafka_client_1 = require("../services/minikafka-client");
const router = (0, express_1.Router)();
// In-memory topic storage (for quick access)
const topics = new Map();
// GET /api/topics - List all topics
router.get('/', (req, res) => {
    const topicList = Array.from(topics.values());
    res.json({ topics: topicList });
});
// POST /api/topics - Create topic
router.post('/', async (req, res) => {
    try {
        const { name, partitionCount } = req.body;
        if (!name || !partitionCount) {
            return res.status(400).json({ error: 'name and partitionCount are required' });
        }
        const client = (0, minikafka_client_1.getMiniKafkaClient)();
        const response = await client.createTopic(name, partitionCount);
        if (response.status === 'OK') {
            const topic = {
                name,
                partitionCount,
                messageCount: 0
            };
            topics.set(name, topic);
            res.json({ topic, message: response.message });
        }
        else {
            res.status(400).json({ error: response.message });
        }
    }
    catch (error) {
        console.error('Error creating topic:', error);
        res.status(500).json({ error: error.message });
    }
});
// GET /api/topics/:name - Get topic details
router.get('/:name', (req, res) => {
    const topic = topics.get(req.params.name);
    if (!topic) {
        return res.status(404).json({ error: 'Topic not found' });
    }
    res.json({ topic });
});
// DELETE /api/topics/:name - Delete topic
router.delete('/:name', (req, res) => {
    const topic = topics.get(req.params.name);
    if (!topic) {
        return res.status(404).json({ error: 'Topic not found' });
    }
    topics.delete(req.params.name);
    res.json({ message: 'Topic deleted' });
});
exports.default = router;
