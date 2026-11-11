"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.incrementMessageCount = incrementMessageCount;
const express_1 = require("express");
const router = (0, express_1.Router)();
let totalMessages = 0;
let messagesPerSecond = 0;
let lastSecondMessages = 0;
// Update throughput every second
setInterval(() => {
    messagesPerSecond = lastSecondMessages;
    lastSecondMessages = 0;
}, 1000);
// GET /api/statistics - Get system statistics
router.get('/', (req, res) => {
    const stats = {
        totalMessages,
        messagesPerSecond,
        topicsCount: 0, // Will be updated from topics store
        partitionsCount: 0 // Will be updated from topics store
    };
    res.json({ statistics: stats });
});
// GET /api/statistics/throughput - Get throughput data
router.get('/throughput', (req, res) => {
    res.json({ messagesPerSecond });
});
// Internal function to update message count
function incrementMessageCount() {
    totalMessages++;
    lastSecondMessages++;
}
exports.default = router;
