import { Router, Request, Response } from 'express';
import { Statistics } from '../types';

const router = Router();

let totalMessages = 0;
let messagesPerSecond = 0;
let lastSecondMessages = 0;

// Update throughput every second
setInterval(() => {
  messagesPerSecond = lastSecondMessages;
  lastSecondMessages = 0;
}, 1000);

// GET /api/statistics - Get system statistics
router.get('/', (req: Request, res: Response) => {
  const stats: Statistics = {
    totalMessages,
    messagesPerSecond,
    topicsCount: 0, // Will be updated from topics store
    partitionsCount: 0 // Will be updated from topics store
  };
  res.json({ statistics: stats });
});

// GET /api/statistics/throughput - Get throughput data
router.get('/throughput', (req: Request, res: Response) => {
  res.json({ messagesPerSecond });
});

// Internal function to update message count
export function incrementMessageCount() {
  totalMessages++;
  lastSecondMessages++;
}

export default router;
