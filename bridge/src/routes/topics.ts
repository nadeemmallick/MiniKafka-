import { Router, Request, Response } from 'express';
import { getMiniKafkaClient } from '../services/minikafka-client';
import { Topic } from '../types';

const router = Router();

// In-memory topic storage (for quick access)
const topics: Map<string, Topic> = new Map();

// GET /api/topics - List all topics
router.get('/', (req: Request, res: Response) => {
  const topicList = Array.from(topics.values());
  res.json({ topics: topicList });
});

// POST /api/topics - Create topic
router.post('/', async (req: Request, res: Response) => {
  try {
    const { name, partitionCount } = req.body;

    if (!name || !partitionCount) {
      return res.status(400).json({ error: 'name and partitionCount are required' });
    }

    const client = getMiniKafkaClient();
    const response = await client.createTopic(name, partitionCount);

    if (response.status === 'OK') {
      const topic: Topic = {
        name,
        partitionCount,
        messageCount: 0
      };
      topics.set(name, topic);
      res.json({ topic, message: response.message });
    } else {
      res.status(400).json({ error: response.message });
    }
  } catch (error: any) {
    console.error('Error creating topic:', error);
    res.status(500).json({ error: error.message });
  }
});

// GET /api/topics/:name - Get topic details
router.get('/:name', (req: Request, res: Response) => {
  const topic = topics.get(req.params.name);
  if (!topic) {
    return res.status(404).json({ error: 'Topic not found' });
  }
  res.json({ topic });
});

// DELETE /api/topics/:name - Delete topic
router.delete('/:name', (req: Request, res: Response) => {
  const topic = topics.get(req.params.name);
  if (!topic) {
    return res.status(404).json({ error: 'Topic not found' });
  }
  topics.delete(req.params.name);
  res.json({ message: 'Topic deleted' });
});

export default router;
