export interface Topic {
  name: string;
  partitionCount: number;
  messageCount: number;
}

export interface Partition {
  id: number;
  messageCount: number;
}

export interface Message {
  offset: number;
  value: string;
  timestamp: number;
  topicName: string;
  partitionId: number;
}

export interface MiniKafkaResponse {
  status: 'OK' | 'ERROR' | 'MSG' | 'PONG';
  message: string;
  data?: Message;
}

export interface Statistics {
  totalMessages: number;
  messagesPerSecond: number;
  topicsCount: number;
  partitionsCount: number;
}

export interface CreateTopicRequest {
  name: string;
  partitionCount: number;
}

export interface SendMessageRequest {
  topicName: string;
  message: string;
}

export interface ConsumeMessageRequest {
  topicName: string;
  partitionId: number;
}
