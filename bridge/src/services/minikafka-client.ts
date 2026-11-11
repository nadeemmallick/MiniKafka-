import net from 'net';
import { Message, MiniKafkaResponse } from '../types';

export class MiniKafkaClient {
  private client: net.Socket | null = null;
  private host: string;
  private port: number;
  private connected: boolean = false;

  constructor(host: string = 'localhost', port: number = 9092) {
    this.host = host;
    this.port = port;
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client = new net.Socket();

      this.client.on('connect', () => {
        this.connected = true;
        console.log(`✅ Connected to MiniKafka at ${this.host}:${this.port}`);
        resolve();
      });

      this.client.on('error', (err) => {
        this.connected = false;
        console.error(`❌ Error connecting to MiniKafka: ${err.message}`);
        reject(err);
      });

      this.client.on('close', () => {
        this.connected = false;
        console.log('🔌 Disconnected from MiniKafka');
      });

      this.client.connect(this.port, this.host);
    });
  }

  disconnect(): void {
    if (this.client) {
      this.client.end();
      this.client = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  async sendCommand(command: string): Promise<MiniKafkaResponse> {
    if (!this.client || !this.connected) {
      throw new Error('Not connected to MiniKafka');
    }

    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error('Client is null'));
        return;
      }

      let responseReceived = false;

      const onData = (data: Buffer) => {
        const response = data.toString().trim();
        responseReceived = true;
        this.client?.removeListener('data', onData);
        
        const parsed = this.parseResponse(response);
        resolve(parsed);
      };

      this.client.on('data', onData);

      // Set timeout
      setTimeout(() => {
        if (!responseReceived) {
          this.client?.removeListener('data', onData);
          reject(new Error('Command timeout'));
        }
      }, 5000);

      this.client.write(command + '\n');
    });
  }

  async createTopic(topicName: string, partitionCount: number): Promise<MiniKafkaResponse> {
    const command = `CREATE_TOPIC ${topicName} ${partitionCount}`;
    return this.sendCommand(command);
  }

  async produceMessage(topicName: string, message: string): Promise<MiniKafkaResponse> {
    const command = `PRODUCE ${topicName} ${message}`;
    return this.sendCommand(command);
  }

  async consumeMessage(topicName: string, partitionId: number): Promise<MiniKafkaResponse> {
    const command = `CONSUME ${topicName} ${partitionId}`;
    return this.sendCommand(command);
  }

  async ping(): Promise<boolean> {
    try {
      const response = await this.sendCommand('PING');
      return response.status === 'PONG';
    } catch (error) {
      return false;
    }
  }

  private parseResponse(response: string): MiniKafkaResponse {
    const parts = response.split(' ');
    const status = parts[0] as 'OK' | 'ERROR' | 'MSG' | 'PONG';
    const message = parts.slice(1).join(' ');

    if (status === 'MSG') {
      // Parse MSG format: MSG offset|value|timestamp
      const dataParts = message.split('|');
      if (dataParts.length === 3) {
        const data: Message = {
          offset: parseInt(dataParts[0]),
          value: dataParts[1],
          timestamp: parseInt(dataParts[2]),
          topicName: '',
          partitionId: 0
        };
        return { status, message, data };
      }
    }

    return { status, message };
  }
}

// Singleton instance
let clientInstance: MiniKafkaClient | null = null;

export function getMiniKafkaClient(): MiniKafkaClient {
  if (!clientInstance) {
    const host = process.env.MINIKAFKA_HOST || 'localhost';
    const port = parseInt(process.env.MINIKAFKA_PORT || '9092');
    clientInstance = new MiniKafkaClient(host, port);
  }
  return clientInstance;
}
