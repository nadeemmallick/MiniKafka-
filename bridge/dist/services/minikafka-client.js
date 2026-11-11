"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MiniKafkaClient = void 0;
exports.getMiniKafkaClient = getMiniKafkaClient;
const net_1 = __importDefault(require("net"));
class MiniKafkaClient {
    constructor(host = 'localhost', port = 9092) {
        this.client = null;
        this.connected = false;
        this.host = host;
        this.port = port;
    }
    connect() {
        return new Promise((resolve, reject) => {
            this.client = new net_1.default.Socket();
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
    disconnect() {
        if (this.client) {
            this.client.end();
            this.client = null;
            this.connected = false;
        }
    }
    isConnected() {
        return this.connected;
    }
    async sendCommand(command) {
        if (!this.client || !this.connected) {
            throw new Error('Not connected to MiniKafka');
        }
        return new Promise((resolve, reject) => {
            if (!this.client) {
                reject(new Error('Client is null'));
                return;
            }
            let responseReceived = false;
            const onData = (data) => {
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
    async createTopic(topicName, partitionCount) {
        const command = `CREATE_TOPIC ${topicName} ${partitionCount}`;
        return this.sendCommand(command);
    }
    async produceMessage(topicName, message) {
        const command = `PRODUCE ${topicName} ${message}`;
        return this.sendCommand(command);
    }
    async consumeMessage(topicName, partitionId) {
        const command = `CONSUME ${topicName} ${partitionId}`;
        return this.sendCommand(command);
    }
    async ping() {
        try {
            const response = await this.sendCommand('PING');
            return response.status === 'PONG';
        }
        catch (error) {
            return false;
        }
    }
    parseResponse(response) {
        const parts = response.split(' ');
        const status = parts[0];
        const message = parts.slice(1).join(' ');
        if (status === 'MSG') {
            // Parse MSG format: MSG offset|value|timestamp
            const dataParts = message.split('|');
            if (dataParts.length === 3) {
                const data = {
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
exports.MiniKafkaClient = MiniKafkaClient;
// Singleton instance
let clientInstance = null;
function getMiniKafkaClient() {
    if (!clientInstance) {
        const host = process.env.MINIKAFKA_HOST || 'localhost';
        const port = parseInt(process.env.MINIKAFKA_PORT || '9092');
        clientInstance = new MiniKafkaClient(host, port);
    }
    return clientInstance;
}
