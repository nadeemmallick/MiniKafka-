// Simple test script to verify MiniKafka connection
const net = require('net');

const client = new net.Socket();
const HOST = 'localhost';
const PORT = 9092;

client.connect(PORT, HOST, () => {
  console.log('✅ Connected to MiniKafka');
  
  // Test PING
  client.write('PING\n');
});

client.on('data', (data) => {
  console.log('📩 Received:', data.toString().trim());
  
  if (data.toString().trim() === 'PONG') {
    console.log('✅ Connection test successful!');
    client.destroy();
  }
});

client.on('error', (err) => {
  console.error('❌ Error:', err.message);
  console.log('⚠️  Make sure MiniKafka server is running on port 9092');
  console.log('   Run: java -cp "target/classes;target/dependency/*" com.minikafka.network.MiniKafkaServer 9092');
  process.exit(1);
});

client.on('close', () => {
  console.log('🔌 Connection closed');
  process.exit(0);
});
