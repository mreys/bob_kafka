const assert = require('assert');
const { createKafkaClient, TOPICS } = require('../src/config/kafka');
const { orderAvroType } = require('../src/producers/avro-producer');
const { decodeOrderMessage } = require('../src/consumers/order-consumer');

async function testE2E() {
  console.log('==> Starting End-to-End Kafka Broker Verification...');

  const kafka = createKafkaClient('e2e-test-client');
  const admin = kafka.admin();

  console.log('1. Connecting Admin & Creating topics if not present...');
  await admin.connect();
  const existingTopics = await admin.listTopics();
  const topicsToCreate = [TOPICS.ORDERS_JSON, TOPICS.ORDERS_AVRO].filter(
    t => !existingTopics.includes(t)
  );

  if (topicsToCreate.length > 0) {
    await admin.createTopics({
      topics: topicsToCreate.map(topic => ({ topic, numPartitions: 1, replicationFactor: 1 })),
      waitForLeaders: true
    });
    console.log(`   ✓ Topics created: ${topicsToCreate.join(', ')}`);
  } else {
    console.log(`   ✓ Topics already exist: ${[TOPICS.ORDERS_JSON, TOPICS.ORDERS_AVRO].join(', ')}`);
  }
  await admin.disconnect();

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `e2e-test-group-${Date.now()}` });

  console.log('2. Connecting Producer to Kafka Broker via SASL_SSL...');
  await producer.connect();
  console.log('   ✓ Producer connected successfully.');

  console.log('3. Connecting Consumer to Kafka Broker via SASL_SSL...');
  await consumer.connect();
  console.log('   ✓ Consumer connected successfully.');

  const testJsonPayload = {
    customerid: 'CUST-E2E-JSON',
    productid: 'PROD-E2E-JSON',
    quantity: 3,
    shopid: 'SHOP-E2E-01'
  };

  const testAvroPayload = {
    customerid: 'CUST-E2E-AVRO',
    productid: 'PROD-E2E-AVRO',
    quantity: 7,
    shopid: 'SHOP-E2E-02'
  };

  await consumer.subscribe({ topic: TOPICS.ORDERS_JSON, fromBeginning: false });
  await consumer.subscribe({ topic: TOPICS.ORDERS_AVRO, fromBeginning: false });

  let receivedJson = false;
  let receivedAvro = false;

  const receivePromise = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timed out waiting for test messages from Kafka broker'));
    }, 20000);

    consumer.run({
      eachMessage: async ({ topic, message }) => {
        const decoded = decodeOrderMessage(topic, message.value);
        if (topic === TOPICS.ORDERS_JSON && decoded.data.customerid === testJsonPayload.customerid) {
          console.log('   ✓ Received and verified JSON message from Kafka.');
          receivedJson = true;
        }
        if (topic === TOPICS.ORDERS_AVRO && decoded.data.customerid === testAvroPayload.customerid) {
          console.log('   ✓ Received and verified Avro message from Kafka.');
          receivedAvro = true;
        }

        if (receivedJson && receivedAvro) {
          clearTimeout(timeout);
          resolve();
        }
      }
    });
  });

  // Give consumer a moment to join group
  await new Promise(r => setTimeout(r, 2000));

  console.log('4. Sending test JSON message...');
  await producer.send({
    topic: TOPICS.ORDERS_JSON,
    messages: [{ key: testJsonPayload.customerid, value: JSON.stringify(testJsonPayload) }]
  });

  console.log('5. Sending test Avro message...');
  await producer.send({
    topic: TOPICS.ORDERS_AVRO,
    messages: [{ key: testAvroPayload.customerid, value: orderAvroType.toBuffer(testAvroPayload) }]
  });

  console.log('6. Waiting for messages to be consumed...');
  await receivePromise;

  await consumer.disconnect();
  await producer.disconnect();

  console.log('==> E2E Broker Test completed successfully! All SASL_SSL connections and messages verified.');
}

testE2E().catch(err => {
  console.error('E2E Test Failed:', err);
  process.exit(1);
});
