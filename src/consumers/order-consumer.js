const fs = require('fs');
const path = require('path');
const avro = require('avsc');
const { createKafkaClient, TOPICS } = require('../config/kafka');

// Load Avro schema
const schemaPath = path.resolve(__dirname, '../../schemas/order.avsc');
const schemaJson = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
const orderAvroType = avro.Type.forSchema(schemaJson);

/**
 * Decodes a Kafka message value according to the topic format.
 *
 * @param {string} topic
 * @param {Buffer|null} value
 * @returns {object}
 */
function decodeOrderMessage(topic, value) {
  if (!value) return null;

  if (topic === TOPICS.ORDERS_AVRO) {
    return {
      format: 'AVRO',
      data: orderAvroType.fromBuffer(value)
    };
  }

  // Default to JSON
  try {
    return {
      format: 'JSON',
      data: JSON.parse(value.toString('utf-8'))
    };
  } catch (err) {
    // If topic was unexpected or fallback fails, try Avro
    try {
      return {
        format: 'AVRO (fallback)',
        data: orderAvroType.fromBuffer(value)
      };
    } catch {
      return {
        format: 'RAW',
        data: value.toString('utf-8')
      };
    }
  }
}

async function runConsumer() {
  const groupId = process.env.KAFKA_GROUP_ID || 'order-demo-group';
  const kafka = createKafkaClient('order-consumer-app');
  const consumer = kafka.consumer({ groupId });

  console.log('==> Starting Order Consumer...');
  console.log(`==> Consumer Group: ${groupId}`);
  console.log(`==> Subscribing to topics: ${TOPICS.ORDERS_JSON}, ${TOPICS.ORDERS_AVRO}`);

  await consumer.connect();
  console.log('==> Connected to Kafka broker via SASL_SSL.');

  await consumer.subscribe({ topic: TOPICS.ORDERS_JSON, fromBeginning: true });
  await consumer.subscribe({ topic: TOPICS.ORDERS_AVRO, fromBeginning: true });

  console.log('==> Consumer is listening for order events...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decoded = decodeOrderMessage(topic, message.value);
      const key = message.key ? message.key.toString('utf-8') : null;
      const timestamp = new Date(parseInt(message.timestamp, 10)).toISOString();

      console.log('----------------------------------------------------');
      console.log(`[EVENT RECEIVED] [${decoded.format}]`);
      console.log(`Topic:      ${topic}`);
      console.log(`Partition:  ${partition} | Offset: ${message.offset}`);
      console.log(`Key:        ${key}`);
      console.log(`Timestamp:  ${timestamp}`);
      console.log('Order Data:');
      if (decoded.data && typeof decoded.data === 'object') {
        console.log(`  Customer ID:  ${decoded.data.customerid}`);
        console.log(`  Product ID:   ${decoded.data.productid}`);
        console.log(`  Quantity:     ${decoded.data.quantity}`);
        console.log(`  Shop ID:      ${decoded.data.shopid}`);
      } else {
        console.log(`  Payload:      ${decoded.data}`);
      }
      console.log('----------------------------------------------------');
    }
  });
}

if (require.main === module) {
  runConsumer().catch(err => {
    console.error('Error running Consumer:', err);
    process.exit(1);
  });
}

module.exports = { runConsumer, decodeOrderMessage };
