const fs = require('fs');
const path = require('path');
const avro = require('avsc');
const { createKafkaClient, TOPICS } = require('../config/kafka');
const { loadOrdersFromCsv, scheduleOrders } = require('../utils/csv-reader');

// Load and parse Avro schema
const schemaPath = path.resolve(__dirname, '../../schemas/order.avsc');
const schemaJson = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
const orderAvroType = avro.Type.forSchema(schemaJson);

async function runAvroProducer() {
  const kafka = createKafkaClient('avro-order-producer');
  const producer = kafka.producer();

  console.log('==> Starting Avro Order Producer...');
  console.log(`==> Target Topic: ${TOPICS.ORDERS_AVRO}`);
  console.log(`==> Avro Schema: ${schemaPath}`);

  await producer.connect();
  console.log('==> Connected to Kafka broker via SASL_SSL.');

  const orders = loadOrdersFromCsv();
  console.log(`==> Loaded ${orders.length} order records from CSV.`);

  await scheduleOrders(orders, async (order, index, elapsedSeconds) => {
    const payload = {
      customerid: order.customerid,
      productid: order.productid,
      quantity: order.quantity,
      shopid: order.shopid
    };

    // Binary Avro encode using avsc
    const avroBuffer = orderAvroType.toBuffer(payload);

    const record = {
      key: order.customerid,
      value: avroBuffer
    };

    const result = await producer.send({
      topic: TOPICS.ORDERS_AVRO,
      messages: [record]
    });

    console.log(
      `[Avro Producer] [t+${elapsedSeconds}s] (Scheduled delay: ${order.delay_seconds}s) ` +
      `Order #${index + 1} sent to ${TOPICS.ORDERS_AVRO} [p:${result[0].partition}, offset:${result[0].baseOffset}, bytes:${avroBuffer.length}]: ` +
      JSON.stringify(payload)
    );
  });

  console.log('==> All Avro order events published successfully.');
  await producer.disconnect();
}

if (require.main === module) {
  runAvroProducer().catch(err => {
    console.error('Error running Avro Producer:', err);
    process.exit(1);
  });
}

module.exports = { runAvroProducer, orderAvroType };
