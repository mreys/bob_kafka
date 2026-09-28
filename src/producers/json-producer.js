const { createKafkaClient, TOPICS } = require('../config/kafka');
const { loadOrdersFromCsv, scheduleOrders } = require('../utils/csv-reader');

async function runJsonProducer() {
  const kafka = createKafkaClient('json-order-producer');
  const producer = kafka.producer();

  console.log('==> Starting JSON Order Producer...');
  console.log(`==> Target Topic: ${TOPICS.ORDERS_JSON}`);

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

    const record = {
      key: order.customerid,
      value: JSON.stringify(payload)
    };

    const result = await producer.send({
      topic: TOPICS.ORDERS_JSON,
      messages: [record]
    });

    console.log(
      `[JSON Producer] [t+${elapsedSeconds}s] (Scheduled delay: ${order.delay_seconds}s) ` +
      `Order #${index + 1} sent to ${TOPICS.ORDERS_JSON} [p:${result[0].partition}, offset:${result[0].baseOffset}]: ` +
      JSON.stringify(payload)
    );
  });

  console.log('==> All CSV order events published successfully.');
  await producer.disconnect();
}

if (require.main === module) {
  runJsonProducer().catch(err => {
    console.error('Error running JSON Producer:', err);
    process.exit(1);
  });
}

module.exports = { runJsonProducer };
