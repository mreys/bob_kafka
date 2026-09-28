const assert = require('assert');
const { loadOrdersFromCsv } = require('../src/utils/csv-reader');
const { orderAvroType } = require('../src/producers/avro-producer');
const { decodeOrderMessage } = require('../src/consumers/order-consumer');
const { TOPICS } = require('../src/config/kafka');

console.log('==> Running Unit Validation Tests...');

// 1. CSV reader test
const orders = loadOrdersFromCsv();
assert(Array.isArray(orders), 'Orders should be an array');
assert.strictEqual(orders.length, 8, 'Should load 8 orders from orders.csv');
assert.strictEqual(orders[0].customerid, 'CUST-1001');
assert.strictEqual(orders[0].quantity, 1);
assert.strictEqual(orders[0].delay_seconds, 0);
console.log('✓ CSV Reader validation passed.');

// 2. Avro serialization & deserialization test
const sampleOrder = {
  customerid: 'CUST-TEST',
  productid: 'PROD-TEST',
  quantity: 5,
  shopid: 'SHOP-TEST'
};

const avroBuffer = orderAvroType.toBuffer(sampleOrder);
assert(Buffer.isBuffer(avroBuffer), 'Avro output must be a Buffer');

const decodedAvro = decodeOrderMessage(TOPICS.ORDERS_AVRO, avroBuffer);
assert.strictEqual(decodedAvro.format, 'AVRO');
assert.deepStrictEqual({ ...decodedAvro.data }, sampleOrder);
console.log('✓ Avro serialization / deserialization passed.');

// 3. JSON serialization & deserialization test
const jsonBuffer = Buffer.from(JSON.stringify(sampleOrder), 'utf-8');
const decodedJson = decodeOrderMessage(TOPICS.ORDERS_JSON, jsonBuffer);
assert.strictEqual(decodedJson.format, 'JSON');
assert.deepStrictEqual(decodedJson.data, sampleOrder);
console.log('✓ JSON serialization / deserialization passed.');

console.log('==> All validation tests passed successfully!');
