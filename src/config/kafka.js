const fs = require('fs');
const path = require('path');
const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9093').split(',');
const USERNAME = process.env.KAFKA_USERNAME || 'admin';
const PASSWORD = process.env.KAFKA_PASSWORD || 'admin-secret';
const CA_PATH = process.env.KAFKA_CA_PATH || path.resolve(__dirname, '../../secrets/ca.crt');

let caContent;
try {
  caContent = fs.readFileSync(CA_PATH, 'utf-8');
} catch (err) {
  console.warn(`[Kafka Config] Warning: Could not read CA cert from ${CA_PATH}: ${err.message}`);
}

/**
 * Creates a Kafka client configured for SASL_SSL authentication.
 *
 * @param {string} clientId
 * @returns {Kafka}
 */
function createKafkaClient(clientId) {
  return new Kafka({
    clientId,
    brokers: BROKERS,
    ssl: {
      rejectUnauthorized: false, // Allows self-signed local CA certs for demo
      ca: caContent ? [caContent] : undefined,
    },
    sasl: {
      mechanism: 'plain',
      username: USERNAME,
      password: PASSWORD,
    },
    logLevel: logLevel.ERROR,
    retry: {
      initialRetryTime: 300,
      retries: 8
    }
  });
}

module.exports = {
  createKafkaClient,
  TOPICS: {
    ORDERS_JSON: process.env.TOPIC_ORDERS_JSON || 'orders-json',
    ORDERS_AVRO: process.env.TOPIC_ORDERS_AVRO || 'orders-avro'
  }
};
