# Kafka Demo Environment Plan

## Top-Level Overview
Design and deploy a containerized single-node Apache Kafka demo environment (KRaft mode) secured with TLS encryption (SSL) and authentication (SASL_SSL using SASL/PLAIN or SCRAM). The demo includes three Node.js services plus sample datasets for timed replay:
1. **CSV Dataset**: Sample CSV file containing test order events (`customerid`, `productid`, `quantity`, `shopid`, `delay_seconds`) allowing deterministic, timed event injection.
2. **JSON Producer**: Reads from the CSV dataset (or generates events) and publishes order events serialized in standard JSON format according to their schedule.
3. **Avro Producer**: Reads from the CSV dataset (or generates events) and publishes order events serialized in Avro binary format using a local Avro schema (via `avsc` without external Schema Registry dependencies) according to their schedule.
4. **Multi-format Consumer**: Subscribes to the order topics, detects or handles both JSON and Avro payloads, deserializes the orders, and logs/processes them.

---

## Architecture Overview
- **Broker**: Single-node Kafka container running in KRaft mode, exposing `SASL_SSL` port `9093` (or `9092`).
- **Security Assets**: Self-signed CA, broker keystore & truststore, client truststore, credentials file.
- **Client Library**: `kafkajs` (or `@confluentinc/kafka-javascript`) with `avsc` for Avro serialization.
- **Topics**: `orders-json` and `orders-avro` (or unified `orders`).

---

## Sub-Tasks

### Sub-Task 1: Security Assets & Kafka Container Infrastructure
- **Intent**: Configure TLS certificates/keys, SASL authentication credentials, and Docker Compose definition for a secure KRaft-based single-node Kafka broker.
- **Expected Outcomes**:
  - Script or assets generating CA, broker certificate, client certificate/truststore, and password configs.
  - Functional `docker-compose.yml` spinning up Kafka in KRaft mode with `SASL_SSL` enabled.
  - Healthcheck / verification of broker readiness over SASL_SSL.
- **Todo List**:
  1. Create a certificate generation script (`scripts/generate-certs.sh`) to produce CA, broker keystore/truststore, and client truststore.
  2. Create JAAS/SASL credential configuration files for broker authentication.
  3. Create `docker-compose.yml` configuring the Kafka broker with KRaft settings, listeners for SASL_SSL, and volume mounts for certs.
  4. Create `Makefile` or setup script to orchestrate certificate generation and container startup.
- **Relevant Context**:
  - `docker-compose.yml`
  - `scripts/generate-certs.sh`
  - `config/kafka_jaas.conf`
- **Status**: `[x] done`

---

### Sub-Task 2: Shared Node.js Kafka Client Configuration, Schemas & CSV Data Reader
- **Intent**: Provide shared utilities for TLS/SASL connection configuration, the Avro order schema definition, sample CSV dataset, and a timed CSV reader helper.
- **Expected Outcomes**:
  - Avro schema file `order.avsc` matching fields (`customerid`, `productid`, `quantity`, `shopid`).
  - Sample CSV data file `data/orders.csv` containing columns: `customerid,productid,quantity,shopid,delay_seconds`.
  - CSV parsing and timed scheduler utility (`src/utils/csv-reader.js`) that emits events at specified relative timestamps.
  - Shared connection helper initializing `kafkajs` client with SASL credentials and SSL CA certs.
- **Todo List**:
  1. Initialize `package.json` with required dependencies (`kafkajs`, `avsc`, `csv-parse`, `dotenv`).
  2. Create `data/orders.csv` with a comprehensive set of test order records and timed offsets in seconds.
  3. Define `schemas/order.avsc` containing record definition for order events.
  4. Create `src/utils/csv-reader.js` to parse CSV rows and emit/schedule events based on `delay_seconds`.
  5. Create `src/config/kafka.js` encapsulating broker connection options, SSL configuration, and SASL authentication parameters.
- **Relevant Context**:
  - `package.json`
  - `data/orders.csv`
  - `schemas/order.avsc`
  - `src/utils/csv-reader.js`
  - `src/config/kafka.js`
- **Status**: `[x] done`

---

### Sub-Task 3: JSON Order Producer Application
- **Intent**: Implement a Node.js producer that reads order events from the CSV file (or falls back to live generator) and publishes them as JSON strings at scheduled relative timestamps over SASL_SSL.
- **Expected Outcomes**:
  - Runnable script/service publishing timed JSON order messages read from CSV over SASL_SSL.
- **Todo List**:
  1. Create `src/producers/json-producer.js`.
  2. Integrate `src/utils/csv-reader.js` to feed order data according to `delay_seconds`.
  3. Serialize payload as JSON buffer/string and publish to `orders-json` topic.
  4. Add structured logging indicating scheduled time vs actual publish time, payload, and Kafka offset.
- **Relevant Context**:
  - `src/producers/json-producer.js`
  - `data/orders.csv`
- **Status**: `[x] done`

---

### Sub-Task 4: Avro Order Producer Application
- **Intent**: Implement a Node.js producer that reads order events from the CSV file (or falls back to live generator) and serializes them with local Avro schema before sending over SASL_SSL at scheduled relative timestamps.
- **Expected Outcomes**:
  - Runnable script/service publishing timed Avro binary encoded order messages read from CSV to Kafka over SASL_SSL.
- **Todo List**:
  1. Create `src/producers/avro-producer.js`.
  2. Load and compile `schemas/order.avsc` using `avsc`.
  3. Integrate `src/utils/csv-reader.js` to feed order data according to `delay_seconds`.
  4. Encode payload into binary buffer using compiled schema and publish to `orders-avro` topic.
  5. Add structured logging indicating scheduled time vs actual publish time, payload, and Kafka offset.
- **Relevant Context**:
  - `src/producers/avro-producer.js`
  - `schemas/order.avsc`
  - `data/orders.csv`
- **Status**: `[x] done`

---

### Sub-Task 5: Consumer Application (JSON & Avro Decoding)
- **Intent**: Implement a Node.js consumer that connects via SASL_SSL, subscribes to both topics, decodes each message according to its format, and displays the processed order.
- **Expected Outcomes**:
  - Runnable consumer receiving messages from `orders-json` and `orders-avro`, validating deserialization, and outputting structured order summaries.
- **Todo List**:
  1. Create `src/consumers/order-consumer.js`.
  2. Set up consumer group subscribing to both `orders-json` and `orders-avro`.
  3. Implement message routing and decoding logic (JSON parse for JSON topic, Avro decode using compiled schema for Avro topic).
  4. Log decoded order details (customer, product, quantity, shop, topic, partition, offset).
- **Relevant Context**:
  - `src/consumers/order-consumer.js`
- **Status**: `[x] done`

---

### Sub-Task 6: Verification, Run Scripts & Documentation
- **Intent**: Provide end-to-end instructions, NPM run scripts, and documentation for easily launching and testing the entire environment.
- **Expected Outcomes**:
  - Complete `README.md` with step-by-step instructions.
  - NPM scripts in `package.json` to start broker, produce JSON, produce Avro, and run consumer.
  - Verified end-to-end test run of all components.
- **Todo List**:
  1. Add NPM scripts (`npm run start:kafka`, `npm run produce:json`, `npm run produce:avro`, `npm run consume`).
  2. Create comprehensive `README.md` with prerequisites, architecture diagram, setup steps, and troubleshooting guide.
- **Relevant Context**:
  - `README.md`
  - `package.json`
- **Status**: `[x] done`
