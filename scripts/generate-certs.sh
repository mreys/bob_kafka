#!/usr/bin/env bash
set -euo pipefail

SECRETS_DIR="./secrets"
rm -rf "${SECRETS_DIR}"
mkdir -p "${SECRETS_DIR}"

PASS="confluent"
DAYS=365

echo "==> 1. Generating CA Key and Certificate..."
openssl req -new -x509 \
  -keyout "${SECRETS_DIR}/ca.key" \
  -out "${SECRETS_DIR}/ca.crt" \
  -days ${DAYS} \
  -subj "/CN=Kafka-Demo-CA/OU=Demo/O=MyOrg/ST=State/C=US" \
  -nodes

echo "==> 2. Generating Broker Private Key and Certificate..."
cat > "${SECRETS_DIR}/san.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost
OU = Demo
O = MyOrg
ST = State
C = US

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = kafka
DNS.3 = kafka-demo
IP.1 = 127.0.0.1
EOF

openssl req -new \
  -newkey rsa:2048 \
  -nodes \
  -keyout "${SECRETS_DIR}/kafka.key" \
  -out "${SECRETS_DIR}/kafka.csr" \
  -config "${SECRETS_DIR}/san.cnf"

openssl x509 -req \
  -in "${SECRETS_DIR}/kafka.csr" \
  -CA "${SECRETS_DIR}/ca.crt" \
  -CAkey "${SECRETS_DIR}/ca.key" \
  -CAcreateserial \
  -out "${SECRETS_DIR}/kafka.crt" \
  -days ${DAYS} \
  -extfile "${SECRETS_DIR}/san.cnf" \
  -extensions v3_req

echo "==> 3. Creating PKCS12 bundle with full chain and private key..."
openssl pkcs12 -export \
  -in "${SECRETS_DIR}/kafka.crt" \
  -inkey "${SECRETS_DIR}/kafka.key" \
  -certfile "${SECRETS_DIR}/ca.crt" \
  -out "${SECRETS_DIR}/kafka.p12" \
  -name localhost \
  -password pass:${PASS}

echo "==> 4. Converting PKCS12 into Java KeyStore (JKS)..."
keytool -importkeystore \
  -srckeystore "${SECRETS_DIR}/kafka.p12" \
  -srcstoretype PKCS12 \
  -srcstorepass ${PASS} \
  -destkeystore "${SECRETS_DIR}/kafka.keystore.jks" \
  -deststoretype JKS \
  -deststorepass ${PASS} \
  -destkeypass ${PASS} \
  -noprompt

echo "==> 5. Creating Broker Truststore containing CA..."
keytool -keystore "${SECRETS_DIR}/kafka.truststore.jks" \
  -alias CARoot \
  -import -noprompt \
  -file "${SECRETS_DIR}/ca.crt" \
  -storepass ${PASS}

echo "==> 6. Writing credential files..."
echo -n "${PASS}" > "${SECRETS_DIR}/ssl_keystore_password"
echo -n "${PASS}" > "${SECRETS_DIR}/ssl_key_password"
echo -n "${PASS}" > "${SECRETS_DIR}/ssl_truststore_password"

echo "==> Certificates and keystores generated cleanly in ${SECRETS_DIR}!"
