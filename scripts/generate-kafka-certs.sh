#!/usr/bin/env bash
#
# Generate a self-signed CA plus broker + client keystores/truststores for
# Kafka over SSL (mTLS). Output lands in ../certs.
#
#   Broker  : kafka.server.keystore.jks  (its identity, signed by the CA)
#             kafka.server.truststore.jks (trusts the CA → can verify clients)
#   Clients : kafka.client.keystore.jks  (its identity, signed by the CA)
#             kafka.client.truststore.jks (trusts the CA → can verify the broker)
#   CA      : ca-cert / ca-key            (the root of trust)
#
# DEMO material. For production regenerate with a strong password and keep the
# keystores in a secret manager — do NOT rely on the values committed here.
#
# Usage:
#   scripts/generate-kafka-certs.sh
#   KAFKA_CERT_PASSWORD=s3cret KAFKA_BROKER_SAN="DNS:broker.prod,IP:10.0.0.5" \
#     scripts/generate-kafka-certs.sh
#
set -euo pipefail

CERT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/certs"
PASSWORD="${KAFKA_CERT_PASSWORD:-changeit}"
VALIDITY="${KAFKA_CERT_VALIDITY:-3650}"
BROKER_CN="${KAFKA_BROKER_CN:-localhost}"
# Subject Alternative Names the broker cert is valid for. Hostname verification
# checks the connect address against these, so list every name/IP clients AND
# other brokers use. Covers the 5-broker cluster (kafka1..kafka5) + host access.
BROKER_SAN="${KAFKA_BROKER_SAN:-DNS:localhost,DNS:kafka,DNS:kafka1,DNS:kafka2,DNS:kafka3,DNS:kafka4,DNS:kafka5,IP:127.0.0.1}"
CLIENT_CN="${KAFKA_CLIENT_CN:-kafka-client}"

echo "→ Writing certs to $CERT_DIR (password: $PASSWORD, validity: ${VALIDITY}d)"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"
# Remove only previously-generated artifacts (keep README.md and any docs).
rm -f ca-cert ca-key ca-cert.srl *.jks kafka_keystore_creds \
      kafka_truststore_creds kafka_sslkey_creds client-ssl.properties

# 1. Root CA --------------------------------------------------------------------
openssl req -new -x509 -nodes -days "$VALIDITY" \
  -keyout ca-key -out ca-cert \
  -subj "/CN=event-driven-kafka-ca/OU=platform/O=example/C=VN"

# 2. Truststores: both broker and clients trust the CA (so each can verify the
#    other's cert during the TLS handshake / mTLS client auth).
for ts in kafka.server.truststore.jks kafka.client.truststore.jks; do
  keytool -keystore "$ts" -alias CARoot -importcert -file ca-cert \
    -storepass "$PASSWORD" -noprompt
done

# 3. Keystore factory: generate a keypair, get it signed by the CA, then import
#    the CA + the signed cert back into the keystore.
make_keystore() {
  local store="$1" cn="$2" san="$3"

  keytool -keystore "$store" -alias localhost -validity "$VALIDITY" \
    -genkeypair -keyalg RSA -keysize 2048 \
    -storepass "$PASSWORD" -keypass "$PASSWORD" \
    -dname "CN=$cn, OU=platform, O=example, C=VN" \
    -ext "SAN=$san"

  keytool -keystore "$store" -alias localhost -certreq -file cert-req \
    -storepass "$PASSWORD" -ext "SAN=$san"

  openssl x509 -req -CA ca-cert -CAkey ca-key -CAcreateserial \
    -in cert-req -out cert-signed -days "$VALIDITY" \
    -extfile <(printf "subjectAltName=%s\n" "$san")

  keytool -keystore "$store" -alias CARoot -importcert -file ca-cert \
    -storepass "$PASSWORD" -noprompt
  keytool -keystore "$store" -alias localhost -importcert -file cert-signed \
    -storepass "$PASSWORD" -noprompt

  rm -f cert-req cert-signed
}

make_keystore kafka.server.keystore.jks "$BROKER_CN" "$BROKER_SAN"
make_keystore kafka.client.keystore.jks "$CLIENT_CN" "DNS:localhost"

# A plaintext credentials file the Kafka broker container reads for its stores.
printf '%s\n' "$PASSWORD" > kafka_keystore_creds
printf '%s\n' "$PASSWORD" > kafka_truststore_creds
printf '%s\n' "$PASSWORD" > kafka_sslkey_creds

# An SSL client config the broker containers reuse for their healthcheck (the
# CLI tools need certs once every listener is SSL). Paths are inside-container.
cat > client-ssl.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ssl.truststore.password=$PASSWORD
ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
ssl.keystore.password=$PASSWORD
ssl.key.password=$PASSWORD
EOF

rm -f ca-cert.srl
echo "✓ Done:"
ls -1 "$CERT_DIR"
