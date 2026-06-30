# Kafka SSL/mTLS certificates

**⚠️ DEMO material — do NOT use in real production.** The keystores below are
committed with the throwaway password `changeit`. For a real deployment,
regenerate with a strong password and keep the key material in a secret manager
(Vault, AWS/GCP secret store, k8s Secret) — never in git.

Regenerate everything with:

```bash
KAFKA_CERT_PASSWORD=<strong-pass> \
KAFKA_BROKER_SAN="DNS:your-broker,IP:10.0.0.5" \
  scripts/generate-kafka-certs.sh
```

| File                          | Used by | Purpose                                            |
|-------------------------------|---------|----------------------------------------------------|
| `ca-cert` / `ca-key`          | —       | Root CA: signs all certs, the shared root of trust |
| `kafka.server.keystore.jks`   | broker  | Broker's TLS identity (CN/SAN = broker hostnames)  |
| `kafka.server.truststore.jks` | broker  | Trusts the CA → can verify client certs (mTLS)     |
| `kafka.client.keystore.jks`   | apps    | Client's TLS identity for mTLS                      |
| `kafka.client.truststore.jks` | apps    | Trusts the CA → verifies the broker cert            |
| `kafka_*_creds`               | broker  | Plaintext password files (for Confluent-style images) |

Default password: `changeit`. Broker cert SAN: `localhost`, `kafka`, `127.0.0.1`.
