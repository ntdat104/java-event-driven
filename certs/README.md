# Kafka SSL/mTLS certificates

**⚠️ DEMO material — do NOT use in real production.** The keystores below are
committed with the throwaway password `changeit`. For a real deployment,
regenerate with a strong password and keep the key material in a secret manager
(Vault, AWS/GCP secret store, k8s Secret) — never in git.

Regenerate everything with:

```bash
KAFKA_CERT_PASSWORD=<strong-pass> \
KAFKA_BROKER_SAN="DNS:broker1.prod,DNS:broker2.prod,IP:10.0.0.5" \
  scripts/generate-kafka-certs.sh
```

| File                          | Used by | Purpose                                            |
|-------------------------------|---------|----------------------------------------------------|
| `ca-cert` / `ca-key`          | —       | Root CA: signs all certs, the shared root of trust |
| `kafka.server.keystore.jks`   | brokers | Broker TLS identity (SAN covers all 5 brokers)     |
| `kafka.server.truststore.jks` | brokers | Trusts the CA → verify client + peer broker certs (mTLS) |
| `kafka.client.keystore.jks`   | apps    | Client TLS identity for mTLS                        |
| `kafka.client.truststore.jks` | apps    | Trusts the CA → verifies the broker cert            |
| `kafka_*_creds`               | brokers | Plaintext password files (for Confluent-style images) |
| `client-ssl.properties`       | brokers | SSL client config the broker healthchecks reuse    |

Default password: `changeit`. Broker cert SAN covers the whole cluster:
`localhost`, `kafka`, `kafka1`..`kafka5`, `127.0.0.1`.
