# Kafka 4.3 generated protocol

This vendored crate is `kafka-protocol-rs` 0.17.0 generated from the message
JSON files shipped in:

`org.apache.kafka:kafka-clients:4.3.0:jar:sources`

Kafka 4.3.0 resolves to upstream commit
`a9ce3221537b8653448750697915607dc7936cf3`. The source JAR and release tag
contain the same `clients/src/main/resources/common/message` schemas.

Regenerate from the vendored crate root:

```text
cargo run --manifest-path protocol_codegen/Cargo.toml -- \
  <schema-directory> src/messages
```

The generator is the upstream `kafka-protocol-rs` generator with two narrow
compatibility changes:

- accept Kafka schemas that encode boolean metadata as `"true"`;
- take explicit schema and output directories instead of cloning Kafka trunk.
- include an explicitly marked unstable latest version. Kafka 4.3 has one:
  `InitProducerId` v6 for KIP-939.

Generated message files must not be edited by hand.
