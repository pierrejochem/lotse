# Lotse guides

Lotse is a message broker written in Rust. It offers reliable queues and persistent streams on one replicated log, with exactly-once delivery.

These guides are for four readers:

- **Setup:** install Lotse, start a cluster and send a first message.
- **Administration:** run a cluster: certificates, users and roles, nodes, retention, monitoring.
- **Development:** write a client against the native protocol, use the Rust, Java and Kotlin clients, or change Lotse itself.
- **Use:** work with streams, queues, exchanges, transactions and the console.

The guides say how to do things. The reasons behind each decision are in the [architecture decision records](https://github.com/pierrejochem/lotse/tree/main/docs/adr); the storage and protocol design is in the [design document](https://github.com/pierrejochem/lotse/blob/main/docs/design.md) (German).
