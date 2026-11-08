# Exchanges and routing

An exchange takes a message from a publisher and passes it to the queues bound to it. The publisher need not know the queues. You create exchanges and bindings at runtime.

## Declare, bind, publish

```java
// Not from the compiled examples.
client.createQueue("invoices-eu");
client.createQueue("invoices-all");
client.declareExchange("invoices", Messages.ExchangeType.TOPIC);
client.bind("invoices", "invoices-eu", "invoice.eu.*");
client.bind("invoices", "invoices-all", "invoice.#");

Published p = client.publish("invoices", "invoice.eu.created", OutMessage.value("#4711"), true);
```

`declareExchange` returns the exchange's id and throws `ClientException.ResourceExists` if the name is taken. `bind` and `unbind` take the exchange, the queue and a binding key. `bindHeaders` binds by headers.

`publish(exchange, routingKey, message, mandatory)` goes to any node. The node routes the message by the bindings it has applied, checks that you may produce to the exchange and to every routed queue, and writes the message to each queue. It returns once every routed queue holds the message. `Published.routed()` lists the queues and the offset in each.

## The four kinds

| Kind | A message goes to a queue when |
| --- | --- |
| direct | The binding key equals the routing key |
| topic | The routing key matches the binding key. Keys are words separated by `.`; `*` matches exactly one word, `#` zero or more. An empty key has no words |
| fanout | Always: every bound queue gets it, and the routing key is ignored |
| headers | The message headers match the binding's headers: all of them if the binding asks for all, any one if it asks for any. A header with a null value matches only a null value. A binding without headers matches everything when it asks for all, and nothing when it asks for any |

A queue bound twice to one exchange gets a message once.

Headers binding:

```java
// Not from the compiled examples.
client.declareExchange("events", Messages.ExchangeType.HEADERS);
client.bindHeaders("events", "audit-eu",
        List.of(new Header(ByteString.utf8("region"), ByteString.utf8("eu"))), true);
client.publish("events", "", OutMessage.value("login").withHeader("region", "eu"), false);
```

The routing rules are those of the `lotse-route` crate; the Java SDK is checked against the same test vectors.

## What can go wrong

- **Not routed.** With `mandatory = true`, a message that matches no queue throws `ClientException.Unroutable` (error `UNROUTABLE`). With `mandatory = false` it is dropped without an error.
- **Partly written.** If some queues could not be written, `ClientException.NotAllWritten` lists the queues that hold the message and the failures. The message is in those queues; do not assume it is in none.
- **Lost answer.** `ClientException.Unknown`. The publish is **not** sent again, because the node may have written it and a second try could write it twice. Decide whether a duplicate matters.
- **Not allowed.** If you lack the producer role on any routed queue, nothing is written and no queue is named (`UNAUTHORIZED`).
- **Unknown exchange.** The name is looked up once more, in case it was deleted and made again with a new id.

Inside a [transaction](transactions.md), the client routes and writes to each queue itself, so the message reaches all its queues or none.

See [ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md) and [ADR 0032](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0032-publish-routed-by-the-client.md).
