package dev.lotse.kotlin

import dev.lotse.client.ClientConfig
import dev.lotse.client.GroupConfig
import dev.lotse.client.OutMessage
import dev.lotse.client.QueueOptions

/**
 * A client for the brokers in [bootstrap] (`host:port`), configured by [configure]:
 *
 * ```
 * val lotse = lotse("10.0.0.1:9092", "10.0.0.2:9092") {
 *     tls(TlsConfig.fromFiles(ca, cert, key))
 *     transactionalId("billing-1")
 * }
 * ```
 */
public fun lotse(vararg bootstrap: String, configure: ClientConfig.Builder.() -> Unit = {}): Lotse =
    Lotse(ClientConfig.builder(*bootstrap).apply(configure).build())

/** A [GroupConfig] from the defaults, changed by [configure]. */
public fun groupConfig(configure: GroupConfig.Builder.() -> Unit): GroupConfig =
    GroupConfig.builder().apply(configure).build()

/** [QueueOptions] from the defaults, changed by [configure]. */
public fun queueOptions(configure: QueueOptions.Builder.() -> Unit): QueueOptions =
    QueueOptions.builder().apply(configure).build()

/** A message with [value] in UTF-8, and a key and headers if given. */
public fun message(value: String, key: String? = null, headers: Map<String, String> = emptyMap()): OutMessage {
    var m = OutMessage.value(value)
    if (key != null) {
        m = m.withKey(key)
    }
    for ((name, v) in headers) {
        m = m.withHeader(name, v.toByteArray(Charsets.UTF_8))
    }
    return m
}
