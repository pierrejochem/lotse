# Lotse – Architektur & Storage-Design

![Lotse architecture](architecture.png)

Oct 27, 2026 · @Pierre Jochem

## Überblick

Ein Enterprise-Message-Broker in Rust, der RabbitMQ-artige **Reliable Queues** und Kafka-artige **Persistent Streams** auf einer gemeinsamen Log-Engine betreibt, mit Exactly-once-Semantik ab dem MVP.

- **Reliable Queues:** Exchanges und Routing, Ack/Nack/Reject, DLQ, Retry und Delayed Delivery, TTL, Prioritäten, Prefetch, Single Active Consumer.
- **Persistent Streams:** partitionierte Logs, Consumer Groups, Replay über Offsets, Retention, Log Compaction.
- **Gemeinsamer Kern:** Security (TLS/mTLS, RBAC, Mandanten), Metadaten, Cluster-Steuerung, Metriken und Tracing.

Architekturprinzipien: Zuverlässigkeit vor Durchsatz, Failover ohne Datenverlust, sichere Defaults, Einfachheit statt unnötiger Komplexität. Connectors zu Datenbanken, HTTP und anderen Messaging-Systemen laufen als separate Prozesse.

## Architekturentscheidungen

Acht Grundentscheidungen sind getroffen. Sie bauen aufeinander auf: Die gemeinsame Log-Engine macht Multi-Raft und Exactly-once für beide Datenmodelle mit einer Implementierung möglich.

| # | Thema | Entscheidung | Begründung |
| --- | --- | --- | --- |
| 1 | Storage | Eine Log-Engine für Queues und Streams; Queues = Log + Ack-Index | Eine Storage- und Replikations-Engine statt zwei |
| 2 | Replikation | Multi-Raft: Raft pro Partition/Queue, gebündelte Heartbeats und Batching pro Node-Paar | Starke Garantien, skaliert auf tausende Queues |
| 3 | Laufzeit | Ein Broker-Prozess pro Node, Services als Module/Crates | Keine Netzwerk-Hops im Datenpfad |
| 4 | Protokolle | Natives Rust-Protokoll zuerst, Adapter-Layer vorgesehen; AMQP 0-9-1 und Kafka-Wire später als Plugins | Schlanker Start, bestehende Clients später nachrüstbar |
| 5 | Garantien | Exactly-once ab MVP: idempotente Producer + Transaktionen | Consume–Ack–Produce atomar über Queues und Streams |
| 6 | Runtime | Tokio | Ökosystem (rustls, openraft-Ideen, tonic), schnell produktiv |
| 7 | Record-Kodierung | Varint-Deltas (Kafka-Stil) | Kompakt, gut komprimierbar |
| 8 | Raft-Snapshots | State + Segment-Referenzen | Kein Kopieren unveränderlicher Segmente |

## Storage-Layout

Das Raft-Log **ist** das Partition-Log: Es gibt kein separates WAL, jeder Raft-Entry ist ein Record-Batch, und `base_offset` plus eins ist der Raft-Index des ersten Records. Ein Entry deckt so viele Indizes ab, wie er Records hat (first..=last); Index 0 bleibt so für „vor dem ersten Entry“ frei.

### Dateien pro Partition/Queue

```
partition-0007/
  00000000000000000000.log        # Record-Batches (Raft-Entries)
  00000000000000000000.index      # sparse: offset -> byte pos
  00000000000000000000.timeindex  # sparse: timestamp -> offset
  00000000000000000000.txnindex   # abgebrochene Txns: (pid, first, last)
  00000000000000000000.pstate     # Producer-State-Snapshot beim Segment-Roll
  raft.meta                       # term, voted_for, commit_index
```

Geschlossene Segmente sind unveränderlich. Das ist die Grundlage für Snapshots per Referenz und für den Segment-Rewrite.

### Batch-Header (fest, little-endian)

```rust
struct BatchHeader {
    base_offset:       u64,  // = Raft-Index des ersten Records − 1
    batch_len:         u32,
    term:              u64,  // Raft-Term (Leader-Epoch)
    magic:             u8,
    crc32c:            u32,  // ab `attributes` bis Batch-Ende
    attributes:        u16,  // compression(3) | txn(1) | control(1) | kind(3)
    last_offset_delta: u32,
    base_ts:           i64,
    max_ts:            i64,
    producer_id:       u64,  // 0 = nicht idempotent
    producer_epoch:    u16,
    base_sequence:     i32,
    record_count:      u32,
}
```

`kind` unterscheidet `DATA`, `ACK`, `QUEUE_STATE` und `TIMER`. Dadurch liegen Queue-Zustände im selben Log und in derselben Raft-Gruppe wie die Nachrichten.

### Record-Kodierung (varint-Deltas)

```
length          varint   // Länge des restlichen Records
attributes      i8       // reserviert
ts_delta        varlong  // relativ zu base_ts
offset_delta    varint   // relativ zu base_offset
key_len         varint   // -1 = null
key             bytes
value_len       varint   // -1 = null (Tombstone für Compaction)
value           bytes
header_count    varint
  [h_key_len varint, h_key bytes, h_val_len varint, h_val bytes]*
```

Komprimiert (zstd oder lz4, laut `attributes`) wird nur der Record-Block. Der Header bleibt immer unkomprimiert, damit Raft, Indizes und Transaktionslogik nie dekomprimieren müssen.

## Transaktionen & Exactly-once

Transaktionsmarker sind gewöhnliche Batches mit `control=1` und `txn=1`. Consumer lesen standardmäßig nur committete Daten (read committed).

### Control-Batches

Genau ein Record pro Marker:

```
key:   version u16 | type u16  (0 = ABORT, 1 = COMMIT)
value: coordinator_epoch u32
```

Ein Marker schließt alle offenen Batches derselben `producer_id` in dieser Partition ab.

### Producer-State und Idempotenz

Pro `producer_id` werden gehalten: Epoch, die letzten 5 Einträge `(base_seq, last_seq, offset)` und `current_txn_first_offset`.

- Duplikat mit bekannter Sequenz → alter Offset wird zurückgegeben.
- Lücke in der Sequenz → `OutOfOrderSequence`.
- Ältere Epoch → `ProducerFenced`.

Der State liegt im Speicher und wird beim Segment-Roll als `.pstate` gesichert. Recovery replayt nur ab dem letzten Snapshot.

### Lesepfad: HW und LSO

- **High Watermark (HW)** = Raft-Commit-Index, von der Mehrheit bestätigt.
- **Last Stable Offset (LSO)** = min(HW, `first_offset` aller offenen Txns).
- `read_committed`-Consumer lesen bis LSO und überspringen abgebrochene Batches über `.txnindex`, ohne das Log zu scannen.

### Transaktions-Koordinator

Der Koordinator führt die interne Partition `__txn_state` als eigene Raft-Gruppe.

1. `InitProducer` vergibt `producer_id` und Epoch (Fencing alter Instanzen).
2. `AddPartitions` persistiert die Mitglieder der Transaktion.
3. `EndTxn` schreibt `PREPARE_COMMIT` (bzw. `PREPARE_ABORT`) in `__txn_state`.
4. COMMIT- bzw. ABORT-Marker gehen an jede beteiligte Partition (idempotent, mit Retry).
5. `COMPLETE_COMMIT` schließt die Transaktion ab.

Fällt der Koordinator nach `PREPARE_COMMIT` aus, schreibt der neue Leader die Marker zu Ende. Ein Timeout ohne `EndTxn` führt zu ABORT.

## Queue-Semantik auf dem Log

Eine Queue ist ein Log plus Ack-Index. Acks, Zustellzähler und Timer sind selbst Records im selben Log, also repliziert und transaktionsfähig.

- **ACK-Batches** enthalten Offset-Ranges `[from, to)`, varint-kodiert (Key leer). Im Speicher entsteht daraus eine Roaring-Bitmap plus Ack-Floor (alles darunter ist bestätigt).
- **Consume → Ack → Produce atomar:** Der Ack wird mit `txn=1` und der `producer_id` des Consumers geschrieben und greift erst mit dem COMMIT-Marker. Bei ABORT wird die Nachricht wieder zustellbar.
- **In-flight-Zustellungen** sind flüchtiger Leader-State. Nach einem Failover wird alles Unbestätigte neu zugestellt. Das ist korrekt, weil nur committete Acks zählen.
- **Zustellversuche für DLQ** werden nur bei Nack oder Timeout persistiert, gebündelt als `QUEUE_STATE`-Record, nicht bei jeder Zustellung.
- **TTL und Delayed Delivery** nutzen `TIMER`-Records mit Fälligkeitszeit. Im Speicher steckt eine Timer-Wheel, die bei Recovery aus Snapshot und Log neu aufgebaut wird.

Prioritäten und Single Active Consumer: siehe Weitere Entscheidungen.

## Replikation & Snapshots

Jede Partition und jede Queue ist eine eigene Raft-Gruppe. Heartbeats und Append-Requests aller Gruppen zwischen zwei Nodes werden gebündelt (Multi-Raft). Eine Schreiboperation ist bestätigt, sobald 2 von 3 Replikaten sie persistiert haben.

### Snapshot-Inhalt

```
snapshot-<last_index>-<term>/
  manifest      // last_index, term, Membership,
                // Segmente (Name, base_offset, Größe, crc)
  pstate        // Producer-State inkl. offener Txns
  queue_state   // Ack-Floor + Roaring-Bitmap, Delivery-Counts, Timer-Wheel
```

- **Segmente werden nicht kopiert.** Das Manifest referenziert sie nur. Das ist sicher, weil geschlossene Segmente unveränderlich sind.
- **Follower-Catch-up:** Erst das Manifest, dann nur fehlende oder abweichende Segmente (crc), dann der State. Danach normale Replikation ab `last_index + 1`.
- **Log-Truncation:** Raft darf Einträge bis `last_index` verwerfen. Segmente löscht ausschließlich die Retention. Ein Snapshot pinnt seine Segmente, bis ein neuerer existiert.

Im Raft-Kern ist ein Snapshot nur `(index, term, membership)`; Inhalt und Übertragung gehören dem Server. Hat der Leader die Einträge kompaktiert, die ein Follower braucht, schickt er `Snapshot`, und der Server hängt seinen neuesten Snapshot an. Ein Follower, dessen Log den letzten Eintrag des Snapshots schon mit gleichem Term enthält, behält sein Log und committet nur: Einträge danach können bestätigte Kopien committeter Einträge sein. Erst ein abweichendes Log wird durch den Snapshot ersetzt.

Cluster-Metadaten (Topics, Queues, Partition-Zuordnung, Membership) liegen in einer eigenen Metadaten-Raft-Gruppe.

### Membership-Änderungen

Neue Replikas kommen als **Learner** dazu: Sie erhalten das Log, stimmen aber nicht ab und zählen nicht zum Quorum. Erst wenn ein Learner aufgeholt hat, wechselt die Gruppe per **Joint Consensus** über die Konfiguration C\_old,new zu C\_new; während des Übergangs braucht jede Entscheidung eine Mehrheit in beiden Konfigurationen. So lassen sich auch mehrere Replikas auf einmal tauschen, etwa bei einer Rack-Migration. Konfigurationen sind eigene Entries im Log und gelten ab dem Anhängen, nicht erst ab dem Commit.

- **Eine Änderung zur Zeit:** Eine neue Änderung beginnt erst, wenn die vorige committet ist. Neue Voter müssen Learner sein, die bis zum Commit-Index aufgeholt haben. Den Schritt von C\_old,new zu C\_new hängt der Leader selbst an, sobald C\_old,new committet ist.
- **Neue Mitglieder beginnen von vorn:** Der Leader repliziert an sie ab dem Snapshot bzw. ab Index 1, nicht ab seinem Log-Ende.
- **Ein entfernter Leader** führt die Gruppe weiter, bis C\_new committet ist, und tritt dann zurück.
- **Wahlen während des Übergangs:** Ein Knoten, den die neueste, noch nicht committete Konfiguration entfernt, darf weiter kandidieren, wenn er in der vorigen Voter war. Er hat oft das längste Log, und ohne ihn kann es sein, dass niemand gewinnt. Er zählt Stimmen nach der neuesten Konfiguration. Ist die Entfernung committet, kandidiert er nicht mehr.
- **Keine Filterung von Stimmen nach Mitgliedschaft:** Ein zurückliegender Voter kennt einen eben beförderten Voter vielleicht noch nicht und muss ihn trotzdem wählen können. Störungen durch entfernte Knoten verhindert Leader-Stickiness: **Check-Quorum ist deshalb immer an** und nicht abschaltbar; Pre-Vote bleibt konfigurierbar.

### Linearisierbare Lesezugriffe

Standard ist **ReadIndex**: Der Leader merkt sich seinen Commit-Index, bestätigt per Heartbeat-Runde mit einem Quorum, dass er noch Leader ist, und antwortet, sobald dieser Index angewendet ist. Gleichzeitige Lesezugriffe teilen sich eine Runde. **Leader-Leases** sind pro Cluster zuschaltbar, wo die Uhrenabweichung begrenzt ist; dann liest der Leader ohne Runde, solange seine Lease läuft.

- **Runden:** Jedes Append und jeder Heartbeat trägt eine Rundennummer (`seq`), die der Follower zurückgibt. Ein Lesezugriff gilt als bestätigt, sobald ein Quorum eine Runde beantwortet hat, die **nach** seinem Eintreffen gesendet wurde; eine frühere Runde beweist die Führung nur für die Zeit davor. Alle Lesezugriffe zwischen zwei `Ready` teilen sich eine Runde.
- **Neuer Leader:** Lesezugriffe warten, bis er einen Eintrag seines eigenen Terms committet hat; vorher kann sein Commit-Index zurückliegen.
- **Abgebrochen:** Tritt der Leader zurück, meldet er offene Lesezugriffe als abgebrochen; der Client fragt beim neuen Leader nach. Follower lesen nicht selbst, der Server leitet an den Leader weiter.
- **Lease:** Mit `lease_ticks` liest der Leader ohne Runde, solange ein Quorum eine Runde beantwortet hat, die vor weniger als `lease_ticks` gestartet wurde. Die Lease muss um mehr als die Uhrenabweichung kürzer sein als das Election-Timeout. Ein neu gestarteter Knoten ignoriert dann ein Election-Timeout lang Stimmanfragen, weil er einen Leader mit laufender Lease vergessen haben kann.

### Leader-Transfer

`transfer_leadership(ziel)` nimmt keine Schreibzugriffe mehr an (Clients werden an das Ziel verwiesen), bringt das Ziel auf den neuesten Stand und schickt ihm `TimeoutNow`. Das Ziel kandidiert sofort ohne Pre-Vote; seine Stimmanfragen tragen ein Transfer-Kennzeichen, das die Leader-Stickiness übergeht. Kommt in einem Election-Timeout kein Wechsel zustande, führt der Leader weiter. Ein Leader, der in seinem Term einen Transfer versucht hat, verzichtet für den Rest des Terms auf die Lease: Ein verzögertes `TimeoutNow` könnte das Ziel sonst noch während einer laufenden Lease an die Macht bringen.

## Retention & Compaction

Gelöscht wird immer segmentweise. Streams und Queues unterscheiden sich nur darin, wann ein Segment frei wird.

- **Streams:** Retention nach Zeit oder Größe. Compaction behält den letzten Wert pro Key; ein `null`-Value ist ein Tombstone. Control-Batches bleiben erhalten, bis alle Daten ihrer Transaktion entfernt sind.
- **Queues:** Ein Segment ist löschbar, sobald der Ack-Floor darüber liegt.
- **Segment-Rewrite:** Eine einzelne, lange unbestätigte Nachricht würde sonst Segmente festhalten. Der Rewrite kopiert die Überlebenden in ein neues Segment, behält Original-Offsets und `QUEUE_STATE` bei. Der nächste Snapshot referenziert die neuen Segmente, danach werden die alten freigegeben.

## Natives Protokoll

TCP mit TLS, eigenes längenpräfixiertes Binärformat, Version pro API-Key. Record-Batches gehen unverändert vom Log auf die Leitung.

| Thema | Entscheidung |
| --- | --- |
| Transport | TCP + TLS 1.3, eigenes Framing, Multiplexing über Correlation-IDs |
| Kodierung | Eigenes Binärformat (feste Felder + varints), Tagged Fields für Erweiterungen |
| Versionierung | Min/max-Version pro API-Key, im Handshake gemeldet; Client wählt pro Request |
| Authentifizierung | mTLS für Service-Identitäten; SASL mit SCRAM-SHA-512 und OAUTHBEARER (OIDC) |
| Zustellung | Streams: Pull (Fetch mit Long-Polling); Queues: Push innerhalb eines Credit-Fensters |
| Cluster-Routing | Smart Client: Metadaten holen, direkt zum Leader; sonst `NOT_LEADER` mit Leader-Hinweis |

### Frames

```
Request:  length u32 | api_key u16 | api_version u16 | correlation_id u32
          | flags u16 | tagged_fields | body
Response: length u32 | correlation_id u32 | flags u16 | tagged_fields | body
Push:     length u32 | api_key u16 | api_version u16 | correlation_id 0
          | flags PUSH | subscription_id u32 | body
```

Requests auf einer Verbindung werden gepipelined; Antworten kommen pro Partition geordnet, aber nicht global. Push-Frames (Zustellung an Queue-Consumer) nutzen `correlation_id = 0` und tragen die Subscription-ID.

### Verbindungsaufbau

1. TLS-Handshake; bei mTLS liefert das Client-Zertifikat den Principal.
2. `ApiVersions`: Broker meldet min/max je API-Key und seine Features.
3. `SaslHandshake` + `SaslAuthenticate` (entfällt, wenn mTLS als alleinige Auth konfiguriert ist).
4. Bei OAUTHBEARER: erneute Authentifizierung vor Token-Ablauf auf derselben Verbindung.

### API-Keys (erster Schnitt)

| Bereich | API-Keys |
| --- | --- |
| Cluster | `ApiVersions`, `Metadata`, `FindCoordinator` |
| Streams | `Produce`, `Fetch`, `ListOffsets`, `GroupHeartbeat`, `CommitOffsets`, `OffsetFetch` |
| Queues | `Publish`, `Subscribe`, `Deliver` (Push), `Credit`, `Ack`, `Nack`, `Unsubscribe` |
| Transaktionen | `InitProducer`, `AddPartitionsToTxn`, `AddAcksToTxn`, `AddOffsetsToTxn`, `EndTxn` |
| Admin | `CreateStream`, `CreateQueue`, `DeclareExchange`, `Bind`, `DeleteResource`, `DescribeConfigs`, `AlterConfigs` |

`Publish` routet über Exchanges und schreibt in die Ziel-Queues. `Produce` schreibt direkt in eine Partition. Beide nutzen dasselbe Batch-Format und dieselben Idempotenzregeln.

### Zero-copy

`Fetch` sendet Segmentbytes ohne Umkodierung. Ohne TLS über `sendfile`, mit TLS über kTLS (Linux), sonst über einen gepufferten Pfad. Bei `read_committed` endet die Antwort am LSO; der Client filtert abgebrochene Batches anhand der mitgelieferten Liste aus `.txnindex`.

## Flow Control & Quotas

Überlast wird gebremst, nicht abgebrochen: Throttle-Zeiten in Antworten, Hard-Stop nur für Schreibvorgänge. Lesen und Acks laufen immer weiter, damit sich Queues leeren können.

| Thema | Entscheidung |
| --- | --- |
| Consumer-Credits (Queues) | Fenster aus Nachrichtenanzahl **und** Bytes; Nachschub über `Credit` |
| Publisher-Backpressure | `throttle_ms` in jeder Antwort; ignoriert der Client es, hält der Broker die Verbindung für diese Zeit an |
| Disk-/Speicherknappheit | Soft-Grenze: `throttle_ms` steigt; Hard-Grenze: Schreiben → `STORAGE_FULL`, Lesen/Acks weiter |
| Quota-Ebene | Mandant als Obergrenze, darunter optional pro Principal/Client-ID (nur enger, nie weiter) |
| Begrenzte Ressource | Speicher (Bytes gesamt inkl. Replikate) |
| Überschreitung | Schreiben → `QUOTA_EXCEEDED`; Lesen und Acks weiter |
| Erfassung | Partition-Leader melden Nutzung alle paar Sekunden an die Metadaten-Gruppe; Durchsetzung lokal, kurzes Überschreiten möglich |

Nach dem MVP folgen Durchsatz-Quotas (Bytes/s rein und raus) sowie Quotas für Verbindungen und Request-Rate, beide über denselben `throttle_ms`-Mechanismus. Quotas auf die Anzahl von Ressourcen sind nicht geplant.

## Feldlayouts der API-Keys

Datenpfad-Requests adressieren Ressourcen nur über u64-IDs aus `Metadata`. Jede Partition oder Queue in einer Antwort trägt ihren eigenen Fehlercode, sodass Teilerfolge möglich sind.

### Konventionen

```
u8/u16/u32/u64/i64  feste Breite, little-endian
varint              ZigZag-varint
string              u16 Länge + UTF-8 (0xFFFF = null)
bytes               u32 Länge + Daten
array<T>            varint Anzahl + Elemente
tagged_fields       am Ende jeder Struktur, für abwärtskompatible Erweiterungen
```

Ein Frame ist standardmäßig höchstens 16 MiB groß (konfigurierbar), ein Record-Batch höchstens 1 MiB. Der Broker prüft `length` vor dem Lesen des Bodys und schließt bei Überschreitung mit `FRAME_TOO_LARGE`.

### Cluster

```
ApiVersions  -> versions: array{api_key u16, min u16, max u16}, features: array<string>

Metadata     <- names: array<string>            // leer = alle
             -> brokers:   array{node_id u32, host string, port u16, rack string}
                resources: array{error u16, name string, resource_id u64,
                                 kind u8,       // STREAM | QUEUE | EXCHANGE
                                 partitions: array{partition u32, leader u32,
                                                   leader_term u64, voters array<u32>}}
```

Eine Queue erscheint als Ressource mit genau einer Partition 0.

### Streams

```
Produce  <- txn_id string?, timeout_ms u32,
            resources: array{resource_id u64, partitions: array{partition u32, batch bytes}}
         -> throttle_ms u32,
            resources: array{resource_id u64, partitions: array{partition u32, error u16,
                       base_offset u64, append_ts i64, error_msg string?}}

Fetch    <- max_wait_ms u32, min_bytes u32, max_bytes u32,
            isolation u8,                       // 0 = uncommitted, 1 = committed
            resources: array{resource_id u64, partitions: array{partition u32,
                       fetch_offset u64, leader_term u64, max_bytes u32}}
         -> throttle_ms u32,
            resources: array{resource_id u64, partitions: array{partition u32, error u16,
                       high_watermark u64, last_stable_offset u64, log_start_offset u64,
                       aborted_txns: array{producer_id u64, first_offset u64},
                       records bytes}}          // rohe Segmentbytes, zero-copy
```

### Consumer Groups

```
GroupHeartbeat <- group_id string, member_id string,      // leer beim Beitritt
                  member_epoch i32,                       // 0 = beitreten, -1 = verlassen
                  rebalance_timeout_ms u32,
                  subscribed array<u64>?,                 // nur bei Änderung
                  owned: array{resource_id u64, partitions array<u32>}?  // nur bei Änderung
               -> throttle_ms u32, error u16, member_id string, member_epoch i32,
                  heartbeat_interval_ms u32,
                  assignment: array{resource_id u64, partitions array<u32>}?
                                                          // vollständig, nur bei Epoch-Wechsel

OffsetFetch    <- group_id string, require_stable u8,     // 1 = auf offene Txn-Commits warten
                  resources: array{resource_id u64, partitions array<u32>}
               -> resources: array{resource_id u64, partitions: array{partition u32, error u16,
                             offset u64, leader_term u64, metadata string?}}

CommitOffsets  <- group_id string, member_id string, member_epoch i32,
                  resources: array{resource_id u64, partitions: array{partition u32,
                             offset u64, leader_term u64, metadata string?}}
               -> resources: array{resource_id u64, partitions: array{partition u32, error u16}}
```

Der Koordinator ist der Leader der `__groups`-Partition, auf die die Gruppe gehasht wird (`FindCoordinator`). Ein Member gibt Partitionen erst frei, wenn seine Antwort sie nicht mehr enthält; der Koordinator vergibt sie erst neu, wenn der alte Besitzer sie im nächsten Heartbeat nicht mehr als `owned` meldet. Neue Fehlercodes: 15 `FENCED_MEMBER_EPOCH` (neu beitreten), 16 `UNKNOWN_MEMBER_ID` (neu beitreten), 17 `FRAME_TOO_LARGE` (Verbindung wird geschlossen).

### Queues

```
Publish     <- txn_id string?, timeout_ms u32,
               entries: array{exchange_id u64, routing_key string, mandatory u8, batch bytes}
            -> throttle_ms u32,
               entries: array{error u16, routed: array{queue_id u64, base_offset u64}}

Subscribe   <- queue_id u64, consumer_tag string, credit_msgs u32, credit_bytes u32,
               exclusive u8
            -> error u16, subscription_id u32

Deliver     (Push, correlation_id 0)
               subscription_id u32, batch bytes,
               deliveries: array{offset u64, delivery_count u16}

Credit      <- subscription_id u32, add_msgs u32, add_bytes u32   // flag NO_RESPONSE

Ack         <- queue_id u64, txn_id string?, ranges: array{from u64, to u64}
            -> error u16

Nack        <- queue_id u64, ranges: array{from u64, to u64}, requeue u8, delay_ms u32
            -> error u16
```

`Deliver` sendet den Batch unverändert aus dem Log. `deliveries` nennt die Records daraus, die diesem Consumer zugestellt sind; bereits bestätigte oder anderen Consumern zugestellte Records im selben Batch ignoriert der Client.

### Transaktionen

```
InitProducer       <- txn_id string?, txn_timeout_ms u32
                   -> error u16, producer_id u64, producer_epoch u16

AddPartitionsToTxn <- txn_id string, producer_id u64, producer_epoch u16,
                      resources: array{resource_id u64, partitions array<u32>}
                   -> resources: array{resource_id u64, partitions: array{partition u32, error u16}}

AddAcksToTxn       <- txn_id string, producer_id u64, producer_epoch u16, queue_ids array<u64>
                   -> queues: array{queue_id u64, error u16}

EndTxn             <- txn_id string, producer_id u64, producer_epoch u16, commit u8
                   -> error u16
```

### Admin

Admin-APIs laufen über das native Protokoll, mit derselben Authentifizierung und Versionierung; `lotse-cli` nutzt sie direkt. Ein REST/JSON-Gateway für die Management-UI folgt nach dem MVP.

```
CreateStream    <- name string, partitions u32, replication u8, configs: array{key string, value string}
                -> error u16, resource_id u64
CreateQueue     <- name string, priority_levels u8,       // 1–10, je Stufe ein Sub-Log
                   single_active u8, dlq string?, configs: array{key string, value string}
                -> error u16, resource_id u64
DeclareExchange <- name string, kind u8,                  // DIRECT | TOPIC | FANOUT | HEADER
                   configs: array{key string, value string}
                -> error u16, resource_id u64
Bind            <- exchange_id u64, queue_id u64, binding_key string,
                   header_match: array{key string, value string}?
                -> error u16
DeleteResource  <- resource_id u64, if_empty u8 -> error u16
DescribeConfigs <- resource_ids array<u64>
                -> resources: array{resource_id u64, error u16,
                               configs: array{key string, value string, source u8}}
AlterConfigs    <- resource_id u64, set: array{key string, value string}, delete array<string>
                -> error u16
```

### Fehlercodes

| Code | Name | Retry | Bedeutung |
| --- | --- | --- | --- |
| 0 | `NONE` | – | Erfolg |
| 1 | `NOT_LEADER` | ja | Falscher Node; Leader-ID und Term als Tagged Field |
| 2 | `FENCED_LEADER_TERM` | ja | Client-Term veraltet; Metadata neu laden |
| 3 | `UNKNOWN_RESOURCE` | nein | ID existiert nicht (mehr) |
| 4 | `OFFSET_OUT_OF_RANGE` | nein | Offset vor Log-Start oder hinter HW |
| 5 | `NOT_ENOUGH_REPLICAS` | ja | Kein Quorum erreichbar |
| 6 | `REQUEST_TIMED_OUT` | ja | Quorum-Bestätigung nicht innerhalb `timeout_ms` |
| 7 | `OUT_OF_ORDER_SEQUENCE` | nein | Lücke in der Producer-Sequenz |
| 8 | `PRODUCER_FENCED` | nein | Neuere Producer-Epoch aktiv |
| 9 | `INVALID_TXN_STATE` | nein | Operation passt nicht zum Txn-Zustand |
| 10 | `QUOTA_EXCEEDED` | später | Speicherkontingent des Mandanten erreicht |
| 11 | `STORAGE_FULL` | später | Hard-Grenze des Nodes erreicht |
| 12 | `CORRUPT_BATCH` | nein | CRC oder Format ungültig |
| 13 | `UNAUTHORIZED` | nein | RBAC verweigert |
| 14 | `UNSUPPORTED_VERSION` | nein | API-Version außerhalb min/max |

## Weitere Entscheidungen

Der Broker heißt **Lotse**. Raft wird selbst implementiert und von Anfang an über deterministische Simulation getestet.

| Thema | Entscheidung | Folge |
| --- | --- | --- |
| Prioritäten | Eigenes Sub-Log (eigene Raft-Gruppe) pro Prioritätsstufe unter einer Queue-ID, höchste nicht leere Stufe zuerst | Transaktionale Acks können mehrere Sub-Logs umfassen; der Koordinator behandelt sie wie Partitionen |
| Single Active Consumer | Leader-lokal: erster Subscriber ist aktiv, Übernahme bei Disconnect oder Credit-Timeout | Vor der Übergabe bricht der Leader offene Ack-Transaktionen des alten Consumers ab |
| Rebalancing | Kooperativ, Zuweisung durch den Broker (KIP-848-Stil) | `GroupHeartbeat` ersetzt `JoinGroup`, `SyncGroup` und `Heartbeat` |
| Group-Koordinator | Interne Partitionen `__groups`, Gruppen per Hash verteilt; Leader = Koordinator | Offset-Commits sind Records und laufen über `AddOffsetsToTxn` in Transaktionen |
| Raft-Implementierung | Eigener Raft-Kern: Partition-Log = Raft-Log, Batching nativ | Mehr Eigenaufwand, passt exakt zum Storage-Design |
| Tests | Deterministische Simulation (I/O, Uhr, Netz, Scheduler hinter Traits), Jepsen-artige Tests vor 1.0 | Tokio nur an den Rändern; `turmoil` oder `madsim` als Bausteine prüfen |
| Berechtigungen | Lesezugriff pro Queue bzw. Stream, nie pro Record | `Deliver` bleibt zero-copy |
| Schemas | Reservierter Header für die Schema-ID; Registry nach dem MVP | Keine Broker-seitige Validierung im MVP |
| Disk-Schwellen | Soft bei 20 % frei, Hard bei 10 % frei, pro Node konfigurierbar | Reserve für Snapshots und Segment-Rewrites |
| Credit-Fenster | Standard 100 Nachrichten / 1 MiB, pro `Subscribe` überschreibbar | – |
| Tiered Storage | Nach dem MVP; Snapshot-Manifest bekommt jetzt schon einen Speicherort pro Segment | Leader lädt geschlossene Segmente nach S3, `Fetch` liest Remote-Bereiche bei Bedarf |
| Kubernetes-Operator | Drain vor Entfernen: Leaderships verschieben, Ersatzreplikat per Joint Consensus, erst dann Pod/PVC löschen; nie zwei Membership-Changes pro Gruppe gleichzeitig | Rolling Upgrades in place: Leaderships wegverschieben, Pod mit gleicher Node-ID und gleichem PVC neu starten, Aufholen abwarten, dann der nächste Node |

## Crate-Struktur

Die Kernlogik ist sans-I/O: Raft, Log-Zustand, Transaktionen und Queue-Semantik sind reine Zustandsmaschinen ohne Tokio-Abhängigkeit. Nur `lotse-server` treibt sie an, und zwar über die Traits aus `lotse-rt`. Dadurch läuft derselbe Server-Code produktiv auf Tokio und im Test in der deterministischen Simulation.

| Ebene | Crate | Aufgabe | Darf abhängen von |
| --- | --- | --- | --- |
| Fundament | `lotse-rt` | Traits `Clock`, `Disk`, `Net`, `Spawn`, `Rng` | – |
| Fundament | `lotse-record` | Batch-Header, varint-Records, CRC32C, zstd/lz4 | – |
| Fundament | `lotse-proto` | Frames, API-Keys, Request/Response-Typen, Fehlercodes | `record` |
| Kern | `lotse-log` | Segmente, Indizes, `.pstate`, Snapshot-Manifest, Recovery | `rt`, `record` |
| Kern | `lotse-raft` | Raft-Zustandsmaschine pro Gruppe, Multi-Raft-Batching pro Node-Paar | `record` |
| Kern | `lotse-txn` | Producer-State, Idempotenz, LSO, Koordinator-Zustand | `record` |
| Domäne | `lotse-queue` | Ack-Index (Roaring), Credits, DLQ, Timer-Wheel, Prioritäts-Sub-Logs, SAC | `record`, `txn` |
| Domäne | `lotse-stream` | Partitionen, Offsets, `__groups`, kooperative Zuweisung | `record`, `txn` |
| Domäne | `lotse-route` | Exchanges, Bindings, Routing-Policies | – |
| Domäne | `lotse-meta` | Metadaten-Zustand: Ressourcen, IDs, Leader, Quota-Nutzung | `raft` |
| Domäne | `lotse-auth` | mTLS-Principals, SASL, RBAC, Mandanten, Quotas | `proto` |
| Integration | `lotse-server` | Verbindungen, Dispatch, Raft-Treiber, Flow Control; generisch über `rt` | alle oben |
| Auslieferung | `lotse-broker` | Binary: Konfiguration, Tokio-Implementierung von `rt` | `server` |
| Auslieferung | `lotse-client` | Rust-SDK (Produce, Fetch, Publish, Subscribe, Txns) | `proto`, `record` |
| Auslieferung | `lotse-cli` | Admin-CLI | `client` |
| Test | `lotse-sim` | Simulations-`rt`, Cluster-Harness, Fault-Injection, Checker | `server`, `client` |

Adapter für AMQP 0-9-1 und Kafka kommen später als `lotse-amqp` und `lotse-kafka` hinzu. Sie hängen nur von `lotse-server` ab, nicht umgekehrt.

### Schnittstellen

```rust
// lotse-rt: die einzige Naht zur Außenwelt
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send;
}

pub trait Disk: Clone + Send + Sync + 'static {
    type File: SegmentFile;
    fn open(&self, path: &Path, mode: OpenMode) -> impl Future<Output = io::Result<Self::File>> + Send;
    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send;
    fn remove(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;
}

pub trait SegmentFile: Send + Sync + 'static {
    fn append(&self, buf: Bytes) -> impl Future<Output = io::Result<u64>> + Send;
    fn read_at(&self, pos: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;
    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send; // Sim: kann Daten "verlieren"
}

pub trait Net: Clone + Send + Sync + 'static { /* listen, connect, TLS-Stream */ }
pub trait Spawn: Clone + Send + Sync + 'static { /* spawn, JoinHandle */ }
pub trait Rng: Clone + Send + Sync + 'static { fn next_u64(&self) -> u64; }

pub trait Runtime: Clock + Disk + Net + Spawn + Rng {}
```

```rust
// lotse-raft: sans-I/O, vom Server getrieben
impl RaftGroup {
    pub fn new(cfg: Config, restored: Restored) -> Self; // HardState + Term-Runs + applied
    pub fn tick(&mut self);                              // logische Zeit; der Server tickt
                                                         // nach seiner Uhr aus lotse-rt
    pub fn step(&mut self, from: NodeId, msg: Message);  // eingehende Nachricht
    pub fn propose(&mut self, records: u64, data: Bytes)
        -> Result<(u64, u64), NotLeader>;                // belegte Indizes first..=last
    pub fn ready(&mut self) -> Ready;                    // in dieser Reihenfolge: HardState,
                                                         // truncate_from, Entries persistieren,
                                                         // committed anwenden, dann senden
    pub fn advance(&mut self);                           // letztes Ready ist durabel
}
// Ausgehende Appends tragen nur fill_from: der Server füllt die Entries
// aus seinem Log, damit der Kern keine Batches puffern muss.
// MultiRaft bündelt die Nachrichten aller Gruppen pro Zielknoten.
```

Der Server sammelt `Ready` aller Gruppen eines Nodes, schreibt die Entries gebündelt über `lotse-log` und verschickt die Nachrichten gebündelt pro Ziel-Node. Erst danach ruft er `advance` auf.

### Simulation

- `lotse-sim` implementiert `Runtime` mit einem Seed: virtuelle Uhr, Scheduler in einem Thread, Netz mit Verzögerung, Verlust, Duplikaten und Partitionen.
- Die Disk-Simulation kennt Torn Writes, verlorene Daten seit dem letzten `sync` und Bitflips, die der CRC erkennen muss.
- Checker prüfen nach jedem Lauf: kein bestätigter Write geht verloren, keine doppelt sichtbaren Transaktionen bei `read_committed`, keine Ack-Verluste in Queues.
- Ein fehlgeschlagener Lauf ist über den Seed exakt reproduzierbar.

## Management-Console

Die Console läuft als eigenes Binary `lotse-console` auf einem dedizierten Node, getrennt von den Brokern. Der Browser spricht nur mit `lotse-console`; `lotse-console` spricht das native Protokoll mit dem Cluster, immer mit der Identität des angemeldeten Benutzers. Die Mockups liegen in [`console/mockups/`](console/mockups/), Screenshots der umgesetzten Console in [`console/`](console/).

```
Browser (React + TS + Radix)
   │  REST/JSON (OpenAPI) + 1 WebSocket, OIDC-Session
   ▼
lotse-console  (Rust, axum, dedizierter Node)
   │  natives Protokoll über lotse-client, SASL OAUTHBEARER mit dem Token des Benutzers
   ▼
Cluster A, Cluster B, …  (Broker-Nodes)
```

| Thema | Entscheidung |
| --- | --- |
| Backend | `lotse-console` in Rust (axum): liefert die gebaute React-App aus und stellt REST + WebSocket bereit; nutzt `lotse-client` |
| Identität | Login per OIDC; das Token geht per SASL OAUTHBEARER an den Cluster. Broker-RBAC entscheidet, das Audit-Log zeigt den echten Benutzer, die Console hat selbst keine Admin-Rechte |
| Browser-API | REST/JSON, beschrieben mit OpenAPI (utoipa); der TypeScript-Client wird generiert (openapi-typescript). Das ist zugleich das geplante HTTP-Gateway |
| Live-Daten | Eine WebSocket-Verbindung mit Abos (Cluster-Übersicht, ein Stream, eine Queue). Die Console fragt pro Thema und Benutzer einmal beim Cluster nach und verteilt alle 2 s an alle Tabs dieses Benutzers. Weil jede Abfrage mit dem Token des Benutzers läuft, wächst die Last mit der Zahl der Benutzer pro Thema, nicht mit der Zahl der Tabs |
| Verlauf | Ringpuffer im Speicher, 24 h in 10-s-Schritten. Der Sampler läuft auch ohne angemeldete Benutzer und nutzt dafür als Einziges eine eigene, rein lesende Metrics-Identität der Console (Konfiguration: metrics.principal). Längerer Verlauf und Alerting bleiben in Prometheus/Grafana, aus der UI verlinkt |
| Betrieb | Ein Console-Node verwaltet mehrere Cluster laut Konfiguration (Cluster-Umschalter). Außer dem Puffer zustandslos; ein Neustart kostet nur Chart-Verlauf |

### Frontend

- React 18 + TypeScript, Radix Primitives, gestylt mit den Lotse-Tokens als CSS-Variablen (Hell und Dunkel).
- Radix-Zuordnung: Tabs (Stream-Bereiche), ToggleGroup (Zeitraum, Startpunkt), DropdownMenu (Cluster, Konto, Zeilenaktionen), Select (Tenant, Einheiten), Dialog (Queue anlegen), AlertDialog (Queue leeren), Switch (Committed only, Single active consumer), Progress (Upgrade), Tooltip, Toast, ScrollArea.
- Jede schreibende Aktion zeigt den gleichwertigen `lotse-cli`-Befehl, damit UI und CLI dieselben Operationen haben.

### Bildschirme

| Bildschirm | Zeigt | Wichtigste Aktionen |
| --- | --- | --- |
| Cluster-Übersicht | Durchsatz, p99-Commit-Latenz, Health, Nodes, auffällige Streams und Queues | Leader verschieben, zu Grafana |
| Stream | Partitionen mit Leader, Replikaten, HW und LSO; Consumer Groups mit Lag und kooperativer Zuweisung | Records lesen, Config ändern |
| Queue | Bereit, in Arbeit, in offenen Txns, Prioritätsstufen, Consumer mit Credits, SAC-Übergaben, Dead Letters | Requeue, Queue leeren |
| Message-Browser | Records eines Streams ab Offset oder Zeit, Txn-Status je Record, Header, Wert, Batch-Daten | Lesen ohne Consumer Group |
| Nodes | Rolling Upgrade Schritt für Schritt, Disk gegen Soft- und Hard-Grenze, Operator-Ereignisse | Upgrade pausieren, Leader verschieben |
| Tenants und Zugriff | Speicher gegen Quota je Tenant, Role Bindings je Principal | Quota ändern, Binding hinzufügen |

### Benutzergruppen

Gruppen kommen aus dem `groups`-Claim des Identity Providers und werden **im Broker** auf Rollen abgebildet, nicht in der Console. Da die Console mit der Identität des Nutzers handelt, gelten über `lotse-cli` dieselben Rechte; die Console blendet nur aus, was der Nutzer nicht darf. Metadaten sehen und Nachrichteninhalte lesen sind getrennte Berechtigungen.

| Gruppe | Bereich | Darf | Darf nicht |
| --- | --- | --- | --- |
| `lotse-platform-admins` | Cluster | Alles: Tenants, Quotas, ACLs, Knoten-Operationen, Upgrades | Nachrichteninhalte lesen, außer über eine Tenant-Gruppe |
| `lotse-operators` | Cluster | Alle Metadaten über Tenants hinweg; Leader verschieben, Knoten drainen, Upgrades, Throttling übersteuern; Break-Glass | ACLs oder Quotas ändern, Inhalte lesen, Daten löschen |
| `lotse-observers` | Cluster | Metadaten lesen: Übersicht, Knoten, Lag, Durchsatz | Jede Änderung, jeder Inhalt |
| `lotse-auditors` | Cluster | Principals, ACLs, Quotas und Audit-Log lesen | Daten und Operationen |
| `lotse-t-<tenant>-admin` | Tenant | Streams und Queues anlegen und löschen im Rahmen der Quota; eigene Principals und ACLs verwalten; Inhalte lesen | Eigene Quota erhöhen, andere Tenants |
| `lotse-t-<tenant>-developer` | Tenant | Streams und Queues anlegen und ändern, Offsets zurücksetzen, Dead-Letter-Queues requeuen oder purgen; Inhalte lesen außerhalb von Produktion | Streams löschen, ACLs verwalten, Inhalte in Produktion lesen |
| `lotse-t-<tenant>-support` | Tenant | Metadaten und Lag; Inhalte lesen, auch in Produktion; aus Dead-Letter-Queues requeuen | Purge, Offset-Reset, Anlegen oder Löschen |

Die Metrik-Identität (`metrics.principal`) ist ein Service-Account, keine Gruppe für Menschen.

- **Pro Cluster:** Dieselbe Gruppe kann je Cluster andere Rechte haben; Developer schreiben in Staging und lesen in Produktion nur Metadaten.
- **Inhalte in Produktion:** nur für `…-support` und `…-admin`, ohne Maskierung; jeder Zugriff auf Inhalte wird im Audit-Log festgehalten.
- **Destruktive Aktionen** (Purge, Offset-Reset, Löschen) sind eine eigene Berechtigung und verlangen in Produktion, den Ressourcennamen einzutippen.
- **Break-Glass ab v1:** Ein Operator erhebt sich mit Pflichtbegründung für höchstens 60 Minuten zum Admin. Das wird auditiert und als Banner auf der Übersicht gezeigt; danach fallen die Rechte von selbst zurück.
- **Audit:** Jeder Schreibzugriff wird festgehalten: wer, über welche Gruppe, in welchem Cluster, vorher und nachher.

**Rollen-Mapping:** Welche IdP-Gruppe welche Rolle bekommt, steht in den Cluster-Metadaten (`__meta`, per Raft repliziert) und wird über eine Admin-API geändert, aus `lotse-cli` und der Console. Damit gilt auf jedem Knoten dasselbe Mapping, und jede Änderung landet im Audit-Log. Die erste Admin-Gruppe legt `lotse-cli cluster init` an.

**Audit-Log:** Audit-Ereignisse gehen in den internen, replizierten Stream `__audit` mit eigener Retention. Jeder Eintrag enthält den Hash seines Vorgängers, sodass nachträgliche Änderungen auffallen. Ein Sink leitet den Stream an Syslog oder ein SIEM weiter. Auditoren lesen ihn in der Console.

**Kennzahlen:** Die Console holt ihre Kennzahlen über die Admin-API `DescribeMetrics`: authentifiziert, auf den Tenant beschränkt und per ACL geprüft. Zusätzlich bietet jeder Knoten einen Prometheus-Endpunkt für Grafana und Alerting; er ist für den Betrieb gedacht und nicht nach Tenants gefiltert.

### Crates und Ordner

- `crates/lotse-console`: axum-Server, OIDC, WebSocket-Fan-out, Ringpuffer; bettet die gebauten Web-Assets ins Binary ein.
- `console/web`: React-App; `npm run build` erzeugt die Assets, `npm run api` generiert den Client aus der OpenAPI-Datei von `lotse-console`.

## Offene Punkte

Die Designentscheidungen für Broker und Console sind getroffen. Offen für die Console:

- [x] Routing und Data-Fetching im Frontend: React Router, TanStack Query mit `openapi-fetch`
- [x] Welche Kennzahlen der Broker für die Console bereitstellt (Admin-API `DescribeMetrics` für die Console, Prometheus-Endpunkt je Node für den Betrieb)
- [ ] OIDC-Login (Authorization Code mit PKCE) und Weitergabe des Tokens per OAUTHBEARER; bis dahin nur `dev`-Modus
- [ ] Admin-APIs für Purge, Requeue und Leader verschieben, danach die Buttons in der UI
- [ ] Einklappbares Menü auf dem Telefon

Umgesetzt sind `lotse-rt`, `lotse-record`, `lotse-log`, `lotse-raft`, `lotse-sim` und `lotse-console` mit `console/web`. Der Raft-Kern ist vollständig: optionales Pre-Vote, immer aktives Check-Quorum mit Leader-Stickiness, No-Op beim Leaderwechsel, Snapshots, Learner, Joint Consensus, Leader-Transfer, ReadIndex und optionale Leases. Die Simulation (`lotse-sim/tests/raft.rs`) prüft ihn unter Nachrichtenverlust, Verzögerung, Duplikaten, Umordnung, Partitionen, Uhren-Drift und Abstürzen, während Knoten kompaktieren, Mitglieder wechseln, die Führung übergeben wird und Clients linearisierbar lesen; 20.000 Seeds laufen durch. Zusammen mit den Unit-Tests erkennt sie 18 eingebaute Fehler. Offen in Raft ist nur noch die Snapshot-Übertragung in Teilstücken, die mit dem Server kommt. Die Console läuft gegen einen eingebauten Demo-Cluster, bis `lotse-client` existiert. Der nächste Schritt ist `lotse-server` mit der Tokio-Implementierung von `lotse-rt`: Raft-Gruppen auf `lotse-log`, das native Protokoll und das Netz zwischen den Knoten.
