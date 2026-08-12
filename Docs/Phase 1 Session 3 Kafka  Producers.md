# Phase 1 · Session 3 — The Kafka Producer & Partitioning

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Make `h3PartitionCell` the Kafka partition key — the decision the entire *"the partition is your lock"* thesis rests on — and remove all blocking I/O from the request thread.
> **Status:** ✅ Verified in kafka-ui — pings from the same res-7 cell consistently land on the same partition, across multiple drivers.
> **Milestone:** 🏁 **Phase 1 complete.** Write path runs end to end.

---

## Part 1 — Topic design

### The ONE ordering guarantee Kafka gives you
**Messages with the same key → same partition → strictly ordered. Across partitions: no ordering at all. Ever.**

With `h3PartitionCell` as the key:
- All pings from one res-7 neighbourhood → one partition → strictly ordered, consumed by exactly **one** consumer.
- Pings from different neighbourhoods → no ordering relationship whatsoever.

> **Say this out loud in an interview:** *"We do not have global ordering, and we don't want it."* Global ordering requires a single partition → one consumer → a few thousand msgs/sec ceiling. We traded global ordering for parallelism, and location data permits it — a ping in Bandra has no causal relationship to a ping in Colaba.

### ⚠️ The boundary-crossing race (the honest limitation)
We key by **cell**, not by **driver**. So when a driver crosses a cell boundary:

```
Ping 1 (cell A) → partition 3
Ping 2 (cell B) → partition 11     ← different partition, NO ordering guarantee
```
Consumer 11 may process ping 2 before consumer 3 processes ping 1 → the driver's "current location" briefly goes **backwards**.

**Our fix:** last-write-wins by timestamp — the Redis write (Phase 3) compares the incoming ping's timestamp against the stored one and drops anything older. Every ping carries a device timestamp, so staleness is *detectable*. The window is milliseconds, and the data self-heals in 4 s.

> **The alternative:** key by `driverId` → perfect per-driver ordering, no boundary race — but **zero regional locality**. ⭐ **You cannot have both per-driver ordering and per-region locality from a single key.** Choosing which to sacrifice, and handling the fallout explicitly, *is* the design.

### Partition count — a one-way door
**You can increase partition count, never decrease it.** Worse: increasing it **rehashes the key space**, so a cell that mapped to partition 3 may now map to 17 — invalidating any per-partition state.

```
Target throughput:      250,000 msgs/sec
Per-consumer rate:      ~5,000 msgs/sec   (bounded by Redis+Cassandra writes, not the broker)
Consumers needed:       250,000 / 5,000 = 50
→ partitions ≥ 50, round up with useful factors →  64 (prod)
                                                    12 (dev)
```

- **Parallelism ceiling:** one partition = one consumer (per group). 12 partitions → a 13th consumer sits idle forever. **Partition count IS your max horizontal scale.**
- **Cost of over-provisioning** (why not 1000): open file handles + memory on every broker, slower rebalances (more metadata to reconcile), and **thinner batching** — batches accumulate per-partition, so more partitions = smaller, less efficient batches.
- **Requirement:** `distinct keys ≫ partition count`. ~200–400 active res-7 cells in a metro vs 64 partitions ✅

### Topic configuration
```
Topic:              driver-location-pings
Partitions:         12 (dev) / 64 (prod)
Replication factor: 1 (dev) / 3 (prod)
Retention:          6 hours          ← NOT the 7-day default
Compression:        lz4 (producer-side)
```

**Why 6h retention, not 7 days:** at 250k/s × 200 B we generate **~4.3 TB/day** of Kafka log. Seven days = 30 TB per replica, **90 TB at RF=3** — to hold data *already durably written to Cassandra*. ⭐ **Kafka here is a transport buffer, not a system of record.** 6 h is enough to survive a consumer outage and replay. Right-sizing retention to Kafka's actual role is a Staff-level instinct; plenty of teams pay for 7 days out of habit.

---

## Part 2 — Producer configuration

Every setting is a position on the durability↔throughput spectrum, justified by **our data's self-healing nature** and **our 50 ms p99 SLO** — not copied from a tutorial.

### 🔥 The `acks` + idempotence correction (learned the hard way)

**I initially specified `acks=1` + `enable.idempotence=true`. Kafka refuses to start:**
```
ConfigException: Must set acks to all in order to use the idempotent producer.
```

**Why they're mutually exclusive:** idempotence works by the broker tracking a **sequence number per producer, per partition**, rejecting anything it's seen. That dedup state lives **on the leader**. If the leader dies and a follower that hadn't replicated those sequence numbers takes over, the new leader doesn't know what it already accepted → a retry gets written twice → guarantee broken. **So idempotence requires `acks=all`.** Kafka refuses to start rather than let you believe in a guarantee you don't have.

**Resolution: `acks=all` + idempotence.** The reasoning that overturned the original `acks=1` call:
- Without idempotence, `max.in.flight.requests=5` means a **retried message can land after a later one** — reordering pings *within a partition*, destroying the exact guarantee our partition key exists to provide. **Losing intra-partition ordering to save a few ms is a bad trade.**
- It costs us nothing against the SLO: `send()` is async (the request thread never waits for the ack), and `linger.ms=10` amortizes the replication round-trip across hundreds of messages.
- In Kafka 3.0+, `enable.idempotence` **defaults to true** — so `acks=all` is effectively the modern default. The original config was fighting the framework.

> ⭐ **The correct way to state our durability posture:** *"We use `acks=all` with the idempotent producer for ordering and no-duplicate guarantees, and we accept the replication cost because our async send path and batching keep it off the critical latency path."*

### Scope of idempotence (a favourite interview trap)
**Idempotent producer prevents duplicates from PRODUCER RETRIES. It does NOT give exactly-once end-to-end.** A consumer that processes a message then crashes before committing its offset **will reprocess it**. That's consumer-side at-least-once — a separate problem, solved in Phase 2.2 via `driverId + timestamp` dedup.

### ⭐ Batching: where throughput actually comes from
`batch.size` (send when full) and `linger.ms` (send when this much time passes) — whichever fires first.

**The default `linger.ms=0` is a throughput trap.** "Send immediately" sounds fast but pays a full network round-trip per message.

We set **`linger.ms=10`**: a message may wait 10 ms to travel with companions. At 250k/s that's *hundreds* of messages per batch instead of one.

> **The counterintuitive rule: a small, bounded delay increases throughput by an order of magnitude.** Batching amortizes fixed per-request costs. The instinct to set `linger.ms=0` "for speed" makes the system *slower* under load.

### Backpressure, not just a memory cap
`buffer.memory: 64MB` — when full (Kafka can't absorb writes as fast as we produce), `send()` **blocks** rather than failing. Deliberate: it propagates slowness up to the HTTP layer instead of silently dropping pings or OOM-ing the JVM.
> ⭐ **Backpressure that surfaces as latency is far better than backpressure that surfaces as data loss.**

### Compression
`lz4` — ~3–5× on our repetitive small JSON at very low CPU (gzip compresses more but burns far more CPU; zstd is the modern middle ground). Applied **per batch**, so it composes with `linger.ms`: bigger batches compress better. One setting saves network **and** disk **and** broker memory.

### Serialization: turn OFF type headers
```yaml
spring.json.add.type.headers: false
```
By default Spring Kafka ships the **fully-qualified Java class name** in a `__TypeId__` header. Turning it off because:
1. It couples producer and consumer to identical package structures — rename a package and every consumer breaks.
2. It leaks internals to any non-Java consumer.

Result: **plain JSON any language can consume.** The consumer gets an explicit target type instead.

---

## Part 3 — The producer code

### `send()` is asynchronous — that's the point
It appends to an in-memory buffer and returns; a **background I/O thread** does the network work. The request thread never waits on Kafka. This is what makes the `202` both **honest** (durability unconfirmed) and **fast** (we didn't block to find out).

`KafkaTemplate` is thread-safe — one shared instance serves every request thread.

### ⚠️ The error path is genuinely lossy — state it plainly
By the time a send fails (after retries), we've **already returned `202`** — there's nobody left to inform. We log and move on.

Defensible **only** because location data self-heals (next ping in ~4 s). For a payment you'd need a **transactional outbox** — write the event to your DB in the same transaction as the business data, then relay it. ⭐ *Name that pattern in interviews; it's the correct answer when data does NOT self-heal.*

> The `whenComplete` callback runs on the **producer's I/O thread** — keep it cheap, never do real work there. And TODO(Phase 6): replace the log with a counter — if Kafka goes down, *every* send fails and per-failure logging becomes a self-inflicted DoS.

### 🏁 The milestone: `log.info` is gone
The placeholder log in `LocationIngestionService` is deleted. **The hot path now contains zero blocking I/O.** At 250k/s a disk write per request is exactly the blocking work this entire design exists to avoid.

Final pipeline: `quality gate → H3 enrichment → Kafka publish`. No database, no business logic, no synchronous service calls.

---

## Verification (confirmed in kafka-ui)

| Observation | Evidence | What it proves |
|---|---|---|
| **Same driver, same neighbourhood** | `driver-001` at 19.0596 and 19.0610 → both key `87608b0b1ffffff`, both **partition 2** | Different res-9 cells, identical res-7 routing key → same partition |
| **⭐ Different drivers, same neighbourhood** | `driver-006`, `-008`, `-010` (lat ~18.91–18.92) → all key `87608b030ffffff`, all **partition 8** | **The property that matters most** — one consumer sees *every* ping in a region, from *every* driver → lock-free regional aggregates |
| Different neighbourhood | `driver-005`, `-007` → key `…036ffffff` → **partition 4** | Distinct regions route independently |
| Adjacent cells ≠ adjacent partitions | Delhi `873da1140` → p1, `873da1143` → p8 | **Expected.** Hashing gives locality *within* a res-7 cell, not *across* neighbouring ones. We don't need the latter. |
| Distribution | Partitions 1, 2, 4, 6, 7, 8 in use from ~8 keys | Healthy spread, no single partition dominating |

> 🔍 **Reading an H3 key:** every res-7 key ends in a run of `f`s (`87608b0b1ffffff`). H3 IDs are fixed-width 64-bit with unused resolution digits filled with `1111` → the `f` padding literally says *"this cell stops at resolution 7."* Res-9 storage cells have fewer trailing `f`s. **The hierarchy is legible right in the hex.**

---

## Key takeaways (revision list)

1. **Same key → same partition → strict order. Across partitions → no order, ever.**
2. **No global ordering, deliberately** — it would cap us at one consumer.
3. **Boundary-crossing race is real** → last-write-wins by device timestamp.
4. **Can't have per-driver ordering AND per-region locality** from one key. Pick, then handle the fallout.
5. **Partition count is a one-way door** — increasing rehashes the key space.
6. **Partition count = max parallelism.** More isn't free: slower rebalances, thinner batches.
7. **Kafka is transport, not storage** → 6 h retention, not 7 days (4.3 TB/day!).
8. **Idempotence REQUIRES `acks=all`** — the broker's dedup state lives on the leader.
9. **Idempotent producer ≠ exactly-once end-to-end.** Consumer redelivery is a separate problem (Phase 2.2).
10. **`linger.ms=0` is a throughput trap.** A small bounded delay = order-of-magnitude gain.
11. **Backpressure as latency > backpressure as data loss.**
12. **Turn off JSON type headers** — don't couple your wire format to Java package names.
13. **Async `send()` is what makes `202` both honest and fast.**
14. **Lossy error path is a choice licensed by self-healing data.** Otherwise: transactional outbox.

---

## 🏁 Phase 1 complete
The write path runs end to end: **HTTP → validate → quality gate → H3 enrich → Kafka**, keyed by geography, with **zero blocking I/O on the request thread**.

## Next → Phase 2 · Session 1 — Consumer Groups & Parallelism
Build the consumer that picks these messages up. Consumer groups, the partition-to-consumer assignment that makes single-writer-per-region real, offset management, and what actually happens during a rebalance.