# GeoPulse

**A high-concurrency location tracking and spatial ingestion engine.**

The part of a ride-hailing platform that watches millions of moving vehicles at once and always knows who's where — without locking a database to death.

Java 21 · Spring Boot 3.5 · Apache Kafka · Redis · Cassandra · Uber H3

---

## The problem

A million active drivers, each sending a GPS ping every 4 seconds:

```
1,000,000 ÷ 4s  =  250,000 writes/second, sustained
```

That number breaks the obvious design. A CRUD service treats each driver as a row you lock, read and update — and at 250k writes/second the **lock contention kills you long before disk or CPU does.** Meanwhile the read side needs to answer *"which drivers are within 2 km of this point?"* in under 100 ms, continuously.

Two more numbers shape everything:

| | |
|---|---|
| **Current position of every driver** | **< 1 GB** — one value per driver, ephemeral |
| **History of those same pings** | **~2 TB/day** — append-only, never updated |

Same events. Three orders of magnitude apart. **No single database is optimal for both.**

## The approach

GeoPulse is not a location *database*. It's a location *pipeline*:

- absorb the write storm **asynchronously**
- index space **at the ingestion edge**
- let the **partitioning scheme — not row locks — guarantee correctness**

> **The core idea: the partition is your lock.** Correctness under a firehose comes from making contention structurally impossible, not from coordinating access to shared state.

---

## Architecture

```
                         Driver fleet
                      (GPS ping every ~4s)
                              │
                              ▼
            ┌─────────────────────────────────────┐
            │  INGESTION SERVICE      (stateless) │
            │  validate → GPS quality gate        │
            │  → H3 encode (res 9 + res 7)        │
            │  → publish → 202 Accepted           │
            └─────────────────────────────────────┘
                              │  key = H3 res-7 cell
                              ▼
            ┌─────────────────────────────────────┐
            │  KAFKA  driver-location-pings       │
            │  partitioned by geography           │
            └─────────────────────────────────────┘
                     │                     │
        ┌────────────┘                     └────────────┐
        ▼                                               ▼
┌──────────────────────┐                   ┌──────────────────────┐
│ consumer group:      │                   │ consumer group:      │
│ location-processor   │                   │ history-writer       │
│ dedupe → hot state   │                   │ → durable history    │
└──────────────────────┘                   └──────────────────────┘
        │                                               │
        ▼                                               ▼
┌──────────────────────┐                   ┌──────────────────────┐
│  REDIS               │                   │  CASSANDRA           │
│  current position    │                   │  trajectory history  │
│  H3 cell index       │                   │  hex occupancy       │
│  TTL-based presence  │                   │  LSM append-optimal  │
└──────────────────────┘                   └──────────────────────┘
        │                                               │
        └───────────────────┬───────────────────────────┘
                            ▼
                ┌──────────────────────┐
                │  QUERY SERVICE       │
                │  "now"  ← Redis      │
                │  "then" ← Cassandra  │
                └──────────────────────┘
```

**Time is the seam.** Redis answers *where is everything right now*; Cassandra answers *where has everything been*. Neither query family crosses it.

The two consumer groups are the reason a Cassandra outage can't touch live tracking: each has its own offsets and its own lag, so the cold path falling behind is invisible to the hot path.

---

## Design decisions

### Spatial indexing: H3 hexagons, applied at the edge

`WHERE lat BETWEEN ... AND lng BETWEEN ...` fails twice. B-tree indexes are one-dimensional, so you narrow by latitude and then scan the band. And more fundamentally: **a bounding box is a range, and ranges can't be sharded.** Routing a ping to a deterministic partition needs a *scalar* key.

So coordinates are converted to an H3 cell ID the moment they arrive, at **two resolutions**:

| Resolution | Size | Used as |
|---|---|---|
| **res 9** | ~0.10 km², ~174 m edge | Redis spatial index, Cassandra partition key |
| **res 7** | ~5.2 km², a neighbourhood | **Kafka partition key** |

Res 7 is res 9's grandparent, so one computation yields both.

**Why hexagons over squares:** a hexagon has 6 neighbours all at the same distance; a square has 8 at two distances (`d` and `d√2`). Radius search — the dominant query — expands in uniform rings instead of a lopsided one.

**Why H3 over S2:** S2's Hilbert-curve ordering enables range scans, which buys nothing here because sharding is hash-based and hashing destroys ID locality by design. H3's uniform adjacency directly serves the query we actually run.

### Partitioning by geography, not by driver

The Kafka partition key is the res-7 cell. Every ping from a neighbourhood — *from every driver* — lands on one partition, owned by exactly one consumer.

That consumer is the **sole writer** for that region, which means regional state (driver counts, cell membership) needs no distributed counter, no lock, no coordination. The read-modify-write in the hot path runs unguarded and is correct *structurally*.

**The tradeoff, stated honestly:** you cannot have both per-driver ordering and per-region locality from a single key. Keying by cell means a driver crossing a cell boundary produces pings on different partitions with no ordering guarantee between them — handled with last-write-wins on the device timestamp.

### Two storage tiers with different physics

**Redis** holds current position (`Hash`), the spatial index (`Sorted Set` scored by timestamp), and presence.

Presence is free: keys carry a **30-second TTL**, refreshed on every ping. Key exists → online. Key gone → dark. No status column, no tombstones, no background job scanning a million rows for stale records.

**Cassandra** holds history in **two tables, one per query**, because it has no joins and no cross-partition aggregation:

| Table | Partition key | Answers |
|---|---|---|
| `driver_trajectory` | `(driver_id, day)` | Where has *this driver* been? |
| `cell_occupancy` | `(h3_cell, hour_bucket)` | Who was in *this cell*? |

Every partition key carries a **time bucket**, because all rows for one key live on one node and an unbounded partition is a time bomb. Bucket width is *derived per table* from how fast that key accumulates rows: trajectory collects from one driver (daily, ~21,600 rows); occupancy collects from a whole neighbourhood (hourly, ~180,000 rows). Both use `TimeWindowCompactionStrategy` and row-level TTLs.

### Throughput: batch the fixed cost

Every network round trip costs the same whether it carries 1 record or 500 — **you pay for the trip, not the payload.**

- **Kafka producer:** `linger.ms=10` — a bounded 10 ms delay that lets hundreds of messages travel together. Counterintuitively, a small delay *multiplies* throughput.
- **Redis:** batch writes go out as **one pipeline** instead of 500 round trips.
- **Cassandra:** ~25× fewer round trips per batch — but deliberately **not** via `BATCH`. A Cassandra batch routes through a single coordinator that then fans out to every partition anyway; the equivalent of pipelining there is **concurrency**, so writes are fired asynchronously and token-aware, straight to the owning replica.

### Correctness under redelivery

Kafka delivery is at-least-once — duplicates are a *when*, not an *if*, and rebalances are their most common source.

Two layers handle it:

- **Hot path:** deduplication via an atomic Redis `SET NX EX`, in a bounded 5-minute window that covers every realistic redelivery path.
- **History path:** *no dedup at all.* The Cassandra primary keys identify the logical event, so a redelivered ping is an upsert of identical data.

> **At-least-once delivery + an idempotent write = an exactly-once *effect*.** A well-chosen primary key replaced an entire subsystem.

### Failing honestly

When Redis is slow or unreachable, the read path returns **`503`, never an empty result or a `404`**.

This matters more than it looks. An empty `nearby` response tells a matching engine "this area has no supply" — a lie it would act on. A `404` on driver location says "definitely offline" — a lie about every driver in the fleet.

> **Never return a value that looks like a valid negative answer when you actually failed to answer.**

Timeouts bound every Redis call (a *slow* dependency is more dangerous than a dead one — it holds threads hostage while looking alive), and a circuit breaker stops calling entirely after repeated failures, which protects the recovering dependency as much as the caller.

---

## API

**Write path**

| | |
|---|---|
| `POST /v1/locations` | Single ping → `202 Accepted` |
| `POST /v1/locations/batch` | Up to 100 pings |

`202`, not `200` — at the moment we reply, the ping is in Kafka but not yet durable. `200` would be a lie about durability.

**Read path — "now" (Redis)**

| | |
|---|---|
| `GET /v1/drivers/nearby?lat=&lng=&radiusMetres=&limit=` | Distance-sorted drivers nearby |
| `GET /v1/drivers/{id}/location` | Current position — `404` means offline |
| `GET /v1/cells/{h3}/count` | Live driver density in a hexagon |

**Read path — "then" (Cassandra)**

| | |
|---|---|
| `GET /v1/drivers/{id}/trajectory?from=&to=&cursor=` | Route replay, cursor-paged, optional downsampling |
| `GET /v1/cells/{h3}/occupancy?from=&to=&cursor=` | Who passed through a hexagon, and when |

Note that `nearby` has **no offset pagination** while `trajectory` does have cursors. Live positions reorder every 4 seconds, so paging them would duplicate some rows and skip others — you cannot paginate a result set that reorders itself. More candidates means a *larger radius*, a different question. History is immutable, so cursor paging there is both coherent and necessary.

---

## Running it

```bash
docker compose up -d                       # Kafka, Redis, Cassandra, Prometheus, Grafana
docker compose exec -T cassandra cqlsh < cassandra/schema.cql

mvn clean install
mvn spring-boot:run -pl ingestion-service    # :8081
mvn spring-boot:run -pl processing-service   # :8082
mvn spring-boot:run -pl query-service        # :8083
```

A Postman collection with assertions covers every endpoint and edge case — including the spatial-index guarantees (same neighbourhood → same partition; k-ring cell counts) and the failure semantics above.

---

## Load testing

Numbers are measured under sustained load with **k6**, not estimated.

**Targets**

| Metric | Target |
|---|---|
| Sustained ingestion throughput | **10,000 writes/second** |
| Ingestion latency (request → `202`) | **p99 < 50 ms** |
| End-to-end freshness (ping → queryable) | **p99 < 2 s** |
| Nearby-query latency | **p99 < 100 ms** |
| Consumer lag | **bounded** — growth is the alarm |

The load generator simulates a realistic fleet: drivers moving along plausible paths rather than jumping randomly, with geographic clustering that mirrors real demand (dense in the centre, sparse at the edges). Uniform random coordinates would hide the exact hot-partition behaviour that matters.

**What the run is designed to expose**

- **Where the ceiling actually is** — ingestion, consumer processing, Redis, or Cassandra
- **Whether partition count is right** — the design assumes a per-consumer throughput figure, and the load test either confirms it or changes the partition math
- **Batch behaviour under pressure** — batches only fill when there's a queue, so the batching economics are invisible at low traffic and decisive at high
- **Whether consumer lag stays flat** — flat lag means we're keeping up; monotonically growing lag means freshness is degrading and will keep degrading

Micrometer → Prometheus → Grafana captures throughput, latency percentiles, per-partition consumer lag, and cell distribution during the run.

---

## Chaos engineering

Every claim about failure behaviour is a *hypothesis* until something is actually broken. Each experiment states its expectation first, defines a blast radius, and runs **under load** rather than against an idle system.

| Experiment | Hypothesis |
|---|---|
| **Kill a consumer instance** | Partitions reassign in seconds; lag spikes then recovers; no data loss; duplicates suppressed by dedup |
| **Stop Redis** | Dedup fails *open* — ingestion continues and nothing is dropped; read path returns `503`, never a false empty |
| **Stop Cassandra** | Live tracking is completely unaffected; history lag grows and drains on recovery; no manual replay |
| **Slow Cassandra (inject latency)** | The most interesting one — if per-record processing exceeds the poll budget, consumers get evicted and trigger a *rebalance storm*. A predicted failure that is deliberately not yet mitigated |
| **Kill the Kafka broker** | Producer buffers fill and `send()` blocks — backpressure surfaces as HTTP latency, not data loss |
| **Hot cell** | Concentrate load into one hexagon: one partition saturates while others idle, in *both* Kafka and Cassandra — the same failure mode at two layers of the stack, from one design decision |
| **Network partition** | Session timeout → eviction → reassignment; the single-writer guarantee holds across the partition |
| **Clock skew** | Last-write-wins depends on device timestamps; a wrong client clock is its weak point |
| **Dead-letter topic unavailable** | A recoverer that can fail turns the safety net into another retry loop |
| **Rolling deploy under load** | Cooperative rebalancing keeps most partitions processing; lag rises modestly rather than topic-wide |

Injection via `docker compose stop/start` for process kills and Toxiproxy for latency and partitions; k6 for sustained load; Grafana for the curves that make each experiment legible.

> An experiment that confirms what you already believed teaches you nothing. **The value is in the surprises.**

---

## Roadmap

This is the first of three services:

1. **GeoPulse** — location tracking and spatial ingestion *(this repository)*
2. **Matching** — real-time distributed driver↔rider matching, consuming `nearby`
3. **Pricing** — dynamic surge pricing and rate limiting, consuming cell density

Known gaps being worked on: hot-cell mitigation via salting and sub-partitioning, fencing tokens for the cases that genuinely need a distributed lock, and downsampling plus cold tiering for history beyond the retention window.