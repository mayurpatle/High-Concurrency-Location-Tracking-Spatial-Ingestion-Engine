# GeoPulse — Design Document

**High-Concurrency Location Tracking & Spatial Ingestion Engine**

> *Project 1 of 3 — the tracking/supply system behind a ride-hailing platform.*
> *(2 — Real-Time Distributed Matching · 3 — Dynamic Pricing & Rate Limiting Guard)*

**Status:** Phases 0–2 complete (ingestion + processing). Phases 3–7 pending.
**Last updated:** after Phase 2 · Session 3.

---

## Table of contents

1. [Problem & scope](#1-problem--scope)
2. [Requirements](#2-requirements)
3. [Capacity estimation](#3-capacity-estimation)
4. [Architecture](#4-architecture)
5. [Decision log](#5-decision-log) ← *the core of this document*
6. [Data model](#6-data-model)
7. [Failure modes](#7-failure-modes)
8. [Configuration reference](#8-configuration-reference)
9. [Build status](#9-build-status)
10. [Chaos engineering plan](#10-chaos-engineering-plan)
11. [Interview talking points](#11-interview-talking-points)

---

## 1. Problem & scope

### The one-line pitch
The part of a ride-hailing platform that watches millions of moving vehicles at once and always knows who's where — **without locking a database to death.**

### The reframe that drives everything
A CRUD app treats each driver as a **row you lock, read, and update.** At ~250,000 writes/second that model dies on **lock contention** long before it runs out of disk or CPU.

So this is **not a location database.** It's a location **pipeline**:

- absorb the write storm **asynchronously**
- index space **at the ingestion edge**
- let the **partitioning scheme — not row locks — guarantee correctness**

> ⭐ **The one idea:** *the partition is your lock.* Correctness under a firehose comes from making contention **structurally impossible**, not from coordinating access to shared state.

### Out of scope (deliberately)
- Driver↔rider matching → **Project 2**
- Surge pricing / rate limiting → **Project 3**
- Dead reckoning and heading-aware matching → consume *our* data, built in Project 2
- Authentication, driver onboarding, trip lifecycle

---

## 2. Requirements

### Functional
| # | Requirement |
|---|---|
| F1 | Ingest `{driverId, lat, lng, timestamp}` + optional `speed/heading/accuracy` from every active driver, continuously |
| F2 | Convert coordinates to a discrete spatial cell **at ingestion time** |
| F3 | Serve *"where is driver X now"* and *"which drivers are near this point now"* |
| F4 | Serve *"where has driver X been"* and *"who was in this area over a time window"* |

### Non-functional
| # | Requirement | Consequence |
|---|---|---|
| N1 | **Write-heavy, relentless** — writes arrive on a metronome, independent of user activity | Optimize the write path first, always |
| N2 | **Low ingestion latency** — accepting a ping must be fast | No blocking I/O on the request thread |
| N3 | **Horizontally scalable** — add nodes, never buy a bigger DB | Partition-based parallelism |
| N4 | ⭐ **Loss-tolerant on individual writes** — location data **self-heals**; a dropped ping is repaired by the next one in 4s | Licenses every availability-over-durability choice below |
| N5 | **Available over strongly consistent** — a 3-second-stale position is operationally fine | AP semantics on the hot store |

> ⭐ **N4 is the keystone requirement.** A payments ledger cannot drop a single write. Location can. That single property is what buys us the throughput — and it is the justification you should reach for whenever asked *"why is that safe?"*

### SLOs
| Metric | Target |
|---|---|
| Ingestion latency (request → `202`) | **p99 < 50 ms**, p50 < 10 ms |
| End-to-end freshness (ping → visible in Redis) | **p99 < 2 s** |
| Nearby-query latency | **p99 < 100 ms** |
| Sustained throughput (load test) | **10k writes/s** (design scales to 250k+) |
| Consumer lag | **Bounded** — growth is the alarm |

---

## 3. Capacity estimation

**Assumptions:** 1,000,000 peak concurrent drivers · 4-second ping interval · ~200 B wire / ~100 B stored.

```
Write throughput   1,000,000 ÷ 4 s              =  250,000 writes/sec
Ingress bandwidth  250,000 × 200 B              =  50 MB/s ≈ 400 Mbps
History/day        250,000 × 86,400 × 100 B     =  ~2.16 TB/day raw
                   × RF 3                       =  ~6.5 TB/day → multiple PB/year
Hot state          1,000,000 × ~200 B + index   =  ~500 MB – 1 GB
Read throughput    ~50,000–100,000 queries/sec  →  write:read ≈ 3:1 to 5:1
```

| Metric | Figure | What it forces |
|---|---|---|
| Write QPS | ~250k/s | **Async pipeline (Kafka); no synchronous DB writes** |
| Ingress | ~50 MB/s | Stateless ingestion tier behind a load balancer |
| History | ~2 TB/day | **Retention + downsampling + cold tiering are mandatory** |
| Hot state | **< 1 GB** | Fits one Redis node; ephemeral by design |
| Write : read | ~3:1 | **Write-heavy — inverted from typical CRUD** |

> ⭐ **The asymmetry that justifies two storage tiers:** the *same* events are **< 1 GB** as "current state" and **> 2 TB/day** as "history." One engine cannot be optimal for both.

**Scoping to reality:** we prove the architecture at **10k writes/s** on a laptop cluster (Phase 6, k6). The design scales to 250k+ by adding partitions and consumers — **nothing structural changes.**

---

## 4. Architecture

```
                    Driver fleet
                 (GPS ping every ~4s)
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  INGESTION SERVICE      (stateless) │
        │  validate → quality gate            │
        │  → H3 encode (res 9 + res 7)        │
        │  → Kafka publish → 202 Accepted     │
        └─────────────────────────────────────┘
                          │  key = H3 res-7 cell
                          ▼
        ┌─────────────────────────────────────┐
        │  KAFKA  driver-location-pings       │
        │  12 partitions (dev) / 64 (prod)    │
        │  6h retention · lz4 · acks=all      │
        └─────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐         ┌──────────────────┐
        │  PROCESSING SERVICE                 │  poison │  …-dlt            │
        │  batch consume → validate           ├────────►│  7-day retention  │
        │  → dedup (Redis SET NX)             │         └──────────────────┘
        │  → fan out to both stores           │
        └─────────────────────────────────────┘
                    │                 │
                    ▼                 ▼
        ┌────────────────┐   ┌────────────────────┐
        │  REDIS         │   │  CASSANDRA         │
        │  current loc   │   │  trajectory history│
        │  TTL presence  │   │  LSM append-optim. │
        │  H3 cell sets  │   │  partitioned by    │
        │  ~1 GB, AP     │   │  (driver, day) etc │
        └────────────────┘   └────────────────────┘
                    │                 │
                    └────────┬────────┘
                             ▼
                 ┌──────────────────────┐
                 │  QUERY SERVICE       │
                 │  nearby / presence   │  ← Redis  ("now")
                 │  trajectory / occup. │  ← Cassandra ("then")
                 └──────────────────────┘
```

### The read/write split
| Store | Answers | Latency | Why it fits |
|---|---|---|---|
| **Redis** | *"where is everything **right now**"* | single-digit ms | In-memory, ephemeral, spatially indexed. **TTL expiry IS the offline signal** — presence for free. |
| **Cassandra** | *"where was everything **before**"* | tens of ms, off hot path | LSM-tree **append-optimized** writes; partitioned per access pattern. |

**Time is the seam.** Neither query family crosses it.

> 🔸 **Gray zone:** smooth map animation wants the last ~30s of a path — more than "now," less than "history." Clean answer: a **capped list in Redis** per driver, so smoothing never touches Cassandra. Optional third micro-tier.

---

## 5. Decision log

> Format: **what we chose · over what · because · and what we gave up.**
> The tradeoff line is the important one — a decision without a stated cost is a decision you haven't finished making.

### 5.1 Architecture-level

**D-01 · Async Kafka pipeline over synchronous DB writes**
- **Context:** 250k writes/sec against a store that must also serve reads.
- **Because:** A synchronous `INSERT…WHERE id=?` dies on lock contention at this rate. Kafka acts as a **shock absorber** between an unbounded bursty write storm and downstream processing.
- **Tradeoff:** ⚠️ Eventual consistency. A ping is not durable at the moment we acknowledge it, and "current location" trails reality by ping latency + consumer lag (a few seconds). **Acceptable only because of N4.**

**D-02 · Two storage tiers over one general-purpose database**
- **Because:** The same events are **< 1 GB** as hot state and **> 2 TB/day** as history. Redis is optimal for in-memory ephemeral state; Cassandra's LSM tree is optimal for an append-heavy firehose. No single engine is good at both.
- **Tradeoff:** ⚠️ Two systems to operate, monitor, and keep consistent. Dual-write failure modes (one succeeds, one fails) must be reasoned about explicitly.

**D-03 · Kafka as transport, not system of record → 6-hour retention**
- **Over:** The 7-day default.
- **Because:** 4.3 TB/day of Kafka log × 7 days × RF 3 = **~90 TB** to hold data *already durably written to Cassandra*. 6h is enough to survive a consumer outage and replay.
- **Tradeoff:** ⚠️ A consumer down longer than 6 hours loses the backlog permanently. Mitigation: alert on lag long before that.

**D-04 · Module boundaries mirror deployment boundaries**
- **Because:** Ingestion, processing, and query scale independently (20 ingest pods vs 8 consumers). Shared code in `common`.
- **Iron rule:** ⭐ **no service depends on another service at compile time** — they communicate only via Kafka/Redis/HTTP.
- **Tradeoff:** ⚠️ More modules, more build complexity than a monolith.

### 5.2 Spatial indexing

**D-05 · Discrete spatial cells over bounding-box queries** ⭐
- **Because:** Two failures of `WHERE lat BETWEEN … AND lng BETWEEN …`: (a) B-tree indexes are **one-dimensional** — you narrow by lat then scan the band; (b) ⭐ **a bounding box cannot be sharded** — a box is a *range*, and ranges span shards. Our architecture needs a **scalar, hashable key**.
- **Note:** PostGIS/R-trees solve (a). **Neither solves (b)** — and (b) is what our capacity math demands.
- **Tradeoff:** ⚠️ Cells have **boundaries** — two drivers 10 m apart can land in different cells. Forces k-ring expansion on every proximity search.

**D-06 · H3 (hexagons) over S2 (squares)**
- **Because:** Our dominant query is **radius search**. A hexagon has **6 neighbours, all at one distance**; a square has **8 at two distances** (`d` and `d√2`), making "one ring out" reach 41% further diagonally. Hexagons also approximate circles better (less over-fetch) and produce no diagonal artifacts in heatmaps.
- **Tradeoff:** ⚠️ H3's hierarchy is **approximate** (a hexagon can't be perfectly tiled by smaller hexagons), and H3 IDs have **no spatial locality**. S2 offers exact containment and Hilbert-curve ordering enabling range scans — **which buys us nothing, because we shard by hash, and hashing destroys ID locality by design.**
- **Note:** ⚠️ Do not conflate this with D-05. *Discreteness* gives shardability (both libraries have it); *hexagons* give uniform adjacency.
- 🔹 You cannot tile a sphere with hexagons alone — H3 contains exactly **12 pentagons** per resolution, mostly over ocean.

**D-07 · Two resolutions: res 9 for storage, res 7 for partitioning** ⭐
- **Because:** One resolution cannot serve both jobs.
    - **Res 9** (~0.10 km², ~174 m edge) → ~200 drivers/cell in a dense city. A 2 km search needs `k=7` rings = `3k²+3k+1` = **169 cells = one pipelined Redis round trip**. *Res 9 minimizes (cells touched × drivers per cell)* — res 10 needs ~1,200 cells; res 8 fetches ~1,400 drivers/cell and discards most.
    - **Res 7** (~5.2 km², a neighbourhood) → the natural *"route this region's traffic to one consumer"* unit.
    - **Res 7 is res 9's grandparent** → derive with one `cellToParent` call. Compute once, get both, guaranteed consistent.
- **Tradeoff:** ⚠️ Coarser partition keys mean **coarser hot spots** — a res-7 cell covering a stadium concentrates 5 km² onto one partition. Accepted deliberately in exchange for locality; mitigated in Phase 5.2.

**D-08 · Enrich at the ingestion edge, not at query time** ⭐
- **Because:** (1) ⭐ **the partitioner needs the cell before the message is routed** — compute it downstream and you *cannot* partition by geography; (2) compute once, use as Kafka key + Redis key + Cassandra key; (3) write-heavy denormalization — microseconds of CPU per write turns every read into an O(1) lookup.
- **Tradeoff:** ⚠️ Resolutions become **constants, not config** — changing one invalidates every stored cell ID. It's a **data migration, not a tunable**.
- 🔹 The cell is an **index, not a replacement** for lat/lng. We keep exact coordinates for distance math, rendering, and replay.

### 5.3 Kafka: producer & topic

**D-09 · Partition key = H3 res-7 cell, not `driverId`** ⭐⭐
- **Because:** This is *the* decision the whole system rests on. Keying by cell means **every ping in a neighbourhood — from every driver — lands on one partition, owned by exactly one consumer.** That consumer is the **sole writer** for that region, so regional aggregates ("how many drivers here?") need **no distributed counter, no locks, no coordination.**
- **Over `driverId`:** which gives perfect load balance and clean per-driver ordering, but scatters a region across every partition, making regional aggregates a fan-out problem.
- **Tradeoff:** ⚠️ **The boundary-crossing race.** A driver moving from cell A to cell B produces pings on different partitions with **no ordering guarantee between them** — "current location" can briefly go backwards. Mitigated by **last-write-wins on device timestamp**.
- ⭐ **You cannot have both per-driver ordering and per-region locality from a single key.** Pick, then handle the fallout explicitly.

**D-10 · 12 partitions (dev) / 64 (prod)**
- **Derivation:** `250,000 target ÷ ~5,000 per-consumer (ESTIMATE) = 50 consumers → ≥ 50 partitions`. Rounded to **64** for headroom and factorability (8 consumers × 8 partitions each; 50 would give 6,6,6,6,6,6,7,7 and the slowest consumer sets your lag).
- **Dev = 12:** not derived — a **usability** choice. Enough to show distribution and run 2–3 consumers, few enough to eyeball in kafka-ui.
- **Tradeoff:** ⚠️ **Partition count is a one-way door** — you can raise it but never lower it, and raising it **rehashes the key space**, invalidating per-partition state. Over-provisioning costs file handles, slower rebalances, and **thinner batching** (batches accumulate per-partition).

**D-11 · `acks=all` + idempotent producer**
- **Initially chose `acks=1`; Kafka refused to start.** Idempotence tracks a **sequence number per producer per partition on the leader** — if a follower without that state is promoted, retries duplicate. **So idempotence requires `acks=all`.**
- **Revised reasoning:** without idempotence, `max.in.flight=5` lets a **retried message land after a later one**, reordering *within* a partition — destroying the exact guarantee D-09 exists to provide. **Losing intra-partition ordering to save a few ms is a bad trade.**
- **Tradeoff:** ⚠️ A replication round-trip per send — but `send()` is async and `linger.ms=10` amortizes it across hundreds of messages, so it stays off the critical latency path.

**D-12 · `linger.ms = 10` (batching)** ⭐
- **Because:** The default `linger.ms=0` ("send immediately") is a **throughput trap** — it pays a full network round-trip per message. Waiting 10 ms lets hundreds of messages travel together.
- ⭐ **A small, bounded delay increases throughput by an order of magnitude.** Batching amortizes fixed per-request costs.
- **Tradeoff:** ⚠️ 10 ms added latency — comfortably inside the 50 ms p99 budget.

**D-13 · `buffer.memory` as backpressure, not just a cap**
- **Because:** When the producer buffer fills, `send()` **blocks** rather than failing. This propagates slowness up to the HTTP layer.
- ⭐ **Backpressure that surfaces as latency is far better than backpressure that surfaces as data loss.**

**D-14 · Disable JSON type headers**
- **Because:** Spring Kafka ships the fully-qualified Java class name in `__TypeId__` by default, coupling producer and consumer to identical package structures and leaking internals to non-Java consumers.
- **Result:** plain JSON any language can consume. Consumers get an explicit target type instead.

### 5.4 Ingestion API

**D-15 · `202 Accepted`, not `200 OK`**
- **Because:** `200` claims "stored and durable" — **a lie**, since we've only handed the ping to Kafka. `202` honestly says *"I've taken responsibility; processing is in flight."*
- **Body is empty** — at 250k/s, **the status code IS the response**.

**D-16 · DTO separate from domain model**
- **Because:** The DTO is a contract with the **outside world** (untrusted, versioned by API); the domain model is a contract with **our own consumers** (validated, and also the Kafka wire format). ⚠️ Fusing them **welds your public API to your Kafka schema** — an internal field change breaks clients; an API version bump forces a schema migration.

**D-17 · `Double` (boxed) in DTOs, `double` (primitive) in the domain model** 🔥
- **Because:** A primitive `double lat` **silently defaults to `0.0`** when the field is *missing* from JSON — and **`0.0` is a valid latitude** (Gulf of Guinea). It passes range validation as a real location. **Silent data corruption, invisible in tests.** The wrapper stays `null` so `@NotNull` fires.
- **The choice flips downstream:** in the domain model, validation has already guaranteed presence, so `double` correctly encodes "always exists" — while `speed/heading/accuracy` stay boxed because **null is meaningful** ("not reported").
- ⚠️ **The mirror trap:** any boxed field that can legitimately be null must be null-checked before touching an arithmetic or comparison operator. *(We hit exactly this — an NPE from `request.accuracy().doubleValue()` on an absent field.)*

**D-18 · Quality gate: accept-and-discard, not reject**
- **Because:** A ping with a 500 m GPS error radius is **worse than no ping** — it would overwrite a good position with a vague one. But the client's JSON is **perfectly valid**, so a `400` would force a pointless retry. We return `202` and drop it.
- **Safe because of N4** — a better fix arrives in ~4 s.
- 🔹 Gate runs **before** enrichment: never spend CPU indexing a ping you're about to discard.

**D-19 · Immutable records as a concurrency strategy**
- **Because:** `LocationPing` is produced on a Tomcat thread, serialized, sent over the network, deserialized on a consumer thread, and read by two writers. Because it **cannot mutate**, none of those hand-offs need a lock or defensive copy. ⭐ **Immutability is a concurrency strategy, not a style preference.**

**D-20 · Virtual threads (Java 21)**
- **Because:** Tomcat's ~200 platform threads each map to an OS thread (~1 MB stack). Under a firehose, requests **queue waiting for a thread** and p99 detonates — *not because work is slow, but because threads are scarce.* Virtual threads cost a few hundred bytes and **unmount from their carrier OS thread when blocked on I/O**. Our endpoint is nearly pure I/O-bound waiting.
- **One config line, zero code changes** — no reactive rewrite.
- **Tradeoff:** ⚠️ They raise the **concurrency ceiling**, not speed. If the bottleneck is CPU, they change nothing. Known sharp edge: blocking inside `synchronized` used to **pin** the carrier thread (largely fixed in Java 24) → **prefer `ReentrantLock` on virtual-thread paths.**

**D-21 · Batch endpoint with all-or-nothing semantics**
- **Because:** HTTP overhead is paid *per request*; batching 50 pings amortizes it ~50× — the difference between 20 ingestion pods and 4. `@Size(max=100)` caps it (⚠️ **never trust a client-controlled collection size**).
- **Tradeoff:** ⚠️ One bad ping rejects all 50. **The alternative is `207 Multi-Status`** with a per-item report. All-or-nothing is right *here* because location self-heals. ⭐ **For payments, partial success would be mandatory** — *"which failure mode does the data's nature permit?"*

### 5.5 Consumer & processing

**D-22 · At-least-once (commit offsets after processing)** ⭐
- **Because:** ⭐ **There are only two orderings and no third option** — you cannot atomically commit a Kafka offset *and* write to Redis (two systems, no shared transaction).
    - Commit **before** processing → crash loses the message **forever**.
    - Commit **after** processing → crash **reprocesses** it.
- **We choose duplication over loss** because our writes are naturally idempotent.
- **Tradeoff:** ⚠️ Duplicates are guaranteed to happen. ⭐ **Rebalances are the most common source in practice** — far more than producer retries.

**D-23 · `CooperativeStickyAssignor`**
- **Because:** The default rebalance protocol is **stop-the-world** — all consumers revoke everything and lag climbs across the *entire* topic. A rolling deploy of 6 instances triggers 12 such pauses. Cooperative rebalancing revokes **only partitions that must move**.

**D-24 · Dedup on `driverId + timestamp`, stored in Redis** ⭐
- **Why dedup at all** (given "idempotent writes"): the Redis write *is* idempotent, but ⚠️ **the Cassandra write is not** — a duplicate appends a **second row for the same instant**, permanently corrupting trajectory and double-counting trip distance. Plus at 250k/s, 1% duplicates = 2,500 wasted writes/sec.
- **Why the business key, not offsets:** an offset identifies a *physical message*. It catches rebalance duplicates but **not** a client that retried its POST and produced two distinct Kafka messages.
- **Why Redis, not a `HashSet`:** ⭐ **a partition can move to another instance. In-memory dedup state doesn't move with it — so it fails exactly when you need it.**
- **Mechanism:** `SET key 1 NX EX 300` — ⭐ **one atomic round trip that both checks and claims.** A separate `exists()`-then-`set()` would race.
- **Tradeoff:** ⚠️ **The window is bounded, not absolute** — a duplicate arriving after 300 s slips through. TTL is what keeps it affordable (21.6 B keys/day without expiry vs ~75 M ≈ 5–7 GB with it).

> 🔹 **Does D-24 contradict "the partition is your lock"?** No. **Kafka's assignment** buys *no concurrent access* (hence no locks). **Shared storage** buys *continuity across reassignment*. Different problems. This is why regional aggregates can live in local variables **during a batch** but must be **flushed** to Redis.

**D-25 · Claim the dedup key BEFORE writing**
- **Because:** Same dilemma as offset commits. Claim-first risks **loss** (crash between claim and write suppresses the redelivery); write-first risks **duplication**. We accept rare loss, consistent with N4.
- 💡 **The airtight answer** is making the *write itself* idempotent via Cassandra's primary key (Phase 4.1) — dedup then becomes a **capacity optimization**, not a correctness mechanism.

**D-26 · Fail OPEN when Redis is unreachable**
- **Because:** Dedup unavailable → either drop everything (total outage) or process everything (possible duplicates). **A duplicate is a minor data-quality issue; dropping all pings is an outage.**
- ⭐ **Ask "what happens when this dependency is down?" for every dependency** — and make the answer a deliberate choice, not an accident of exception handling.

**D-27 · Deny-by-default error classification** ⭐
- **Because:** ⭐ **Enumerating what's PERMANENT is unbounded** (you can't predict every bug); **enumerating what's TRANSIENT is a short, knowable list** (timeouts, connection failures). An allow-by-default blocklist gives the **dangerous behaviour to everything you forgot.**
- An unanticipated exception is more likely a **code defect** than a network blip — quarantining surfaces the bug immediately.

**D-28 · Bounded retry: 1s → 2s → 4s, then DLT**
- **Not immediately:** hammering an overloaded dependency with instant retries makes it *more* overloaded — the **retry storm** that turns a degradation into an outage.
- **Bounded:** ⚠️ retries run *inside* the listener and **consume the `max.poll.interval.ms` budget**. Retry too long → evicted for not polling → **a transient blip becomes a rebalance storm**.
- ⭐ **A bounded failure is infinitely better than an unbounded one.**
- **Tradeoff:** ⚠️ In-listener retry **blocks that consumer thread**. At larger scale: Spring Kafka's non-blocking `@RetryableTopic`, which parks failures on delay topics.

**D-29 · Dead-letter topic with a dedicated `KafkaTemplate`** 🔥
- **The poison pill:** without this, a permanently-broken message fails → no commit → redelivered → fails → **forever.** That partition stops advancing and **every driver in that neighbourhood is stuck** — ⚠️ **while the consumer looks perfectly healthy** (polling, heartbeating, logging).
- 🔥 **The bug that cost three attempts:** `processing-service` had **no producer config**, so Spring Boot's default `StringSerializer` couldn't serialize either DLT payload shape (raw `byte[]` for deserialization failures, `LocationPing` for listener failures) → publish threw → **recoverer failed → record went back into retry.** Fixed with `DelegatingByTypeSerializer`.
- ⭐ **A recoverer that can fail turns your safety net into another retry loop.** The DLT path deserves the same dependency scrutiny as the main path.
- **DLT retention = 7 days** vs 6 hours on the main topic — ⭐ **retention here is sized to human response time, not machine throughput.** Config follows purpose.

**D-30 · Batch consumption** ⭐
- **The arithmetic:** 500 records × (Redis + Cassandra round trips) = **1,000 trips ≈ 1 second**. Batched: **~2 trips ≈ 20–40 ms**. ~25× fewer.
- ⭐ **You pay for the network TRIP, not the payload** — ~1 ms whether it carries 1 record or 500. Same principle as `linger.ms`, opposite end of the pipe.
- **Tradeoff:** ⚠️ **Failure granularity coarsens** — an exception fails the whole batch and offsets commit per batch, so **all 500 are redelivered**. Mitigated by `BatchListenerFailedException(msg, cause, index)`, which commits everything before the failing index and DLTs only that record. **This is what makes dedup matter more, not less.**

**D-31 · Backpressure comes free from the pull model** ⭐
- **Because:** The consumer **only fetches when ready**. If Cassandra slows, we poll less often and Kafka simply holds the messages. **No queue to overflow — Kafka's log IS the buffer**, already sized (6 h retention).
- **Compare push:** explicit flow control, buffering, and a drop policy required.
- **The one thing it can't fix:** ⚠️ the **poll deadline**. `max.poll.records × worst-case per-record time` must stay ≪ `max.poll.interval.ms`.

**D-32 · Count, don't log, on the hot path**
- **Because:** Logging is a **blocking disk write per record**; counting is an in-memory increment scraped periodically. At 250k/s, per-record logging is a **self-inflicted DoS** — and ⚠️ logging *validation failures* hands an attacker a free way to fill your disks.
- **Consequence:** the consumer is **silent on success**, so **consumer lag becomes the health signal**. This is precisely why observability isn't optional.

---

## 6. Data model

### `LocationPing` (Kafka message / domain model)
```java
record LocationPing(
    String driverId,
    double lat, double lng,     // primitives — validation guarantees presence
    long   timestamp,           // DEVICE clock (measurement time, not arrival)
    Double speed, heading, accuracy,   // boxed — null is MEANINGFUL
    String h3Cell,              // res 9 — storage/index key
    String h3PartitionCell      // res 7 — Kafka routing key
)
```

> **Why the device timestamp:** a phone may buffer pings while offline and flush later. The moment of **measurement** is what matters, not arrival. This also makes `driverId + timestamp` a natural idempotency key.

> **Why capture `speed`/`heading`/`accuracy` now, when nothing consumes them?** ⭐ **Ingestion is the one irreversible moment.** You can always add a consumer later; you can never collect last month's headings. Their uses: dead reckoning (smooth map animation between pings), heading-aware matching (a driver 200 m away heading *away* is worse than one 400 m away approaching), quality gating, and anomaly detection (300 km/h = GPS glitch or spoofed client).

> ⚠️ **Business data ≠ observability.** These fields describe **the driver** (→ Redis/Cassandra). Micrometer/OTel metrics describe **our system** (→ Prometheus/Grafana). Same word "telemetry," two universes.

### Key schemas
| Store | Key | Purpose |
|---|---|---|
| Kafka | `h3PartitionCell` (res 7) | Routing → single writer per region |
| Redis (dedup) | `dedup:ping:{driverId}:{timestamp}` | `SET NX EX 300` |
| Redis (current) | `driver:{id}:loc` — *Phase 3* | O(1) position lookup |
| Redis (spatial) | `cell:{h3Cell}` set — *Phase 3* | k-ring proximity search |
| Cassandra | `(driverId, day)` clustered by ts — *Phase 4* | Trajectory |
| Cassandra | `(h3Cell, time-bucket)` — *Phase 4* | Hex occupancy |

> Same data, **two Cassandra tables** — query-driven modeling: one schema per access pattern.

### API contract
| Method | Path | Status | Store |
|---|---|---|---|
| `POST` | `/v1/locations` | `202` / `400` | → Kafka |
| `POST` | `/v1/locations/batch` | `202` / `400` | → Kafka (max 100) |
| `GET` | `/v1/drivers/{id}/location` *(P3)* | `200` / **`404` = offline** | Redis |
| `GET` | `/v1/drivers/nearby` *(P3)* | `200` | Redis |
| `GET` | `/v1/cells/{h3}/count` *(P3)* | `200` | Redis |
| `GET` | `/v1/drivers/{id}/trajectory` *(P4)* | `200` | Cassandra |
| `GET` | `/v1/cells/{h3}/occupancy` *(P4)* | `200` | Cassandra |

> 🔹 **Presence for free:** the `404` isn't a special case — we store **no "offline" flag.** TTL expiry *is* the offline signal.

---

## 7. Failure modes

| Failure | Behaviour | Recovery | Data loss? |
|---|---|---|---|
| Ingestion pod dies | LB routes elsewhere (stateless) | Auto | In-flight requests only |
| Kafka publish fails after `202` | Logged (soon: counted) | **None** — response already sent | ⚠️ **Yes, that ping.** Licensed by N4. For payments → **transactional outbox** |
| Producer buffer full | `send()` **blocks** → backpressure to HTTP | Auto on drain | No |
| Consumer pod dies | Partitions reassigned | Auto (cooperative) | No — resumes from last commit |
| Consumer crashes mid-batch | Uncommitted records **redelivered** | Auto | No — but **duplicates** (→ dedup) |
| Poison message | 1 attempt → **DLT**, partition keeps moving | Manual: inspect, fix, replay | No — quarantined with metadata |
| Cassandra transient failure | Retry 1s → 2s → 4s → DLT | Auto | No, if it recovers in 7 s |
| Redis down (dedup) | **Fail open** — process anyway | Auto | No — possible duplicates |
| Slow consumer > poll deadline | **Evicted → rebalance** | Auto, ⚠️ but can cascade into a **rebalance storm** | No |
| Consumer down > 6 h | Backlog **expires** | ⚠️ **Manual — data lost** | ⚠️ Yes |
| Driver crosses cell boundary | Pings on different partitions, **no ordering** | Last-write-wins on timestamp | No — brief backwards jump |
| Hot cell (stadium) | One partition overloaded, lag grows | ⚠️ **Not yet handled — Phase 5.2** | No, but freshness degrades |

---

## 8. Configuration reference

### 🔬 Provenance — measured vs assumed
> ⭐ **When you write a number in a config, know whether it's measured, derived, defaulted, or guessed — and say which.** Most tuning-related incidents come from treating a guess as a measurement.

| Value | Source |
|---|---|
| `max.poll.interval.ms = 300000` | **Kafka default**, written out explicitly *for visibility* — a setting you can't see is one you won't think about |
| `session.timeout.ms = 45000`, `heartbeat.interval.ms = 3000` | Kafka defaults |
| `max.poll.records = 500` | Kafka default |
| `linger.ms = 10`, `batch.size = 64KB` | **Derived** from the 50 ms p99 budget |
| Res 9 / res 7 | **Derived** from driver density + k-ring math |
| 64 partitions | **Derived** from the estimate below |
| **~5 ms/record** | ⚠️ **ESTIMATE** — Redis (0.2–1 ms) + dedup (~0.5 ms) + Cassandra (1–5 ms) **sequential**. Batching (D-30) pushes it far lower. **To be measured in Phase 6.** |
| **~5,000 records/sec/consumer** | ⚠️ **ESTIMATE**, derived from the above — and the basis of the 64-partition figure |
| `dedup TTL = 300s` | **Chosen** — covers realistic redelivery paths with margin |
| `max-accuracy-metres = 100` | **Guess** — externalized because the right value is discovered in production |

### The poll-deadline arithmetic
```
time between polls = max.poll.records × time to process one record

healthy   500 × 5ms   = 2.5s    vs 300s   ✅ huge margin
batched   500 × ~60µs = 30ms    vs 300s   ✅ enormous margin
degraded  500 × 800ms = 400s    vs 300s   ❌ EVICTED → rebalance storm
```
> **Two timeouts, two questions:** heartbeat/session (background thread) = ***is the process alive?*** · poll interval = ***is it making progress?*** A consumer stuck in a slow write is **alive and heartbeating while making zero progress** — hence both checks.

### Version notes & traps
| Item | Note |
|---|---|
| **H3 v4** | 🔥 Renamed everything. `geoToH3`→`latLngToCellAddress`, `h3ToParent`→`cellToParentAddress`, `kRing`→`gridDisk`. Most tutorials online are v3 and won't compile. |
| **Kafka 4.x** | ZooKeeper **removed entirely** — KRaft is the only mode. |
| **Single-broker dev** | Must override internal-topic RF to 1, or the first consumer group start fails. |
| **Advertised listeners** | 🔥 Must be reachable *from where the client runs* → **two listeners** (containers use `kafka:29092`, host apps use `localhost:9092`). |
| **`host.docker.internal`** | The same trap reversed — a container reaching a host service. ⭐ *"localhost" is always relative to who's asking.* |
| **Cassandra `local-datacenter`** | Must match `CASSANDRA_DC` exactly, or "No node was available" at startup. |
| **DLT topic suffix** | ⚠️ Differs by Spring Kafka version (`.DLT` vs `-dlt`) — **pin it explicitly.** |
| **`auto-offset-reset`** | ⚠️ Applies **only when there is no committed offset**. It is *not* "where to resume after restart." |
| **PowerShell** | Mangles `-D` args. Quote the whole argument, or use `$env:VAR`. |

---

## 9. Build status

| Phase | Status | Delivered |
|---|---|---|
| **0 · Foundations** | ✅ | Capacity math, API contract, SLOs, Docker Compose stack, 4-module Maven project |
| **1 · Ingestion** | ✅ | Validated endpoint, `202`, quality gate, H3 dual-resolution enrichment, Kafka producer keyed by region |
| **2 · Processing** | ✅ | Consumer groups, at-least-once, dedup, error classification, DLT, batch consumption |
| **3 · Redis hot state** | ⬜ | Current location + TTL presence, H3 cell sets, k-ring nearby query |
| **4 · Cassandra history** | ⬜ | Query-driven schema, write-behind batching, trajectory + occupancy APIs |
| **5 · Coordination** | ⬜ | Consistent hashing, **hot-cell mitigation**, fencing tokens |
| **6 · Scale & observability** | ⬜ | Micrometer/Prometheus/Grafana, k6 load test, **replaces the estimates above** |
| **7 · Hardening** | ⬜ | Resilience, README, resume writeup |

### Current pipeline
```
HTTP → validate → quality gate → H3 enrich → Kafka (keyed by region)
     → batch consume → dedup → [writes pending]
```
Everything is built **except the writes themselves.** `toProcess` is assembled and dropped.

### Known gaps
- ⚠️ Hot-cell / hot-partition problem **unhandled** (Phase 5.2)
- ⚠️ No metrics — the consumer is silent, so lag in kafka-ui is the only signal (Phase 6.1)
- ⚠️ Per-record dedup round trip **not pipelined** — deliberate: *optimizing before measuring is how you end up with complicated code that's no faster*
- ⚠️ Throughput numbers are **estimates**, not measurements (Phase 6.2)
- ⚠️ No integration tests — only an HTTP-level Postman suite; downstream assertions are manual
- 🗑️ `SpatialDebugController` is temporary — delete in Phase 3

---

## 10. Chaos engineering plan

> **To be executed after Phase 7.** Listed now so the system is built with these experiments in mind.
> **The premise:** every row in §7 is a *claim*. Chaos engineering is how you find out which ones are true.

### Method
For each experiment: state the **hypothesis** first, define the **blast radius**, run it under **k6 load** (not idle), and record whether the system behaved as §7 predicts. ⭐ **An experiment that confirms what you already believed teaches you nothing — the value is in the surprises.**

### Planned experiments

**C-01 · Kill a consumer instance mid-load**
*Hypothesis:* partitions reassign within seconds, lag spikes then recovers, no data loss, some duplicates suppressed by dedup.
*Watch:* rebalance duration, lag curve, duplicate count.

**C-02 · Kill Redis**
*Hypothesis:* dedup **fails open** (D-26); ingestion continues; duplicates rise but nothing is dropped. Current-location reads fail.
*This directly tests the fail-open decision under real conditions.*

**C-03 · Kill Cassandra**
*Hypothesis:* writes fail → retried 1s/2s/4s → DLT. Redis hot path **unaffected** — "where is this driver now" keeps working while history is down.
*This tests whether the two tiers are genuinely independent.*

**C-04 · Slow Cassandra (inject 800 ms latency)** ⭐
*Hypothesis:* the **most interesting experiment.** Per §8, `500 × 800ms = 400s > 300s` → consumers evicted → **rebalance storm**.
*This is a predicted failure we have NOT mitigated. Does it actually cascade? Does lowering `max.poll.records` prevent it?*

**C-05 · Kill the Kafka broker**
*Hypothesis:* producer buffers, then `send()` blocks → backpressure surfaces as HTTP latency (D-13), not data loss. Recovers on broker return.

**C-06 · Hot cell simulation** ⭐
*Hypothesis:* concentrate 50% of load into one res-7 cell (a stadium). One partition saturates, its lag grows unboundedly while others idle.
*This is the known gap from §9 — the experiment should demonstrate exactly why Phase 5.2 exists.*

**C-07 · Network partition between consumer and Kafka**
*Hypothesis:* session timeout → eviction → reassignment. Tests split-brain behaviour and whether the single-writer guarantee holds across the partition.

**C-08 · Clock skew on the device timestamp**
*Hypothesis:* last-write-wins by timestamp misbehaves if a client's clock is wrong. Tests the D-09 mitigation's weak point.

**C-09 · DLT unavailable**
*Hypothesis:* per D-29 — the recoverer fails and records return to retry. Does the pipeline stall? *(We saw this accidentally; worth reproducing deliberately.)*

**C-10 · Rolling deploy under load**
*Hypothesis:* `CooperativeStickyAssignor` keeps most partitions processing; lag rises modestly rather than topic-wide.

### Tooling
`docker compose stop/start` for process kills · **Toxiproxy** or `tc netem` for latency/partition injection · **k6** for sustained load · **Grafana** for the lag/latency/throughput curves that make each experiment legible.

---

## 11. Interview talking points

The soundbites worth having ready:

1. **The reframe:** *"It's not a location database, it's a location pipeline. The partitioning scheme — not row locks — guarantees correctness."*
2. **The keystone:** *"We relaxed per-write durability because the high-frequency, self-healing nature of location data makes it safe — and that relaxation is exactly what buys us the throughput."* ⭐ *Reasoning from the data's properties to the system's guarantees.*
3. **The partition key:** *"Keying by H3 cell rather than driver ID means one consumer owns a whole region, so regional aggregates need no distributed counter. You can't have both per-driver ordering and per-region locality from one key — I chose locality and handle the boundary race with last-write-wins."*
4. **Two resolutions:** *"Res 9 minimizes cells-touched × drivers-per-cell for a 2 km search. Res 7 is its grandparent, so one computation yields both."*
5. **H3 vs S2:** *"S2's Hilbert ordering enables range scans, which buys us nothing because we shard by hash. H3's uniform adjacency directly serves our dominant query."*
6. **Durability posture:** *"`acks=all` with the idempotent producer for ordering and no-duplicate guarantees — the replication cost stays off the critical path because `send()` is async and batching amortizes it."*
7. **Batching:** *"A small bounded delay increases throughput by an order of magnitude. You pay for the network trip, not the payload."*
8. **At-least-once:** *"There are only two commit orderings and no third — you cannot atomically commit an offset and write to Redis. You're choosing your failure mode."*
9. **Error classification:** *"Deny by default. Enumerating what's permanent is unbounded; enumerating what's transient is a short list."*
10. **The poison pill:** *"One malformed message can take a region offline indefinitely while the consumer looks perfectly healthy. That's why per-partition lag is the metric that matters."*
11. **Capacity honesty:** *"I estimated ~5 ms per record from typical Redis and Cassandra latencies, then measured it under load. If the measured p99 differs, the partition math changes and I revisit it."*
12. **Non-technical version:** *"I'm building the part of Uber that quietly watches millions of moving cars at once and always knows who's where."*