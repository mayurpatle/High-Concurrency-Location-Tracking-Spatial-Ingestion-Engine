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

> **The one idea:** *the partition is your lock.* Correctness under a firehose comes from making contention **structurally impossible**, not from coordinating access to shared state.

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
| N4 | **Loss-tolerant on individual writes** — location data **self-heals**; a dropped ping is repaired by the next one in 4s | Licenses every availability-over-durability choice below |
| N5 | **Available over strongly consistent** — a 3-second-stale position is operationally fine | AP semantics on the hot store |

> **N4 is the keystone requirement.** A payments ledger cannot drop a single write. Location can. That single property is what buys us the throughput — and it is the justification you should reach for whenever asked *"why is that safe?"*

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

>**The asymmetry that justifies two storage tiers:** the *same* events are **< 1 GB** as "current state" and **> 2 TB/day** as "history." One engine cannot be optimal for both.

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
- **Iron rule:** **no service depends on another service at compile time** — they communicate only via Kafka/Redis/HTTP.
- **Tradeoff:** ⚠️ More modules, more build complexity than a monolith.

### 5.2 Spatial indexing

**D-05 · Discrete spatial cells over bounding-box queries** 
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

**D-08 · Enrich at the ingestion edge, not at query time** 
- **Because:** (1) **the partitioner needs the cell before the message is routed** — compute it downstream and you *cannot* partition by geography; (2) compute once, use as Kafka key + Redis key + Cassandra key; (3) write-heavy denormalization — microseconds of CPU per write turns every read into an O(1) lookup.
- **Tradeoff:** ⚠️ Resolutions become **constants, not config** — changing one invalidates every stored cell ID. It's a **data migration, not a tunable**.
- 🔹 The cell is an **index, not a replacement** for lat/lng. We keep exact coordinates for distance math, rendering, and replay.

### 5.3 Kafka: producer & topic

**D-09 · Partition key = H3 res-7 cell, not `driverId`**
- **Because:** This is *the* decision the whole system rests on. Keying by cell means **every ping in a neighbourhood — from every driver — lands on one partition, owned by exactly one consumer.** That consumer is the **sole writer** for that region, so regional aggregates ("how many drivers here?") need **no distributed counter, no locks, no coordination.**
- **Over `driverId`:** which gives perfect load balance and clean per-driver ordering, but scatters a region across every partition, making regional aggregates a fan-out problem.
- **Tradeoff:** ⚠️ **The boundary-crossing race.** A driver moving from cell A to cell B produces pings on different partitions with **no ordering guarantee between them** — "current location" can briefly go backwards. Mitigated by **last-write-wins on device timestamp**.
- **You cannot have both per-driver ordering and per-region locality from a single key.** Pick, then handle the fallout explicitly.

**D-10 · 12 partitions (dev) / 64 (prod)**
- **Derivation:** `250,000 target ÷ ~5,000 per-consumer (ESTIMATE) = 50 consumers → ≥ 50 partitions`. Rounded to **64** for headroom and factorability (8 consumers × 8 partitions each; 50 would give 6,6,6,6,6,6,7,7 and the slowest consumer sets your lag).
- **Dev = 12:** not derived — a **usability** choice. Enough to show distribution and run 2–3 consumers, few enough to eyeball in kafka-ui.
- **Tradeoff:** ⚠️ **Partition count is a one-way door** — you can raise it but never lower it, and raising it **rehashes the key space**, invalidating per-partition state. Over-provisioning costs file handles, slower rebalances, and **thinner batching** (batches accumulate per-partition).

**D-11 · `acks=all` + idempotent producer**
- **Initially chose `acks=1`; Kafka refused to start.** Idempotence tracks a **sequence number per producer per partition on the leader** — if a follower without that state is promoted, retries duplicate. **So idempotence requires `acks=all`.**
- **Revised reasoning:** without idempotence, `max.in.flight=5` lets a **retried message land after a later one**, reordering *within* a partition — destroying the exact guarantee D-09 exists to provide. **Losing intra-partition ordering to save a few ms is a bad trade.**
- **Tradeoff:** ⚠️ A replication round-trip per send — but `send()` is async and `linger.ms=10` amortizes it across hundreds of messages, so it stays off the critical latency path.

**D-12 · `linger.ms = 10` (batching)** 
- **Because:** The default `linger.ms=0` ("send immediately") is a **throughput trap** — it pays a full network round-trip per message. Waiting 10 ms lets hundreds of messages travel together.
- **A small, bounded delay increases throughput by an order of magnitude.** Batching amortizes fixed per-request costs.
- **Tradeoff:** ⚠️ 10 ms added latency — comfortably inside the 50 ms p99 budget.

**D-13 · `buffer.memory` as backpressure, not just a cap**
- **Because:** When the producer buffer fills, `send()` **blocks** rather than failing. This propagates slowness up to the HTTP layer.
- **Backpressure that surfaces as latency is far better than backpressure that surfaces as data loss.**

**D-14 · Disable JSON type headers**
- **Because:** Spring Kafka ships the fully-qualified Java class name in `__TypeId__` by default, coupling producer and consumer to identical package structures and leaking internals to non-Java consumers.
- **Result:** plain JSON any language can consume. Consumers get an explicit target type instead.

### 5.4 Ingestion API

**D-15 · `202 Accepted`, not `200 OK`**
- **Because:** `200` claims "stored and durable" — **a lie**, since we've only handed the ping to Kafka. `202` honestly says *"I've taken responsibility; processing is in flight."*
- **Body is empty** — at 250k/s, **the status code IS the response**.

**D-16 · DTO separate from domain model**
- **Because:** The DTO is a contract with the **outside world** (untrusted, versioned by API); the domain model is a contract with **our own consumers** (validated, and also the Kafka wire format). ⚠️ Fusing them **welds your public API to your Kafka schema** — an internal field change breaks clients; an API version bump forces a schema migration.

**D-17 · `Double` (boxed) in DTOs, `double` (primitive) in the domain model**
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

**D-22 · At-least-once (commit offsets after processing)** 
- **Because:** **There are only two orderings and no third option** — you cannot atomically commit a Kafka offset *and* write to Redis (two systems, no shared transaction).
    - Commit **before** processing → crash loses the message **forever**.
    - Commit **after** processing → crash **reprocesses** it.
- **We choose duplication over loss** because our writes are naturally idempotent.
- **Tradeoff:** ⚠️ Duplicates are guaranteed to happen. **Rebalances are the most common source in practice** — far more than producer retries.

**D-23 · `CooperativeStickyAssignor`**
- **Because:** The default rebalance protocol is **stop-the-world** — all consumers revoke everything and lag climbs across the *entire* topic. A rolling deploy of 6 instances triggers 12 such pauses. Cooperative rebalancing revokes **only partitions that must move**.

**D-24 · Dedup on `driverId + timestamp`, stored in Redis** 
- **Why dedup at all** (given "idempotent writes"): the Redis write *is* idempotent, but ⚠️ **the Cassandra write is not** — a duplicate appends a **second row for the same instant**, permanently corrupting trajectory and double-counting trip distance. Plus at 250k/s, 1% duplicates = 2,500 wasted writes/sec.
- **Why the business key, not offsets:** an offset identifies a *physical message*. It catches rebalance duplicates but **not** a client that retried its POST and produced two distinct Kafka messages.
- **Why Redis, not a `HashSet`:** **a partition can move to another instance. In-memory dedup state doesn't move with it — so it fails exactly when you need it.**
- **Mechanism:** `SET key 1 NX EX 300` — **one atomic round trip that both checks and claims.** A separate `exists()`-then-`set()` would race.
- **Tradeoff:** ⚠️ **The window is bounded, not absolute** — a duplicate arriving after 300 s slips through. TTL is what keeps it affordable (21.6 B keys/day without expiry vs ~75 M ≈ 5–7 GB with it).

> 🔹 **Does D-24 contradict "the partition is your lock"?** No. **Kafka's assignment** buys *no concurrent access* (hence no locks). **Shared storage** buys *continuity across reassignment*. Different problems. This is why regional aggregates can live in local variables **during a batch** but must be **flushed** to Redis.

**D-25 · Claim the dedup key BEFORE writing**
- **Because:** Same dilemma as offset commits. Claim-first risks **loss** (crash between claim and write suppresses the redelivery); write-first risks **duplication**. We accept rare loss, consistent with N4.
- **The airtight answer** is making the *write itself* idempotent via Cassandra's primary key (Phase 4.1) — dedup then becomes a **capacity optimization**, not a correctness mechanism.

**D-26 · Fail OPEN when Redis is unreachable**
- **Because:** Dedup unavailable → either drop everything (total outage) or process everything (possible duplicates). **A duplicate is a minor data-quality issue; dropping all pings is an outage.**
- **Ask "what happens when this dependency is down?" for every dependency** — and make the answer a deliberate choice, not an accident of exception handling.

**D-27 · Deny-by-default error classification**
- **Because:** ⭐ **Enumerating what's PERMANENT is unbounded** (you can't predict every bug); **enumerating what's TRANSIENT is a short, knowable list** (timeouts, connection failures). An allow-by-default blocklist gives the **dangerous behaviour to everything you forgot.**
- An unanticipated exception is more likely a **code defect** than a network blip — quarantining surfaces the bug immediately.

**D-28 · Bounded retry: 1s → 2s → 4s, then DLT**
- **Not immediately:** hammering an overloaded dependency with instant retries makes it *more* overloaded — the **retry storm** that turns a degradation into an outage.
- **Bounded:** ⚠️ retries run *inside* the listener and **consume the `max.poll.interval.ms` budget**. Retry too long → evicted for not polling → **a transient blip becomes a rebalance storm**.
- **A bounded failure is infinitely better than an unbounded one.**
- **Tradeoff:** ⚠️ In-listener retry **blocks that consumer thread**. At larger scale: Spring Kafka's non-blocking `@RetryableTopic`, which parks failures on delay topics.

**D-29 · Dead-letter topic with a dedicated `KafkaTemplate`** 
- **The poison pill:** without this, a permanently-broken message fails → no commit → redelivered → fails → **forever.** That partition stops advancing and **every driver in that neighbourhood is stuck** — ⚠️ **while the consumer looks perfectly healthy** (polling, heartbeating, logging).
- 🔥 **The bug that cost three attempts:** `processing-service` had **no producer config**, so Spring Boot's default `StringSerializer` couldn't serialize either DLT payload shape (raw `byte[]` for deserialization failures, `LocationPing` for listener failures) → publish threw → **recoverer failed → record went back into retry.** Fixed with `DelegatingByTypeSerializer`.
- ⭐ **A recoverer that can fail turns your safety net into another retry loop.** The DLT path deserves the same dependency scrutiny as the main path.
- **DLT retention = 7 days** vs 6 hours on the main topic — **retention here is sized to human response time, not machine throughput.** Config follows purpose.

**D-30 · Batch consumption** 
- **The arithmetic:** 500 records × (Redis + Cassandra round trips) = **1,000 trips ≈ 1 second**. Batched: **~2 trips ≈ 20–40 ms**. ~25× fewer.
- **You pay for the network TRIP, not the payload** — ~1 ms whether it carries 1 record or 500. Same principle as `linger.ms`, opposite end of the pipe.
- **Tradeoff:** ⚠️ **Failure granularity coarsens** — an exception fails the whole batch and offsets commit per batch, so **all 500 are redelivered**. Mitigated by `BatchListenerFailedException(msg, cause, index)`, which commits everything before the failing index and DLTs only that record. **This is what makes dedup matter more, not less.**

**D-31 · Backpressure comes free from the pull model**
- **Because:** The consumer **only fetches when ready**. If Cassandra slows, we poll less often and Kafka simply holds the messages. **No queue to overflow — Kafka's log IS the buffer**, already sized (6 h retention).
- **Compare push:** explicit flow control, buffering, and a drop policy required.
- **The one thing it can't fix:** ⚠️ the **poll deadline**. `max.poll.records × worst-case per-record time` must stay ≪ `max.poll.interval.ms`.

**D-32 · Count, don't log, on the hot path**
- **Because:** Logging is a **blocking disk write per record**; counting is an in-memory increment scraped periodically. At 250k/s, per-record logging is a **self-inflicted DoS** — and ⚠️ logging *validation failures* hands an attacker a free way to fill your disks.
- **Consequence:** the consumer is **silent on success**, so **consumer lag becomes the health signal**. This is precisely why observability isn't optional.

### 5.6 Redis Hot State 

**D-33 · Hash over JSON String for current location**

- **Because**: Redis stores small hashes in a compact listpack encoding, meaningfully more memory-efficient than a serialized JSON string at a million keys. Also avoids a serialization round trip on read.
- **Note**: the deciding factor is memory, not field-level access — we always write the whole object anyway.
- **Tradeoff**: ⚠️ Slightly more code than SET/GET of a JSON blob; field names are stored per-key (mitigated by the listpack encoding).

**D-34 · Sorted Set over Set for the cell index**

- **Because:** ⚠️ A whole-key TTL on cell:{h3} would expire the entire set, evicting drivers who are still actively pinging. Redis Sets have no per-member expiry. A Sorted Set with score = last-seen timestamp gives per-member freshness, and lets reads filter stale members via ZRANGEBYSCORE before any sweep runs.
Reads are correct immediately; cleanup is only housekeeping.
- **Over:** Redis 7.4+ hash-field TTLs (HEXPIRE) — genuinely per-field, but newer and less portable.
- **Tradeoff:** ⚠️ Requires an explicit sweep (ZREMRANGEBYSCORE), and the opportunistic sweep never reaches cells that go completely silent — memory leaks slowly while reads stay correct. → Phase 5.

**D-35 · TTL as presence — no online flag, no tombstones**

- **Because:** Key exists → pinged recently → online. Key gone → dark. This eliminates an is_online column, a background job scanning a million rows for stale timestamps, and the race where a crashed driver app stays "online" until the sweeper catches up. One EXPIRE per write rep- laces all of it.
- **Why 30 s:** pings arrive every 4 s → tolerates ~7 consecutive misses. Enough to survive a tunnel, short enough that a dead driver disappears fast.
- **Tradeoff: ⚠**️ A direct false-offline vs ghost-driver dial. Too short and a driver in a tunnel vanishes; too long and dead drivers linger in proximity results.

**D-36 · Read-then-write in two pipelines, no Lua script, no lock**

- **Context:** The move logic is a read-modify-write on shared state (read previous cell → SREM old → ZADD new) — the canonical case for a mutex.
- **Because:** Pings are keyed by H3 res-7 cell → one partition → one consumer thread owns every driver in a region. The interleaving a lock would prevent cannot occur. Also: a pipeline sends all commands then collects all replies, so you cannot branch on a reply mid-pipeline — hence read trip, then write trip.
- **Over a Lua script:** atomic server-side, correct under any concurrency, but you're writing Lua and per-driver invocation loses batching.
- **Tradeoff: ⚠**️ Correctness now depends on D-09 holding. Change the partition key and this silently becomes a data race. This dependency should be stated in the code comment — and it is.
- **Interview line:** "We didn't need a Lua script or a distributed lock because the partitioning guarantees a single writer per region."

**D-37 · Stale-write guard: last-write-wins on device timestamp**

- **Because:** The D-09 boundary race means an older ping can arrive after a newer one. Without the guard, a driver's map marker jumps backwards — ⚠️ maddening to reproduce, since it only fires when someone crosses a partition boundary at the wrong moment.
>= not >: an equal timestamp is a duplicate that slipped past the dedup window; rewriting it is harmless but pointless.
Tradeoff: ⚠️ Trusts the

**D-38 · H3 cell sets over Redis GEOSEARCH**

- The alternative: GEOADD + GEOSEARCH is two commands. No k-rings, no cell bookkeeping, no move logic. Returns drivers sorted by true distance. Genuinely less code.
- What GEO actually is: a Sorted Set whose score is a geohash (52-bit lat/lng interleaving). GEOSEARCH computes covering geohash ranges, range-scans, then filters by distance.
- Because: it's one Sorted Set — singular. Every driver in the coverage area lives in one key, and a Redis key lives on one node. In Redis Cluster a single key is atomic and indivisible, so ⚠️ it cannot shard: one machine's read throughput, forever, and adding nodes does nothing. Our cell:{h3} keys distribute across hash slots naturally — different neighbourhoods hit different nodes.
- Same principle as D-05: a range can't be sharded, a discrete key can. Different layer, identical reasoning.
- Three supporting reasons: (1) we already carry cell IDs for Kafka and Cassandra keys — GEOADD would be a second spatial index to keep consistent; (2) ⭐ the cell is a unit of aggregation, so density is ZCOUNT at O(log n) whereas GEO needs a radius-search-and-count; (3) cell membership is stable and cacheable, while "within 2 km of an arbitrary point" is unique per query.
- Tradeoff: ⚠️ GEO is simpler and more accurate. It computes true haversine natively; we over-include and must filter in Java (D-39). All of D-33–D-37's complexity — move logic, SREM/ZADD, ghost drivers — disappears with GEO. ⭐ For a single-node deployment, GEO is probably the better choice — its one weakness never bites if you never shard.
- The hybrid worth naming: H3 sets for sharding and aggregation plus a per-cell GEO index for precise intra-cell distance. Over-engineering here, but knowing it exists is a good signal.

**D-39 · True-distance filter after the k-ring lookup**

- Because: k-rings over-include — a hexagon disk covering a 2 km radius is not a circle; corners of edge cells stick out. Without the filter you'd return drivers 2.4 km away for a 2 km query.
- Cells are the COARSE filter (cheap candidate lookup); true distance is the FINE filter.
- Tradeoff: ⚠️ Work moves from Redis into our JVM — we fetch candidates we then discard. Bounded by the radius, so acceptable; it's the direct cost of D-38.

**D-40 · ZRANGEBYSCORE with a freshness floor, not ZRANGE**

- Because: stale members are filtered at read time, so a driver who stopped pinging never appears — even though the opportunistic sweep hasn't removed them. ⭐ Reads are correct immediately; cleanup is only housekeeping.
- Tradeoff: ⚠️ The reader's floor must match the writer's presence TTL. If the floor is longer, you return drivers whose position hash has already expired — the index says they're there, the position is gone.

**D-41 · Sort by distance in Java, not Redis**

- Because: Redis can sort by score, but our score is a timestamp, not a distance — and there's no way to sort by distance-from-an-arbitrary-point without a GEO index. The candidate set is bounded by the radius, so the sort is cheap.

**D-42 · Hard ceiling on client-controlled radius (@Max(10000))**

- Because: a 50 km request means k = ceil(50000/300) = 167 rings → 3(167²)+3(167)+1 ≈ **84,000 cells in one pipeline**. That's a client-controlled amount of work — the same vulnerability class as the uncapped batch size (D-21).
- Any parameter that scales your workload needs a ceiling.

**D-43 · Query service has no Kafka dependency**

- Because: reads are bursty and user-driven; writes are a relentless metronome. They scale independently, so they're separate deployments. The absence is the module boundary (D-04) made visible — even in the health endpoint, which shows Redis but no Kafka component.

**D-44 · Cache only cellCount — not nearby, not location**

- The rule: cache what is expensive-or-repeated AND tolerates staleness — not "cache reads."
- Not nearby/location: already two round trips to an in-memory store (~2–5 ms). A cache saves milliseconds and costs staleness on data whose entire value is freshness, on top of the 2 s pipeline lag we already carry.
- Yes cellCount: many callers ask the same question repeatedly (Project 3's pricing engine polls hundreds of cells) and the answer tolerates staleness — a 5-second-old supply count is fine for "is this area busy?"
- Tradeoff: ⚠️ Per-instance cache. Three query pods = three independent caches, so a client can see different counts depending on routing. Fine for a density figure. Cluster-wide consistency would mean caching in Redis — but that's caching Redis in Redis, which only pays when the backing computation is expensive, not a single ZCOUNT.

**D-45 · Short TTL over stale-while-revalidate (the thundering-herd call)**

- The herd: a popular entry expires and ⚠️ 500 concurrent requests all miss at the same instant. The cache didn't reduce load — it SYNCHRONIZED it into a spike.
- Three known mitigations: single-flight locking · probabilistic early expiry ("XFetch") · stale-while-revalidate.
- Chose: plain expireAfterWrite(5s). Caffeine's refreshAfterWrite requires a CacheLoader, which CaffeineCacheManager doesn't supply for @Cacheable methods — wiring one needs an ObjectProvider to dodge the circular dependency (the cached service depends on the cache manager).
- Because: stale-while-revalidate earns its complexity when the backing call is expensive — a Cassandra aggregation, an external API. Here it's a single ZCOUNT against memory. Paying that machinery to shield Redis from a burst of O(log n) lookups is optimizing the wrong thing.
- Tradeoff: ⚠️ We keep the herd exposure. At our scale the herd stampedes a ZCOUNT, which Redis absorbs trivially — an accepted risk, not an unnoticed one.
- 🔹 maximumSize(10_000): an unbounded cache is a memory leak with good PR.

**D-46 · Redis timeouts on the read path**

- Because: a slow dependency is more dangerous than a dead one. If Redis hangs, query threads block; enough of them and the whole service is unresponsive — including endpoints that never touch Redis, like /actuator/health. A dead dependency fails fast and you handle it; a slow one holds your resources hostage while looking alive.
- Values: timeout: 200ms, connect-timeout: 500ms, pool max-active: 16, max-wait: 100ms. 200 ms is generous — our p99 for the whole query is 100 ms, so a single call taking 200 ms already means something is wrong.
- Never make an unbounded network call.

**D-47 · Circuit breaker on Redis reads**

- Because: after repeated failures, stop calling entirely and fail instantly. It protects the dependency as much as us — without it, our retries become a self-inflicted DDoS on a recovering Redis.
- Config: 20-call window, 50% failure threshold, minimum 10 calls (don't trip on a tiny sample), 10 s open, 3 half-open trials.
- Tradeoff: ⚠️ With @CircuitBreaker outside @Cacheable, cache hits register as successes in the breaker's window, slightly diluting the failure rate. Acceptable at this scale; worth knowing it's there.

**D-48 · Fail loudly: 503, never a valid-looking negative**

- Because: the fallback is a product decision, not a technical one, and it differs per endpoint:

- | `GET` |    `/drivers/nearby` | `503` |  An empty list is a lie a matching engine acts on — and Project 3 reads it as zero supply, potentially triggering surge |
- | `GET`|  `/drivers/{id}/location` | `503, not 404` | 404 means "definitely offline." ⚠️ We don't know that — we failed to look. During an outage that's a lie about every driver in the fleet |
- | `GET` |  `/cells/{h3}/count` | -1, not 0 | 0 reads as "no supply" — exactly the wrong signal. -1 means unknown |
- Never return a value that looks like a valid negative answer when you actually failed to answer. Failing loudly beats lying quietly.
- Retry-After: 10 matches the breaker's open window so well-behaved clients back off instead of hammering us.

**D-49 · No offset pagination — radius expansion instead**

- Because: offset pagination assumes a stable ordering between requests. Our drivers move every four seconds — by the time page 2 is requested the distance ordering has changed, so you'd get duplicates on some pages and silently skip others. ⭐ You cannot paginate a result set that reorders itself.
- Instead: a caller needing more candidates re-queries with a larger radius — semantically a different question with a naturally superset answer. That's why the API exposes radiusMetres, not offset.
- Tradeoff: ⚠️ No way to walk a large result set incrementally. A caller wanting 400 drivers must fetch all 400 at once (bounded by limit ≤ 500 and the radius cap).

**D-50 · Return metadata, not a bare array**

- Because: a bare list can't distinguish "searched and found nothing" from "found 400 and gave you 50." NearbyResponse carries totalFound, truncated, radiusMetres, cellsScanned, originCell.
- Capture totalFound before truncating — after subList() the information is gone.
- An empty result still returns full metadata, so the caller knows what radius and how many cells were actually searched.

**D-51 · Cassandra for history (LSM-tree over B-tree)**

- Because: the capacity math (~2.16 TB/day raw, PB/year) rules out a single node — but the deeper fit is the write pattern. An LSM write appends to a memtable + commit log and returns: no seek, no read-before-write, no in-place update. A B-tree update must locate the page, read, modify, and write back — random I/O. Our workload is enormous write volume, no updates ever, and reads that are always "contiguous time range for one key." ⭐ For a firehose of appends, LSM is simply the right shape.
- 🔹 Corollary: INSERT and UPDATE are the same operation — a new cell with a timestamp, latest wins at read. No existence check, so writes are naturally idempotent when the primary key identifies the logical event (exploited in 4.2).
- Tradeoff: ⚠️ No joins, no cross-partition aggregation, no query planner to rescue a badly-shaped query. Reads are only efficient along the paths you designed for.

**D-52 · Query-driven modelling: one table per query**

- Because: Cassandra inverts relational modelling — start from the queries, not the entities. A table not shaped for a query makes that query impossible (or ALLOW FILTERING, i.e. a cluster scan).
- Denormalization is correct here: every ping is written to two tables. Safe because the data is immutable — a past location never changes, so the copies cannot diverge. No update anomaly without updates.
- General rule: duplicate freely when the data is immutable; be very careful when it isn't. (Applies equally to caches, materialized views, CQRS read models.)
- Tradeoff: ⚠️ Write volume doubles — ~4.3 TB/day instead of 2.16. The alternative (one table + app-side filtering) means scanning partitions you don't need, which is far worse at read time and doesn't scale.

**D-53 · Every partition key carries a time bucket 🔥**

- Because: all rows for one partition key live on one node, so an unbounded partition is unbounded data on one machine. Guideline: < ~100 MB, ideally < 100,000 rows. The "obvious" PRIMARY KEY (driver_id, ts) grows forever — ~1M rows/108 MB after 50 days, ~7.9M rows/790 MB after a year. ⚠️ A partition key with no natural bound is a time bomb that detonates months after you ship.
- Tradeoff: ⚠️ A multi-bucket query touches N partitions instead of 1. One large unbounded read traded for several small bounded ones — the right trade, since the small ones are predictable and run in parallel.

**D-54 · Bucket width derived per table from row accumulation rate**

| Table | Partition key | Row rate per key | Bucket | Rows/partition |
|---|---|---|---|---:|
| `driver_trajectory` | `(driver_id, day)` | 1 driver × 900/hr | daily | ~21,600 |
| `cell_occupancy` | `(h3_cell, hour_bucket)` | ~200 drivers × 900/hr | hourly | ~180,000 |

- Because: occupancy's key collects from a crowd, not one source — a daily bucket would hold 4.3 million rows. Same technique, ~8× the concentration, so the bucket narrows to land in the same safe range.
- Bucket width is not a project-wide convention — it's derived per table from how fast that table's partition key accumulates rows.

**D-55 · Composite partition key syntax — the double parentheses**

- PRIMARY KEY ((driver_id, day), ts) — the inner parens make (driver_id, day) the partition key.
- ⚠️ PRIMARY KEY (driver_id, day, ts) is valid CQL that compiles, runs, and slowly destroys your cluster — it partitions by driver alone, silently recreating the unbounded partition D-53 exists to prevent.

**D-56 · driver_id as a clustering column in cell_occupancy**

- Because: two drivers pinging in the same millisecond share (partition, ts). An insert with an existing primary key is an upsert — one would silently overwrite the other, with no error, warning, or log line. In a busy cell that happens constantly.
- Clustering columns must collectively guarantee uniqueness. In a relational store a duplicate key throws; here it succeeds and eats your data.

**D-57 · CLUSTERING ORDER BY (ts DESC)**

- Physical sort order should match your dominant read direction.
- Because: rows are physically stored in clustering order. "Recent history" is the dominant read, and reading from the front of a partition beats seeking into it.

**D-58 · TimeWindowCompactionStrategy over the default SizeTiered**

- Because: STCS merges SSTables by size, repeatedly rewriting old immutable data for no benefit. TWCS groups SSTables by time window and stops compacting a window once it closes — far less write amplification. Combined with TTL, expired data drops as whole SSTables instead of scattering tombstones.
- Windows: 1 day for trajectory, 6 hours for occupancy — matched to each table's bucket and volume.
- Tradeoff: TWCS windows by each cell's write timestamp (when Cassandra received it), not by our ts column — so backfilled or late-arriving events land in the current window and compaction is unaffected. ⭐ The real cost is on reads: one day's partition can end up spread across SSTables from several windows, so a read touches more files.

**D-59 · TTL as the retention policy: 30 days / 7 days**

- Because: keeping raw 4-second pings forever is impossible (~6.5 TB/day with RF 3). A row-level default_time_to_live makes expiry automatic, and TWCS makes it cheap.
- Different per table: trajectory (trip history, disputes) keeps 30 days; occupancy (analytics, higher volume) keeps 7 days. Retention is set per table, by purpose.
- Tradeoff: ⚠️ Raw history older than 30 days is gone. Production would downsample (1 point/minute) and tier to S3 before expiry.

**D-60 · NetworkTopologyStrategy from day one, even with one DC**

- Because: SimpleStrategy ignores racks and DCs, so all replicas can land in one rack — one rack failure loses the data. Migrating Simple → NTS on a live cluster is a risky manual procedure.
- ⚠️ RF=1 is dev-only. Production: 'datacenter1': 3 to survive a node loss and read at QUORUM.

**D-61 · Multi-bucket reads as parallel single-partition queries, not IN**

- Because: IN (day1, day2, …) routes through a single coordinator that fans out and gathers — an anti-pattern at scale. The app issues one query per bucket, in parallel, and merges.
- ⭐ You always know exactly how many partitions a query touches — which is precisely the predictability that lets Cassandra scale.

**D-62 · A second consumer group for history, not a second write in the same listener ** 

- Because: one listener doing both writes fails three ways. (1) Freshness coupling — offsets commit only after the listener returns, so a Cassandra latency spike stalls the Redis writes queued behind it; ⭐ the cold path's worst day becomes the hot path's every day. (2) Failure coupling — Cassandra down means the batch is retried and dead-lettered, and the Redis writes in it stop too, re-welding the tiers D-02 separated. (3) The dedup trap — with claim-before-write (D-25), a Cassandra failure means the redelivery is suppressed by the dedup key, so every ping during the outage silently loses its history row. That's the classic dual-write problem: two systems, no shared transaction, a partial success indistinguishable from a full one.
- Chose: same topic, different group.id → own copy of every message, own offsets, own lag. Kafka already retains 6h, so fan-out costs nothing new.
- Kafka turns write-behind from a data-loss risk into a lag metric. A classic in-memory write-behind queue loses everything on a crash; ours is a replicated, replayable log.
- 🔹 D-03's retention gains a concrete meaning: 6 hours is now the longest Cassandra outage we can absorb without losing history.
- Tradeoff: ⚠️ Kafka read traffic doubles (cheap — page cache — but real); the stores can momentarily disagree; two lag metrics with different alarm thresholds; and for now both groups share a JVM, so a process crash or a boot-time Cassandra outage still takes the hot path down. TODO(Phase 7).

**D-63 · No dedup on the history path **

- Because: both primary keys identify the logical event, so a redelivered ping is an upsert of identical data. ⭐ At-least-once delivery + idempotent write = exactly-once effect — no dedup, no coordination, no Redis in the loop. This is the airtight answer D-25 pointed toward.
- 🔧 Corrects D-24's stated rationale. Session 2.2 justified dedup partly by claiming a duplicate Cassandra write "appends a second row for the same instant." True for a schema keyed on an ingestion UUID; false for the schema designed in 4.1. ⭐ A well-chosen primary key can do the job of an entire subsystem.
- 🔹 Open for Phase 6: hot-path dedup may be partly redundant too (the Redis write is idempotent via ZADD + the stale-write guard), except for a duplicate arriving after a driver's hash expired.

**D-64 · Concurrent async writes, never a Cassandra BATCH**

- Because: a BATCH goes to one coordinator; a logged batch first writes the whole thing to a batchlog on two other nodes, then applies each statement to its partition's replicas. Our pings span hundreds of partitions — so the coordinator holds every mutation in memory, does extra writes, and fans out to nearly every node anyway. Cassandra warns above 5 KB and rejects above 50 KB; a 500-ping two-table batch is ~100 KB.
- The unifying principle: batching amortizes a fixed per-request cost paid to ONE destination. Redis pipeline → one server. Kafka producer batch → one partition leader. Many destinations → a batch is a middleman who still makes all the trips. ⭐ The Cassandra equivalent of pipelining is concurrency — total latency ≈ the slowest single write, not the sum.
- When BATCH is right: an unlogged batch within one partition, or a logged batch for atomicity across denormalized tables — textbook-wise, exactly our two-table write. We skip it because ⭐ Kafka's redelivery already does the batchlog's job: if one table succeeds and the other fails, the batch is redelivered and both are rewritten harmlessly.
- Tradeoff: ⚠️ A brief window where one table has a ping and the other doesn't. Acceptable for history and analytics; not for anything read transactionally.

**D-65 · Prepared statements, prepared once at startup**

- Because: beyond parse caching, the prepared metadata is what enables token-aware routing — the driver hashes the partition key client-side and sends the write directly to a replica that owns it, with no extra coordinator hop. An unprepared string hides the partition key, so the driver picks an arbitrary coordinator that forwards.
- Tradeoff: ⚠️ Preparing at construction means Cassandra must be reachable at boot — see the startup-coupling caveat below.

**D-66 · LOCAL_QUORUM for history writes**

- Because: ⚠️ the self-healing argument (N4) is weaker here. A lost hot-path ping is repaired in 4 seconds; a lost history point is a permanent gap — the next ping doesn't fill in the past. With RF=3, LOCAL_QUORUM writes + LOCAL_QUORUM reads give R + W = 4 > 3 → read-your-writes. LOCAL_ keeps the quorum in-datacenter.
- Decisions compose: D-62 moved history off the critical path, so its latency no longer touches freshness — decoupling is what makes stronger durability cheap.
- 🔹 setIdempotence(true) because we proved repeatability — it licenses driver-level retry on another node and speculative execution, neither of which the driver will do otherwise.

**D-67 · Optional fields left UNSET, never null**

- Because: ⚠️ in Cassandra a null is a TOMBSTONE — a deletion marker every future read must scan past. Reads warn above 1,000 and fail above 100,000. A driver that never reports heading plants 21,600 tombstones/day in its partition; with all three optional fields absent, ~65,000/day — edging toward a query that fails outright.
- 🔹 Same instinct as the Redis writer (never write the string "null"), but here a null costs you reads forever.

**D-68 · Bucket functions are a shared contract in common**

- Because: day and hour_bucket are part of the primary key, so every writer and reader must compute them identically, on any machine, forever. Two failure modes:
- ⚠️ Timezone: this JVM runs -Duser.timezone=Asia/Kolkata. A default-zone bucket computes IST days locally and UTC days on a server — the reader looks in the wrong partition. India's half-hour offset misaligns even hourly buckets. Use UTC explicitly.
- ⚠️ Wall clock: bucket from the device timestamp, never arrival. Bucketing by arrival means a redelivery crossing midnight computes a different partition key — turning the idempotent upsert into a second copy in another partition, silently breaking D-63.
-  A bucket function must be a pure function of the event.

**D-69 · A dead-letter topic per consumer group**

- Because: a message can be poison for one consumer and fine for another (history chokes on a field the hot path never reads). A shared DLT mixes both groups' failures, and replaying it re-feeds messages to a group that already handled them.
- A DLT belongs to a consumer group, not to a topic.

**D-70 · Patient error policy on the cold path (30 min total, 60 s per wait)**

- Because: the two paths should react to failure differently. A hot-path ping that can't be written in seconds is stale and useless → quarantine fast. The cold path has Kafka holding the data for 6 hours → a transient outage should become lag, never quarantine.
- The poll deadline bounds each individual wait, not total patience: Spring Kafka's whole-batch retry pauses the consumer and polls before each backoff sleep, so the deadline resets between retries — but ⚠️ one attempt plus one wait must still fit inside max.poll.interval.ms. Hence 60 s per wait (≪ 300 s), 30 min total.
- Why bounded, not infinite: ⚠️ an exception misclassified as transient could otherwise freeze a partition until 6h retention expires — reintroducing the poison pill at a different layer.
- Tradeoff: ⚠️ A very long outage trickles a few batches into the DLT for manual replay. The real safety net is alerting on history lag (Phase 6).

**D-71 · Register the Cassandra driver's transient exceptions**

- Because: deny-by-default (D-27) treats anything unregistered as permanent. Without opting in AllNodesFailedException, DriverTimeoutException, WriteTimeoutException, UnavailableException, OverloadedException, RequestThrottlingException, ⚠️ every Cassandra timeout would be dead-lettered as if it were poison.
- The flip side of deny-by-default: every new dependency is a list you must maintain.

**D-72 · Bounded driver concurrency as backpressure**

- concurrency-limiting throttler: 512 in flight, 10,000 queued, then fail fast with RequestThrottlingException (registered transient). 500 pings × 2 tables × N threads would otherwise exceed the driver's per-connection limits.
- Bounded concurrency is backpressure.

**D-73 · auto.offset.reset differs per path**

- History: earliest, even in production — old data is exactly what history is for, and a new group should backfill everything retained. Hot path: arguably latest in production — ⚠️ a 2-hour-old position isn't merely useless, it's wrong for "now." Set per-listener rather than globally.

**D-74 · Multi-bucket reads as N parallel single-partition queries, never IN**

-  Because: WHERE driver_id=? AND day IN (…) is legal and looks clean, but it routes through one coordinator that fans out, gathers every result, and holds it in memory. ⭐ That's the BATCH anti-pattern (D-64) on the read side — a middleman who makes all the trips anyway. One executeAsync per bucket, fired together and merged client-side, gives each node one small sequential read and no coordination.
- 🔹 Confirms D-61 with an implementation: you always know exactly how many partitions a query touches — surfaced to callers as partitionsRead.
- Tradeoff: ⚠️ Bucket count is client-controlled, so it needs a ceiling — 365 days is 365 concurrent queries; a year of hourly occupancy is 8,760. Capped at 30 days (trajectory) and 24 hours (occupancy). Same vulnerability class as D-21 and D-42.
- 🔹 Per-partition over-fetch: each bucket asks for the full limit because any one bucket might hold the whole page. The standard cost of merging parallel sorted streams; bounded and acceptable at these page sizes.

** D-75 · Cursor pagination on history — and why it works here but not on nearby**

- Because: D-49 rejected pagination for live spatial search — drivers move every 4 seconds, so the ordering reshuffles between requests and pages would duplicate and skip. ⭐ History is the opposite: it's immutable. Past positions never change, nothing is inserted into the middle of an old partition, and clustering order is fixed on disk. The ordering is stable by construction, so paging is not only possible but necessary (a driver's day is ~21,600 rows).
- Cursor, not offset: ⚠️ offset paging makes the server scan and discard the first N rows — cost grows with depth, which is why "page 500" is slow in every offset-paginated system. WHERE ts < :cursor against CLUSTERING ORDER BY (ts DESC) is a seek: ⭐ the same cost at page 500 as at page 1.
- 🔹 Cursors are opaque (base64) on purpose. They encode a timestamp today; a client that parsed one would be coupled to our paging internals.
- 🔹 Fetch limit + 1 to detect a next page without a second query — cheaper and more honest than a COUNT.
- ⚠️ Not to be confused with the driver's own PagingState: that is intra-query paging (5,000 rows at a time) whose state can't safely cross an HTTP request. Ours is a domain-level cursor the client can hold and hand back.

**D-76 · Composite cursor (ts, driver_id) for occupancy**

- Because: many drivers share a millisecond inside one cell, so ⚠️ a plain ts < cursor would skip every remaining row at the boundary timestamp when a page ends mid-tie — silent data loss in the API, invisible unless you look for it. Cassandra compares clustering tuples natively: WHERE (ts, driver_id) < (:ts, :driver) is still a seek.
- The clustering key chosen in D-56 for write-side uniqueness is exactly what read-side paging needs — one decision, two payoffs.
- 🔹 Tuple comparison needs both components, and there's no "unbounded" value for the second — so the first page uses a \uffff sentinel that sorts above any realistic driver id. (Alternative: two prepared statements, cleaner but double the statements.)

**D-77 · Optional downsampling on trajectory reads**

- Because: a day at 4-second cadence is 21,600 points ≈ 2 MB of JSON for a feature that draws a line viewed at city zoom — where 4-second resolution isn't visible anyway. sampleSeconds keeps one point per interval.
- ⚠️ The cursor is derived from the RAW page, before downsampling — otherwise the next request would skip everything sampled away.
- 🔹 Summary statistics (distance, duration, max speed, bounding box) are often the real question behind "show me the trajectory" — but they're derived data, better computed once and stored than recomputed per request. A Project 2 concern.

**D-78 · Empty history is 200, not 404**

- Because: deliberate contrast with /drivers/{id}/location, where 404 is meaningful — TTL expiry means offline (D-35). ⭐ A trajectory with no points is a genuine answer: we searched the partitions and found nothing in that window. Absence of history is not absence of driver.
- 🔹 Consistent with D-48 read in reverse: don't return a negative-looking status for a successful search that found nothing, just as you don't return a valid-looking negative for a failed one.

**D-79 · No circuit breaker on history reads (for now)**

- Because: unlike the hot path (D-47), history reads are not latency-critical and Cassandra's own request timeout already bounds them.
- Tradeoff: ⚠️ A slow Cassandra will hold query-service threads for up to the driver timeout per request. Acceptable while history reads have no user-facing SLO. TODO(Phase 7): revisit if they get one.

**D-80 · Don't hand-roll a consistent-hashing ring**

- Context: Cassandra shards by consistent hashing (Murmur3 token ring, num_tokens = vnode count) and Redis Cluster does the equivalent via 16,384 hash slots. Both are inherited, not built.
- Because: we'd need our own ring only to route requests to a specific instance — a stateful hot-cell cache, or a WebSocket gateway pinning each driver's connection. ⚠️ Kafka's assignment already gives region→consumer affinity. Adding a ring on top would be a second, competing source of truth about who owns what.
- Two independent notions of ownership is worse than one you didn't write.
- Revisit if: Phase 6 shows we need instance-affinity Kafka can't express.

**D-81 · Accept Kafka's plain modulo — and treat partition count as a decision, not a setting**

- Context: Kafka assigns partitions by hash(key) % partitionCount — not consistent hashing. The naive modulo, where changing N relocates ~K×(N−1)/N keys.
- Because it's the right trade for Kafka: partitions are not just a distribution mechanism, they're the unit of ordering and log storage. Consistent hashing exists to move keys between nodes cheaply — but moving a key to a different partition puts its messages in a different log, breaking the same-key ordering guarantee. ⭐ You can move a key between nodes; you cannot move it between partitions without breaking what you were promised. Cassandra makes the opposite trade because it has no ordering guarantee to protect.
- Consequence, sharpened from D-10: going 12 → 24 partitions remaps ~50% of H3 cells.  The single-writer guarantee holds at any instant, but the writer's identity changes — and any in-memory per-region state is split across two consumers, with neither holding the full picture for a window.
- Tradeoff: ⚠️ Partition count must be sized from throughput with headroom, up front. There is no cheap rebalance.

**D-82 · The single-writer guarantee, stated formally**

- It is a composition of three Kafka properties, not a feature: deterministic partitioning · exclusive partition→consumer assignment · sequential per-partition processing. cell → partition → consumer → thread.
- What it buys over a lock: zero acquisition cost, no deadlock, no expiry/renewal, work continues when a holder dies — and ⭐ no contention point to collapse. A lock degrades as more workers want the resource; partition affinity has no shared resource to want, so it scales linearly with partitions and consumers.
- Four boundaries where it ends:
  - ⚠️ Async work that outlives the listener. Revocation happens during poll(), which only runs after the listener returns — so a synchronous listener is inherently safe. Fire-and-forget async work is still in flight when the partition moves. ⭐ This is the real reason CassandraHistoryWriter calls allOf(...).join() — not error handling.
  - 🔥 It is per-partition-key, so driver-keyed state is unprotected — see D-83.
  - ⚠️ Exclusion ≠ ownership continuity. Ownership moves on every rebalance. This is why dedup state lives in Redis rather than a HashSet (D-24) — two different problems, two mechanisms.
  - ⚠️ It assumes a stable partition count (D-81).

**D-83 · Tolerate the cross-partition TOCTOU race rather than closing it**

-  The race: the hot-path writer reads driver:{id}:loc, then writes it — read-then-write, not atomic. A driver crossing from cell A (partition 3) to cell B (partition 11) can be handled by two threads, both passing the stale-timestamp check on the same read. Result: a ghost left in cell A, and the driver hash written backwards.
- Why we accept it — three independent mechanisms bound the damage:
  -  Score-based read filtering (D-40): the stale ZADD writes the old timestamp as the member's score, and every read filters by a freshness floor → ⭐ the ghost exists in Redis and is invisible to the system; the sweep evicts it shortly after.
  - Self-healing data (N4): the driver hash is wrong for at most one ping interval (~4 s).
  - Last-write-wins: subsequent pings all carry newer timestamps — divergence is transient by construction.
- 🔹 D-40 was chosen for per-member expiry and happens to close this race too. Freshness-filtered reads are a remarkably load-bearing decision.
- The proper fix, priced: a Lua script makes read-check-write atomic, at one script invocation per driver instead of two pipelined batches. D-36 weighed exactly this. Revisit if Phase 6 shows ghosts are measurable.
- Interview framing: "We identified a TOCTOU race across partition boundaries, quantified its window as one ping interval, and rely on properties the data already has" — stronger than claiming correctness.

**D-84 · In-memory per-cell aggregates, flushed periodically**

- Because: at 250k pings/sec a Redis increment per ping is 250,000 extra round trips per second — a second write path as costly as the first. The answer changes slowly and needs only seconds of accuracy. Accumulating locally gives ⭐ one write per cell per interval instead of one per ping (~99% fewer), with no synchronization, because D-82 means one thread owns every cell it touches.
- This is the guarantee cashing out. Without it: ConcurrentHashMap with atomic merges at best, a distributed counter at worst.
- 🔹 The inner maps are plain HashMap deliberately — a concurrent map there pays for a guarantee we already hold structurally. (The outer map is concurrent: multiple listener threads plus the rebalance listener.)
- 🔹 What it measures: pings, not drivers and not batches. cell:activity:X = 50 means 50 pings since the TTL reset — one driver parked there for 200 s contributes 50. ⭐ The right hot-cell metric, since partition load is proportional to messages, not entities. Distinct-driver supply density remains ZCARD cell:{h3}.

**D-85 · Flush on partition revocation**

- Because: D-82's third boundary. ⚠️ When a rebalance moves a partition, the old owner's counters are stranded — silently lost on every deploy, scale event and rebalance.
- onPartitionsRevokedBeforeCommit runs before offsets commit and before the new owner starts — the only correct moment to persist state for a partition you're about to lose.
- Any in-memory aggregate keyed by partition needs a revocation hook.
- Tradeoff: ⚠️ The hook's absence is invisible under light testing — it only loses data when a rebalance lands mid-batch. Verify by watching for the flush log line during a rebalance.

**D-86 · Additive flush (INCRBY, not SET) **

- Because: a dying owner's partial flush and the new owner's fresh count must add up, not clobber. ⚠️ With SET, a new owner starting from zero would overwrite a larger flushed value and the count would go backwards.
- Additive state needs no handoff. Choosing INCRBY didn't merely avoid a clobber — it removed the entire "transfer state to the new owner" problem, along with any ordering requirement between the two flushes. Commutative operations are dramatically easier to distribute than assignments (the insight behind CRDTs).
- Tradeoff: ⚠️ Only works for commutative aggregates. Distinct-driver counts would need a HashSet per cell and a PFADD/set-union flush.
- 🔹 Redelivery after a crash may double-count a few pings — acceptable for a density signal, and far better than losing counts or having them regress.

**D-87 · Swallow the flush exception — deliberately**

- Because: the inverse of D-22's "never swallow in a listener," and correct because this data is derived, not the system of record. Rethrowing would fail the batch and block the offset commit — ⚠️ turning a metrics problem into an ingestion problem.
- The rule isn't "never swallow" — it's "let the failure propagate to the extent the data matters." Never let derived data take down the primary path.

**D-88 · Partition by the entity your invariants are about**

- The scope condition on D-82: the guarantee works when an invariant fits inside one partition key. It fails when an invariant spans entities that cannot be co-partitioned.
- The concrete case — Project 2: "a driver may be assigned to at most one ride" (driver-keyed) and "a ride has exactly one driver" (ride-keyed). One invariant, two keys — you cannot partition by both.
- When two invariants disagree about which entity they're about, partitioning alone cannot give you mutual exclusion. That requires real coordination: a distributed lock, a CAS, or a single-writer service for the decision. → Session 5.3.

**D-89 · Detect hot cells by skew, not by absolute threshold**

- Because: the question isn't which cell is busiest but which cell is busy enough relative to the rest that its partition is at risk. ⚠️ 100 pings/sec is nothing at peak and a crisis at 3am.
- Median, not mean: ⚠️ a mean is dragged upward by the very outliers you're hunting — with 7 cells at 2 and one at 100, the mean is ~14, so the airport looks 7× above average instead of 50×. ⭐ Median is robust to exactly the thing we're detecting.
- Two signals, both required: cell activity is the leading indicator (fires before anything breaks, which matters when mitigation takes time); per-partition lag is the confirming one (distinguishes "busy but fine" from "drowning"). ⭐ Leading indicator to decide, lagging indicator to confirm.
- Tradeoff: ⚠️ Every ratio-based detector needs an absolute floor, or it goes haywire whenever the denominator shrinks — at 3am a median of 2 makes an ordinary cell look "10× hot."

**D-90 · The detector reads from Redis, not from inside the consumer**

- Because: a consumer sees only its own partitions and cannot compute a global median without cross-consumer coordination. Every consumer already flushes counters to Redis additively (D-86), so Redis holds the complete picture.
- Commutative writes to a shared store give you a global view for free. (Third payoff from D-86: rebalance handling → salting → global aggregation.)
- Tradeoff: ⚠️ @Scheduled runs on every instance — same scan, same verdict, same idempotent write. Wasteful but harmless; the proper fix is leader election (ShedLock). TODO(Phase 7). ⚠️ Also uses KEYS (O(n), blocking) — acceptable at hundreds of cells every 30s, becomes SCAN at scale. ⭐ Using it knowingly with a TODO differs from using it by accident.

**D-91 · TTL on the published hot-cell verdict**

- Because: if the detector dies, the set expires and the system reverts to unsalted behaviour. Fail back to the simple behaviour, not to whatever the last verdict happened to be. A permanent set would leave a cell salted forever after it went quiet, sacrificing its single-writer property for nothing.

**D-92 · Salt the Kafka key for flagged cells**

- Mechanism: "87608b473ffffff" → "87608b473ffffff#0".."#3", spreading 125 pings/sec across ~4 consumers at ~31/sec each — without touching partition count and without changing anything for the other 400 cells.
- Random, not round-robin: ⚠️ round-robin needs a shared counter across ingestion pods — coordination on the hot path. ⭐ Prefer statistical evenness over coordinated perfection when the sample size is large.
- The salt never enters the payload. It influences Kafka's routing decision only; it never reaches Redis or Cassandra, and the consumer is completely untouched (it reads h3PartitionCell from the message body). ⭐ The split is a transport concern, not a data-model concern.
- Tradeoff: ⚠️ We give up single-writer ownership for that cell — four consumers each hold part of its state. Affordable only because ⭐⭐ our aggregates are additive (D-86): four consumers doing INCRBY on the same key produce the correct total with zero coordination. Commutativity chosen for rebalance handling is what makes salting viable.

**D-93 · Hot-cell registry cached locally, polled**

- Because: consulted on every ping (250k/sec) — a Redis lookup per ping would be a round trip on the hot path. Poll every 5s into a volatile field instead.
- 🔹 The reference is replaced, not mutated, so readers always see a complete snapshot — cheaper and safer than locking a mutable collection.
- Fail toward normal behaviour: on a Redis error, keep the previous set rather than clearing it. If Redis is down we'd rather keep salting a recently-hot cell than suddenly dump its full load onto one partition at the worst moment.
- Tradeoff: ⚠️ Staleness of a few seconds. Acceptable because mitigation is a load optimization, not a correctness mechanism.

**D-94 · No per-cell bucket narrowing in Cassandra (deliberately)**

- Context: the airport's cell_occupancy hourly partition is ~8× normal. The technique would be 10-minute buckets for hot cells (~30k rows instead of ~180k), with deterministic reads since the reader computes buckets from the time range.
- Why we're NOT doing it: ⚠️ dynamic widths mean the reader must know which cells were hot at the time the data was written — historical, versioned metadata, forever. Get it wrong and you read the wrong partitions and silently return nothing. Meanwhile 180k rows is within Cassandra's guideline, write concentration spreads by token, and adding nodes helps.
- Options in order: leave it → narrow the bucket for all cells (simple, uniform, no metadata) → only then per-cell widths, stored as data alongside the rows, never in config.
- The general lesson: not every instance of a problem deserves a fix, and the same problem at two layers can warrant different answers. ⭐ Kafka's hot partition is ACUTE (evictions, rebalance storms) — acute problems justify complex fixes. ⭐ Cassandra's is CHRONIC (a fat partition, skewed node load) — chronic problems often don't.
- TODO(Phase 6): measure before engineering.

**D-95 · Hysteresis and cool-down on salt transitions (designed, not yet built)**

- The transition problem: when a cell flips unsalted → salted, traffic moves from one partition to four, and ⚠️ for a brief window pings for the same driver are in flight on different partitions with no ordering between them — the D-09 boundary race hitting every driver in that cell at once. Covered by the stale-write guard and self-healing, but worth naming rather than discovering.
- Any threshold-triggered state change needs hysteresis, or it chatters at the boundary — a cell sitting at 10× would transition on every scan. Salt at 10×, unsalt below 5×, plus a minimum salted period.
- TODO(Phase 6): tune both against real load rather than guessing.

**D-96 · No distributed lock in Project 1 — exhaust the alternatives first** ⭐⭐

- **Because:** every candidate was solved more cheaply. **Partitioning** for the move logic (D-82), **atomic `SET NX`** for dedup (D-24), **idempotent primary keys** for history (D-63), **commutative merges** for counters (D-86).
- **The order to try:** partitioning → idempotency → atomic primitives (`SET NX`, `INCR`, a Cassandra LWT, a unique constraint) → single-writer service → *then* a lock.
- ⭐ **A lock is what's left when none of those work** — specifically, when **one invariant spans two entities that cannot be co-partitioned** (D-88).
- **Tradeoff:** ⚠️ None taken in this project; the cost is deferred to Project 2, where the driver-assignment invariant genuinely requires it.

**D-97 · SET NX is an atomic primitive, not a lock**

- Four failure modes, and the fourth is fundamental:
  - ⚠️ A bare DEL release can delete someone else's lock — if yours expired and another holder acquired it. Needs an owner-ID compare atomic with the delete → a Lua script; GET-then-DEL is the race itself.
  - ⚠️ There is no correct TTL. Too short → expires mid-operation → two holders. Too long → a crashed holder blocks everyone. Only a trade between duplicate execution and stalled work.
  - ⚠️ Clock and pause hazards. The TTL is wall-clock; a GC pause, VM suspension or slow disk can stall you past your own expiry — you can hold an expired lock and be entirely unaware of it.
  - 🔥 A lock cannot stop work already in flight.
- The core insight: a lock guarantees mutual exclusion among processes that are checking it. It cannot guarantee mutual exclusion among writes already on their way to the datastore.

T1 A acquires (TTL 30s)   T5 B acquires — legitimately, it IS free
T3 A pauses 40s (GC)      T6 B writes, releases
T4 Lock expires           T7 A wakes, writes — no lock left to check

- 🔹 A GC pause is just the easy example — a network delay does the same. A's write leaves before expiry and arrives after B's. No pause required.

**D-98 · Fencing tokens: safety lives in the resource, not the lock **

- Because: the in-flight-write problem cannot be fixed at the lock. Every acquisition mints a monotonically increasing token, and the resource rejects any write carrying a token older than one it has already seen.
- The lock's job reduces to handing out increasing numbers. The lock is advisory; the token is enforcement.Because: the in-flight-write problem cannot be fixed at the lock. Every acquisition mints a monotonically increasing token, and the resource rejects any write carrying a token older than one it has already seen.
- Enforcement is per-resource: Redis → a Lua script (check-and-write must be atomic; GET-then-SET is the TOCTOU we're closing). Cassandra → an LWT (UPDATE … IF fence < ?), which runs Paxos and costs ~4× a normal write — you're paying for consensus.
- ⚠️ If the resource can't check a token — a payment API, a filesystem, an email send — you cannot fence. Make the operation idempotent (a provider-deduped idempotency key) or accept an advisory lock and design for double execution. ⭐ This is why idempotency keys are ubiquitous in payment APIs.
- Tradeoff: ⚠️ Fencing changes the resource's schema — which is exactly why it's hard to retrofit, and why we designed it now rather than later.

**D-99 · Token source: single Redis INCR, with the durability caveat named**

- Because: Redis is single-threaded, so INCR is atomic and strictly increasing — adequate for a first implementation.
- ⚠️ The caveat: ⭐ fencing quality equals counter durability. Redis replication is asynchronous, so a promoted replica that missed the last increments will reissue tokens it already gave out — and a repeated token defeats fencing entirely.
- 🔹 Mitigated slightly by comparing with >= rather than >, so a repeated token is also rejected.
- For a true correctness lock: source tokens from a consensus system (ZooKeeper zxid, etcd revision) or a durable DB sequence. TODO(Project 2): decide based on what a double assignment actually costs.

**D-100 · Single-instance Redis, not Redlock**

- Because: ⭐ Redlock is a timing-dependent algorithm being used for a correctness-dependent job. Its safety assumes bounded clock drift and bounded pauses; in an asynchronous system neither is bounded, and when those assumptions break it grants the lock to two holders and doesn't know it.
- Kleppmann's decisive form: with fencing tokens you don't need Redlock (the token catches the failure anyway); without them Redlock doesn't save you (it can still grant concurrent access under a pause). ⭐ Either way the 5-instance complexity buys nothing.
- Antirez's counter: Redlock targets efficiency locks, where duplicate execution is wasteful but harmless. Fair — and not our case.
- The synthesis to carry:

| | Efficiency lock | Correctness lock |
|---|---|---|
| **Duplicate execution is** | Wasteful | **Harmful** |
| **Example** | Two workers recomputing a cache | **Two rides assigned to one driver** |
| **Timing assumptions** | Acceptable | **Not acceptable** |
| **What you need** | Simple `SET NX` | **Consensus + fencing tokens** |


**D-101 · Make races harmless rather than preventing them**

- The observation: fencing tokens make the lock's failure mode safe without making the lock correct. Two processes can both believe they hold it — only one write survives, deterministically the newer one. You haven't prevented the race; you've made its outcome correct.
- The same pattern appears four times in this project, at four different layers: last-write-wins on device timestamps (D-37) · idempotent Cassandra primary keys (D-63) · additive counter flushes (D-86) · fencing tokens (D-98).
- Preventing races is expensive and fragile; making races harmless is cheap and robust. Reach for the second whenever the data permits it.

**D-102 · Lock usage protocol (for Project 2)**

- The TTL is the real backstop, not release(). The release goes in a finally but is not relied upon — ⭐ a crashed process releases nothing.
- ⚠️ A false from release() is not an error — the TTL expired and someone else may hold it now. Never treat a successful release as proof your write landed.
- ⚠️ A rejected fenced write ≠ a failed one. You were legitimately superseded, so the state your decision rested on has changed. Reacquire and re-evaluate — do not retry, or you reapply a decision made from stale data.






---

## 6. Data model

### `LocationPing` (Kafka message / domain model)
```
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

Cassandra : 

| Table | Partition key | Clustering | Bucket | TTL | Compaction |
|---|---|---|---|---|---|
| `driver_trajectory` | `(driver_id, day)` | `ts DESC` | daily | 30d | TWCS, 1-day windows |
| `cell_occupancy` | `(h3_cell, hour_bucket)` | `ts DESC, driver_id` | hourly | 7d | TWCS, 6-hour windows |

Redis: 

| Store | Key | Type | Contents | Expiry |
|---|---|---|---|---|
| Redis | `cell:activity:{h3-res7}` | String | Ping volume since reset, flushed additively | TTL 900s |
| Redis | `hotcells:current` | Set | Cells currently flagged hot | TTL 5 min — safety valve (D-91) |


### Add to §6

| Store | Key | Type | Contents |
|---|---|---|---|
| Redis | `lock:{resource}` | String | Owner UUID, `PX` TTL — **no caller in Project 1** |
| Redis | `fence:{resource}` | Counter | Monotonic fencing token, minted on acquisition |
| Redis | `fenced:{resource}` | Hash | `fence` (highest token seen) + `value` — **the enforcement point** |

> ⚠️ **Built with no caller in Project 1, deliberately.** Normally building what you don't need is a mistake; the narrow justification is that the *design* is the valuable part, fencing is **hard to retrofit** (it changes the resource's schema), and the consumer is **specified, not hypothetical** — Project 2's driver assignment.



Storage layout: the partition key is hashed to a token that picks the node — ⚠️ drv-01 on consecutive days lands on unrelated nodes; there is no "nearby" in token space. Within a partition, rows are written contiguously in ts DESC order, so a range query binary-searches to an offset and reads forward. Range queries are nearly free inside a partition and impossible across them.

The hash is one-way: key → node is trivial, node → key is impossible. That is why a query missing any partition-key component fails with ALLOW FILTERING — the error is the model enforcing itself.


> Same data, **two Cassandra tables** — query-driven modeling: one schema per access pattern.

### API contract
| Method | Path | Status | Store |
|---|---|-------|---|
| `POST` | `/v1/locations` | `202` / `400` | → Kafka |
| `POST` | `/v1/locations/batch` | `202` / `400` | → Kafka (max 100) |
| `GET` | `/v1/drivers/{id}/location` | `200` / **`404` = offline** | Redis |
| `GET` | `/v1/drivers/nearby?lat=&lng=&radiusMetres=&limit=` | `200` / `400` | Redis · radius ≤ 10 km, limit ≤ 500 |
| `GET` | `/v1/cells/{h3}/count` | `200` | Redis · ZCOUNT, O(log n) |
| `GET` | `/v1/cells/count?lat=&lng=` | `200` | Redis · resolves the cell for you |
| `GET` | `/v1/drivers/{id}/trajectory` *(P4)* | `200` | Cassandra · ISO-8601 UTC instants · ≤ 30 day-buckets · limit ≤ 5000|
| `GET` | `/v1/cells/{h3}/occupancy` *(P4)* | `200` | Cassandra · ≤ 24 hour-buckets · composite cursor|
| `GET` | `/v1/admin/hot-cells` | ⚠️ **Operational, not product.** Returns the verdict and the raw counts it was computed from, so a caller can verify the decision rather than trust it. `TODO(Phase 7)`: move behind actuator / internal-only. |


> 🔹 **Presence for free:** the `404` isn't a special case — we store **no "offline" flag.** TTL expiry *is* the offline signal.

**The Query Algorithm :** 

1. originCell = h3(lat, lng, res 9)
2. k          = ceil(R / ~300m)
3. cells      = gridDisk(originCell, k)              → 3k²+3k+1 cells
4. pipeline:  ZRANGEBYSCORE cell:{c} (now-30s) +inf  → candidates   ◄ trip 1
5. union driver IDs
6. pipeline:  HMGET driver:{id}:loc lat lng ts heading → positions  ◄ trip 2
7. filter by TRUE haversine distance ≤ R
8. sort by distance, cap at limit

** Multi Bucket read Algorithm** 

1. buckets = TimeBuckets over [from, to], NEWEST FIRST, capped
2. one executeAsync per bucket, all fired together     ◄ latency ≈ slowest single read
3. consume buckets in order; rows arrive in clustering order within each
4. stop at `limit`; the last row's key becomes the next cursor

-  Response shape mirrors D-50: both history responses carry metadata — nextCursor, partitionsRead, plus distinctDrivers on occupancy (one driver pinging five times is five rows and one driver). partitionsRead makes the partition model visible in the API — ask for 24 hours of occupancy and it returns 24.
-  /cells/{h3}/count and /cells/{h3}/occupancy are the same question in two tenses — served by two engines chosen for two different physics. That split was decided by arithmetic in Session 0.1; everything since has been consequences.
- 


---

## 7. Failure modes

| Failure                                               | Behaviour                                        | Recovery                                            | Data loss? |
|-------------------------------------------------------|--------------------------------------------------|-----------------------------------------------------|---|
| Ingestion pod dies                                    | LB routes elsewhere (stateless)                  | Auto                                                | In-flight requests only |
| Kafka publish fails after `202`                       | Logged (soon: counted)                           | **None** — response already sent                    | ⚠️ **Yes, that ping.** Licensed by N4. For payments → **transactional outbox** |
| Producer buffer full                                  | `send()` **blocks** → backpressure to HTTP       | Auto on drain                                       | No |
| Consumer pod dies                                     | Partitions reassigned                            | Auto (cooperative)                                  | No — resumes from last commit |
| Consumer crashes mid-batch                            | Uncommitted records **redelivered**              | Auto                                                | No — but **duplicates** (→ dedup) |
| Poison message                                        | 1 attempt → **DLT**, partition keeps moving      | Manual: inspect, fix, replay                        | No — quarantined with metadata |
| Cassandra transient failure                           | Retry 1s → 2s → 4s → DLT                         | Auto                                                | No, if it recovers in 7 s |
| Redis down (dedup)                                    | **Fail open** — process anyway                   | Auto                                                | No — possible duplicates |
| Slow consumer > poll deadline                         | **Evicted → rebalance**                          | Auto, ⚠️ but can cascade into a **rebalance storm** | No |
| Consumer down > 6 h                                   | Backlog **expires**                              | ⚠️ **Manual — data lost**                           | ⚠️ Yes |
| Driver crosses cell boundary                          | Pings on different partitions, **no ordering**   | Last-write-wins on timestamp                        | No — brief backwards jump |
| Hot cell (stadium)                                    | One partition overloaded, lag grows              | ⚠️ **Not yet handled — Phase 5.2**                  | No, but freshness degrades |
| Driver stops pinging                                  | Hash TTL expires → treated as offline            | Auto on next ping                                   | No — by design |
| Driver moves between cells                            | SREM old + ZADD new in one pipeline              | Auto                                                |  No |
| Cell goes completely silent                           | ⚠️ Stale members never swept                     | ⚠️ Not handled — Phase 5                            |No — reads filter by score; memory leaks|
| Out-of-order ping (boundary race)                     | Skipped by the stale-write guard                 | Auto                                                |⚠️ Yes, that ping — correct behaviour |
| Device clock skew                                     | Valid updates may be suppressed by last-write-wins | ⚠️ Not handled                                      | ⚠️ Possible — chaos C-08 |
| Driver hash expires between the two query round trips | Candidate skipped                                | Auto                                                | 	No — two non-atomic reads of expiring data, correctly handled |
| Query radius over the cap                             | 400 before any Redis work                        | —                                                   | No — guard, not a failure |
| Reader freshness floor > writer TTL                   | ⚠️ Returns indexed drivers whose position is gone | Config must match                                   | No, but empty/partial results  |
| Redis slow (not down)                                 |Timeout at 200 ms → breaker trips after 10 calls → instant 503  | Auto, half-open after 10 s                          | No |
| Redis down (read path)                                | nearby/location → 503; cellCount → -1               | nearby/location → 503; cellCount → -1               | No — and no false negatives   |
| Query pod restarts                                    | Per-instance cache empties, refills on demand                                    | Auto                                                | No   |
| Partially-written driver hash                         |      Treated as no record → 404                                 | Next ping repairs it                                | No   |
| Query missing a partition-key component               | Rejected (ALLOW FILTERING required) | Fix the query                                       | No — guard, not failure     |
| Two drivers ping in the same ms, same cell            | Distinct rows (driver_id clustering)    | -                                                   | No — would be silent loss without D-56     |
| Busy cell concentrates writes                         |  ⚠️ Hot partition on one Cassandra node  | ⚠️ Not handled — Phase 5.2                          | No, but node load skews     |
| Row reaches TTL                                       | Dropped with its SSTable (TWCS) | -  | By design — retention policy     |
| Single Cassandra node lost (RF=1, dev)                | ⚠️ Data unavailable | ⚠️ Manual  |⚠️ Yes in dev; RF=3 in prod  |
| **Cassandra slow** | Only the history group's lag grows | Auto | No — ⭐ hot-path freshness untouched |
| **Cassandra down (running)** | History retries 1→2→4→...→60 s for 30 min; no rebalance; hot path unaffected | Auto on return; lag drains | No, if outage < 6h retention |
| **Cassandra down > 30 min** | A batch per 30 min trickles to the history DLT; the rest waits in Kafka | Manual replay from DLT | No — guaranteed, recoverable |
| **Cassandra down at BOOT** | ⚠️ Driver connects eagerly → context fails → the Redis hot path can't start either | Manual (or `RECONNECT_ON_INIT=true` → wait instead of crash) | No, but total outage |
| **Poison record in a history batch** | Records before the index flushed, then that one → history DLT | Manual inspect/replay | No |
| **Node restart loses statement cache** | Driver **re-prepares transparently** | Auto | No |
| **`/actuator/health` during a Cassandra outage** | ⚠️ Reports **DOWN** — a K8s readiness probe would pull the pod and take the healthy hot path with it | ⚠️ Not handled — Phase 7.1 health groups | No, but self-inflicted outage |
| **Unknown driver / empty window** | `200` with zero points — not `404` | — | No — a genuine answer |
| **Requested range exceeds the bucket cap** | Silently truncated to the newest N buckets | — | No — guard; `partitionsRead` shows what was read |
| **Malformed cursor** | `400` — a client error, not a server fault | — | No |
| **Page ends mid-tie on `ts` (occupancy)** | Composite cursor resumes exactly | — | No — ⚠️ would be silent row loss with a `ts`-only cursor |
| **Slow Cassandra on a read** | ⚠️ Holds a query thread up to the driver timeout | Auto | No — no breaker yet (D-79) |
| **Unset column read as primitive** | 🐍 `getDouble()` returns `0.0` — a valid speed and a valid heading (due north) | `isNull()` check before `get` | ⚠️ Would silently turn "not reported" into "stationary, facing north" |
| **Driver crosses a partition boundary** | ⚠️ TOCTOU race — ghost in the old cell, driver hash written backwards | Auto (freshness filter + next ping) | No — ⭐ ghost is invisible to reads; hash wrong for ≤1 ping interval |
| **Rebalance with unflushed counters** | Revocation hook flushes before handover | Auto | No — ⚠️ would be silent loss without D-85 |
| **Redis unavailable during a flush** | Logged and **swallowed**; batch proceeds | Next flush | ⚠️ One interval of a *derived* metric — deliberate (D-87) |
| **Partition count increased** | ⚠️ ~50% of cells remap; in-memory regional state splits across two consumers | Manual — plan capacity up front | No, but a window of incomplete aggregates |
| **Rebalance listener silently detached** | ⚠️ Counters leak on every deploy, with no error | Verify the flush log line | ⚠️ Yes — invisible under light testing |
| **Hot cell, unmitigated** | ⚠️ One partition saturates while others idle → lag → eviction → **rebalance storm** | Salting (D-92) | No, but ⭐ **lag > presence TTL makes the busiest cell read as EMPTY** |
| Detector dies | Hot-cell set **expires**; system reverts to unsalted | Auto on restart | No — deliberate (D-91) |
| Redis unreachable by the registry | **Previous hot set retained** | Auto | No — fail toward normal (D-93) |
| Salt transition mid-stream | ⚠️ Brief cross-partition ordering gap for that cell's drivers | Stale-write guard + self-heal | No — transient |
| Cell oscillates at the threshold | ⚠️ Transitions every scan | ⚠️ **Not handled — hysteresis pending (D-95)** | No, but repeated churn |
| **Lock holder crashes** | Lock frees at **TTL** (release never runs) | Auto | No — ⭐ TTL is the real backstop |
| **Holder stalls past its own TTL (GC/network)** | ⚠️ **Two processes believe they hold the lock** | **Fencing token rejects the stale write** | No — ⭐ the race happens, its outcome is correct |
| **Stale fenced write arrives** | Rejected by the resource | **Reacquire and re-evaluate** | No — ⚠️ retrying would reapply a stale decision |
| **Redis failover loses `INCR`s** | ⚠️ **Token reissued → fencing defeated** | ⚠️ Not handled — consensus source needed | ⚠️ **Yes, potentially** — the one failure mode this design can't absorb |
| **Resource can't check tokens (external API)** | ⚠️ Lock is **advisory only** | Idempotency key at the provider | ⚠️ Possible double execution |

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
|presence-ttl-seconds = 30 |  Derived from the 4 s ping cadence (~7 tolerated misses) |
|radiusMetres max = 10000, limit max = 500 |Chosen — DoS ceilings, sized so worst-case k-ring stays ~1,100 cells|
| Redis timeout = 200ms                                         |    Derived — 2× the 100 ms whole-query p99 SLO                                                                |
|  Breaker: 50% over 20 calls, min 10, 10 s open                                        |  Chosen — standard Resilience4j starting point, to be tuned under load                                                                  |
| cellCount cache TTL = 5 s                                                                                      | Chosen — staleness a density figure tolerates                                                                                                                                        |
|  Cache maximumSize = 10_000                                                                                     | Chosen — a city has hundreds of active cells; bounded memory over hit rate                                                                                                                                        |
| Trajectory bucket = daily | **Derived** — 1 driver × 21,600 rows/day ≈ 2 MB |
| Occupancy bucket = hourly | **Derived** — ~200 drivers × 900/hr ≈ 180k rows; daily would be 4.3M |
| ~200 drivers per busy res-9 cell | ⚠️ **ESTIMATE** from ~2,000 drivers/km² dense-city density — verify with real data |
| Partition guideline < 100 MB / < 100k rows | **Industry guideline**, not a hard limit |
| TTL 30d / 7d | Chosen by purpose (trip history vs analytics) |
| Keyspace RF = 1 | ⚠️ **Dev only** |                                                                                                                |                                                                                                                                                                                                                   |
| History backoff: 60 s max interval | **Derived** — must sit well under `max.poll.interval.ms` (300 s) |
| History backoff: 30 min total | **Chosen** — rides out a node restart/rolling upgrade, bounded well below 6h retention |
| Throttler: 512 concurrent / 10,000 queued | **Chosen** — starting point; `TODO(Phase 6)` tune from measured throughput |
| `spring.cassandra.request.timeout = 2s` | **Driver default**, written out for visibility (it bounds each retry attempt) |
| `LOCAL_QUORUM` | **Derived** from R + W > RF with RF=3 |
| **Cassandra `BATCH`** | 🔥 **Not a performance feature.** Warns >5 KB, rejects >50 KB. Use concurrent async writes. |
| **Null in CQL** | 🔥 Writes a tombstone, not "no value." Use unset. |
| **Second `CommonErrorHandler` bean** | 🔥 Spring Boot wires the default listener factory only if exactly one exists. A second bean makes it ambiguous → ⚠️ the default factory silently loses its handler and the hot path stops dead-lettering. No startup error. Build extra handlers inline, not as beans. |
| **`BatchListenerFailedException` index** | 🐍 **A promise:** Spring commits offsets for every record before it. ⚠️ Throwing **before doing the work** commits offsets for records you never wrote. **Flush, then throw.** (This was a real bug in the 2.3 hot-path consumer.) |
| **Eager `CqlSession` init** | Cassandra must be reachable at boot. `RECONNECT_ON_INIT=true` turns crash into wait — ⭐ cosmetic architecturally; the real fix is not needing Cassandra to boot the hot path. |
| Trajectory bucket cap = 30 days | **Chosen** — 30 concurrent queries is a reasonable client-controlled ceiling |
| Occupancy bucket cap = 24 hours | **Chosen** — hourly buckets accumulate 24× faster, so the cap is tighter |
| Trajectory `limit` default 1000 / max 5000 | **Derived** — 21,600 points/day ≈ 2 MB, so a page must be well below a day |
| `geopulse.activity.ttl-seconds = 900` | **Chosen** — a quiet cell ages out rather than holding a stale count |
| Flush interval = per batch | **Chosen** — the batch already bounds in-flight work, so at most one batch of counts is at risk. `TODO(Phase 6)`: compare against a time-based interval |
| Virtual nodes 100–200 per physical node | **Industry norm, cited for context** — we don't operate a ring ourselves |
| `median-multiplier = 10.0` | **Chosen** — high enough to ignore normal variation, low enough to fire before lag shows |
| `min-pings = 500` (prod) / `20` (test) | **Chosen floor** — the production value is unreachable at Postman volumes |
| `salt = 4` | **Chosen** — each unit is one more partition holding part of the cell's state; kept small |
| `scan-interval-ms = 30000` (prod) / `5000` (test) | **Chosen** — detection is slow-moving; mitigation takes minutes to matter |
| `refresh-ms = 5000` (registry poll) | **Chosen** — staleness is harmless for a load optimization |
| Lock TTL (10s in the Project 2 sketch) | **Chosen** — ⚠️ there is no correct value; a trade between duplicate execution and stalled work |
| Fence comparison `>=` not `>` | **Derived** — also rejects a repeated token from the D-99 replication failure |
| Single Redis for tokens | **Chosen for now**, with the durability caveat documented (D-99) |



### The poll-deadline arithmetic
```
time between polls = max.poll.records × time to process one record

healthy   500 × 5ms   = 2.5s    vs 300s   ✅ huge margin
batched   500 × ~60µs = 30ms    vs 300s   ✅ enormous margin
degraded  500 × 800ms = 400s    vs 300s   ❌ EVICTED → rebalance storm
```
> **Two timeouts, two questions:** heartbeat/session (background thread) = ***is the process alive?*** · poll interval = ***is it making progress?*** A consumer stuck in a slow write is **alive and heartbeating while making zero progress** — hence both checks.

### Version notes & traps
| Item                             | Note |
|----------------------------------|---|
| **H3 v4**                        | 🔥 Renamed everything. `geoToH3`→`latLngToCellAddress`, `h3ToParent`→`cellToParentAddress`, `kRing`→`gridDisk`. Most tutorials online are v3 and won't compile. |
| **Kafka 4.x**                    | ZooKeeper **removed entirely** — KRaft is the only mode. |
| **Single-broker dev**            | Must override internal-topic RF to 1, or the first consumer group start fails. |
| **Advertised listeners**         | 🔥 Must be reachable *from where the client runs* → **two listeners** (containers use `kafka:29092`, host apps use `localhost:9092`). |
| **`host.docker.internal`**       | The same trap reversed — a container reaching a host service. ⭐ *"localhost" is always relative to who's asking.* |
| **Cassandra `local-datacenter`** | Must match `CASSANDRA_DC` exactly, or "No node was available" at startup. |
| **DLT topic suffix**             | ⚠️ Differs by Spring Kafka version (`.DLT` vs `-dlt`) — **pin it explicitly.** |
| **`auto-offset-reset`**          | ⚠️ Applies **only when there is no committed offset**. It is *not* "where to resume after restart." |
| **PowerShell**                   | Mangles `-D` args. Quote the whole argument, or use `$env:VAR`. |
| **RedisCallback serialization**  |🔥 Asymmetric: you SEND raw bytes but RECEIVE deserialized objects. The callback gives you the low-level connection for writing; the template still owns reading, and StringRedisTemplate's serializer has already converted results. (byte[]) casts on results throw ClassCastException. For bytes both ways: RedisTemplate<byte[], byte[]> with RedisSerializer.byteArray().  |
| **Composite partition key syntax** | 🔥 `((a, b), c)` vs `(a, b, c)` — both valid CQL, radically different storage. The wrong one compiles and silently creates unbounded partitions. |
| **Duplicate primary key** | 🔥 An upsert, not an error. Missing a uniqueness-guaranteeing clustering column loses data with no signal. |
| **IntelliJ `.cql` files** | No built-in CQL file type — create via **New → File** with the full name, or map `*.cql` to SQL for highlighting. |
| **`row.getDouble()` on an unset column** | 🐍 Returns `0.0`, not `null` — ⭐ the D-17 **`Double` vs `double`** trap one layer down. Whenever a primitive has a legal default, absence must be checked explicitly (`row.isNull(...)`). |
| **`CqlSession` keyspace** | `spring.cassandra.keyspace-name` must be set in every service that prepares unqualified statements, or `prepare()` fails at startup with *"No keyspace has been specified."* |
| **Stale `common` jar** | 🐍 `mvn spring-boot:run -pl <module>` resolves `common` from `~/.m2` as-is → `NoClassDefFoundError` for newly added shared classes. Always `mvn clean install` from the root after touching `common`. |
| **`Error` bypasses error handling** | ⚠️ A `NoClassDefFoundError` inside an async write is an `Error`, not an `Exception` — it skips the retry/DLT classifier entirely and stops the container. **"Consumer stopped"** is a distinct failure mode from **"consumer is dead-lettering"**, and only lag monitoring catches it. |
| **`ConcurrentKafkaListenerContainerFactoryCustomizer`** | ❌ **Does not exist** in Spring Boot 3.5 — the Kafka auto-config provides no customizer hook. Use `ConcurrentKafkaListenerContainerFactoryConfigurer` and declare a bean **named** `kafkaListenerContainerFactory`. |
| **Bean-name conditions** | 🐛 Boot's default factory is conditional on that **exact bean name**. ⚠️ Rename the method and Boot quietly creates its own alongside yours; `@KafkaListener` picks Boot's and **the rebalance listener never fires** — no error. Same failure class as the `CommonErrorHandler` ambiguity in 4.2. `@Primary` is also needed, since `historyListenerFactory` shares the type. |
| **`spring-boot-*` version skew** | 🐛 `ClassNotFoundException: org.springframework.boot.thread.Threading` means a Boot 4.x jar on a 3.5 classpath (the class moved in the 4.x module split). ⭐ **The first suspect is any dependency we pinned by hand** — which is exactly what the BOM (D-01) exists to prevent. `mvn clean install -U`, then `mvn dependency:tree` for any `spring-boot-*` that isn't 3.5.16. |
| `GET`-then-`DEL` release | 🔥 **Not atomic — it is the race.** Use a Lua script comparing the owner ID. |
| `SET NX` then `INCR` | 🔥 Leaves a window where you hold the lock with **no token**; a crash there strands it. One script. |
| Cassandra LWT cost | ⚠️ **~4× a normal write (Paxos).** Fine for a rare assignment decision, ruinous on a hot path. |



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
- ⚠️ No downsampling or cold tiering — raw history simply expires at TTL
- ⚠️ Schema managed by a hand-applied .cql file, not a versioned migration tool
- ⚠️ History lag has no alerting — the safety net for D-70's bounded patience (Phase 6.1)
- ⚠️ /actuator/health aggregates Cassandra, so a readiness probe would re-couple the paths (Phase 7.1 health groups)
- ⚠️ Both consumer groups share a JVM — a crash or a boot-time Cassandra outage still takes the hot path down (Phase 7)
- ⚠️ No circuit breaker on history reads (D-79)
- ⚠️ Trajectory summary statistics not implemented — derived data, better precomputed (Project 2)
- ⚠️ Cross-partition TOCTOU race accepted, not closed (D-83) — revisit if Phase 6 measures ghosts
- ⚠️ Per-batch flush only covers the first record's partition; a thread owning several partitions relies on later batches or revocation (D-84 TODO)
- ⚠️ No alerting on the rebalance-listener path — its failure mode is silent (D-85)
- ⚠️ No hysteresis or cool-down on salt transitions (D-95)
-  ⚠️ @Scheduled detector runs on every instance — no leader election (D-90)
- ⚠️ KEYS in the detector and admin endpoint — becomes SCAN at scale
-  ⚠️ /v1/admin/hot-cells sits on the public query surface
- ⚠️ Fencing tokens come from a single Redis INCR — async replication can reissue one (D-99)
-  ⚠️ DistributedLockService has no caller and therefore no production exercise





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

*status:* Done ✅ in session 4.2 

**C-04 · Slow Cassandra (inject 800 ms latency)** ⭐
*Hypothesis:* the **most interesting experiment.** Per §8, `500 × 800ms = 400s > 300s` → consumers evicted → **rebalance storm**.
*This is a predicted failure we have NOT mitigated. Does it actually cascade? Does lowering `max.poll.records` prevent it?*

**C-05 · Kill the Kafka broker**
*Hypothesis:* producer buffers, then `send()` blocks → backpressure surfaces as HTTP latency (D-13), not data loss. Recovers on broker return.

**C-06 · Hot cell simulation** ⭐
*Hypothesis:* concentrate 50% of load into one res-7 cell (a stadium). One partition saturates, its lag grows unboundedly while others idle.
*This is the known gap from §9 — the experiment should demonstrate exactly why Phase 5.2 exists.*

*status* ✅ *Hypothesis per D-89/D-92 —* concentrating 50% of load into one hexagon saturates one partition while others idle; the detector flags it within one scan; salted keys spread it across 4 partitions and lag recovers. The control case is disabling salting and confirming the eviction → rebalance-storm cascade actually happens.

**C-07 · Network partition between consumer and Kafka**
*Hypothesis:* session timeout → eviction → reassignment. Tests split-brain behaviour and whether the single-writer guarantee holds across the partition.

**C-08 · Clock skew on the device timestamp**
*Hypothesis:* last-write-wins by timestamp misbehaves if a client's clock is wrong. Tests the D-09 mitigation's weak point.

**C-09 · DLT unavailable**
*Hypothesis:* per D-29 — the recoverer fails and records return to retry. Does the pipeline stall? *(We saw this accidentally; worth reproducing deliberately.)*

**C-10 · Rolling deploy under load**
*Hypothesis:* `CooperativeStickyAssignor` keeps most partitions processing; lag rises modestly rather than topic-wide.

**C-11 · Redis slow, not dead (inject 2 s latency via Toxiproxy)**

*Hypothesis:* per D-46/D-47 — timeouts fire at 200 ms, the breaker trips after 10 calls, responses become instant 503s, and /actuator/health keeps responding. This is the experiment that tests the "slow is worse than dead" claim directly — the most valuable of the read-path experiments.

**C-12 · Cache stampede on a hot cell**

*Hypothesis:* per D-45 — at TTL expiry, concurrent requests for one cell all miss simultaneously. Does the ZCOUNT burst matter at all? If it does, we implement single-flight; if not, the decision to skip it is validated.

**C-13 · Hot cell in Cassandra**

*Hypothesis:* concentrating traffic into one res-9 cell makes its cell_occupancy partition grow fastest and loads one Cassandra node disproportionately, while driver_trajectory stays evenly spread (it's keyed by driver). Pair with C-06 — the same hot-cell load should reveal the hot-partition problem in both Kafka and Cassandra simultaneously.

**C-14 · Cassandra down at boot**

*Hypothesis:* the context fails and the hot path can't start. Confirms the startup-coupling gap and validates the Phase 7 split. ✅ Observed accidentally.

**C-15 · Readiness probe during a dependency outage**

*Hypothesis:* /actuator/health reports DOWN and an orchestrator would evict a pod whose hot path is perfectly healthy. Tests whether health groups actually fix it.

**C-16 Slow Cassandra on the READ path**

*Hypothesis per D-79:* with no breaker, query-service threads block for up to the driver timeout per request, and enough concurrent history reads degrade the service — including the Redis-backed endpoints sharing its thread pool. Tests whether D-79's "acceptable for now" survives contact with load.

**C-17 · Page a live-writing partition**

*Hypothesis per D-75:* because history is append-only and clustering order is fixed, cursor paging through a partition that is actively receiving new rows produces no duplicates or gaps — new rows land at the newest end, which paging has already passed.

**C-18 · Rebalance mid-batch, under load**

*Hypothesis per D-85/D-86:* starting an instance while a burst is in flight flushes in-memory counters before handover, and because the flush is additive, the total is exactly the number of pings sent — no loss, no regression. The control case is deliberately removing the revocation hook and confirming counts do go missing.

**C-19 · Force the cross-partition race**

*Hypothesis per D-83:* drive one driver rapidly back and forth across a res-7 boundary under load and measure how often a ghost appears in the old cell, and for how long. Quantifies whether the Lua-script fix is warranted.

**C-20 · Hot cell exceeding the presence TTL**

*Hypothesis per D-89:* drive a cell hard enough that its partition's lag exceeds 30s, and confirm drivers disappear from nearby while still pinging. This is the airport complaint reproduced deliberately, and it validates the diagnostic that separates infrastructure failure from manipulation.

**C-21 · Threshold oscillation**

*Hypothesis per D-95:* hold a cell right at 10× and confirm it flips salted/unsalted on every scan, and that each transition produces a brief cross-partition ordering gap. Quantifies whether hysteresis is worth building.

**C-22 · Fencing under a simulated pause**

*Hypothesis per D-97/D-98:* hold a lock, suspend the process past its TTL (SIGSTOP), let a second process acquire and write, then resume the first. Its write must be rejected. The control case is disabling the fence check and confirming the stale write silently overwrites — the "looks right, corrupts data under GC pause" failure.

**C-23 · Redis failover during token issuance**

*Hypothesis per D-99:* promote a replica mid-load and check whether any fencing token is reissued. Quantifies whether the Redis counter is adequate or a consensus source is required before Project 2.


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
13. **Why not GEOSEARCH:** *"GEOSEARCH is simpler and more accurate, and I'd use it for a single-node system. We use H3 cell sets because a GEO index lives in one key and therefore one node — it can't shard. Our cell keys distribute naturally across a cluster. We're also already carrying H3 cells for Kafka partitioning and Cassandra keys, so cells double as a unit of aggregation for downstream services, not just a search index."*
14. **The over-inclusion filter:** *"k-rings return cells that overlap the radius, and a hexagon disk isn't a circle — so the cells are a coarse filter to find candidates cheaply, and true haversine distance is the fine filter. Skip that step and a 2 km query returns drivers 2.4 km away."*
15. **Failing honestly:** *"During a Redis outage, a 404 on driver location would tell a matching engine that every driver in the fleet is offline, and an empty nearby list would tell the pricing engine the city has no supply. Both are actionable lies. We return 503 — never a value that looks like a valid negative answer when you actually failed to answer."*
16. **Why no pagination:** *"Offset pagination assumes a stable ordering between requests, and our drivers move every four seconds — page 2 would duplicate some and skip others. You can't paginate a result* set that reorders itself. A caller needing more candidates expands the radius, which is a different question with a superset answer."*
17. **The caching test:** *"We cache cell density but not nearby search. The test isn't 'is it a read' — it's 'is it repeated or expensive, AND does it tolerate staleness?' Nearby search is already two round trips to memory, and staleness is exactly what we're fighting."*
18. **Immutability licenses denormalization:** *"We write every ping to two tables — one keyed by driver, one by cell. In a relational system that's a divergence hazard. Here it's safe because the data is immutable: a past location never changes, so the copies can't drift apart. The rule is duplicate freely when data is immutable, be careful when it isn't."*
19. **Deriving bucket width:** *"Both tables use time-bucketed partition keys, but different widths. Trajectory is one driver per partition, so daily buckets give about 21,000 rows. Occupancy collects every driver in a cell — about eight times the rate — so a daily bucket would hold 4.3 million rows. Hourly brings it back to 180,000. Bucket width isn't a convention; it's derived from how fast each key accumulates rows."*
20. **The same failure at two layers:** *"Partitioning by location creates a hot-partition risk in Kafka and again in Cassandra's occupancy table — same cause, geography is unevenly populated, and the same family of fixes."*
21. **Decoupling the tiers:** *"History writes run in a separate consumer group, not a second write in the same listener. One listener means a Cassandra compaction spike stalls the Redis updates behind it, and a Cassandra outage becomes a live-tracking outage. With two groups, a Cassandra failure is just lag on the cold path — and Kafka's retention tells me exactly how long an outage I can absorb: six hours."*
22. **Why not BATCH:** *"Batching amortizes a fixed cost paid to one destination — a Redis pipeline goes to one server, a Kafka batch to one partition leader. Our Cassandra writes span hundreds of partitions, so a batch is a middleman that still makes every trip, plus batchlog writes. The Cassandra equivalent of pipelining is concurrency: fire them async and token-aware, and total latency is the slowest write rather than the sum."*
23. **Exactly-once without exactly-once:** *"We don't get exactly-once delivery; we make redelivery harmless. The history primary keys identify the logical event, so a redelivered ping is an upsert of identical data. At-least-once plus an idempotent write gives an exactly-once effect — and it let us delete the dedup step entirely on that path."*
24. **Paging and the shape of your data:** *"Our live spatial search has no pagination and our history API does. It's the same reasoning both times: offset paging assumes stable ordering. Drivers move every four seconds, so a live result set reorders between requests — page two would duplicate some and skip others. History is immutable, so the ordering is fixed and cursor paging is coherent. And a cursor is a seek rather than a scan, so deep pages cost the same as the first."*
25. **One key, two payoffs:** *"The occupancy table has driver_id as a clustering column because two drivers can ping in the same millisecond, and without it one would silently overwrite the other. That same column turned out to be what read-side pagination needed — a ts-only cursor would skip rows whenever a page ended mid-tie. A uniqueness decision on the write side solved a correctness problem on the read side."*
26. **Multi-partition reads:** *"A multi-day query is N parallel single-partition reads driven by the application, never day IN (...) — that routes through one coordinator that fans out and gathers, which is the same anti-pattern as a Cassandra batch on the write side. The upside is that you always know exactly how many partitions a query touches. We return that count in the response."*
27. **Why Kafka doesn't use consistent hashing:** *"Cassandra shards by consistent hashing; Kafka uses plain modulo. That looks like a mistake until you see what it's protecting — partitions are the unit of ordering, not just distribution. Consistent hashing exists to move keys between nodes cheaply, but moving a key to a different partition puts its messages in a different log and breaks the same-key ordering guarantee. Kafka chose the guarantee, which is exactly why partition count is a one-way door."*
28. **The guarantee and its scope:** *"'The partition is your lock' is a composition of three Kafka properties, not a feature — and it's per-partition-key. Our cell-keyed state is protected; our driver-keyed state isn't, and there's a real read-modify-write race when a driver crosses a boundary. We bounded it rather than closing it: the ghost is invisible because reads filter by freshness, and the data self-heals in four seconds."*
29. **Additive state:** *"In-memory counters flush with INCRBY, not SET. That one choice removed the whole problem of transferring state to a new owner after a rebalance — a partial flush and a fresh count simply add up, in any order. Commutative operations are far easier to distribute than assignments."*
30. **Why SET NX isn't a lock:** *"A lock guarantees mutual exclusion among processes that are checking it — it can't stop a write already in flight. If a holder stalls past its TTL from a GC pause or a slow network, its write lands after the next holder's and overwrites it, while the lock was working correctly the whole time. You fix that at the resource, not the lock: every acquisition mints a monotonic fencing token, and the resource rejects anything older than it's already seen."*
31. **The Redlock question:** *"Redlock is a timing-dependent algorithm doing a correctness-dependent job — it assumes bounded clock drift and bounded pauses, and neither is bounded. The decisive form of the argument: with fencing tokens you don't need Redlock, and without them Redlock doesn't save you. The real question is never which lock library, it's what happens if the lock fails. If the answer is wasted CPU, use the simple thing. If it's corrupted data, you need consensus and an enforcing resource."*
32. **The pattern across the system:** *"Fencing tokens don't prevent the race — they make its outcome correct. That's the same shape as last-write-wins on device timestamps, idempotent Cassandra keys, and additive counter flushes. Four layers, one idea: preventing races is expensive and fragile, making them harmless is cheap and robust."*
33. 

