# Foundations & Capacity Math

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Do the arithmetic that *forces* the architecture, so every later decision (Kafka, two databases, retention policy) is justified by a **number**, not a preference.
> **Code in this session:** none — by design. We design here; building starts in Session 0.2.

---

## Why this session has no code

A Staff engineer does the capacity math *before* opening an IDE. When an interviewer asks "why Kafka? why two databases? why not just Postgres?", the answer should be a number you computed — not a stack you cargo-culted. This session produces those numbers.

## The core reframe (the thread through the whole project)

A CRUD app treats each driver as a **row you lock, read, and update**. At ~250,000 writes/second that model dies on **lock contention** long before it runs out of disk or CPU.

So we are **not** building a location *database*. We're building a location *pipeline*:

- absorb the write storm **asynchronously**,
- index space **at the ingestion edge**,
- and let the **partitioning scheme — not row locks — guarantee correctness.**

Hold onto that last line. It recurs in every phase and pays off hard in Phase 5 ("the partition is your lock").

---

## Part 1 — Requirements

Two buckets. Interviewers care far more about the second one for a system like this.

### Functional (what it does)
- Ingest a location ping — `{driverId, lat, lng, timestamp}` plus optional `speed / heading / accuracy` — from every active driver, continuously.
- Convert each coordinate to a **spatial cell** (H3) at ingestion time.
- Serve **"where is driver X *now*"** and **"which drivers are near this point *now*."**
- Serve **"where has driver X *been*"** and **"who was in this area over a time window."**

### Non-functional (the qualities that shape everything)
- **Write-heavy, relentlessly.** Writes arrive on a metronome (every driver, every 4s), independent of user activity.
- **Low ingestion latency** — accepting a ping must be fast, or the firehose backs up.
- **Horizontally scalable** — scale by adding nodes/partitions, never by buying a bigger database.
- **Loss-tolerant on individual writes** — a dropped ping is self-healing; the next one arrives in 4 seconds. *(Unpacked in Part 4 — it's the license for the whole async design.)*
- **Highly available over strongly consistent** — a location that's 3 seconds stale is operationally fine.

---

## Part 2 — Capacity Math (back-of-envelope)

**The discipline:** state your assumptions out loud, then compute. Every number lands on an architectural decision.

### Assumptions
- Peak concurrent active drivers: **1,000,000** (one large region-cluster)
- Ping interval: **4 seconds**
- Payload: **~200 bytes** on the wire (JSON) / **~100 bytes** stored (binary + H3 id)

### Write throughput
```
1,000,000 drivers ÷ 4 s  =  250,000 writes/second
```
A quarter-million writes a second, sustained, forever. A synchronous `INSERT ... WHERE id=?` cannot survive this — lock contention alone kills it.
**→ This is why the ingest path is asynchronous through Kafka, not direct-to-DB.**

### Ingress bandwidth
```
250,000/s × 200 bytes  =  50 MB/s  ≈  400 Mbps of raw ingest
```
Manageable — but only spread across a fleet of stateless ingestion nodes behind a load balancer.
**→ This is why ingestion is a horizontally-scaled tier, not one box.**

### History storage (Cassandra), per day
```
250,000/s × 86,400 s        =  21.6 billion pings/day
21.6e9 × ~100 bytes         =  ~2.16 TB/day  (raw)
× replication factor 3      =  ~6.5 TB/day   →  multiple PB/year
```
You **cannot** keep raw 4-second pings forever.
**→ This forces a retention policy:** keep raw pings hot for N days → downsample older data (one point/minute instead of per-4s) → tier the cold tail to object storage (S3). *(Retention design is a Staff-level signal on its own.)*

### Hot-state memory (Redis) — the beautiful asymmetry
```
1,000,000 drivers × ~200 bytes (current location only)  ≈  200 MB
+ H3 cell → driver spatial index                        →  ~500 MB – 1 GB total
```
Under a gigabyte. Fits comfortably on a single Redis node. Compare to **2+ TB/day** of history. Same raw events — but "where is everyone *right now*" is tiny because it's **ephemeral**: only the latest point per driver is kept, and offline drivers age out via TTL.
**→ This asymmetry (sub-GB hot vs TB/day cold) is the entire justification for two storage tiers with different engines.** One database cannot be optimal for both.

### Read throughput (rough)
```
~50,000 – 100,000 "nearby" queries/second at peak
Write : read  ≈  3:1 to 5:1   →  write-dominated
```
Note the inversion: most CRUD apps are read-heavy (many views, few edits). This is the **opposite** — writes dominate, uniform and relentless; reads are bursty and user-driven.
**→ We optimize the write path first, always.**

### The capacity envelope — and what each number buys

| Metric | Figure | Architectural consequence |
|---|---|---|
| Write QPS (peak) | ~250k/s | Async pipeline (Kafka); no synchronous DB writes |
| Payload | ~200 B wire / ~100 B stored | Small messages → batching pays off |
| Ingest bandwidth | ~50 MB/s | Stateless ingestion tier behind a load balancer |
| History storage | ~2 TB/day raw | Retention + downsampling + cold tiering |
| Hot-state memory | < 1 GB | Fits Redis easily; ephemeral by design |
| Read QPS | ~50–100k/s | Secondary; served from Redis hot tier |
| Write : read | ~3:1 (write-heavy) | Optimize the write path above all |

### Scoping to reality
We prove the architecture at **10k writes/s** on a laptop-scale cluster (the k6 target in Phase 6). The *design* scales to 250k+ by adding Kafka partitions and consumers — **nothing structural changes.**

> **Interview framing:** "I load-tested 10k writes/s; the design scales linearly to 250k+ by adding partitions and consumers." Honest, and demonstrates you understand the scaling axis.

---

## Part 3 — The API Contract

Two surfaces. We nail the **write** contract now (built in Phase 1) and sketch the **read** contract (built in Phase 3+).

### Write — the ingestion endpoint
```
POST /v1/locations
```
```json
{
  "driverId": "7f3e4d2a-1b6c-4e8f-9a3d-2c5b8e1f0a4d",
  "lat": 19.076090,
  "lng": 72.877656,
  "timestamp": 1719900000000,
  "speed": 12.5,
  "heading": 270.0,
  "accuracy": 5.0
}
```

### Why `202 Accepted`, not `200 OK`
This is a real design decision, not a formality. `200 OK` means "done, stored, durable." But we're **asynchronous** — at the moment we reply, we've only handed the ping to Kafka; the consumer hasn't processed it yet. Returning `200` would be **lying about durability**.
**`202 Accepted` honestly says: "I've taken responsibility for this; processing is in flight."** That single status code signals you understand the async contract.

### Idempotency
`driverId + timestamp` is a natural idempotency key. Kafka redelivery is a **when, not an if** — at-least-once delivery means duplicates happen, so the consumer must recognize and drop a replayed ping. *(Dedup designed in Phase 2.)*

### Batch variant (Staff addition)
```
POST /v1/locations/batch      // array of pings
```
A driver app that was briefly offline flushes a buffered batch; batching also amortizes HTTP overhead — at 250k/s, fewer-but-larger requests materially help throughput.

### Read — served in Phase 3+ (sketch)
```
GET /v1/drivers/{id}/location                → current point      (Redis)      200 | 404 if offline
GET /v1/drivers/nearby?lat=&lng=&radius=     → nearby drivers      (Redis)      200
GET /v1/cells/{h3}/count                     → density in a cell   (Redis)      200
GET /v1/drivers/{id}/trajectory?from=&to=    → historical path     (Cassandra)  200
GET /v1/cells/{h3}/occupancy?from=&to=       → who was here, when  (Cassandra)  200
```

> **Presence for free:** the `404` on current-location-when-offline isn't a special case. We store **no** "offline" flag — TTL expiry *is* the offline signal. Key exists → recently pinged; key gone → dark.

---

## Part 4 — SLOs & the Insight That Unlocks Everything

### Service Level Objectives (what k6 will hold us to)
- **Ingestion latency** (request → `202`): **p99 < 50 ms**, p50 < 10 ms. The endpoint only validates, H3-encodes, and publishes. *This is the primary number the load test measures.*
- **End-to-end freshness** (ping received → visible in Redis): **p99 < 2 s**. = ping latency + Kafka + consumer processing + lag. The budget for "how stale is *now*."
- **Nearby-query latency:** **p99 < 100 ms** (from Redis).
- **Sustained throughput:** **10k writes/s** in the load test (design scales to 250k+).
- **Consumer lag:** **bounded** — lag is *the* health metric. If it grows without bound, we're not keeping up and freshness degrades. Alert on it.

### The insight: loss-tolerance → AP semantics
**This system is loss-tolerant on individual writes, and that changes the entire calculus.**

A payment ledger cannot drop a single write — every cent must be durable. But location is **self-correcting**: lose one ping and the next arrives in 4 seconds to repair the picture. That single property lets us choose **availability and throughput over per-write durability and strong consistency.**

In **CAP** terms, the current-location store is unapologetically **AP** — during a network partition we'd rather serve a slightly-stale position than refuse service. Cassandra's **tunable consistency** then lets us dial the durability knob per-operation for history, where it matters more.

> **The Staff-level soundbite:** "We relaxed per-write durability because the high-frequency, self-healing nature of location data makes it safe — and that relaxation is exactly what buys us the throughput." This is reasoning *from the data's properties to the system's guarantees*, not cargo-culting a stack. It's the judgment that separates Senior from Staff.

---

## Key takeaways (revision list)

1. **250k writes/s** → async pipeline (Kafka), never synchronous DB writes.
2. **Sub-GB hot vs TB/day cold** → two storage tiers, two engines, by design.
3. **Multiple PB/year** → retention + downsampling + cold tiering are mandatory, not optional.
4. **`202 Accepted`** is the honest response for an async ingest contract.
5. **Loss-tolerant + self-healing data** → we choose **AP** (availability) deliberately.
6. **Write-heavy (3:1)** → optimize the write path first, always.
7. **The partition is your lock** → correctness from partitioning, not row locks (Phase 5 payoff).

## What we locked this session
The numbers that justify: async ingestion (250k/s), two storage tiers (sub-GB hot vs TB/day cold), a retention policy (PB/year forces it), the `202` async contract, and loss-tolerant AP semantics.

---

## Next → Phase 0 · Session 2 — Local Stack & Project Scaffolding
Where code begins. We stand up the local infrastructure with Docker Compose (Kafka, Redis, Cassandra, Prometheus/Grafana) and scaffold the Maven project structure, so that in Phase 1 we can write the ingestion endpoint against real infrastructure running locally.