# High-Concurrency Location Tracking & Spatial Ingestion Engine

*Project 1 of 3 — the tracking/supply system behind a ride-hailing platform.*
*(2 — Real-Time Distributed Matching & Order Fulfillment · 3 — Highly Available Dynamic Pricing & Rate Limiting Guard.)*

**One-line pitch:** the part of Uber that quietly watches millions of moving cars at once and always knows who's where — without locking a database to death.

## Stack
Java 21 (virtual threads) · Spring Boot 3.5.16 · Maven · Apache Kafka 4.2 (KRaft) · Redis 8 · Cassandra 5 · Uber H3 4.3 · k6 (load) · Micrometer + Prometheus + Grafana · Testcontainers · Docker Compose

## 📐 [DESIGN.md](DESIGN.md) — the consolidated architecture document
Every design decision and tradeoff from Phase 0 onward in one place: requirements, capacity math, a numbered **decision log** (D-01…D-32), the data model, a failure-mode matrix, config provenance, and the **chaos engineering plan**. Start here for the "why"; the session docs below are the detailed build notes.

Also: **[api-testing-guide.md](api-testing-guide.md)** — how to run the Postman suite plus the manual verification Postman can't cover.

## How these docs work
One `.md` per session, named `phase-X-session-Y-<topic>.md`, numbered so they sort in order. Each file captures the **concepts, tradeoffs, code, and interview soundbites** for that session — designed to stand alone as revision material.

Legend: ✅ done · 🔜 next · ⬜ planned

---

## Roadmap

### Phase 0 — Foundations & capacity math
- ✅ **0.1 · [Foundations & Capacity Math](phase-0-session-1-foundations-and-capacity-math.md)** — requirements, back-of-envelope capacity, API contract, SLOs
- ✅ **0.2 · [Local Stack & Scaffolding](phase-0-session-2-local-stack-and-scaffolding.md)** — Docker Compose (Kafka/Redis/Cassandra/Prometheus/Grafana), Maven multi-module, connectivity smoke test

### Phase 1 — Ingestion layer *(getting pings in)*
- ✅ **1.1 · [The Ingestion Endpoint](phase-1-session-1-ingestion-endpoint.md)** — DTO vs domain model, validation, `202 Accepted`, error handling, quality gate, virtual threads
- ✅ **1.2 · [H3 Geospatial Indexing](phase-1-session-2-h3-geospatial-indexing.md)** — why discrete cells shard and boxes don't, H3 vs S2, dual resolutions (9 storage / 7 partition), k-rings, edge enrichment
- ✅ **1.3 · [Kafka Producer & Partitioning](phase-1-session-3-kafka-producer-and-partitioning.md)** — partition count math, ordering guarantees, `acks`+idempotence, batching, backpressure  🏁 *Phase 1 complete*

### Phase 2 — Processing layer *(consuming & fanning out)*
- ✅ **2.1 · [Consumer Groups & Parallelism](phase-2-session-1-consumer-groups-and-parallelism.md)** — partition assignment as our lock, offset commit orderings, at-least-once, concurrency, rebalancing
- ✅ **2.2 · [Idempotency, Dedupe & DLT](phase-2-session-2-idempotency-dedupe-and-dlt.md)** — Redis `SET NX` dedup, bounded TTL window, retryable vs permanent classification, poison pills, dead-letter topic
- ✅ **2.3 · [Batch Consumption & Throughput](phase-2-session-3-batch-consumption-and-throughput.md)** — round-trip economics, batch listeners, per-record failure isolation, concurrency, backpressure  🏁 *Phase 2 complete*

### Phase 3 — Hot state (Redis)
- 🔜 3.1 · Current-location model, TTL/ephemerality, the write path
- ⬜ 3.2 · Spatial queries — Redis GEO vs H3-cell sets, k-ring neighbor search
- ⬜ 3.3 · The "drivers near me" query API

### Phase 4 — Persistent history (Cassandra)
- ⬜ 4.1 · Query-driven data modeling, partition & clustering key design
- ⬜ 4.2 · Write-behind path, micro-batching, tunable consistency
- ⬜ 4.3 · Trajectory + hex-occupancy history APIs

### Phase 5 — Distributed coordination & correctness *(the Staff edge)*
- ⬜ 5.1 · Consistent hashing — partition→node affinity, "the partition is your lock," rebalancing
- ⬜ 5.2 · The hot-cell / hot-partition problem — salting, sub-partitioning, dynamic splits
- ⬜ 5.3 · Distributed locks done right — `SET NX`/Redlock, fencing tokens, when you actually need one

### Phase 6 — Scale, load test & observability
- ⬜ 6.1 · Metrics — Micrometer + Prometheus + Grafana, consumer lag, hot-cell distribution
- ⬜ 6.2 · Load testing with k6 — simulate 10k+ drivers, ramp, measure p99 + lag
- ⬜ 6.3 · Bottleneck analysis + the numbers for the README/resume

### Phase 7 — Hardening & portfolio polish
- ⬜ 7.1 · Failure modes & resilience — rebalance storms, Redis failover, graceful degradation
- ⬜ 7.2 · README, architecture diagrams, the "SDE-2 edge" writeup, resume bullets

---

## The one idea to remember
**The partition is your lock.** We get correctness under a 250k-writes/second firehose by making contention *impossible through partitioning* — not by locking rows. Everything else is in service of that.