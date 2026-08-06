# High-Concurrency Location Tracking & Spatial Ingestion Engine

*Project 1 of 3 — the tracking/supply system behind a ride-hailing platform.*
*(2 — Real-Time Distributed Matching & Order Fulfillment · 3 — Highly Available Dynamic Pricing & Rate Limiting Guard.)*

**One-line pitch:** the part of Uber that quietly watches millions of moving cars at once and always knows who's where — without locking a database to death.

## Stack
Java 21 (virtual threads) · Spring Boot 3.x · Maven · Apache Kafka · Redis 7 · Cassandra 5 · Uber H3 · k6 (load) · Micrometer + Prometheus + Grafana · Testcontainers · Docker Compose

## How these docs work
One `.md` per session, named `phase-X-session-Y-<topic>.md`, numbered so they sort in order. Each file captures the **concepts, tradeoffs, code, and interview soundbites** for that session — designed to stand alone as revision material.

Legend: ✅ done · 🔜 next · ⬜ planned

---

## Roadmap

### Phase 0 — Foundations & capacity math
- ✅ **0.1 · [Foundations & Capacity Math](phase-0-session-1-foundations-and-capacity-math.md)** — requirements, back-of-envelope capacity, API contract, SLOs
- ✅ 0.2 · Local stack (Docker Compose: Kafka, Redis, Cassandra, Prometheus/Grafana) + Maven scaffolding

### Phase 1 — Ingestion layer *(getting pings in)*
- ✅ 1.1 · The ping endpoint, DTO + validation, domain model, non-blocking design
- ✅ 1.2 · H3 integration — lat/lng → cell ID at the edge (resolutions, why hexagons, k-rings)
- ⬜ 1.3 · Kafka producer + the partitioning decision, idempotency keys, ordering

### Phase 2 — Processing layer *(consuming & fanning out)*
- ⬜ 2.1 · Consumer groups, parallelism, offset management
- ⬜ 2.2 · Idempotency + redelivery/dedupe, error handling, dead-letter topic
- ⬜ 2.3 · Batch consumption, concurrency, backpressure & throughput tuning

### Phase 3 — Hot state (Redis)
- ⬜ 3.1 · Current-location model, TTL/ephemerality, the write path
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