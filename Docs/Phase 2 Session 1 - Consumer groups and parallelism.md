# Phase 2 · Session 1 — Consumer Groups & Parallelism

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Build the consumer, and turn *"the partition is your lock"* from a slogan into a structural guarantee we can point at.
> **Status:** ✅ Verified — a second instance joined the group and partitions redistributed evenly, with zero code changes.

---

## Part 1 — How consumption actually works

### The consumer group is the unit of scaling
A **consumer group** = consumers sharing a `group.id`. Kafka's guarantee: **every partition is assigned to exactly one consumer in the group.** Consumers may hold many partitions; a partition never has two owners.

| Consumers (12 partitions) | Assignment | Result |
|---|---|---|
| 1 | all 12 | works, no parallelism |
| 3 | 4 each | balanced |
| 12 | 1 each | **max useful parallelism** |
| 15 | 1 each, **3 idle** | wasted pods |

⭐ **This exclusivity IS our lock.** partition → exactly one consumer means every ping in a res-7 neighbourhood is processed by **one thread on one machine**. No two nodes ever touch the same region's state concurrently. That's a **structural guarantee from Kafka's assignment model — not something we implement.**

> A *different* `group.id` = a second independent group that gets its **own copy** of every message with its own offsets. Useful for backfills; a nasty surprise if caused by a typo in a deploy config.

### Offsets and where at-least-once comes from
Each partition is an append-only log with increasing offsets. The **committed offset** (stored in Kafka's `__consumer_offsets`) is where a new owner resumes.

The only real question is *when* you commit — and there are exactly two orderings:

| Ordering | Crash window | Result |
|---|---|---|
| Commit **before** processing | after commit, before write | **message lost forever** (at-most-once) |
| Commit **after** processing | after write, before commit | **message reprocessed** (at-least-once) |

⭐ **There is no third option.** You cannot atomically commit a Kafka offset *and* write to Redis — two systems, no shared transaction. **You are choosing your failure mode: lose data, or duplicate it.**

**We choose at-least-once** — duplicates are fixable, loss isn't. A duplicate location ping just rewrites the same coordinates; combined with a timestamp comparison it's a **no-op**. Our writes are **idempotent by nature**, which turns at-least-once from a problem into a non-issue.

> ⚠️ **Interview trap:** this is a *different* problem from the **producer** idempotence configured in Session 1.3. That prevented duplicates from **producer retries**. This is **consumer redelivery**. Same word, different layer.

### Concurrency: threads × instances
`concurrency: N` creates N consumer threads *inside one instance*, all joining the same group. Parallelism has two dimensions: threads per instance, and instance count.

**Rule:** `concurrency` must not exceed the partitions that instance can be assigned, or threads idle.
*3 instances × concurrency 4 = 12 threads for 12 partitions = one each.* ✅

⭐ **Each partition → exactly one thread → records processed sequentially in offset order.** Your listener is **effectively single-threaded per partition**, so per-partition state needs **no synchronization**. This is why regional aggregates can later live in plain local variables.

### 🔥 The two settings behind most production incidents

| Setting | Meaning |
|---|---|
| `max.poll.records` (500) | records returned per `poll()` |
| `max.poll.interval.ms` (5 min) | deadline to call `poll()` again, or you're declared **dead** |

**The rebalance storm loop:**
```
consumer slows (Cassandra hiccup) → misses poll deadline → EVICTED
   → rebalance → its partitions move to others
   → THEY now have more load → they miss the deadline → EVICTED
   → rebalance storm: the cluster does nothing but rebalance
```
**This is the #1 Kafka production failure mode.**

**The defence is arithmetic:** `max.poll.records × per-record time ≪ max.poll.interval.ms`
→ `500 × ~5ms = 2.5s` vs `300,000ms`. Enormous margin, deliberately.

> **Heartbeats vs poll interval — separate checks, on purpose.** Heartbeats run on a *background thread* and prove **the process is alive**. The poll interval proves **the process is making progress**. A consumer stuck in a slow write keeps heartbeating while failing to poll — which is exactly why both exist.

---

## Part 2 — The consumer

### 🔥 `ErrorHandlingDeserializer` is not optional — the poison pill
Without it, one malformed message throws **during deserialization, before your listener runs** → the offset never advances → the consumer retries the same bad message **forever** → **the entire partition stalls indefinitely.**

The wrapper catches the failure and passes a **null payload** to your listener, so the offset can move past it. (Session 2.2 routes these to a dead-letter topic instead of dropping.)

### ⚠️ `auto-offset-reset` is widely misunderstood
It applies **only when the group has no committed offset at all** (brand-new group, or expired offsets). It is **NOT** "where to resume after a restart" — a restarting consumer with committed offsets resumes exactly where it left off, regardless of this setting.

- **dev: `earliest`** — so a fresh consumer drains the messages already produced in Phase 1 (otherwise you start it, see nothing, and wonder what broke).
- **prod: arguably `latest`** — on a brand-new group you rarely want to replay hours of stale location data. *A 2-hour-old position isn't merely useless, it's **wrong**.*

### Turn OFF auto-commit
With `enable-auto-commit: true`, Kafka commits on a **timer, independently of whether you processed anything** — so a crash between a timer commit and the actual write **silently loses messages**. We use `ack-mode: batch` so commits are tied to the listener returning successfully.

### ⭐ Throwing is a signal, not just an error
An exception escaping the listener **blocks the offset commit → triggers redelivery.** That IS the at-least-once mechanism.

> **Therefore:** a careless `try/catch` that swallows a transient failure silently converts at-least-once into **at-most-once** — you lose data with no error surfacing. **Exception handling in a consumer is a correctness decision, not hygiene.**

### Serialization config
Because we disabled type headers producer-side (Session 1.3), the consumer must be told the target type explicitly:
```yaml
spring.json.value.default.type: com.geopulse.common.model.LocationPing
spring.json.trusted.packages: com.geopulse.common.model
spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
```

---

## Part 3 — Rebalancing (the operationally painful part)

Group membership isn't static. **Any** join or leave — deploy, crash, scale-up, poll-deadline miss — forces Kafka to recompute ownership.

**With the default protocol, a rebalance is stop-the-world:**
1. All consumers stop and **revoke** their partitions
2. The coordinator computes a new assignment
3. Partitions are handed out; consumers resume

Between 1 and 3, **nobody consumes anything** — lag climbs across the *entire* topic, not just partitions that moved. **A rolling deploy of 6 instances triggers 12 rebalances**, each pausing the whole group.

### The fix: `CooperativeStickyAssignor`
```yaml
partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
Computes the new assignment first and revokes **only the partitions that must move** — everything else keeps processing. "Sticky" also minimizes movement. One config line, strictly better for our workload.

### ⚠️ What happens to the single-writer guarantee during a rebalance?
It holds — with a wrinkle. Since we commit **after** processing, a partition that moves mid-batch may have records processed but not committed. The new owner resumes from the last commit and **reprocesses them**. Briefly the same records are handled twice — by two different machines, though **not simultaneously** (the old owner already revoked).

> ⭐ **Rebalances are the most common source of duplicate delivery in practice — far more than producer retries.** This is the concrete reason we need dedup + timestamp comparison (Session 2.2 / Phase 3).

---

## Verification (confirmed live)

| Test | Result | What it proves |
|---|---|---|
| One instance | Owns all 12 partitions (3 per thread × 4 threads) | Group formed; internal concurrency works |
| Fresh group, `earliest` | Immediately drained all Phase 1 messages | Full pipeline: HTTP → Kafka → consumer |
| **Second instance joins** | **Partitions redistributed evenly, zero code changes** | ⭐ Horizontal scaling by adding a process — no ownership config, no coordination service, no manual sharding |
| Same neighbourhood pings | Consistently same `partition=` → same thread | Single-writer property visible in our own logs |
| Kill an instance | Survivor absorbs all 12 partitions | Automatic failover; new owner resumes from last commit |

### 🪟 Windows/PowerShell gotcha
```powershell
# PowerShell mangles -D args (splits at the dot). Quote the whole thing:
mvn spring-boot:run -pl processing-service "-Dspring-boot.run.jvmArguments=-Dserver.port=8092"

# Cleaner alternative — env var, session-scoped, no shell parsing fight:
$env:SERVER_PORT=8092; mvn spring-boot:run -pl processing-service
```
PowerShell parses arguments far more aggressively than bash. Anything containing `=`, `.`, or a leading `-` is a mangling candidate — **quote the whole argument**. (PowerShell 7+ also has `--%`, the stop-parsing operator.)

---

## Key takeaways (revision list)

1. **Partition → exactly one consumer.** That exclusivity *is* the lock — a structural Kafka guarantee, not our code.
2. **Partition count = parallelism ceiling.** Extra consumers idle.
3. **Only two commit orderings exist:** lose data, or duplicate it. No exactly-once across two systems.
4. **At-least-once is safe here because our writes are naturally idempotent.**
5. **Producer idempotence ≠ consumer redelivery.** Different layers.
6. **Listener is effectively single-threaded per partition** → no synchronization for per-partition state.
7. **Rebalance storms** come from poll-deadline misses. Guard with `max.poll.records × per-record time ≪ max.poll.interval.ms`.
8. **Heartbeat = alive; poll interval = making progress.** Both needed.
9. **`ErrorHandlingDeserializer` prevents the poison pill** from stalling a partition forever.
10. **`auto-offset-reset` only applies with NO committed offset** — not on restart.
11. **Throwing from a listener is the redelivery mechanism.** Swallowing exceptions destroys at-least-once.
12. **Use `CooperativeStickyAssignor`** — default rebalancing stops the world.
13. **Rebalances are the main real-world source of duplicates.**

---





## Next → Phase 2 · Session 2 — Idempotency, Dedupe & Dead-Letter Topic
Handle exactly the duplicate delivery we just watched a rebalance cause: dedup on `driverId + timestamp`, classify retryable vs non-retryable failures, and route poison messages to a dead-letter topic instead of dropping them.F