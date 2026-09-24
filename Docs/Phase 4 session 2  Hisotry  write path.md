# Phase 4 · Session 2 — The History Write Path

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Write history to Cassandra **without letting it touch the hot path** — a second consumer group, concurrent idempotent writes, and an error policy with patience the hot path doesn't have.
> **Status:** ✅ Verified — Cassandra stopped mid-stream; live tracking unaffected, history lag grew and drained on recovery, DLT empty.

---

## Part 1 — Decoupling the hot and cold paths

### 🔥 Why the obvious plan (one listener, two writes) fails
```java
redisLocationWriter.writeAll(toProcess);
cassandraWriter.writeAll(toProcess);   // ❌ three distinct failures
```

**1 · Freshness coupling.** Offsets commit only after the listener returns, so the next batch waits for *both* writes. Cassandra latency spikes — compaction, GC, slow disk — into hundreds of ms. ⭐ **A compaction storm in the HISTORY store degrades "where is my driver right now." The cold path's worst day becomes the hot path's every day.**

**2 · Failure coupling.** Cassandra down → listener throws → batch retried and dead-lettered → **the Redis writes in that batch stop with it.** A history outage becomes a live-tracking outage — exactly what the two-tier split (D-02) existed to prevent. ⚠️ **One listener quietly re-welds them.**

**3 · The dedup trap** (the subtle one):
```
1. dedup claim for ping P   → succeeds, key set for 300s
2. Redis write              → succeeds
3. Cassandra write          → FAILS
4. batch redelivered
5. dedup claim for P        → "already processed" → SKIPPED
6. Cassandra write never happens → P silently missing from history
```
D-25 accepted "rare loss" because the claim→write window was tiny. With two writers it isn't: ⚠️ **every ping during a Cassandra outage loses its history row, and nothing reports it.** The classic **dual-write problem** — two systems, no shared transaction, a partial success indistinguishable from a full one.

### ⭐ The fix: a second consumer group
```
                        ┌─ geopulse-location-processor ─► Redis      hot · 2s freshness SLO
driver-location-pings ──┤
                        └─ geopulse-history-writer ─────► Cassandra  cold · lag-tolerant
```
Same topic, different `group.id` → **own copy of every message, own offsets, own lag.** Kafka already retains 6h, so fan-out costs nothing new.

| Failure | Result |
|---|---|
| Cassandra slow | Only the history group's lag grows. Freshness untouched. |
| Cassandra down | History falls behind; live tracking carries on; Kafka holds the backlog. |
| Redis down | History keeps writing — the cold path never touches Redis. |

### ⭐ Kafka makes write-behind safe
This is **write-behind**: hot store updates promptly, durable store catches up asynchronously. Normally dangerous because a classic in-memory write-behind queue loses everything on a crash. Ours is a **replicated, replayable log**.

> ⭐ **Kafka turns write-behind from a data-loss risk into a lag metric.**

And D-03's 6-hour retention gains a concrete meaning: ⭐ **6 hours is the longest Cassandra outage we can absorb without losing history** — a number you can state in an incident review.

### ⭐⭐ The history path needs NO dedup — and a correction
Trajectory key `((driver_id, day), ts)`; occupancy key `((h3_cell, hour_bucket), ts, driver_id)`. A redelivered ping has the same driver, day and timestamp → **same primary key → upsert of identical data.**

> ⭐ **At-least-once delivery + idempotent write = exactly-once EFFECT.** No dedup, no coordination, no Redis in the loop. This is the "airtight answer" D-25 pointed toward.

🔧 **Correction to Session 2.2:** I justified dedup partly by claiming a duplicate Cassandra write *"appends a second row for the same instant."* True for a schema keyed on something other than the logical event (an ingestion UUID, say) — **false for the schema we actually designed in 4.1.**
> ⭐ **A well-chosen primary key can do the job of an entire subsystem.**

*(Open question for Phase 6: the Redis write is also idempotent via `ZADD` + the stale-write guard, so hot-path dedup may be redundant too — except for a duplicate arriving after a driver's hash expired, which could briefly resurrect them. A write-time freshness check might cover that more cheaply than a round trip per record.)*

### The honest costs
- ⚠️ **Kafka read traffic doubles** — two groups read every message. Cheap (page cache) but real.
- ⚠️ **The stores can momentarily disagree** — Redis knows a position before history records it. Fine: they answer different questions under different freshness contracts.
- ⚠️ **Two lag metrics with different meanings** — hot-path lag alarms in seconds; history lag only as it approaches 6 hours.
- ⚠️ **Same JVM for now** — a process crash still takes both down, and Cassandra must be up at boot. `TODO(Phase 7)`: separate deployable (also the right production shape — history writes are heavier and should scale independently, per D-04).

---

## Part 2 — Writing to Cassandra efficiently

### 🔥 The trap: `BATCH` is not batching
Everything so far taught that batching wins (Redis pipelining, `linger.ms`). **In Cassandra, wrapping 500 pings in a `BATCH` usually makes things slower.**

A batch goes to **one coordinator** as a single request. A *logged* batch (the default) first writes the whole batch to a **batchlog on two other nodes**, then applies each statement to its partition's replicas. Our 500 pings span up to 500 partitions scattered by hash — so the coordinator holds every mutation in memory, does extra batchlog writes, **and fans out to nearly every node anyway.**

Cassandra tries to stop you: it **warns above 5 KB**, **rejects above 50 KB**, and warns when an unlogged batch spans >10 partitions. A 500-ping batch across both tables is ~100 KB — **double the reject threshold.**

> ⭐⭐ **The unifying principle: batching wins by amortizing a fixed per-request cost paid to ONE destination.** A Redis pipeline → one server. A Kafka producer batch → one partition leader. A multi-partition Cassandra write has **many** destinations — a batch just inserts a middleman who then makes all the trips anyway.
>
> ⭐ **The Cassandra equivalent of pipelining is CONCURRENCY:** fire each write async, token-aware, straight to its owner. Total latency ≈ the **slowest single write**, not the sum.

**When `BATCH` *is* right:** (a) an **unlogged** batch where every statement hits the **same partition** — one mutation, one replica set; (b) a **logged** batch for **atomicity across denormalized tables** — textbook-wise, exactly our two-table write.

**So why not use (b)?** ⭐ **Kafka's redelivery already does the batchlog's job** — if trajectory succeeds and occupancy fails, the batch is redelivered and both are rewritten harmlessly. We pay for that machinery regardless. The cost of skipping it is a brief window where one table has a ping and the other doesn't — acceptable for history and analytics.

### Prepared statements enable token-aware routing
Parsed once; afterwards only an ID + bound values travel. But the bigger benefit: ⭐ **the prepared metadata tells the driver which variables form the partition key**, so it hashes client-side, computes the token, and sends the write **directly to a replica that owns it** — no extra coordinator hop. An unprepared string hides the partition key, so the driver picks an arbitrary coordinator that forwards.

**Prepare once, at startup.** Preparing per write adds a round trip and triggers re-preparation warnings.

### Consistency: `LOCAL_QUORUM` for history
⭐ **The rule: if R + W > RF, every read overlaps the latest write** (read-your-writes). RF=3 with `LOCAL_QUORUM` both ways → 2 + 2 = 4 > 3. `LOCAL_` keeps the quorum in-datacenter, so you never wait on a cross-region round trip.

**Why stronger than the hot path:** ⚠️ the self-healing argument (N4) is **weaker here** — a lost hot-path ping is repaired in 4s, but **a lost history point is a permanent gap; the next ping doesn't fill in the past.**

> ⭐ **Decisions compose:** Part 1 moved history off the critical path, so its latency no longer touches freshness — **decoupling is what makes stronger durability cheap.**

`setIdempotence(true)` because we *proved* these writes are repeatable — which lets the driver retry on another node mid-request and enables speculative execution. It won't do either for a statement it can't assume is safe.

### 🔥 A Cassandra null is a TOMBSTONE
Writing `null` isn't "no value" — it writes a **deletion marker** that every future read must scan past. Reads **warn above 1,000** tombstones and **FAIL above 100,000**.

> A driver that never reports `heading` plants **21,600 tombstones/day** in their partition; with all three optional fields absent, **~65,000/day** — edging toward a query that fails outright.

**Fix: leave optional values UNSET, not null.** Unset columns are simply not written. Same instinct as the Redis writer (never write the string `"null"`) — but here ⭐ **a null costs you reads forever.**

### 🔥 Buckets are a contract
`day` and `hour_bucket` are part of the primary key, so **every writer and reader must compute them identically, on any machine, forever.** Hence `TimeBuckets` lives in `common`. Two ways to break it:

**The timezone trap.** This JVM runs with `-Duser.timezone=Asia/Kolkata`. A default-zone bucket computes **IST days on your laptop and UTC days on a server** — a 20:00 UTC ping lands in tomorrow's partition here and today's there, and the reader looks in the wrong one. ⚠️ **India's half-hour offset misaligns even HOURLY buckets.** Use UTC, explicitly, always.

**The wall-clock trap.** Bucket by **device timestamp**, never arrival time. ⚠️ Bucket by arrival and a redelivery crossing midnight computes a **different partition key** — turning the idempotent upsert into a **second copy in another partition**, silently breaking the exactly-once effect Part 1 depends on.

> ⭐ **A bucket function must be a pure function of the event.**

### Layered retries
The driver may retry a statement on another node in milliseconds; the Kafka error handler retries the whole batch over seconds. ⭐ **Fine as long as each layer is bounded** — unbounded stacked on unbounded multiplies into the retry storm from 2.2.

---

## Part 3 — Wiring it in

### A DLT per consumer group
A message can be **poison for one consumer and fine for another** (history chokes on a field the hot path never reads). One shared DLT mixes both groups' failures, and replaying it re-feeds messages to a group that already handled them.
> ⭐ **A DLT belongs to a consumer group, not to a topic.**

### ⭐ Patience: the poll deadline bounds each WAIT, not the total
Spring Kafka's whole-batch retry **pauses the consumer and polls it before each backoff sleep**, so an extended retry sequence doesn't trigger a rebalance. But the reset happens only *between* retries: **if one processing attempt plus its backoff exceeds `max.poll.interval.ms`, the consumer still leaves the group.**

```
each wait:  1s → 2s → 4s … capped at 60s      (≪ 300s deadline) ✅
attempt:    ≤ ~2s (spring.cassandra.request.timeout)
total:      30 minutes of patience
```

**Why bounded at 30 min, not infinite:** long enough to ride out a node restart or rolling upgrade; short enough that an exception we **misclassified as transient** can't silently freeze a partition until 6h retention expires. ⚠️ The price: a very long outage trickles a few batches into the DLT for replay. **The real safety net is alerting on history lag (Phase 6).**

### ⭐ The flip side of deny-by-default (D-27)
Our classifier treats anything unregistered as **permanent**. The hot path's list names Redis and Spring exceptions — nothing from the Cassandra driver. Without registering `AllNodesFailedException`, `DriverTimeoutException`, `WriteTimeoutException`, `UnavailableException`, `OverloadedException`, `RequestThrottlingException`, **every Cassandra timeout would be dead-lettered as if it were poison.**

> ⭐ **Deny-by-default buys safety against unknown bugs and charges you this: every new dependency is a list you must maintain.**

### 🔥 The `CommonErrorHandler` bean trap
The history error handler is deliberately **NOT a `@Bean`**. Spring Boot wires the default listener factory with the application's `CommonErrorHandler` **only if exactly one exists.** Declare a second and the lookup becomes ambiguous — ⚠️ **the DEFAULT factory silently loses its handler, and the hot path's poison messages stop reaching the DLT.** No startup error; you'd notice only when a poison pill vanished.

### 🐛 `BatchListenerFailedException`'s index is a PROMISE
```java
throw new BatchListenerFailedException(msg, cause, i);
```
Spring commits offsets for **every record before index `i`**, on the strength of that promise.

⚠️ **The hot-path consumer from Session 2.3 had this bug:** it validated in the loop and wrote only *after* it — so throwing at index `i` committed offsets for records `0…i-1` that **were never written to Redis.** On the hot path the next ping repaired it (which is why nothing looked wrong); on the history path the same shape is **permanent loss.**

**Fix — flush before throwing**, in both consumers:
```java
if (bad) {
    writer.writeAll(collectedSoFar);   // make the promise true
    throw new BatchListenerFailedException(..., i);
}
```

### Per-path `auto.offset.reset`
The hot path would arguably use **`latest`** in production — ⚠️ *a 2-hour-old position isn't merely useless, it's **wrong** for "now."* History wants **`earliest`** everywhere — old data is exactly what it's for. Set per-listener via `properties = "auto.offset.reset=earliest"`.

### Bounded driver concurrency
`concurrency-limiting` throttler: 512 in flight, 10,000 queued, then fail fast with `RequestThrottlingException` (registered transient). ⭐ **Bounded concurrency is backpressure.**

---

## Verification

| Test | Result |
|---|---|
| Startup | **Two** `partitions assigned` lines — one per group; both visible in kafka-ui → Consumers |
| First history start | `earliest` **backfills** everything retained — Kafka as a replay log, filling a brand-new store with history it never saw live |
| Old test pings | Land in **July 2024** partitions — bucketed by *device* timestamp, exactly as designed |
| Partition-key query | `WHERE driver_id=? AND day=?` returns rows, newest first |
| **Idempotency** ⭐ | Folder 06 run 4× → 8 Kafka messages, **`COUNT(*) = 1`**. No dedup on this path — the primary key alone |
| **Cassandra stopped** ⭐⭐ | Nearby queries **pass**; hot lag **0**, history lag **grows**; retries at 1→2→4→…→60s with **no rebalance** |
| Cassandra restarted | Lag drains, outage-window pings present, **DLT empty, no manual replay**. Driver **re-prepares transparently** after the node lost its statement cache |

### 🔧 Correction to Session 4.1 (D-58)
I said TWCS struggles when old events arrive late because they "land in old windows." **Not so:** TWCS windows by each cell's **write timestamp** (when Cassandra received it), not by our `ts` column. Backfilled rows land in **today's** window and compaction is fine. ⭐ **The real cost is on reads** — one day's partition can end up spread across SSTables from several windows, so a read touches more files.

### ⚠️ Two caveats the outage test surfaced
**Health aggregation re-couples the paths.** During the outage `/actuator/health` reports **DOWN** (Cassandra is a component). ⚠️ **A Kubernetes readiness probe on that endpoint would pull the pod — taking the healthy hot path out with it, undoing Part 1 at the orchestration layer.** Fix: Spring Boot **health groups** — liveness excludes dependencies; readiness includes only what *this* deployable can't run without. `TODO(Phase 7.1)`.

**Startup still needs Cassandra.** The driver connects eagerly, so booting with Cassandra down fails the whole context — ⚠️ **the Redis hot path can't start either.** `RECONNECT_ON_INIT=true` turns crash into wait (good for dev ergonomics and crash-looping pods) but ⭐ **is cosmetic architecturally: the fix that matters is not needing Cassandra to boot the hot path at all.**

> ⭐ **Running outages are absorbed; boot-time outages aren't.** Both caveats disappear with the separate deployable.

---

## Key takeaways (revision list)

1. ⭐ **One listener re-welds the tiers you deliberately separated.**
2. **The dual-write trap:** claim-before-write + two writers = silent history loss during an outage.
3. ⭐ **Kafka turns write-behind from a data-loss risk into a lag metric.**
4. **Retention gains meaning:** 6h = the longest absorbable Cassandra outage.
5. ⭐⭐ **At-least-once + idempotent write = exactly-once effect.** A good primary key replaces a subsystem.
6. 🔥 **Cassandra `BATCH` is not batching** — it's a middleman that still makes every trip.
7. ⭐ **Batching amortizes a fixed cost paid to ONE destination.** Many destinations → use concurrency.
8. **Prepared statements enable token-aware routing**, not just parse caching.
9. **Decoupling made `LOCAL_QUORUM` affordable** — decisions compose.
10. 🔥 **A null is a tombstone.** Leave optionals unset.
11. 🔥 **Bucket functions are contracts** — UTC explicitly, from the device timestamp, shared in `common`.
12. **A DLT belongs to a consumer group, not a topic.**
13. ⭐ **The poll deadline bounds each wait, not total patience.**
14. ⭐ **Deny-by-default charges you a list per dependency.**
15. 🔥 **A second `CommonErrorHandler` bean silently disarms the default factory.**
16. 🐛 **`BatchListenerFailedException`'s index is a promise — flush before you throw.**
17. **`auto.offset.reset` differs per path:** `latest` for hot, `earliest` for history.
18. ⚠️ **Health aggregation can re-couple decoupled paths** at the orchestration layer.

---

## Next → Phase 4 · Session 3 — History Read APIs
`TimeBuckets` gets its second customer, and a multi-day query becomes **N parallel single-partition reads** driven by the application. Trajectory replay, hex occupancy, and the paging semantics that *do* work when the underlying data is immutable — unlike the live spatial search (D-49).