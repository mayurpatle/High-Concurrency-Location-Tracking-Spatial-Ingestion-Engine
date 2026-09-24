# Phase 2 · Session 3 — Batch Consumption & Throughput Tuning

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Stop processing records one at a time. Batch the downstream I/O, tune concurrency, and remove the last blocking log from the hot path.
> **Milestone:** 🏁 **Phase 2 complete.** The processing pipeline is built — only the writes themselves remain.

---

## Part 1 — Batch consumption

### What was already happening
`poll()` has **always** returned up to 500 records — Kafka fetches in batches natively. But our listener signature took a *single* `ConsumerRecord`, so Spring looped and called us 500 times. **We were receiving in bulk and processing one at a time.**

Fine when the listener just logs. Ruinous once it does I/O.

### ⭐ The round-trip arithmetic (the core insight)

**Per-record (before):**
```
500 records × (1 Redis round trip + 1 Cassandra round trip)
= 1,000 network round trips per poll
= 500 × ~2ms  ≈  1 SECOND per batch
```

**Batched (after):**
```
500 records → 1 pipelined Redis call + 1 batched Cassandra call
= ~2 network round trips per poll
≈ 20–40ms per batch
```

**≈25× fewer round trips.**

> ⭐ **Notice WHAT we eliminated: network latency, not computation.** Each round trip costs ~1ms whether it carries 1 record or 500 — **you pay for the TRIP, not the payload.** Amortizing that fixed cost is the single biggest throughput lever in any I/O-bound pipeline.

This is **the same principle as the producer's `linger.ms`** (Session 1.3) — same insight, opposite end of the pipe: **batch the fixed cost, not the variable one.**

### 🔬 This revises the ~5ms/record estimate
Sequentially it was 2–7ms of round-trip latency per record. **Batched, the per-record cost collapses toward the *marginal* cost of adding one more item to an existing request — tens of microseconds.** The old estimate was for the wrong architecture. Phase 6 measures the real number.

### The tradeoffs — batching isn't free

| Cost | Detail |
|---|---|
| **Failure granularity coarsens** ⚠️ | An exception fails the **whole batch**; offsets commit per batch → **all 500 redelivered**, including the 499 that succeeded. *This is why the 2.2 dedup matters MORE now, not less — it makes wholesale redelivery survivable.* |
| **Memory grows** | 500 records held at once, × `concurrency` batches in flight. At ~200 B that's ~100 KB/batch — trivial here, but check your heap if you raise `max-poll-records` to thousands. |
| **Latency floor rises** | A record arriving early waits for the batch to fill and process. Bounded by poll timing → milliseconds. Irrelevant against our **2-second freshness SLO**. |

### ⭐ `BatchListenerFailedException` — what keeps batching safe
```java
throw new BatchListenerFailedException(msg, cause, index);   // ← the INDEX
```
Spring commits everything **before** that index and sends **only that record** to the DLT.

> Without it: one malformed record in a batch of 500 means all 500 are redelivered — and if that record is *permanently* broken, you loop forever **on the whole batch**. With it you get **per-record failure isolation AND batch throughput.** That's the combination you want.

### 🔧 Why the dedup call is deliberately left un-batched
It's still one Redis round trip *per record* — the last un-batched I/O in the method. Pipelining `SET NX` and reading back 500 individual results needs `executePipelined` or a Lua script: real complexity, unmeasured benefit.

> ⭐ **Optimizing before measuring is how you end up with complicated code that's no faster.** Left as a `TODO(perf)` with the reasoning written down. Phase 6 decides.

### The write layer must be batch-shaped
```java
// TODO(Phase 3): redisWriter.writeAll(toProcess)     — one pipelined call
// TODO(Phase 4): cassandraWriter.writeAll(toProcess) — one batched call
```
Both take the **List**, not a single ping. **That signature is the whole point of this session** — if the write layer is record-shaped, the round-trip savings never materialize.

---

## Part 2 — Concurrency & backpressure

> ⚠️ **Honest framing:** most of this is knobs you cannot set correctly without measurement. Sane starting values below; real numbers come from Phase 6. **Tuning by intuition is how you end up slower.**

### Concurrency: ceiling and floor
| Bound | Reason |
|---|---|
| **Ceiling** | Threads beyond your assigned partitions sit **idle**. 12 partitions, 1 instance → `concurrency: 12` is the max useful value. |
| **Floor** | Too few and a slow batch on one partition idles the instance. `concurrency: 1` means a Cassandra hiccup on partition 3 blocks partitions 0–11 too. |

⭐ **Our work is I/O-bound WAITING, not CPU** — so threads-per-core intuitions don't apply. A thread blocked on a Cassandra response burns no CPU.

> **The binding constraint is usually the downstream connection pool.** 4 threads writing to Cassandra need 4 connections. Set concurrency above your pool size and threads queue on connection acquisition — **worse than not having them.**
>
> ⭐ **Rule: concurrency should match your downstream write capacity, not your core count.**

### Fetch tuning (a knob you probably shouldn't touch yet)
```yaml
fetch.min.bytes: 65536      # wait for 64KB → fewer, fatter broker requests
fetch.max.wait.ms: 100      # ...but never wait longer than this
```
The default `fetch.min.bytes: 1` means "return immediately with whatever exists" — latency-optimal, throughput-poor. `fetch.max.wait.ms` is the **latency bound**, the exact mirror of the producer's `linger.ms`. Under load 64KB fills in microseconds and it never triggers; when traffic is sparse it stops us stalling.

> Defaults are usually fine. Reach for this **after** measuring a bottleneck at the fetch layer — shown here mainly so you recognize it.

### ⭐ Backpressure — we get it for free (a good interview point)
**The consumer only fetches when it's ready.** If Cassandra slows, our listener takes longer, we `poll()` less often, and Kafka simply **holds the messages**. No queue to overflow, no memory to exhaust — **Kafka's log IS the buffer**, already sized for it (6h retention).

> Compare a **push** model: a producer shoves messages at a consumer regardless of readiness → you need explicit flow control, buffering, and a drop policy when buffers fill.
> ⭐ **Kafka's pull model gives us backpressure structurally.**

**The one thing backpressure can't fix: the poll deadline.** Slow down enough and you're evicted (Session 2.2 arithmetic). The safety valve is bounding batch size:
```
500 × worst-case per-record time  ≪  max.poll.interval.ms
batched: 500 × ~60µs ≈ 30ms   vs   300,000ms   ✅
```
If Phase 6 shows otherwise, `max-poll-records` is the number to lower.

### Consumer lag is the health metric
**Lag is the observable symptom of backpressure.** Steady non-zero lag is fine. ⚠️ **GROWING lag is the alarm** — it means freshness is degrading and will keep degrading. Alert wired in Phase 6.1.

---

## 🗑️ The last log dies
The success-path `log.info` and `@Slf4j` are removed. The consumer is now **silent on success** — which is correct.

> ⭐ **On a hot path: logs are for exceptions, metrics are for everything else.** Counting is an in-memory increment scraped periodically; logging is a **blocking disk write per batch**.

**Verification without logs:** check **consumer lag in kafka-ui** (Consumers → `geopulse-location-processor`). Send pings → lag spikes → returns to zero. That's the health signal from here on.

> Losing the ability to eyeball it working is exactly **why observability isn't optional** — and why Phase 6 exists.

---

## Key takeaways (revision list)

1. **You pay for the network TRIP, not the payload** — ~1ms whether it carries 1 record or 500.
2. **Batching downstream I/O ≈ 25× fewer round trips.** Biggest lever in an I/O-bound pipeline.
3. **Same principle as `linger.ms`**, opposite end of the pipe.
4. **Batching coarsens failure granularity** → dedup matters more, not less.
5. **`BatchListenerFailedException` + index** = per-record isolation *with* batch throughput.
6. **The write layer must take a List**, or the savings never materialize.
7. **Don't optimize before measuring** — the un-batched dedup call stays, with a documented TODO.
8. **Concurrency matches downstream write capacity, not core count.**
9. **I/O-bound waiting ≠ CPU-bound work** — thread-per-core intuitions don't apply.
10. **Pull-based consumption gives backpressure structurally.** Kafka's log is the buffer.
11. **The poll deadline is what backpressure can't save you from** — bound your batch size.
12. **Growing lag is the alarm**, not non-zero lag.
13. **Count, don't log, on the hot path.**

---

## 🏁 Phase 2 complete — where the pipeline stands

```
HTTP → validate → quality gate → H3 enrich → Kafka (keyed by region)
     → consumer group (one owner per partition) → dedup → [writes pending]
```

Everything in that chain is built **except the writes themselves**. The consumer receives batches, isolates poison records to the DLT, suppresses duplicates, retries transient failures with bounded backoff, and rebalances cleanly across instances. It just doesn't store anything yet — `toProcess` is assembled and dropped.

## Next → Phase 3 · Session 1 — Redis Hot State
Where the system becomes **queryable**. Current location with TTL-based presence (offline detection for free), the H3 cell sets that make "drivers near me" an O(1) lookup, and the k-ring search designed back in Session 1.2 finally doing real work.