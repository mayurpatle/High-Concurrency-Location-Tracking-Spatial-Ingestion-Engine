# Phase 2 · Session 2 — Idempotency, Dedupe & Dead-Letter Topic

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Handle the duplicate delivery a rebalance causes, classify failures as retryable vs permanent, and quarantine poison messages instead of letting them stall a partition forever.
> **Status:** ✅ Verified — dedup suppressed duplicates inside the TTL window (and let one through *at* the boundary, demonstrating the tradeoff live); poison message quarantined to the DLT with **zero retries**.

---

## Part 1 — Deduplication

### Why dedup at all, if our writes are "naturally idempotent"?
Because "idempotent" is only true of the **final** write. Look at what a duplicate actually costs:

| Store | Duplicate effect |
|---|---|
| **Redis** (current location) | `SET` twice = same result. **Genuinely idempotent.** ✅ |
| **Cassandra** (history) | ⚠️ **Appends a SECOND ROW for the same instant.** Trajectory shows two points at one timestamp; trip-distance calculations double-count. **History is permanently corrupted** — in the store whose entire job is being the durable record. |
| **Capacity** | At 250k/s, even **1% duplicates = 2,500 wasted writes/sec** against both datastores. |

> ⭐ **Dedup exists because the history store isn't idempotent and duplicates are expensive** — not because "duplicates are bad" in the abstract.

### The key: `driverId + timestamp` — the BUSINESS identity
A given driver reports one position at a given millisecond, so that pair uniquely identifies a **logical** ping regardless of how many times it's physically delivered.

**Why not Kafka offsets?** An offset identifies a *physical message*. It would catch rebalance duplicates but **NOT** a driver app that retried its POST and produced two distinct Kafka messages. The business key catches both.
> ⭐ **Dedup on the business identity, not the transport identity.**

### 🔥 Why the state CANNOT live in memory
The naive answer is a `HashSet`. It fails in a specific, fatal way:

> A partition can move to another instance during a rebalance. If dedup state lives in instance A's heap and the partition moves to instance B, **B has no idea what A already processed — and reprocesses everything.** Which is *precisely* the scenario dedup exists to prevent.
> **In-memory dedup fails exactly when you need it.**

So the state must be **shared** → Redis, which we already run.

### Doesn't this contradict "the partition is your lock"? No — two different problems:
| Mechanism | What it buys |
|---|---|
| **Kafka partition assignment** | **No concurrent access** → hence no locks needed |
| **Shared storage (Redis)** | **Continuity across reassignment** → state outlives ownership |

*This is why regional aggregates can later live in local variables **during a batch** (partition guarantee) but must be **flushed to Redis** (ownership can move).*

### The mechanism: one atomic operation
```
SET dedup:ping:driver-018:1719900016000 1 NX EX 300
```
`NX` = "only set if absent." Returns whether it succeeded:
- **true** → we're the first to see this ping → **process it**
- **false** → someone already claimed it → **skip**

⭐ **A single atomic round trip that both CHECKS and CLAIMS.** A separate `exists()`-then-`set()` would race: two threads could both see "absent" and both proceed.

### The TTL is what makes this affordable
Without expiry we'd accumulate **21.6 billion keys/day** → unbounded growth → guaranteed OOM.

At **300s**: ~75M keys ≈ **5–7 GB Redis**. Bounded. Tunable to 60s if memory is tight.

Sized to cover realistic duplicate sources:
- Rebalance-driven reprocessing: **seconds**
- Producer retries: **milliseconds**
- Client retry after timeout: **seconds**

> ⚠️ **State this plainly:** the dedup window is **bounded, not absolute**. A duplicate arriving 6 minutes late slips through. *"We prevent all duplicates"* is false; *"we prevent duplicates within a 5-minute window, which covers every realistic redelivery path"* is accurate — and much better in an interview.

### ⚠️ Claim BEFORE or AFTER the writes? (same dilemma as offset commits)
| Order | Crash window | Consequence |
|---|---|---|
| **Claim first** (ours) | between claim and write | ping **lost** — redelivery is suppressed |
| Write first | between write and claim | ping **duplicated** |

We claim first: **rare loss beats rare duplication**, because location self-heals in 4s while corrupted history does not. Consistent with every other choice in this system.

> 💡 **The genuinely airtight answer** is making the *write itself* idempotent — which Cassandra can do via its primary key (Phase 4.1). Dedup then becomes an **optimization to save capacity** rather than a correctness mechanism.

### Failing OPEN when Redis is down
```java
return claimed == null || claimed;   // null = Redis call failed → treat as claimed
```
If Redis is unreachable: drop everything (dedup unavailable → refuse) or process everything (accept possible duplicates)? **We process.** A duplicate is a minor data-quality issue; dropping all pings is a total outage.

> ⭐ **Ask "what happens when this dependency is down?" for every dependency you add** — and make the answer a deliberate choice, not an accident of exception handling.

---

## Part 2 — Error classification

### Two fundamentally different failures
| Type | Meaning | Response |
|---|---|---|
| **Transient** | The operation failed but *might succeed later*. Cassandra timeout, Redis reset, network blip, node restarting. **Nothing is wrong with the message.** | **Retry with backoff** |
| **Permanent** | Will *never* succeed. Malformed payload, missing required field, a bug in our parsing. **The message is unprocessable.** | **Quarantine immediately** |

### 🔥 Why retrying a permanent failure is *harmful*, not just wasteful — the poison pill
```
consumer fails → doesn't commit → redelivers same message → fails → FOREVER
```
That partition **stops advancing**. Every ping behind it — every driver in that neighbourhood — is stuck.
**One malformed message takes an entire region offline indefinitely.**

> ⚠️ **And the consumer looks HEALTHY.** It's polling, heartbeating, logging. Nothing crashes. It's just not making progress on one partition — and unless you watch **per-partition lag**, you won't notice until someone asks why drivers in Andheri vanished.

### Retry strategy: exponential backoff, bounded
**Not immediately** — hammering an overloaded dependency with instant retries makes it *more* overloaded. That's the **retry storm**: a struggling dependency gets a traffic spike precisely when it can least handle one, turning a degradation into an outage.

**Bounded (7s total: 1s → 2s → 4s)** — retries happen *inside* the listener, so they consume the `max.poll.interval.ms` budget. Retry too long → Kafka evicts you for not polling → **a transient blip becomes a rebalance storm.**

> ⭐ **A bounded failure is infinitely better than an unbounded one.**

**Caveat:** in-listener retry **blocks that consumer thread** — during a 7s retry sequence that partition processes nothing. Fine for us (outages are rare, location self-heals). At larger scale: Spring Kafka's non-blocking **`@RetryableTopic`**, which parks failures on separate delay topics so the main flow keeps moving. *Name it in interviews as what you'd reach for if retry latency became a problem.*

### ⭐ Classification: DENY BY DEFAULT
```java
handler.setClassifications(Map.of(
        QueryTimeoutException.class,            true,
        TransientDataAccessException.class,     true,
        RedisConnectionFailureException.class,  true,
        SocketTimeoutException.class,           true
), false);   // ← everything else: NOT retryable
```

**Why this way round:** an exception type you didn't anticipate is far more likely a **code defect** than a passing network blip. Retrying a defect wastes 7s per record and can stall a partition; quarantining it surfaces the bug immediately.

> ⭐ **Enumerating what's PERMANENT is unbounded** (you can't predict every bug). **Enumerating what's TRANSIENT is a short, knowable list** — timeouts and connection failures. An allow-by-default blocklist gives the dangerous behaviour to everything you forgot.

---

## Part 3 — The Dead-Letter Topic

### `return` → `throw` — the meaningful change from 2.1
In 2.1 we silently dropped undeserializable records to keep the partition moving. Pragmatic, but corrupt data vanished without trace. Now it's quarantined with full failure metadata.
> ⭐ **A dropped message is a mystery; a DLT'd message is a bug report.**

### 🔥 The bug that cost us three attempts: a failing recoverer becomes a retry loop
**Symptom:** `Record in retry and not yet recovered`, repeating. DLT empty.

**Two wrong diagnoses first** (both aimed at the exception classifier — which was fine). **Actual root cause:**

`processing-service` had **no producer configuration** — we'd only ever written a consumer block. Spring Boot's default producer serializers are `StringSerializer` for key **and value**. But DLT records arrive in two shapes:

| Failure type | DLT record value |
|---|---|
| Deserialization failure | raw **`byte[]`** that failed to parse |
| Listener failure | a deserialized **`LocationPing`** |

`StringSerializer` handles **neither** → publish throws → **recoverer fails** → `DefaultErrorHandler` puts the record **back into retry** → forever.

**Fix:** a dedicated `KafkaTemplate` with `DelegatingByTypeSerializer`, which picks the right serializer per payload type.

> ⭐ **A recoverer that can fail turns your safety net into another retry loop.** The DLT path deserves the same *"what if this dependency is down?"* scrutiny as the main path. Production setups often add `setResetStateOnRecoveryFailure(false)` or a fallback that logs-and-drops after N recovery attempts, so a broken DLT can't stall the pipeline.

### ⚠️ Misleading log wording (worth knowing)
`RetryListener.failedDelivery` fires on **attempt 1 even for records headed straight to the DLT.** So a single "attempt 1 failed" line does **NOT** prove retries are happening. The real signal was `not yet recovered` — which says the **recoverer** was failing, not the classifier.

### ⚠️ DLT topic naming differs by Spring Kafka version
We hand-created `driver-location-pings.DLT`; our version auto-created **`driver-location-pings-dlt`**. Result: messages landed in the auto-created topic while we stared at the empty hand-made one.

**Fix — pin the destination explicitly:**
```java
new DeadLetterPublishingRecoverer(dltKafkaTemplate,
        (record, ex) -> new TopicPartition("driver-location-pings-dlt", -1));
```
`-1` = let Kafka choose the partition. (The recoverer otherwise tries to **mirror the source partition number**, which fails when the DLT has fewer partitions than the main topic — the benign `non-existent partition` warning.)

### DLT topic config — sized for HUMANS, not throughput
```
Partitions: 3          (failures are rare; no parallelism needed)
Retention:  7 days     ← vs 6 HOURS on the main topic
```
> ⭐ The main topic is a **transport buffer**; the DLT is a **forensic record**. You need time to notice the alert, investigate, fix, and replay. **Retention here is sized to human response time.** Config follows purpose.

---

## Verification

### Test 1 — Dedup (accidentally a perfect experiment)
Sent the same ping (identical `driverId` + `timestamp`) repeatedly. Kafka received **7 messages**; the consumer logged **2**.

| Offset | Time | Outcome |
|---|---|---|
| 8 | 11:14:30 | **first sighting → claimed → processed** ✅ |
| 9–12 | 11:14:32 → 11:18:12 | within 5 min of offset 8 → key alive → **deduped** ✅ |
| 13 | 11:20:58 | **6m 28s after offset 8** → 300s TTL had **expired** → claimed again → processed |
| 14 | 11:21:16 | within 5 min of offset 13 → **deduped** ✅ |

> ⭐ **The one that got through crossed the TTL boundary** — a live demonstration of the bounded-window tradeoff, with real timestamps. Couldn't have designed a cleaner illustration.

Redis confirmed a single key per logical ping:
```powershell
docker compose exec redis redis-cli KEYS "dedup:ping:driver-018*"
# → dedup:ping:driver-018:1719900016000     (ONE key for 3-4 identical requests)
docker compose exec redis redis-cli TTL "dedup:ping:driver-018:1719900016000"
# → a number < 300, counting down = the memory bound working
```
> 🔍 `TTL` returning **-2** means the key doesn't exist (not "no expiry" — that's -1).

### Test 2 — The poison pill 🧪
**Produce malformed JSON directly to the topic**, bypassing the validated endpoint (which would never allow it):
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --topic driver-location-pings --bootstrap-server localhost:9092
```
At the `>` prompt, paste this deliberately-broken line, press Enter, then Ctrl-C:
```
{"this is not":"a valid ping"
```

**What to look for:**

| Check | Expected | Meaning |
|---|---|---|
| Consumer log | **ONE** `Delivery attempt 1 failed` — **no attempt 2 or 3** | Classification works: bad bytes will never become valid JSON on attempt 4 |
| `driver-location-pings-dlt` | The message appears | Recoverer published successfully |
| DLT message **Headers** | `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset` | ⭐ **This metadata is the whole payoff** — trace any quarantined message to its exact origin and cause |
| Send a normal ping after | Flows through **immediately** | ⭐ **The partition kept moving.** Without this machinery, one malformed message would block that partition — and every driver in that neighbourhood — forever |

**Result:** ✅ Confirmed. 2 poison messages quarantined, zero retries, main topic unaffected.

---

## Deep dive: `max.poll.records` vs `max.poll.interval.ms`

The naming misleads — the second is a **deadline**, not a limit on a quantity.

```
while (true) {
    records = consumer.poll()   // max.poll.records = how many you get (default 500)
    process(records)            // your listener runs, once per record
}                               // max.poll.interval.ms = deadline to get BACK here
```

**`max.poll.interval.ms` (default 300000 = 5 min):** *"You polled at 10:00:00. If I don't hear another poll() by 10:05:00, I'll assume you're dead"* → **evicted from the group, partitions reassigned.**

### Why TWO timeouts?
| Check | Runs on | Question it answers |
|---|---|---|
| `heartbeat.interval.ms` / `session.timeout.ms` | **background thread** | **Is the process alive?** |
| `max.poll.interval.ms` | main consumer loop | **Is the process making progress?** |

⭐ A consumer stuck in a slow Cassandra write is **alive and heartbeating while making zero progress.** Heartbeats can't detect that — hence the second check.

> 📜 **History:** `max.poll.interval.ms` was introduced in Kafka 0.10.1 precisely because the old design conflated these. Before it, session timeout covered both, so anyone with slow processing needed a huge session timeout — meaning real crashes took forever to detect. Splitting them gives a **fast liveness check** (3s heartbeat / 45s session) alongside a **slow progress check** (5 min).

### The arithmetic
```
time between polls = max.poll.records × time to process one record
healthy:   500 × 5ms   = 2.5s      vs 300s deadline  ✅ huge margin
degraded:  500 × 800ms = 400s      vs 300s deadline  ❌ EVICTED
```
**You didn't crash. You just got slow. Kafka evicts you anyway** → partitions move to consumers already struggling with the same slow dependency → they blow the deadline → **rebalance storm.**

**Two levers:** lower `max.poll.records` (usually better) or raise `max.poll.interval.ms` (also delays detecting a *genuinely* stuck consumer).

> ⭐ **Rule: `max.poll.records × WORST-CASE per-record time ≪ max.poll.interval.ms`.** Worst-case, not average — the deadline is blown by your bad days.

### 🔬 Provenance of our numbers — measured vs assumed
| Number | Source |
|---|---|
| `max.poll.interval.ms = 300000` | **Kafka default**, written out explicitly *for visibility* — a setting you can't see is one you won't think about |
| `session.timeout.ms = 45000` | Kafka default |
| `heartbeat.interval.ms = 3000` | Kafka default (~⅓ of session timeout) |
| `max.poll.records = 500` | Kafka default |
| **~5 ms/record** | ⚠️ **OUR ESTIMATE** — Redis write (0.2–1ms) + dedup `SET NX` (~0.5ms) + Cassandra write (1–5ms) + negligible CPU ≈ 2–7ms **sequential**. Batching/pipelining (Session 2.3) pushes it lower. **To be measured in Phase 6.** |
| **~5,000 records/sec/consumer** | ⚠️ **OUR ESTIMATE**, derived from the above — and the basis of the 64-partition figure from Session 1.3 |

> ⭐ **The habit:** when you write a number in a config or design doc, know whether it's **measured, derived, defaulted, or guessed** — and say which. Most tuning-related production incidents come from someone treating a guess as a measurement.
>
> **Interview framing:** *"I estimated ~5ms from typical Redis and Cassandra latencies, then measured it under load in Phase 6."* If the measured p99 turns out to be 15ms, the partition math from Session 1.3 changes and we revisit it.

---

## Key takeaways (revision list)

1. **Dedup exists because Cassandra history is NOT idempotent** — and duplicates cost real capacity.
2. **Dedup on business identity** (`driverId + timestamp`), not transport identity (offsets).
3. **In-memory dedup fails exactly when needed** — during the rebalance it's meant to protect against.
4. **Partition guarantee = no concurrency; shared storage = continuity across reassignment.** Different problems.
5. **`SET NX EX` checks and claims atomically** in one round trip.
6. **The dedup window is bounded, not absolute** — say so out loud.
7. **Claim-before-write trades rare loss for rare duplication.** The airtight fix is an idempotent write (Phase 4.1).
8. **Fail open when Redis is down** — a duplicate beats a total outage.
9. **Transient = retry with backoff; permanent = quarantine now.** Conflating them → infinite loops or lost data.
10. **The poison pill stalls a partition while the consumer looks healthy.** Watch per-partition lag.
11. **Bound your retries** — they burn the poll-interval budget.
12. **Deny-by-default classification** — the permanent list is unbounded, the transient list is short.
13. **A failing recoverer becomes another retry loop.** Scrutinize the DLT path too.
14. **Pin the DLT topic name explicitly** — framework conventions differ across versions.
15. **DLT retention is sized to human response time**, not throughput.
16. **`max.poll.records` = how much you bite off; `max.poll.interval.ms` = how long you may chew before Kafka assumes you choked.**

---

## Next → Phase 2 · Session 3 — Batch Consumption & Throughput Tuning
Where the last `log.info` dies. Batch listeners, pipelined writes, concurrency tuning, and backpressure — making the consumer fast enough to keep up with the firehose, and replacing that ~5ms estimate with something we've actually reasoned about.