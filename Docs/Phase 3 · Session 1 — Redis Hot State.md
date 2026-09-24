# Phase 3 · Session 1 — Redis Hot State

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** The pipeline finally **stores** something. Current position, the spatial index, and presence — written in two round trips per batch, with no locks.
> **Status:** ✅ Verified — driver hash written and read back; TTL expiry observed live.

---

## Part 1 — The data model

### The governing constraint
**This store answers "right now," and nothing else.** Everything follows: one position per driver (not a history), held in memory, allowed to expire.

> ⭐ **~1 GB for a million drivers vs ~2 TB/day for the same events as history.** That asymmetry is the entire reason this tier exists.

### Three problems, three structures
| Question | Structure |
|---|---|
| "Where is driver X?" | Hash — point lookup |
| "Who is in cell Y?" | Sorted Set — membership + freshness |
| "Is driver X online?" | **Free** — see TTL below |

### Hash, not JSON String, for current location
| Option | Verdict |
|---|---|
| **String** holding serialized JSON | Simple, but any read pulls the whole blob and any write rewrites it |
| **Hash** with fields | ✅ **Chosen** |

**The deciding factor is memory, not field-level access** (we always write the whole thing anyway): Redis stores small hashes in a compact **`listpack`** encoding (formerly `ziplist`), meaningfully more memory-efficient than a JSON string for small objects. **At a million drivers that difference is real.** It also avoids a serialization round trip on read.

### 🔥 The move problem — where a naive implementation breaks
Driver was in cell A, now pings from cell B. Just `ZADD cell:B` and the driver is in **both** — a proximity search near A returns someone who left. ⚠️ **Ghost drivers accumulate forever.**

Every write must therefore:
```
1. read the driver's PREVIOUS cell
2. if changed:  SREM cell:{old}  →  ZADD cell:{new}
3. write the new position
```

### ⭐ Why that read-modify-write needs no lock
That sequence is exactly the pattern that would normally need a lock — two threads writing the same driver could interleave and corrupt the index.

**Except it can't happen here.** Pings are keyed by H3 res-7 cell → one partition → **one consumer thread owns every driver in a region.**

> ⭐ **This is where "the partition is your lock" stops being a slogan and does actual work.** We perform an unguarded read-modify-write on shared state, and it is safe **structurally**.

⚠️ **The one wrinkle:** a driver crossing a *partition* boundary (cells A and B on different partitions) can be touched by two threads. That's the Session 1.3 boundary race → handled by the **stale-write guard** (below).

### 🔥 Set vs Sorted Set — the per-member expiry problem
A TTL on `cell:{h3}` expires the **whole set**, evicting **every driver in that cell — including ones still actively pinging.** Wrong. Redis Sets have no per-member expiry.

| Option | Verdict |
|---|---|
| **Sorted Set**, score = timestamp; sweep with `ZREMRANGEBYSCORE` | ✅ **Chosen** |
| Redis 7.4+ hash field TTLs (`HEXPIRE`) | Genuinely per-field, but newer and less portable |

**Why the Sorted Set wins:** ⭐ **the score IS the last-seen timestamp**, so we get per-driver freshness for free *and* can filter stale members at query time (`ZRANGEBYSCORE` with a cutoff) **before any sweep runs**.

> ⭐ **Reads are correct immediately; cleanup is just housekeeping.** That's strictly better than eviction-by-TTL.

### ⭐ TTL as presence — the elegant part
We store **no "online" flag** and write **no "offline" record.**

**Key exists → pinged recently → online. Key gone → dark.**

> **What this eliminates:** an `is_online` column, a background job scanning a million rows for stale timestamps, and the race where a crashed driver app stays "online" until the sweeper catches up. **One `EXPIRE` call per write replaces all of it.**

**Why ~30 s:** pings arrive every 4 s, so 30 s tolerates ~7 consecutive misses — enough to survive a tunnel or brief network drop, short enough that a dead driver disappears fast. ⚠️ **A tunable tradeoff: false-offline vs ghost-driver.**

### The final model
| Key | Type | Contents | Expiry |
|---|---|---|---|
| `driver:{id}:loc` | **Hash** | lat, lng, ts, h3, speed, heading | **TTL 30 s**, refreshed per ping |
| `cell:{h3Cell}` | **Sorted Set** | member = driverId, score = timestamp | **swept by score** |
| `dedup:ping:{id}:{ts}` | String | `1` | TTL 300 s *(Session 2.2)* |

> ⚠️ **Two different TTLs, two different jobs — keep them straight:**
> - **Dedup 300 s** — how long we remember *"already processed this exact ping."* Sized to **redelivery latency** (rebalance = seconds, retries = ms).
> - **Presence 30 s** — how long a driver counts as *online.* Sized to **ping cadence** (~7 missed pings).
>
> The driver hash **does** get a whole-key TTL, and that's correct — the key holds exactly one driver. Only the *cell index* needed the sorted-set treatment.
>
> 🧪 **Testing gotcha:** re-sending a ping with the **same timestamp** after the driver key expires but within 300 s gets **deduped** — so the driver does *not* come back online. Correct (it's a duplicate, not a new observation) but confusing. **Always bump the timestamp for a fresh ping.**

---

## Part 2 — The batched write path

### Why two round trips, not one
A pipeline **sends all commands then collects all replies** — you cannot branch on a reply mid-pipeline. And step 1 of the move logic is a *read*. So: **read everything (trip 1) → decide in Java → write everything (trip 2).**

| Option | Verdict |
|---|---|
| **Lua script** — atomic server-side read-modify-write | Correct under any concurrency, but you're writing Lua, and per-driver invocation loses batching |
| **Read-then-write in two pipelines**, relying on the single-writer guarantee | ✅ **Chosen** — simpler, stays in Java, and correct **because of D-09** |

> ⭐ **The concrete payoff of the partition-key decision.** Interview line: *"We didn't need a Lua script or a distributed lock because the partitioning guarantees a single writer per region."*

### ⭐ The stale-write guard (last-write-wins)
```
if (prev != null && prev.timestamp >= ping.timestamp()) continue;
```
The boundary race means an **older ping can arrive after a newer one**. Without this, the driver's map marker **jumps backwards** — ⚠️ *a bug that's maddening to reproduce, because it only fires when someone crosses a partition boundary at the wrong moment.*

`>=` rather than `>`: an **equal** timestamp is a duplicate that slipped past dedup (e.g. after the 300 s window). Rewriting it is harmless but pointless.

### ⭐ `ZADD` is idempotent by nature
Re-adding an existing member just **updates its score** — which is exactly the "refresh presence" semantic we want. **A redelivered ping produces the identical end state.**

> This is the concrete instance of the claim we've been making since Session 2.1: *at-least-once is safe here because the writes are naturally idempotent.*

### Other implementation notes
- **`HMGET` just `h3` + `ts`, not `HGETALL`.** At 250k/s, transferring lat/lng/speed/heading we're about to overwrite is pure waste.
- **Optional fields only written when present.** ⚠️ Writing the string `"null"` would be worse than absent — a reader can't distinguish "not reported" from "the literal text null."
- **Opportunistic sweep:** `ZREMRANGEBYSCORE` on the cell we're already writing to. Cheap (O(log n + m), m usually 0) and it runs naturally wherever there's traffic — **no cron, no separate cleanup job.**

### ⚠️ Known gap — the sweep doesn't reach silent cells
We only sweep cells we happen to **write** to. A cell that goes **completely silent** is never swept and keeps its stale members.

**Reads stay correct** (queries filter by score), but **memory leaks slowly.** Proper fix: a background sweeper or a generous key-level TTL. → `TODO(Phase 5)`.

---

## Part 3 — Verification

### The three tests that prove the design
| Test | Method | Proves |
|---|---|---|
| **1 · Multiple drivers, one cell** | Two drivers within ~100 m → `ZRANGE` that cell | Both in one sorted set — **the primitive the entire nearby search is built on** |
| **2 · The move** ⭐ | Same driver from a different cell → `ZRANGE` both cells | **Gone from the old, present in the new.** Without `SREM` they'd be in both — *the failure that's easiest to write and hardest to notice* |
| **3 · Stale-write guard** | Send an *older* timestamp → `HGET ... ts` | Stored `ts` **unchanged** — last-write-wins protecting against the boundary race |

### ✅ TTL presence, observed live
```
HGETALL driver:driver-001:loc  →  (empty array)     # no ping yet
[send a ping]
HGETALL driver:driver-001:loc  →  lat, lng, ts, h3, speed, heading
[wait > 30s]
HGETALL driver:driver-001:loc  →  (empty array)     # TTL fired
```
> ⭐ **That third result is the feature.** No cleanup job ran. No "offline" flag was written. No tombstone. The driver stopped existing in the hot tier because they stopped reporting.

### 🔍 Reading the H3 hierarchy in the hex
```
res 9 (storage):    89608b0b13bfffff      ← fewer trailing f's
res 7 (partition):   87608b0b1ffffff      ← more trailing f's
                       ^^^^^^^ shared prefix
```
H3 IDs are fixed-width 64-bit with unused resolution digits filled with `1111` (= `f`). **A coarser cell has MORE trailing f's** — and the shared `608b0b1` shows the parent/child relationship directly.

---

## Key takeaways (revision list)

1. **Hot state is < 1 GB; the same events are 2 TB/day as history.** That asymmetry justifies the tier.
2. **Hash over JSON String** — Redis's `listpack` encoding is meaningfully more memory-efficient at a million keys.
3. **The move problem creates ghost drivers** if you don't `SREM` the old cell.
4. ⭐ **The read-modify-write needs no lock — the partition guarantees a single writer per region.**
5. **Whole-key TTL on a shared set is wrong** — it evicts active members. Sorted Set + score = per-member freshness.
6. ⭐ **Score-based filtering makes reads correct immediately;** the sweep is just housekeeping.
7. ⭐ **TTL as presence** eliminates the offline flag, the scanner job, and the crashed-app race.
8. **Two TTLs, two purposes:** dedup (300 s, redelivery latency) vs presence (30 s, ping cadence).
9. **Pipelines can't branch on replies** → read trip, then write trip.
10. **`ZADD` is naturally idempotent** — the concrete reason at-least-once is safe here.
11. **The stale-write guard is load-bearing**, not defensive padding.
12. **`HMGET` the fields you need**, not `HGETALL`.
13. ⚠️ **The opportunistic sweep misses silent cells** — memory leaks, reads stay correct.

---

## Next → Phase 3 · Session 2 — Spatial Queries
Build the k-ring "drivers near me" search on top of this index — and compare it head-to-head against Redis's built-in **`GEOSEARCH`**, the obvious alternative we've been implicitly rejecting since Session 1.2. Worth being able to argue that choice both ways.