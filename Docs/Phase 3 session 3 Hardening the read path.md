# Phase 3 · Session 3 — Hardening the Read Path

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Make the read path survive load and failure — selective caching, timeouts, circuit breaking, honest degradation, and result metadata.
> **Milestone:** 🏁 **Phase 3 complete.** The full loop closes: a coordinate goes in one end and comes out as an answer to a spatial question.

---

## Part 1 — Caching

### ⭐ The rule: cache what is expensive-or-repeated AND tolerates staleness
**Not "cache reads."** Apply the test that actually matters: *what does the cache save, and what does it cost in correctness?*

| Endpoint | Cached? | Reasoning |
|---|---|---|
| `/drivers/nearby` | ❌ **No** | Already 2 round trips to an **in-memory** store (~2–5 ms). A cache saves milliseconds and costs **staleness on data whose entire value IS freshness** — on top of the 2 s pipeline lag we already have. |
| `/drivers/{id}/location` | ❌ **No** | Same reasoning; it's a single `HGETALL`. |
| `/cells/{h3}/count` | ✅ **Yes** | **Many callers asking the same question** (Project 3's pricing engine polls hundreds of cells repeatedly) **AND the answer tolerates staleness** — a 5-second-old supply count is fine for "is this area busy?" |

### 🔥 The thundering herd — what caching *introduces*
A popular cell's entry expires at T=5 s. In that instant **500 concurrent requests all miss and all hit Redis.**

> ⚠️ **The cache didn't reduce load — it SYNCHRONIZED it into a spike.** With an expensive backing call, those simultaneous requests can take the datastore down.

**Three standard mitigations** (knowing all three is worth more than implementing one):

| Mitigation | How | Cost |
|---|---|---|
| **Locking / single-flight** | First misser acquires a lock and does the work; others wait for its result | Coordination; waiters block |
| **Probabilistic early expiry** ("XFetch") | Each request has a growing chance of refreshing *before* expiry → herd spread across time | Elegant, lock-free, less obvious |
| **Stale-while-revalidate** | Serve the stale value immediately, refresh in background | Best latency — and free if staleness is already acceptable |

### 🐛 `refreshAfterWrite requires a LoadingCache`
Caffeine's `refreshAfterWrite` needs a `CacheLoader` to know *how* to reload. `CaffeineCacheManager` doesn't supply one for `@Cacheable` methods.

**Chose: drop the refresh, keep a short `expireAfterWrite(5s)`.**

> ⭐ **The reasoning:** stale-while-revalidate earns its complexity when the backing operation is **expensive** — a Cassandra aggregation, an external API call. Here it's a single `ZCOUNT` against memory. Paying a `CacheLoader` plus `ObjectProvider` gymnastics (to dodge the circular dependency, since the cached service would depend on the cache manager) to protect Redis from a burst of O(log n) lookups is **optimizing the wrong thing**.
>
> **The interview-worthy outcome:** we identified the thundering herd, chose the simpler mitigation deliberately, and can name the sophisticated one and when it *would* be warranted. Better than implementing it blindly or not knowing it exists.

### ⚠️ This is a per-instance cache
Three query pods = three independent caches, so a client can see different counts depending on routing. Fine for a density figure. Cluster-wide consistency would mean caching **in** Redis — *but then you're caching Redis in Redis*, which only makes sense when the underlying computation is expensive, not a single `ZCOUNT`.

**Bounded `maximumSize(10_000)`:** ⭐ *an unbounded cache is just a memory leak with good PR.*

---

## Part 2 — Graceful degradation

### ⭐ A slow dependency is more dangerous than a dead one
If Redis **hangs**, our query thread blocks. Enough concurrent requests and **every thread in the pool is blocked** — now the whole query service is unresponsive, *including endpoints that never touch Redis*, like `/actuator/health`.

> **A dead dependency fails fast and you handle it. A slow one holds your resources hostage while looking alive.** That's cascading failure.

### Three defences

**1 · Timeouts — the floor. Never make an unbounded network call.**
```yaml
spring.data.redis:
  timeout: 200ms          # THE most important setting in the file
  connect-timeout: 500ms
  lettuce.pool:
    max-active: 16        # caps how many threads can wait on Redis at once
    max-wait: 100ms       # fail fast rather than queue indefinitely
```
200 ms is generous: our p99 SLO for the *whole query* is 100 ms, so a single Redis call taking 200 ms means something is already wrong.

**2 · Circuit breaker — after repeated failures, stop calling entirely.**
```yaml
sliding-window-size: 20
failure-rate-threshold: 50
minimum-number-of-calls: 10        # don't trip on a tiny sample
wait-duration-in-open-state: 10s
permitted-number-of-calls-in-half-open-state: 3
```
> ⭐ **It protects the dependency as much as it protects us.** Without it, our retries become a **DDoS on a recovering Redis.**

**3 · Fallback — and this is a PRODUCT decision, not a technical one.**

| Endpoint | On failure | Why |
|---|---|---|
| `/drivers/nearby` | **`503`** | "No drivers found" would be a **lie a matching engine acts on** — and Project 3 would read it as zero supply, potentially triggering surge |
| `/drivers/{id}/location` | **`503`, NOT `404`** | `404` means *"definitely offline."* ⚠️ **We don't know that — we failed to look.** During an outage that's a lie about every driver in the fleet |
| `/cells/{h3}/count` | **`-1`** (not `0`) | Density tolerates staleness, but `0` reads as "no supply" — exactly the wrong signal. `-1` means *unknown* |

> ⭐⭐ **The principle: never return a value that looks like a valid negative answer when you actually failed to answer. Failing loudly beats lying quietly.**

`503` + `Retry-After: 10` (matching the breaker's open window) lets a well-behaved client back off instead of hammering us.

### ✅ Verified
```powershell
docker compose stop redis
```
- `/drivers/nearby` → **503** with `Retry-After`
- `/drivers/{id}/location` → **503**, *not* 404
- `/actuator/health` → **still responds** (the service isn't wedged)
- After ~10 calls, responses become **instant** rather than waiting 200 ms — ⭐ *that speed-up IS the breaker working*
- `docker compose start redis` → recovers automatically after the open window, **no restart**

---

## Part 3 — Result semantics

### 🔥 Why there is NO offset pagination
The instinct is `?offset=50&limit=50`. **Don't.**

> ⭐ **Offset pagination assumes a STABLE ORDERING between requests.** Our drivers move **every four seconds**. By the time page 2 is requested the distance ordering has changed — drivers shifted, some went offline, new ones pinged in. You'd get **duplicates on some pages and silently skip others.**
>
> **You cannot paginate a result set that reorders itself between requests.**

**The correct answer for a live spatial query is radius expansion.** A matching engine needing more candidates re-queries with a **larger radius** — semantically a *different question* with a naturally superset answer. That's why the API exposes `radiusMetres` and not `offset`.

> *"Why no pagination?" is exactly the question that separates copying a REST template from understanding your data.*

### Two gaps a bare array can't close
1. **"No drivers" vs "no drivers *nearby*"** — did we search and find nothing, or is it 4 am? The caller needs to know whether to widen or give up.
2. **Truncated vs complete** — `limit=50` returning 50: were there exactly 50, or 400?

### `NearbyResponse`
```java
record NearbyResponse(
    List<NearbyDriver> drivers,
    int     totalFound,     // matched within radius BEFORE the limit
    boolean truncated,      // totalFound > limit
    double  radiusMetres,   // echoed: what was actually searched
    int     cellsScanned,   // debugging + capacity work
    String  originCell      // correlate with /cells/{h3}/count
)
```
⚠️ **Capture `totalFound` BEFORE truncating** — after `subList()` the information is gone.

`cellsScanned: 169` is the k-ring math confirmed in the response — a nice debugging affordance.

---

## 🏁 Phase 3 complete — the loop closes

```
HTTP → validate → H3 → Kafka → consume → dedup → Redis
                                                    ↓
                                          Query API → "who's near me?"
```

A coordinate goes in one end and comes out as an answer to a spatial question. Everything between is **asynchronous, partitioned by geography, lock-free, deduplicated, poison-resistant, and degrades gracefully** when its dependency fails.

⭐ **The two queries Projects 2 and 3 need are live:** `nearby` is the matching primitive; `cellCount` is the surge signal.

---

## Key takeaways (revision list)

1. ⭐ **Cache what's expensive-or-repeated AND stale-tolerant** — not "cache reads."
2. **Don't cache data whose entire value is freshness.**
3. ⭐ **The thundering herd: a cache can SYNCHRONIZE load rather than reduce it.**
4. **Three mitigations:** single-flight, probabilistic early expiry, stale-while-revalidate.
5. **Stale-while-revalidate earns its complexity only when the backing call is expensive.**
6. **An unbounded cache is a memory leak with good PR.**
7. ⭐ **A slow dependency is more dangerous than a dead one** — it holds resources hostage while looking alive.
8. **Never make an unbounded network call.** Timeouts are the floor.
9. ⭐ **The circuit breaker protects the dependency as much as you** — retries against a recovering service are a self-inflicted DDoS.
10. ⭐⭐ **Never return a valid-looking negative when you failed to answer.** `503`, not `404`; `-1`, not `0`.
11. **Fallback choice is a product decision**, and it differs per endpoint.
12. ⭐ **You cannot paginate a result set that reorders itself.** Radius expansion, not offset.
13. **Return metadata, not a bare array** — `totalFound` + `truncated` let the caller decide what to do next.

---

## Next → Phase 4 · Session 1 — Cassandra Data Modelling
Redis knows where everyone is **now**; nothing knows where they've **been**. Cassandra rewards **query-driven schema design** — you model tables around the *questions you'll ask*, not around entities — which feels backwards coming from relational thinking. Partition and clustering key design, and why the same data gets written to two different tables.