# Phase 3 · Session 2 — Spatial Queries

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Build "who's near me" on the H3 index — and be able to argue, in both directions, why we didn't just use Redis `GEOSEARCH`.
> **Status:** ✅ Verified — nearby search returns distance-sorted results, the over-inclusion filter excludes in-ring-but-out-of-radius drivers, `404` = offline.
> **Milestone:** 🏁 **The system answers questions**, not just absorbs data.

---

## Part 1 — H3 cell sets vs Redis `GEOSEARCH`

### The question an interviewer WILL ask
Redis has had native geospatial support since 3.2:
```
GEOADD    drivers 72.8295 19.0596 driver-001
GEOSEARCH drivers FROMLONLAT 72.8295 19.0596 BYRADIUS 2 km ASC
```
**Two commands.** No H3, no k-rings, no cell bookkeeping, no move logic. Returns drivers sorted by true distance. **Genuinely less code than what we built.**

### What Redis GEO actually is
Not a separate data type — a **Sorted Set whose score is a geohash** (a 52-bit interleaving of lat/lng into one number). `GEOSEARCH` computes which geohash ranges cover your radius, range-scans them, then filters by true distance. Clever and well-implemented.

### 🔥 The problem is one word: Sorted Set. **Singular.**
Every driver in your entire coverage area lives in **one key**. And **a Redis key lives on one node.**

| | Redis GEO | H3 cell sets |
|---|---|---|
| Keys | **One** for the whole city | **One per cell** |
| Redis Cluster | ⚠️ **Cannot shard** — a single key is atomic and indivisible | ✅ Keys distribute across hash slots naturally |
| Read ceiling | **One machine, forever.** Adding nodes does nothing | Queries for different neighbourhoods hit different nodes |

> ⭐ **This is the same reason we rejected bounding boxes in Session 1.2 — a range can't be sharded, a discrete key can.** Consistent principle, different layer.

### Three more reasons (descending importance)
1. **We already have the cell IDs.** Computed at ingestion, carried on every message, used as the Kafka *and* Cassandra partition key. `GEOADD` would mean maintaining a **second** spatial index alongside the one we need anyway — two things to keep consistent, for one query.
2. ⭐ **Cells are a shared vocabulary across the whole system.** Project 3's surge pricing asks *"how many drivers in this hex?"* → `ZCOUNT cell:{h3}`, O(log n). With GEO you'd run a radius search and count — a fundamentally different, slower operation. **The cell is a unit of aggregation, not just a search index.**
3. **Cell membership is precomputable and cacheable.** *"Which drivers are in cell X"* is a stable answer. *"Which drivers are within 2 km of this arbitrary point"* is unique per query.

### ⚖️ The honest other side (a one-sided comparison is a weak answer)
- **`GEOSEARCH` is MORE ACCURATE** — true haversine distance, sorted. Our k-ring returns *cells overlapping a radius*, which over-includes at the edges; we filter in Java. **Less code in Redis, more code in us.**
- **It's dramatically simpler** — no move logic, no `SREM`/`ZADD` bookkeeping, no ghost drivers. **All of Session 3.1's complexity disappears.**
- ⭐ **For a single-node deployment it is probably the better choice.** Its main weakness never bites if you're never sharding.

### 🎤 The framing to use
> *"GEOSEARCH is simpler and more accurate, and I'd use it for a single-node system. We use H3 cell sets because a GEO index lives in one key and therefore one node — it can't shard. Our cell keys distribute naturally across a cluster. We're also already carrying H3 cells for Kafka partitioning and Cassandra keys, so cells double as a unit of aggregation for downstream services, not just a search index."*

That shows you know the alternative, know its strengths, and chose against it for a **specific structural reason.** Much stronger than *"H3 is what Uber uses."*

> 💡 **The hybrid worth naming:** you could use **both** — H3 sets for sharding and aggregation, plus a per-cell GEO index for precise distance *within* a cell. Over-engineering at our scale, but knowing it exists is a good signal.

---

## The query algorithm

```
1. originCell = h3(lat, lng, res 9)
2. k          = ceil(R / ~300m)                        → rings needed
3. cells      = gridDisk(originCell, k)                → 3k²+3k+1 cells
4. pipeline:  ZRANGEBYSCORE cell:{c} (now-30s) +inf    → candidates   ◄ trip 1
5. union the driver IDs
6. pipeline:  HMGET driver:{id}:loc lat lng ts heading → positions    ◄ trip 2
7. filter by TRUE haversine distance ≤ R
8. sort by distance, cap at limit
```

> ⭐ **Two round trips regardless of radius** — whether that's 19 cells or 169. That's the payoff of pipelining, and the reason res 9 was chosen.

### Step 4 uses `ZRANGEBYSCORE`, not `ZRANGE`
**Stale members are filtered at READ time.** A driver who stopped pinging 5 minutes ago never appears — *even though the opportunistic sweep hasn't removed them yet.* This is the Session 3.1 design doing its job: **reads are correct immediately; cleanup is only housekeeping.**

### 🔥 Step 7 is not optional — k-rings OVER-INCLUDE
A hexagon disk covering a 2 km radius **is not a circle** — corners of edge cells stick out. Without the distance filter you'd return drivers **2.4 km away** for a 2 km query.

> ⭐ **Cells are the COARSE filter (cheap candidate lookup); true distance is the FINE filter.**

---

## Part 2 — Implementation notes

**`countInCell` is the argument for cells, in one method.** `ZCOUNT` on a single key, O(log n), no distance math, no candidate fetching. When Project 3 needs supply density per hex across a whole city, the difference versus a GEO radius-search-and-count is enormous.

**Sorting happens in Java, deliberately.** Redis can sort by score — but our score is a **timestamp**, not a distance. There's no way to have Redis sort by distance-from-an-arbitrary-point without a GEO index. The candidate set is bounded by the radius, so the sort is cheap.

**`HMGET` only the fields we need**, not `HGETALL` — pulling `speed`/`accuracy` we won't use is wasted bandwidth on every query.

**The null-position skip is a real race, handled honestly:**
```java
if (fields == null || fields.get(0) == null || fields.get(1) == null) continue;
```
Between fetching the cell index (trip 1) and the positions (trip 2), a driver's hash TTL can fire. The index said they were there; a moment later they're gone. ⭐ **Not an error — two non-atomic reads of expiring data.** Making it atomic would mean a Lua script or transaction for a case that's both rare and harmless.

---

## Part 3 — The read API

| Endpoint | Notes |
|---|---|
| `GET /v1/drivers/nearby?lat=&lng=&radiusMetres=&limit=` | The query this whole system exists to answer — and Project 2's matching primitive |
| `GET /v1/drivers/{id}/location` | **`404` = offline** |
| `GET /v1/cells/{h3}/count` | Supply density, O(log n) |
| `GET /v1/cells/count?lat=&lng=` | Same, resolves the cell for you |

### 🔥 `@Max(10000)` on radius — a DoS guard, not a nicety
Without it, a 50 km request means:
```
k = ceil(50000/300) = 167 rings
3(167²) + 3(167) + 1 ≈ 84,000 cells in ONE pipeline
```
> ⭐ **That's a client-controlled amount of work** — the same class of vulnerability as the uncapped batch size from Session 1.1. **Any parameter that scales your workload needs a ceiling.**

### `404` is not a special case
We store **no "offline" flag** and write no tombstone. The TTL having fired **IS** the offline signal. Presence for free.

### What query-service does NOT have
**No Kafka dependency at all.** Reads are bursty and user-driven; writes are a relentless metronome. They scale independently, so they're separate deployments — and that absence is the module boundary made visible in the health endpoint.

---

## 🐛 Two bugs worth remembering

### 1. `@ComponentScan` doesn't reach `common`
```
Parameter 1 of constructor in NearbyDriverService required a bean of type
'com.geopulse.common.spatial.H3IndexService' that could not be found.
```
`@SpringBootApplication` scans from **its own package down**. `QueryServiceApplication` is in `com.geopulse.query`, so it never sees `com.geopulse.common.spatial`.

**Fix:** `@SpringBootApplication(scanBasePackages = "com.geopulse")` on all three main classes.

> 💭 **The better design question:** *should a shared library module contain Spring `@Component`s at all?* `common` is meant to be a plain library, and annotating its classes makes every consumer's scan config a dependency. Two cleaner options:
> - **Auto-configuration** in `common`, registered via `META-INF/spring/...AutoConfiguration.imports` — how real Spring Boot starters work; consumers get the beans with zero config.
> - **Drop `@Component`** and declare `@Bean` in each service that needs it — explicit, no magic.
>
> ⭐ Widening the scan is fine at this size, but **if an interviewer asks "how do you share beans across modules?", the answer they want is auto-configuration**, not `scanBasePackages`.

### 2. `ClassCastException: String cannot be cast to [B`
```java
candidateIds.add(new String((byte[]) member));   // ❌ throws
candidateIds.add((String) member);               // ✅
```
> ⭐ **The asymmetry to remember: with `RedisCallback`, you SEND raw bytes but RECEIVE deserialized objects.** The callback gives you the low-level connection for *writing* commands; the template still owns *reading* results, and `StringRedisTemplate`'s serializer has already converted them.
>
> For bytes both ways: `RedisTemplate<byte[], byte[]>` with `RedisSerializer.byteArray()`.
>
> ⚠️ The same bug lurked in `RedisLocationWriter.readPreviousState` — it just hadn't thrown yet, because it only executes when a driver **already has stored state**.

---

## Verification

| Test | Expected | What it proves |
|---|---|---|
| Nearby, 2000 m | `drv-01/02/03` sorted by distance; **`drv-far` (7 km) absent** | Basic search + distance sort |
| **Nearby, 200 m** ⭐ | **Fewer results; `drv-03` (~310 m) excluded** | ⭐ **The over-inclusion filter.** `k=1` = 7 cells, and drv-03's cell **IS in the ring** — excluded only by true distance |
| Radius 50000 | `400` | The DoS ceiling |
| `lat=200` | `400` | Read endpoints validate too |
| `/drivers/drv-01/location` | `200` + res-9 cell (15 chars, starts `89`) | Point lookup |
| `/drivers/ghost/location` | **`404`** | Offline-by-absence |
| Wait 30 s, re-query | `200` → **`404`** | ⭐ TTL presence, no cleanup job |
| `/cells/count` | `count ≥ 1` | Supply density — Project 3's input |

🗑️ **`SpatialDebugController` deleted** — flagged for removal back in Session 1.2; query-service now owns spatial reads properly.

---

## Key takeaways (revision list)

1. **Redis GEO is one Sorted Set → one key → one node. It cannot shard.** That's the primary reason we don't use it.
2. **Same principle as rejecting bounding boxes:** a range can't shard, a discrete key can.
3. ⭐ **The cell is a unit of aggregation, not just a search index** — `ZCOUNT` for density is O(log n); GEO would need a radius search.
4. **Be honest about the tradeoff:** GEO is simpler and more accurate, and it's the right call on a single node.
5. **Two round trips regardless of radius** — pipelining is what makes 169 cells cheap.
6. **`ZRANGEBYSCORE` filters staleness at read time** — correctness doesn't wait for the sweep.
7. ⭐ **k-rings over-include; the true-distance filter is load-bearing.**
8. **Sort in Java** — Redis's score is a timestamp, not a distance.
9. **The null-position skip is a real race**, correctly handled by skipping.
10. ⭐ **Any client-controlled parameter that scales work needs a ceiling.**
11. **`404` = offline, with no offline flag stored anywhere.**
12. **`@ComponentScan` starts at the app's own package** — shared beans need `scanBasePackages` or (better) auto-configuration.
13. ⭐ **`RedisCallback`: send bytes, receive deserialized objects.**

---

## Next → Phase 3 · Session 3 — Hardening the Read Path
Caching, the thundering-herd problem, graceful degradation when Redis is slow, and the `limit`/pagination semantics a real matching engine would need. Then **Phase 4**, where Cassandra finally answers *"where has this driver been?"*