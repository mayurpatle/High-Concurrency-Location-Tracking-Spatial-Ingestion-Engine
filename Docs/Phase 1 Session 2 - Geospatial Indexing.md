# Phase 1 · Session 2 — H3 Geospatial Indexing

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Convert lat/lng into discrete hexagonal cell IDs **at the ingestion edge** — the field that makes every downstream partitioning guarantee possible.
> **Status:** ✅ Verified — two points ~200m apart produced different res-9 storage cells but the **same** res-7 partition cell.

---

## Part 1 — Why discretize space, and why hexagons

### What we're rejecting: `WHERE lat BETWEEN a AND b AND lng BETWEEN c AND d`

It fails twice:

1. **B-tree indexes are one-dimensional.** A compound index on `(lat, lng)` sorts by latitude first — the query narrows by lat, then filters *every row in that latitude band* by longitude. In a dense city that band is enormous.
2. **A bounding box cannot be sharded.** ⭐ A box is a **range**, and ranges span shards. You can't hash a region. Our architecture depends on routing a ping to a deterministic partition, and that requires a **scalar key**.

> PostGIS and R-trees genuinely solve problem #1. **Neither solves #2** — and #2 is what our capacity math demands.

**The fix:** chop the Earth into fixed cells, each with an ID. A coordinate maps to exactly one cell. "Where is this?" becomes a hash computation; "who's near here?" becomes "which cells overlap my radius → fetch those keys." **A 2-D range query becomes a 1-D key lookup — the only shape that shards.**

### ⚠️ Two separate decisions (interviewers test this)

| Decision | Reason |
|---|---|
| **Discrete cells** (vs bounding boxes) | Deterministic scalar key → **shardable**. *This is the big one.* |
| **H3 over S2** | **Uniform adjacency** for radius search. We don't need range scans. |

**Determinism is NOT why we picked H3** — S2 gives deterministic scalar IDs too. Any grid does.

### Why hexagons

| | Square | Hexagon |
|---|---|---|
| Neighbours | **8**, at **2** distances (`d` edge-sharing, `d√2 ≈ 1.41d` corner-sharing) | **6**, all edge-sharing, all at **one** distance |
| "Adjacent" means | Ambiguous — do diagonals count? | Unambiguous |

Three consequences that match our needs:
- **Uniform radius search.** Our core query is a circle ("drivers within 2km"). With hexagons every ring outward is one consistent distance step. With squares, "one ring out" reaches **41% further diagonally** — a lopsided search region.
- **Better circle approximation** → less over-inclusion → fewer candidate drivers fetched and discarded → **fewer Redis reads per query**.
- **No directional bias in aggregates.** Square grids produce visible diagonal artifacts in heatmaps (relevant to Project 3's surge maps). Hexagons don't.

### The honest tradeoff: H3 vs S2

Hexagons **don't subdivide cleanly** — a hexagon cannot be perfectly tiled by smaller hexagons. H3's hierarchy is therefore **approximate**: each cell has 7 children that spill slightly over the parent's boundary.

| | H3 (hexagons) | S2 (squares) |
|---|---|---|
| Adjacency | **Uniform** — 6 equidistant neighbours | 8 neighbours, 2 distances |
| Hierarchy | Approximate | **Exact** containment |
| ID ordering | No spatial locality in IDs | **Hilbert curve** → range scans work |
| Best at | **Radius / neighbour search** | Rollups, range queries, sorted stores |

**We pick H3** because our dominant query is radius search (uniform adjacency wins) and our sharding is **hash-based**, so S2's ID ordering buys us nothing — *hashing destroys ID locality by design*. If we stored cells in a B-tree and did range scans, S2 would be the better call. **It's a genuine judgment, not a default.**

> 🔹 **Quirk worth knowing:** you **cannot tile a sphere with hexagons alone** — topology forbids it. H3 includes exactly **12 pentagons** at every resolution, positioned mostly over ocean. Irrelevant at city scale, but knowing they exist signals you've read the spec.

### The resolution ladder

| Res | Avg edge | Avg area | Feels like |
|---|---|---|---|
| 5 | ~8.5 km | ~253 km² | a whole city |
| 6 | ~3.2 km | ~36 km² | a large district |
| 7 | ~1.2 km | ~5.2 km² | **a neighbourhood** ← our PARTITION res |
| 8 | ~460 m | ~0.74 km² | a few city blocks |
| 9 | ~174 m | ~0.10 km² | **one city block** ← our STORAGE res |
| 10 | ~66 m | ~0.015 km² | a building |

---

## Part 2 — Choosing resolutions (with the math)

**Two competing pressures:**
- *Too coarse* (res 5, ~253 km²) → tens of thousands of drivers per cell. As a Kafka key: catastrophic hot partition. As a Redis key: a huge set to fetch and filter.
- *Too fine* (res 12, ~2000 m²) → most cells empty, a 2 km search touches thousands of cells, key space explodes.

**Key insight: one resolution can't serve both jobs. We use two.**

### Res 9 — STORAGE / index (Redis key, Cassandra partition key)
At ~2,000 drivers/km² (dense city): **~200 drivers per cell**. Big enough to be meaningful, small enough to fetch whole.

### Res 7 — PARTITIONING (Kafka partition key)
Partitioning wants **locality**. Too many distinct keys → adjacent areas scatter across partitions, destroying the single-writer-per-region property. Too few → hot partitions. A neighbourhood is the natural "route this region's traffic to one consumer" unit.

> 💡 **Res 7 is res 9's grandparent.** One `cellToParent` call derives the coarse key from the fine one — **compute one cell, get both**, guaranteed consistent.

### The k-ring math that justifies res 9

For a **2 km** search radius at res 9 (~300 m across):
```
k ≈ 2000 m / 300 m           = 7 rings
cells in a k-ring = 3k² + 3k + 1
k=7  →  3(49) + 21 + 1       = 169 cells
```
**169 Redis keys = one pipelined round trip.** Compare: res 10 → ~1,200 cells (too many); res 8 → ~30 cells but ~1,400 drivers each, mostly discarded.

> ⭐ **Res 9 minimizes (cells touched × drivers per cell).** That product *is* the optimization — a great thing to say out loud in an interview.

### Why resolutions are constants, not config
Changing a resolution **invalidates every stored cell ID** in Redis and Cassandra. It's a **data migration, not a tunable**. Making it look configurable would be a trap.

---

## Part 3 — Enrichment at the edge

### Why compute the cell at ingestion, not at query time
1. ⭐ **The partitioner needs it before the message is routed.** Compute it downstream in the consumer and you *cannot* partition by geography — routing already happened. **This alone forces the choice.**
2. **Compute once, use many.** One cell ID becomes the Kafka key, the Redis index key, and the Cassandra partition key.
3. **Write-heavy denormalization.** Spend microseconds of CPU per write to turn every read into an O(1) key lookup instead of a scan. Right trade even at 3:1 write:read, because the reads it saves are the **latency-critical** ones.

### Two caveats to carry forward
- **The cell is an index, not the location.** We keep exact lat/lng too — needed for distance math, map rendering, trajectory replay. Discretizing is **additive, never a replacement**.
- **Cells have boundaries.** Two drivers 10 m apart can land in different cells if they straddle an edge. This is *precisely* why nearby-search must expand into neighbouring cells (k-rings) and **never trust a single cell**.

### Implementation notes

**`H3IndexService` — why wrap the library:**
- `H3Core.newInstance()` loads a **native library** — doing that per call would be ruinous at 250k/s. Load **once at startup**.
- Keeps H3's API behind our own stable interface (swapping to S2 later touches one class).
- **Thread safety:** `H3Core` is thread-safe and stateless after construction → one shared instance, **no locking**. A lock here would be a contention point on the hot path — exactly what this architecture exists to avoid.
- Fails fast at startup if the native lib won't load: *a half-working ingestion service is worse than a dead one.*

**Cells stored as `String`** (H3's 15-char hex form, e.g. `8928308280fffff`) rather than raw `long`: this value becomes a Redis key, Kafka message key, and Cassandra partition key — all string-oriented — and it's far easier to debug in logs and kafka-ui. Costs a few bytes per message; worth it.

**Order matters — quality gate BEFORE enrichment.** Never spend CPU indexing a ping you're about to discard. At 250k/s, work avoided is capacity gained.

**`ringsForRadius` deliberately OVER-estimates.** Too few rings = **silently missing nearby drivers** (a correctness bug, invisible in testing). Too many rings = a few wasted Redis lookups (measurable, trivial). ⭐ **When a search is approximate, always err toward the recoverable mistake.**

### 🔥 H3 v4 renamed everything
Tutorials you find online are mostly **v3** and won't compile against 4.3.1:

| v3 (old, everywhere online) | v4 (what we use) |
|---|---|
| `geoToH3` / `geoToH3Address` | `latLngToCellAddress` |
| `h3ToParent` | `cellToParentAddress` |
| `kRing` | `gridDisk` |

---

## Test Api Endpoins

1. All the cells  within any radius : 

```
http://localhost:8081/v1/debug/spatial/cell?lat=19.076090&lng=72.877656
```

gives the cell counts  within a  radius  by calculating to the  total k rings required

2. Storge cell and  partioncell of a given latitude :

```
http://localhost:8081/v1/debug/spatial/cell?lat=19.076090&lng=72.877656
```

gives the storage cell and partition cell of a lat/lon


## Verification (all confirmed live)

| Test | Expected | What it proves |
|---|---|---|
| Two points ~200 m apart | **Different** `storageCell`, **same** `partitionCell` | ✅ Fine-grained indexing + coarse-grained routing — the whole design in one observation |
| 2000 m radius | `rings: 7`, `cellCount: 169` | `3k²+3k+1` holding on live data |
| 600 m radius | `rings: 2`, `cellCount: 19` | Formula scales as predicted |
| POST a ping | Log shows `h3=8961… partitionCell=8760…` | Coordinates became a spatial index at the edge |

> 🗑️ `SpatialDebugController` is **temporary** — delete it in Phase 3 when query-service owns spatial reads. A debug endpoint on a public ingestion service is a smell; short-lived and clearly labelled is the compromise.

---

## Key takeaways (revision list)

1. **Bounding boxes can't be sharded** — ranges span shards. Discrete cells give a scalar, hashable key.
2. **Two decisions, don't conflate:** discrete cells (vs boxes) = shardability; H3 (vs S2) = uniform adjacency.
3. **Hexagons: 6 neighbours, 1 distance.** Squares: 8 neighbours, 2 distances → lopsided radius search.
4. **H3's hierarchy is approximate; S2's is exact.** We don't care — we hash, so ID locality is worthless to us.
5. **Two resolutions:** res 9 storage (~200 drivers/cell), res 7 partitioning (a neighbourhood). Res 7 is res 9's grandparent → derive, don't recompute.
6. **Res 9 minimizes (cells touched × drivers per cell)** for a 2 km search: 169 cells, one round trip.
7. **Enrich at the edge because the partitioner needs the cell before routing.**
8. **The cell is an index, not a replacement** for lat/lng.
9. **Cells have boundaries** → always expand with k-rings; never trust one cell.
10. **Err toward the recoverable mistake** in approximate search (over-estimate rings).
11. Resolutions are **constants, not config** — changing one is a data migration.

---

## Next → Phase 1 · Session 3 — The Kafka Producer & Partitioning
Where the placeholder `log.info` finally dies and `h3PartitionCell` becomes the **Kafka partition key** — the decision the entire *"the partition is your lock"* argument rests on. Idempotency keys, ordering guarantees, producer acks/batching, and the partition-count math.