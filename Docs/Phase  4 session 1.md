# Phase 4 · Session 1 — Cassandra Data Modelling

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Design the history store — and absorb the mindset shift that makes Cassandra modelling feel backwards coming from relational thinking.
> **Status:** ✅ Schema applied and verified; `ALLOW FILTERING` error observed (the model enforcing itself).

---

## Part 1 — The mindset shift

### Why Cassandra: the write pattern, not just the volume
The capacity math (~2.16 TB/day, multiple PB/year) rules out any single node. But the deeper fit is **LSM-tree** storage:

| | LSM (Cassandra) | B-tree (Postgres) |
|---|---|---|
| A write | Append to memtable + commit log, **return** | **Locate** the page, read it, modify, write back |
| Disk I/O | Sequential, no seek | **Random** |
| Read-before-write | ❌ None | ✅ Required |

Our workload is enormous write volume, **no updates ever** (a past location never changes), and reads that are always *"contiguous time range for one key."* ⭐ **For a firehose of appends, LSM is simply the right shape.**

> 🔹 **Corollary that trips people up:** in Cassandra, **`UPDATE` and `INSERT` are the same operation.** Both write a new cell with a timestamp; latest wins at read time. There's no "does this row exist?" check — which means writes are **naturally idempotent** if the primary key identifies the logical event. *(We exploit this in 4.2.)*

### ⭐ Invert the modelling process
| Relational | Cassandra |
|---|---|
| Normalize → define entities → eliminate redundancy | **Start with the queries** |
| Schema first; queries adapt via **joins** | **One table per query pattern** |
| Query planner rescues a badly-shaped request | ⚠️ **No joins. No cross-partition aggregation. No planner.** |

> ⚠️ **If your table isn't shaped for your query, the query is simply not possible** — or needs `ALLOW FILTERING`, which is Cassandra's way of saying *"this will scan your cluster and you'll regret it."*

### Denormalization is correct, not shameful
We write every ping to **two tables**. In relational thinking that's a data-integrity hazard — two copies that could diverge.

⭐ **It's safe here for one specific reason: our data is immutable.** A past location never changes, so the copies **cannot** diverge. There's no update anomaly because there are no updates.

> ⭐ **The general rule: duplicate freely when the data is immutable; be very careful when it isn't.**

### The primary key — where all the power and all the danger live
```sql
PRIMARY KEY ( (partition_key) , clustering_column1, clustering_column2 )
```

| Part | Job |
|---|---|
| **Partition key** | **WHICH NODE.** Hashed to a token; the token picks placement. Everything with the same partition key lives together, physically adjacent on disk. |
| **Clustering columns** | **SORT ORDER WITHIN** that partition. Rows are *stored* sorted — which is why a range query is a cheap sequential read, not a scan. |

**Three consequences — the whole art:**
1. ⚠️ **You can only query efficiently by partition key.** A query without it must hit **every node**. `WHERE driver_id = ?` is fast; `WHERE speed > 50` is a disaster.
2. 🔥 **Partition size is what kills you.** All rows for one key live on **one node** → an unbounded partition is unbounded data on a single machine. **Keep partitions under ~100 MB and ideally under 100,000 rows.**
3. ⭐ **Therefore the partition key almost always needs a time bucket.**

### 🔥 The arithmetic that forces bucketing
The "obvious" trajectory key is `PRIMARY KEY (driver_id, timestamp)`. Clean, and **wrong**:
```
One driver @ 4s:  86,400 ÷ 4  = 21,600 rows/day (~2.16 MB)
After 50 days:    ~1M rows,   ~108 MB   ← already past the guideline
After 1 year:     ~7.9M rows, ~790 MB   ← the partition is a liability
```
> ⚠️ **The partition grows FOREVER.** Any partition key with no natural bound is a time bomb that **detonates in production months after you ship.**

**The fix:** make it `(driver_id, day)`. Each partition is now bounded at ~21,600 rows / ~2 MB, and a new one starts every day automatically.

**The honest cost:** a 7-day query touches 7 partitions instead of 1. ⭐ **You've traded one large read for several small ones — the right trade, because the small ones are predictable and bounded while the large one grows without limit.** And they run in parallel.

---

## Part 2 — The two tables

### Table 1 — `driver_trajectory`
*"Where has THIS DRIVER been between T1 and T2?"* → filter by driver, range over time.

```sql
PRIMARY KEY ((driver_id, day), ts)
WITH CLUSTERING ORDER BY (ts DESC)
```

🔥 **Note the DOUBLE PARENTHESES.** `(driver_id, day)` is a **composite partition key**. Writing `PRIMARY KEY (driver_id, day, ts)` is **valid CQL that compiles, runs, and slowly destroys your cluster** — it partitions by driver alone, recreating the unbounded partition above.

| Option | Why |
|---|---|
| `CLUSTERING ORDER BY (ts DESC)` | Newest-first *on disk*. "Recent history" dominates, and reading from the front of a partition beats seeking into it. ⭐ **Physical sort order should match your dominant read direction.** |
| `TimeWindowCompactionStrategy` | Default `SizeTiered` merges by size, **repeatedly rewriting old immutable data for no benefit.** TWCS groups SSTables by time window and **stops compacting a window once closed** → far less write amplification, and expired data drops as **whole SSTables** rather than scattering tombstones. |
| `default_time_to_live = 2592000` | 30-day retention — the policy the capacity math demanded. Production pairs it with downsampling (1 pt/min) + cold tiering to S3. |

```sql
-- Today: exactly ONE partition
SELECT ts, lat, lng FROM driver_trajectory WHERE driver_id=? AND day=?;

-- Window within a day: one partition, sequential range read
SELECT ts, lat, lng FROM driver_trajectory
WHERE driver_id=? AND day=? AND ts >= ? AND ts <= ?;
```
> ⚠️ **Multi-day = N parallel single-partition queries, driven by your application.** `IN (day1, day2…)` coordinates through one node and is an anti-pattern at scale. Feels like work the DB should do — but **you always know exactly how many partitions a query touches.**

### Table 2 — `cell_occupancy`
*"Which drivers were in THIS CELL between T1 and T2?"* → the key **inverts**.

```sql
PRIMARY KEY ((h3_cell, hour_bucket), ts, driver_id)
```

### 🔥 Why HOURLY here when trajectory used DAILY
```
A busy res-9 cell: ~200 drivers × 900 pings/hour = 180,000 rows/HOUR
  daily bucket  → 4.3 MILLION rows/partition  ✗ catastrophic
  hourly bucket → 180,000 rows/partition      ✓ acceptable
```
⭐ **Same technique, different width, because this key accumulates ~8× faster.** Trajectory has **one driver** per partition; occupancy has **every driver in a neighbourhood**.

> ⭐ **Bucket width is DERIVED per table from the row rate of that table's partition key — not a convention you pick once for a project.** A key that collects from one source can afford a wide bucket; a key that collects from a crowd cannot.

### 🔥 `driver_id` as a clustering column is load-bearing
Two drivers pinging in the same millisecond produce identical `(partition, ts)`. In Cassandra an insert with an existing primary key is an **UPSERT** — so one would **silently overwrite** the other. **No error, no warning, nothing in the logs.** You'd quietly lose data every time two drivers ping together, which in a busy cell is constantly.

> ⭐ **Clustering columns must collectively guarantee uniqueness.** In a relational store a duplicate key throws; here it succeeds and eats your data.

### Side by side
| | `driver_trajectory` | `cell_occupancy` |
|---|---|---|
| Question | Where has *this driver* been? | Who was in *this cell*? |
| Partition key | `(driver_id, day)` | `(h3_cell, hour_bucket)` |
| Clustering | `ts DESC` | `ts DESC, driver_id` |
| Rows/partition | ~21,600 | ~180,000 |
| Bucket | **daily** | **hourly** |
| TTL | 30 days | 7 days (analytics horizon, higher volume) |

> ⚠️ **The honest cost: we've doubled write volume** — ~4.3 TB/day instead of 2.16. Say it out loud rather than hoping nobody does the math. The alternative (one table + application-side filtering) means scanning partitions you don't need — far more expensive at read time, and it doesn't scale at all.

---

## Part 3 — Keyspace & verification

### `NetworkTopologyStrategy` even with one datacenter
`SimpleStrategy` ignores topology — it walks the token ring without regard for racks, so **all three replicas can land in one rack** and one rack failure loses your data. Migrating Simple → NTS on a live cluster is a **risky manual procedure**. ⭐ **Use NTS from day one, because you might not always have one DC.**

⚠️ **RF=1 is DEV ONLY.** Production: `'datacenter1': 3` — survive a node loss, read at QUORUM. With RF=1 there is **no redundancy**.

### ⭐ The `ALLOW FILTERING` error is a feature
```sql
SELECT * FROM driver_trajectory WHERE driver_id='drv-01';   -- ✗ fails
```
Without the **full** partition key there's no token, so there's no node to ask. `ALLOW FILTERING` would let it through — **by scanning the entire cluster.**

> ⭐ **The hash is a one-way street: key → node is trivial, node → key is impossible.** The error is the database refusing to let you write a query that works fine on your laptop and melts in production.

### Storage layout, visualised
**Placement:** `(driver_id, day)` → hash → token → node. ⚠️ `drv-01` on the 21st and on the 22nd are **different partitions on unrelated nodes** — hashing deliberately destroys adjacency, exactly as it does for H3 cells across Kafka partitions. **There is no "nearby" in token space.**

**On disk:** within one partition, rows are written in `ts DESC` order, contiguously. A range query **binary-searches to an offset and reads forward** — which is why `WHERE ts >= ?` is nearly free *inside* a partition and impossible *across* partitions.

> 🔮 **Foreshadowing Phase 5:** a busy cell concentrates load onto one node — the **hot partition** problem, the same shape as the hot-Kafka-partition problem. ⭐ **One design decision (partition by location) creates the identical failure mode at two completely different layers of the stack.** Same family of fixes: salting, sub-partitioning.

---

## Key takeaways (revision list)

1. **LSM suits appends** — no seek, no read-before-write. B-trees pay random I/O on every update.
2. **`INSERT` and `UPDATE` are the same operation** → writes are naturally idempotent given the right key.
3. ⭐ **Model per query, not per entity.** One table per query pattern.
4. ⭐ **Denormalize freely when the data is immutable** — copies can't diverge without updates.
5. **Partition key = which node. Clustering columns = sort order within.**
6. ⚠️ **You can only query efficiently by partition key.**
7. 🔥 **Every partition key needs a bound** — unbounded partitions are time bombs.
8. ⭐ **Bucket width is derived from the row rate of that table's key**, per table.
9. 🔥 **Double parentheses** make a composite partition key. Getting it wrong compiles and runs.
10. **`CLUSTERING ORDER` should match your dominant read direction.**
11. **TWCS for time-series** — stops compacting closed windows; TTL drops whole SSTables.
12. 🔥 **Clustering columns must guarantee uniqueness** — a duplicate key is a silent upsert, not an error.
13. **Multi-partition queries are N parallel reads driven by your app.**
14. **NTS from day one**, even with one DC.
15. ⭐ **`ALLOW FILTERING` is the model enforcing itself** — the hash is one-way.
16. **Doubling write volume is the honest price** of query-driven modelling.

---

## Next → Phase 4 · Session 2 — The Write Path
The consumer finally writes to these tables. Batching (and why Cassandra `BATCH` is **not** what you think it is), tunable consistency, prepared statements, and the write-behind path that keeps a slow disk from back-pressuring ingestion.