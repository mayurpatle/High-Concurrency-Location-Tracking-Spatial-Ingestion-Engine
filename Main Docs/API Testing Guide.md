# API Testing Guide

> Companion to `geopulse-postman-collection.json` — **39 requests across 8 folders**, every one with assertions.
> For *performance* testing see **[LOAD-TEST-REPORT.md](../loadtest-report/LOAD-TEST-REPORT.md)** — this suite answers *"does it work?"*, not *"how fast is it?"*

---

## Setup

**1. Infrastructure up:**
```powershell
docker compose up -d
docker compose ps          # wait for cassandra to show (healthy) — up to ~90s
docker compose exec -T cassandra cqlsh < cassandra/schema.cql
```

**2. Services running** (separate terminals):
```powershell
mvn spring-boot:run -pl ingestion-service     # :8081
mvn spring-boot:run -pl processing-service    # :8082
mvn spring-boot:run -pl query-service         # :8083
```

**3. Import into Postman:** Import → File → `geopulse-postman-collection.json`

**4. Run it:** Collection → **Run**.

> ⚠️ **Set a ~1000 ms delay in the Collection Runner.** The pipeline is **asynchronous** (HTTP → Kafka → consumer → Redis), so folders 05 and 07 need the seed to have landed first. An empty `nearby` or `trajectory` result is almost always this, not a bug.

> ⚠️ **Folder 08 needs test-friendly config** in `processing-service/application.yml` — the production hot-cell floor is unreachable at Postman volumes:
> ```yaml
> geopulse.hotcell:
>   scan-interval-ms: 5000    # 30000 in prod
>   min-pings: 20             # 500 in prod
> ```

Collection variables (`ingestionUrl`, `processingUrl`, `queryUrl`, `dedupTimestamp`) are pre-set; override if your ports differ.

---

## What the collection covers

| Folder | Requests | What it proves |
|---|---|---|
| **00 — Health** | 3 | All three services up; processing connected to Redis **and** Cassandra; query has **no Kafka** component (by design) |
| **01 — Happy path** | 2 | `202 Accepted` (not 200), empty body; optional fields may be omitted |
| **02 — Validation** | 7 | Every DTO constraint, plus **no stack-trace leakage** |
| **03 — Quality gate** | 2 | Low-accuracy pings accepted-and-discarded; boundary value passes |
| **04 — Batch** | 3 | Multi-ping ingest, all-or-nothing semantics, size cap |
| **05 — Query API** | 9 | Nearby search, the over-inclusion filter, truncation metadata, radius cap, `404`-as-offline, cell density |
| **06 — Dedup** | 2 | Producer does *not* dedup (correct); consumer does |
| **07 — History API** | 11 | Trajectory + occupancy from Cassandra, cursor paging, downsampling, empty-vs-404 |
| **08 — Hot cell** | 4 | Median-based skew detection flags exactly the busiest cell |

---

## ⭐ The six assertions that matter most

**1. Missing `lat` → 400** *(02)*
With a primitive `double`, Jackson defaults a missing field to `0.0` — **a valid latitude** (Gulf of Guinea). It would pass range validation as a real location. **Silent data corruption.** The `Double` wrapper stays null so `@NotNull` catches it.

**2. Point A vs Point B → same partition cell** *(05)*
Two points ~400 m apart must produce a **different `storageCell`** (res 9, fine-grained for search) and the **same `partitionCell`** (res 7, one neighbourhood).
> This single assertion encodes the whole design: same partition cell → same Kafka partition → same consumer → **single writer per region → no distributed lock needed.**

**3. `cellsScanned` = 169 at 2 km, 7 at 200 m** *(05)*
The k-ring math (`3k²+3k+1`) asserted through the **real** API — the debug endpoint that used to prove this was deleted in Phase 3.3.

**4. The 200 m search returns FEWER drivers** *(05)*
`k = ceil(200/300) = 1` ring = 7 cells — and `drv-03`'s cell **is inside that ring**. It's excluded anyway, because the query filters by **true haversine distance**.
> **k-rings over-include: a hexagon disk is not a circle.** Cells are the *coarse* filter; true distance is the *fine* filter. Skip it and a 2 km query returns drivers 2.4 km away.

**5. Page 2 doesn't overlap page 1** *(07)*
History is **immutable**, so unlike the live spatial search the ordering is stable between requests and paging is coherent. And the cursor is a **seek**, not an offset **scan** — page 500 costs the same as page 1.
> **The occupancy cursor is composite — `(ts, driverId)`.** Many drivers share a millisecond in one cell; a plain `ts < cursor` would silently skip every remaining row at a boundary timestamp. **The clustering key chosen for *write*-side uniqueness is exactly what *read*-side paging needs.**

**6. The detector flags exactly the busiest cell** *(08)*
The test **recomputes the median itself** from the raw counts the endpoint returns, then asserts the flagged cell is the one with the highest traffic — verifying the detector's **decision**, not just that it wrote a key.
> Median, not mean: with 7 cells at 2 and one at 100, the mean is ~14, making the airport look 7× above average instead of 50×. **The outlier drags the baseline toward itself.**

---

## Manual verification — what Postman can't assert

The API is only the front door. Five behaviours live downstream and need eyeballs.

### 1. Quality gate — the ping must NOT reach Kafka
After `03 — accuracy 500m`: **kafka-ui** → `driver-location-pings` → Messages → search `driver-lowaccuracy`.

✅ **Expected: no message.** The API returned `202` (the client did nothing wrong — its JSON is valid), but we dropped the ping rather than let a 500 m error radius overwrite a good position. Compare with `driver-boundary` (accuracy 99), which **should** be there.

### 2. Geographic partitioning — the core property
After `04 — Valid batch of 3`, check the **Key** and **Partition** columns in kafka-ui. Bandra / Andheri / Colaba are different res-7 cells and should route independently. Then send two pings from **nearby** points and confirm **same key → same partition**.

> ⚠️ Different keys *may* land on the same partition by hash collision — that's fine. The guarantee is one-directional: **same key ⟹ same partition.** The reverse isn't promised.

### 3. Dedup — one Redis key for two sends
```powershell
# TWO messages on the topic — the producer never dedups, and shouldn't
# ONE key in Redis — the consumer suppressed the duplicate
docker compose exec redis redis-cli KEYS "dedup:ping:driver-dedup*"
docker compose exec redis redis-cli TTL  "dedup:ping:driver-dedup:1719900016000"
```
**TTL values:** positive = seconds remaining · `-1` = no expiry · `-2` = **key does not exist**.

> ⚠️ **The window is bounded, not absolute.** Re-run the pair more than 300 s apart and the second *will* be processed again — by design.

### 4. History idempotency — the best demo in the project
Run folder 06 three or four times, then:
```powershell
docker compose exec cassandra cqlsh -e "SELECT COUNT(*) FROM geopulse.driver_trajectory WHERE driver_id='driver-dedup' AND day='2024-07-02';"
```
**Count stays at 1.** Eight Kafka messages, **no dedup anywhere on the history path**, one row.
> ⭐ **At-least-once delivery + an idempotent primary key = an exactly-once *effect*.** The key choice did an entire subsystem's job.

🔹 Note the day is **July 2024** — because we bucket by the **device timestamp** baked into that request, not by arrival time.

### 5. The poison pill — not reachable via the API
Our endpoint validates, so malformed data can never enter through it. Inject directly:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --topic driver-location-pings --bootstrap-server localhost:9092
```
Paste, press Enter, then Ctrl-C:
```
{"this is not":"a valid ping"
```

| Check | Expected |
|---|---|
| Consumer log | **ONE** `Delivery attempt 1 failed` — **no attempt 2 or 3** |
| `driver-location-pings-dlt` | Message appears |
| DLT message **Headers** | `kafka_dlt-exception-fqcn`, `-exception-message`, `-original-partition`, `-original-offset` |
| Send a normal ping after | Flows through **immediately** |

> ⭐ **That last row is the point.** Without this machinery, one malformed message would block its partition — and every driver in that neighbourhood — **forever, while the consumer looked perfectly healthy.**

### 6. Consumer health — lag, not logs
The consumer is deliberately **silent on success** (logs are blocking disk I/O; on a hot path you count, you don't log). Its health signal is **kafka-ui → Consumers → `geopulse-location-processor`**.

Send a burst → **lag spikes → returns to zero.** Steady non-zero lag is fine; ⚠️ **growing lag is the alarm.**

> ⭐ And graph it **per partition**, not summed: total lag of 1,000 across 12 partitions could be 83 each (a burst) or **1,000 on one (a region going dark)**. Identical aggregate, opposite situations. The Grafana dashboard does this — see `monitoring/grafana/provisioning/dashboards/`.

---

## Endpoint reference

| Method | Path | Status | Notes |
|---|---|---|---|
| `POST` | `/v1/locations` | `202` / `400` | :8081 · Empty body on success |
| `POST` | `/v1/locations/batch` | `202` / `400` | :8081 · Max 100 items, all-or-nothing |
| `GET` | `/v1/drivers/nearby` | `200` / `400` | :8083 · Redis · `radiusMetres` ≤ 10000, `limit` ≤ 500 |
| `GET` | `/v1/drivers/{id}/location` | `200` / **`404` = offline** | :8083 · Redis |
| `GET` | `/v1/cells/{h3}/count` | `200` | :8083 · `ZCOUNT`, O(log n) |
| `GET` | `/v1/cells/count?lat=&lng=` | `200` | :8083 · Resolves the cell for you |
| `GET` | `/v1/drivers/{id}/trajectory` | `200` / `400` | :8083 · Cassandra · ISO-8601 UTC, cursor paging, `limit` ≤ 5000 |
| `GET` | `/v1/cells/{h3}/occupancy` | `200` / `400` | :8083 · Cassandra · composite cursor |
| `GET` | `/v1/admin/hot-cells` | `200` | :8083 · ⚠️ **Operational, not product** — returns the verdict *and* the raw counts, so a caller can verify the decision |
| `GET` | `/actuator/health` | `200` | All three services |
| `GET` | `/actuator/prometheus` | `200` | All three · scraped every 15 s |

> ⚠️ **Empty ≠ 404 on the history endpoints.** `/drivers/{id}/location` returns `404` because TTL expiry *means* offline. A trajectory with no points is a genuine answer: we searched the partitions and found nothing in that window. **Absence of history is not absence of driver.**

🗑️ `/v1/debug/spatial/*` **deleted in Phase 3.3** — query-service now owns spatial reads.

---

## Two API behaviours worth understanding before you test them

**`202`, not `200`.** At the moment we reply, the ping is in Kafka but not durable. `200` would be a lie about durability; `202` says *"I've taken responsibility; processing is in flight."*

**`nearby` has no pagination; `trajectory` does.** Drivers move every 4 seconds, so a live result set **reorders between requests** — page 2 would duplicate some rows and skip others. ⭐ **You cannot paginate a result set that reorders itself.** More candidates means a *larger radius*, which is a different question. History is immutable, so cursor paging there is both coherent and necessary.

---

## Known gaps in this suite

Knowing what your tests *don't* cover matters as much as what they do.

- **No automated integration tests.** This is an HTTP-level suite. Testcontainers-based tests (spin up Kafka + Redis, assert the consumer actually wrote what it should) would automate the manual checks above.
- **Dedup and history-idempotency assertions are manual** — both *could* now be automated via the query API. Worth adding.
- **No read-path load coverage.** The `nearby` p99 < 100 ms SLO is **untested under write pressure** — read latency on an idle system is a different question. See the load-test report's known gaps.
- **Single-request latency proves little.** The `202` timing assertion runs on an idle laptop; the meaningful numbers come from k6.
- **`/v1/admin/hot-cells` sits on the public query surface.** Should move behind actuator or an internal-only route.