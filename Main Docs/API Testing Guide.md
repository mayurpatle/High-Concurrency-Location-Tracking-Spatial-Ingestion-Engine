# API Testing Guide

> Companion to `geopulse-postman-collection.json` — 22 requests with assertions, covering everything built through Phase 2.

---

## Setup

**1. Infrastructure up:**
```powershell
docker compose up -d
docker compose ps          # wait for cassandra to show (healthy) — up to ~90s
```

**2. Services running** (separate terminals):
```powershell
mvn spring-boot:run -pl ingestion-service     # :8081
mvn spring-boot:run -pl processing-service    # :8082
```

**3. Import into Postman:** Import → File → `geopulse-postman-collection.json`

**4. Run it:** Collection → **Run** (Collection Runner). All 22 requests execute in order with assertions.

Collection variables (`ingestionUrl`, `processingUrl`, `dedupTimestamp`) are pre-set; override them if your ports differ.

---

## What the collection covers

| Folder | Requests | What it proves |
|---|---|---|
| **00 — Health** | 2 | Services up; processing service genuinely connected to Redis **and** Cassandra |
| **01 — Happy path** | 2 | `202 Accepted` (not 200), empty body, sub-50ms; optional fields may be omitted |
| **02 — Validation** | 7 | Every DTO constraint, plus **no stack-trace leakage** |
| **03 — Quality gate** | 2 | Low-accuracy pings accepted-and-discarded; boundary value passes |
| **04 — Batch** | 3 | Multi-ping ingest, all-or-nothing semantics, size cap |
| **05 — H3 spatial** | 4 | Dual-resolution hierarchy, k-ring `3k²+3k+1` math |
| **06 — Dedup** | 2 | Producer does *not* dedup (correct); consumer does |

### ⭐ The three assertions that matter most

**1. Missing `lat` → 400** (`02 — MISSING lat`)
With a primitive `double`, Jackson defaults a missing field to `0.0` — **a valid latitude** (Gulf of Guinea). It would pass range validation as a real location. **Silent data corruption.** The `Double` wrapper stays null so `@NotNull` catches it.

**2. Point A vs Point B** (`05 — THE KEY TEST`)
Two points ~400m apart must produce a **different `storageCell`** (res 9, fine-grained for search) and the **same `partitionCell`** (res 7, one neighbourhood).
> This single assertion encodes the whole design: same partition cell → same Kafka partition → same consumer → **single writer per region → no distributed lock needed.**

**3. k-ring count = 3k²+3k+1** (`05 — 2000m radius`)
7 rings, 169 cells. That's one pipelined Redis round trip for a 2km search — the reason res 9 was chosen (it minimizes *cells touched × drivers per cell*).

---

## Manual verification — what Postman can't assert

The API is only the front door. Three behaviours live downstream and need eyeballs.

### 1. Quality gate — the ping must NOT reach Kafka
After running `03 — accuracy 500m`:

Open **kafka-ui** → `driver-location-pings` → **Messages** → search `driver-lowaccuracy`.

✅ **Expected: no message.** The API returned `202` (the client did nothing wrong — its JSON is valid), but we dropped the ping rather than let a 500m error radius overwrite a good position. Compare with `driver-boundary` (accuracy 99), which **should** be there.

### 2. Geographic partitioning — the core property
After running `04 — Valid batch of 3`:

**kafka-ui** → `driver-location-pings` → **Messages**. Check the **Key** and **Partition** columns:

| Driver | Location | Expect |
|---|---|---|
| `driver-b1` | Bandra | key `87608b0b1ffffff`-ish, partition X |
| `driver-a1` | Andheri | **different** key, likely different partition |
| `driver-c1` | Colaba | **different** key, likely different partition |

Then send two pings from **nearby** points (the Point A / Point B coordinates from folder 05) and confirm **same key → same partition**.

> ⚠️ Different keys *may* land on the same partition by hash collision — that's fine and expected. The guarantee is one-directional: **same key ⟹ same partition.** The reverse isn't promised.

### 3. Dedup — one Redis key for two sends
After running folder `06`:

```powershell
# TWO messages on the topic — the producer never dedups, and shouldn't
# (check kafka-ui: driver-location-pings, search driver-dedup)

# ONE key in Redis — the consumer suppressed the duplicate
docker compose exec redis redis-cli KEYS "dedup:ping:driver-dedup*"
# → dedup:ping:driver-dedup:1719900016000

# TTL counting down from 300 = the memory bound working
docker compose exec redis redis-cli TTL "dedup:ping:driver-dedup:1719900016000"
```

**TTL return values:** a positive number = seconds remaining · `-1` = key exists with no expiry · `-2` = **key does not exist**.

> ⚠️ **The window is bounded, not absolute.** Re-run the pair more than 300s apart and the second *will* be processed again — by design.

### 4. The poison pill — not reachable via the API
Our endpoint validates, so malformed data can never enter through it. Inject directly:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --topic driver-location-pings --bootstrap-server localhost:9092
```
At the `>` prompt, paste and press Enter, then Ctrl-C:
```
{"this is not":"a valid ping"
```

| Check | Expected |
|---|---|
| Consumer log | **ONE** `Delivery attempt 1 failed` — **no attempt 2 or 3** |
| `driver-location-pings-dlt` | Message appears |
| DLT message **Headers** | `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset` |
| Send a normal ping after | Flows through **immediately** |

> ⭐ **That last row is the point.** Without this machinery, one malformed message would block its partition — and every driver in that neighbourhood — **forever, while the consumer looked perfectly healthy.**

### 5. Consumer health — lag, not logs
The consumer is deliberately **silent on success** (logs are blocking disk I/O; on a hot path you count, you don't log). So its health signal is lag:

**kafka-ui** → **Consumers** → `geopulse-location-processor`

Send a burst of pings → **lag spikes → returns to zero**.

> Steady non-zero lag is fine. ⚠️ **Growing lag is the alarm** — freshness is degrading and will keep degrading.

---

## Endpoint reference

| Method | Path | Status | Notes |
|---|---|---|---|
| `POST` | `/v1/locations` | `202` / `400` | Single ping. Empty body on success. |
| `POST` | `/v1/locations/batch` | `202` / `400` | Max 100 items, all-or-nothing. |
| `GET` | `/v1/debug/spatial/cell` | `200` | 🗑️ **Temporary** — delete in Phase 3. |
| `GET` | `/v1/debug/spatial/neighbours` | `200` | 🗑️ **Temporary** — delete in Phase 3. |
| `GET` | `/actuator/health` | `200` | Both services. |

**Not yet built** (Phase 3+): `GET /v1/drivers/{id}/location`, `GET /v1/drivers/nearby`, `GET /v1/cells/{h3}/count`, `GET /v1/drivers/{id}/trajectory`, `GET /v1/cells/{h3}/occupancy`

---

## Known gaps in this suite

Worth naming, because knowing what your tests *don't* cover is as important as what they do:

- **No automated integration tests.** This is a manual/CI-runnable HTTP suite. Testcontainers-based tests (spin up Kafka + Redis, assert the consumer actually wrote what it should) would let you assert the *downstream* behaviours that are manual above. Natural addition alongside Phase 3.
- **No load testing.** Response times here are single-request. Phase 6 uses **k6** for the 10k writes/sec run that replaces our estimated numbers with measured p99s.
- **Dedup assertion is manual.** It can't be automated until the query API exists (Phase 3.3) — then "was this written once?" becomes an HTTP call.
- **The 50ms SLO assertion is soft.** A single request on an idle laptop proves little. The real number comes from k6 under sustained load.