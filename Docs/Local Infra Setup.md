# Phase 0 · Sprint 2 — Local Stack & Project Scaffolding

> **Project:** High-Concurrency Location Tracking & Spatial Ingestion Engine — *Project 1 of 3*
> **Session goal:** Stand up the entire backing infrastructure locally (one `docker compose up`), scaffold the multi-module code, and prove — with a health check — that our services can reach every datastore.
> **Outcome:** Phase 0's build goal is DONE. Runnable skeleton in `geopulse-phase0-skeleton.zip`.

---

## Pinned versions (verified current, mid-2026)

| Component | Version / image | Why this one |
|---|---|---|
| Spring Boot | `3.5.16` | Last 3.x line; 4.x moved to Spring Framework 7 / Jakarta EE 11 / Jackson 3 — more integration friction. Concepts identical. |
| Java | `21` | LTS; **virtual threads** suit a high-fan-out ingestion edge. |
| Kafka | `apache/kafka:4.2.0` | KRaft-native; **ZooKeeper removed entirely in 4.x**. |
| Kafka UI | `ghcr.io/kafbat/kafka-ui` | The maintained fork (old `provectus/kafka-ui` is dead). |
| Redis | `redis:8-alpine` | Current line; AGPL option (doesn't affect cache use). *Valkey is a drop-in BSD alternative.* |
| Cassandra | `cassandra:5.0` | Current GA. |
| H3 | `com.uber:h3:4.3.1` | v4 API (`latLngToCell`, `gridDisk`) — v3 method names are gone. |
| Prometheus / Grafana | `prom/prometheus:v3.7.3` / `grafana/grafana:12.3.0` | Metrics pull + dashboards. |

---

## Part 1 — Project skeleton (Maven multi-module)

```
geopulse/
├── pom.xml                 # parent (packaging=pom, aggregator)
├── common/                 # shared library JAR: models, DTOs, H3 utils, constants
├── ingestion-service/      # write path:  REST -> H3 -> Kafka producer
├── processing-service/     # consumer:    Kafka -> Redis + Cassandra
├── query-service/          # read path:   APIs -> Redis + Cassandra
├── docker-compose.yml      # local infra
└── monitoring/prometheus.yml
```

**Core principle:** module boundaries mirror **deployment** boundaries. Ingestion, processing, and query scale independently (e.g. 20 ingestion pods vs 8 consumers), so they're separate modules. Shared code lives in `common`.

**The iron rule:** *no service depends on another service at compile time.* They communicate only via Kafka / Redis / HTTP. Compile-time coupling between services is the microservices anti-pattern we structurally prevent.

Key POM concepts:
- **Parent** `packaging=pom` = aggregator, produces no jar; inherits Spring Boot **BOM** (curated, mutually-compatible dependency versions → we omit version numbers downstream).
- `dependencyManagement` = version *policy* ("IF you use X, use this version"); `dependencies` = inherited *unconditionally*. Keep the latter tiny.
- **`common` is a plain library** (`packaging=jar`, **no `spring-boot-maven-plugin`**, no `main()`). The three services are executable Spring Boot apps (they *have* the plugin). H3 lives in `common`; services get it transitively.

---

## Part 2 — Kafka in KRaft mode

**KRaft:** Kafka stores its own cluster metadata via a built-in Raft quorum of *controller* nodes — no external ZooKeeper. **Combined mode** (dev): one process is both `broker` (data) and `controller` (metadata). Production splits them (dedicated controller quorum) for isolation + independent scaling.

### Gotcha #1 — the advertised-listeners trap (the #1 time-waster)
A client connects to a bootstrap address once, then Kafka replies "reconnect to my *advertised* address." So the advertised address must be reachable *from where the client runs*. Our UI is a **container** (needs `kafka:29092`); our Spring services run on the **host** (need `localhost:9092`). One listener can't serve both → **two listeners**:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

### Gotcha #2 — the replication-factor footgun
Internal topics (`__consumer_offsets`, transaction log) default to **RF=3**. On a 1-broker cluster the first consumer group start fails ("can't place 3 replicas"). Override to 1:

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

---

## Part 3 — Redis + Cassandra

**How much config each needs tells you their job.** Redis is nearly config-free (disposable state); Cassandra needs heap caps + a datacenter name + a 90s startup budget (durable, clustered DB).

- **Redis is disposable — a feature.** The hot tier is self-healing: lose it, and every driver's next ping (≤4s) repopulates it. AOF is on here for dev convenience, but in production you'd often run the hot tier with **persistence off** for max throughput. Knowing *when durability is optional* is a Staff instinct.
- **`MAX_HEAP_SIZE` saves your afternoon.** Uncapped, Cassandra grabs ~¼ of host RAM and triggers OOM kills alongside Kafka/etc. 1G heap is comfortable; 512M on an 8GB machine.
- **`datacenter1` is a tripwire.** `GossipingPropertyFileSnitch` + `CASSANDRA_DC: datacenter1` names the local DC. The DataStax driver *refuses to connect without being told the local datacenter* (a guard against cross-region routing). Our config must say `local-datacenter: datacenter1` — exact match, or "No node was available" at startup.

**Startup timing:** Cassandra's CQL port (9042) opens ~60–120s after `up`. A "Connection refused" during that window is *not* an error — it's still booting. Ready signal in logs: `Starting listening for CQL clients on /0.0.0.0:9042`. Crash (Exited/137/"Killed") = out-of-memory → raise Docker's RAM or lower the heap.

---

## Part 4 — Prometheus + Grafana

Observability is **foundational, not final**, because this pipeline's health *is* numbers (p99 latency, freshness, and especially **consumer lag**) you can't read from logs. We lay the rails now, light them up in Phase 6.

- **Prometheus** = pull-based collector + TSDB. It *scrapes* an HTTP `/metrics` endpoint on each target every 15s. Pull means a non-responsive target is *itself* a signal ("service down") — which push can't distinguish from "up but quiet."
- **Grafana** = dashboards querying Prometheus.

### Gotcha #3 — `host.docker.internal` (Part 2's trap, reversed)
Now a **container** (Prometheus) must reach a service on the **host** (our Spring app). Inside a container `localhost` = the container itself, so to escape to the host you use `host.docker.internal`. Same lesson: *"localhost" is always relative to who's asking.* (Scrape targets stay commented until Phase 6.)

---

## Part 5 — Bootstrap apps + connectivity smoke test

Each service got a `@SpringBootApplication` main class (packages `com.geopulse.{ingestion,processing,query}`) and an `application.yml`. Distinct ports **8081 / 8082 / 8083** so all three coexist (8080/9090/3000 are the UI/Prometheus/Grafana).

**Spring Boot's core trick:** *classpath + properties = wired infrastructure.* We never wrote connection code — putting `spring-boot-starter-data-redis` in the POM is enough for auto-configuration to build the connection from `application.yml`. That's why each service's starters (chosen per-module in Part 1) make its auto-config different.

Property-name gotchas (Boot 3.x): Cassandra is under **`spring.cassandra.*`** (not `spring.data.cassandra.*`); Redis is under **`spring.data.redis.*`** (not `spring.redis.*`).

### The smoke test
```bash
mvn clean install                          # first successful full build (services now have main())
mvn spring-boot:run -pl processing-service # talks to BOTH stores — best proof
curl http://localhost:8082/actuator/health
```
Pass = `cassandra: UP` **and** `redis: UP` in the response. A running Java app opened real connections to both datastores.

> Kafka has **no default Spring Boot health indicator**, so it won't appear in `/actuator/health`. The producer→Kafka path is proven in Phase 1.3 (publish a ping, watch it land in kafka-ui).

---

## Key takeaways (revision list)

1. Module boundaries = deployment boundaries; **no service→service compile-time dependency**.
2. `common` = plain jar (no boot plugin); services = executable apps.
3. KRaft killed ZooKeeper; combined mode for dev, split roles for prod.
4. **Advertised listeners** must be reachable *from where the client runs* → dual listeners.
5. Single-broker → override internal-topic **RF to 1**.
6. Redis hot tier is **disposable/self-healing**; durability is optional there.
7. Cassandra: **cap the heap**; **local-datacenter must match**; boots slowly.
8. Prometheus is **pull-based**; `host.docker.internal` reaches host from a container.
9. **Classpath + properties = wired infrastructure** (no manual connection code).

## What we locked this session
The full local stack (Kafka + UI, Redis, Cassandra, Prometheus, Grafana) in one Compose file, a wired 4-module Maven project, three runnable services, and a passing connectivity smoke test.

---

## Next → Phase 1 · Session 1 — The Ingestion Endpoint
The first real feature. We design the `LocationPing` domain model + the request DTO with validation (in `common`), then build the non-blocking `POST /v1/locations` endpoint in `ingestion-service` that returns `202 Accepted`. First code that does something.