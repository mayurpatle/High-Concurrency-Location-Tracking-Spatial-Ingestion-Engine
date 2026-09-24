## Redis Commands 

### docker compose exec redis redis-cli KEYS "*"

- it is  used to display all the keys in the  redis 

### docker compose exec redis redis-cli HGETALL "driver:driver-001:loc"

- it is  used to inspect the  driver hash data strucuture stored in the  redis 


### docker compose exec redis redis-cli TTL "driver:driver-001:loc" 

- it  is used to check the TTL remaning  for the key to expire

### docker compose exec redis redis-cli ZRANGE "cell:<paste-h3-here>" 0 -1 WITHSCORES

- Inspect the spatial index — use the h3 value from the HGETALL above:

### 3c. The three tests that prove the design

Test 1 — Multiple drivers in one cell. Send pings for two different drivers at nearby coordinates (within ~100m, so same res-9 cell):

```
{"driverId":"driver-A","lat":19.0596,"lng":72.8295,"timestamp":<now>,"accuracy":5.0}
{"driverId":"driver-B","lat":19.0598,"lng":72.8297,"timestamp":<now>,"accuracy":5.0}
```

Test 2 — The move. Send driver-A again from a different cell (change lat to 19.0700):

```
docker compose exec redis redis-cli ZRANGE "cell:<OLD-h3>" 0 -1
docker compose exec redis redis-cli ZRANGE "cell:<NEW-h3>" 0 -1
```
- driver-A should be gone from the old cell and present in the new one. If the SREM weren't there, they'd be in both — a ghost driver that a proximity search near the old location would happily return. Worth confirming, because it's the failure that's easiest to write and hardest to notice.

Test 3 — The stale-write guard. Send driver-A with an older timestamp than the last one (subtract 60000):

```
docker compose exec redis redis-cli HGET "driver:driver-A:loc" ts
```

- The stored ts should be unchanged — the older ping was skipped. That's last-write-wins protecting you from the boundary race, verified.
