import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';
import { createDriver, moveDriver, buildPing } from './fleet.js';

const BASE = __ENV.INGESTION_URL || 'http://localhost:8081';
const FLEET_SIZE = Number(__ENV.FLEET_SIZE || 20000);

// Custom metrics. k6's built-in http_req_duration covers the whole request;
// these let us slice by what we care about.
const ingestLatency = new Trend('ingest_latency', true);
const accepted      = new Counter('pings_accepted');
const failed        = new Counter('pings_failed');
const acceptRate    = new Rate('accept_rate');

export const options = {
  scenarios: {
    fleet: {
      // OPEN-LOOP, and this is the single most important choice in the file.
      //
      // The default VU model is CLOSED-LOOP: each VU sends, waits, sends again.
      // If the system slows, the load AUTOMATICALLY DECREASES — so you cannot
      // distinguish "handling 10k/s comfortably" from "capped at 10k/s because
      // that's all it can do." The test politely throttles itself to whatever
      // you can serve.
      //
      // constant-arrival-rate fires at a fixed rate regardless of whether
      // previous requests completed. If we can't keep up, k6 SAYS SO.
      // Real traffic is open-loop — a million phones don't slow down because
      // your server is struggling.
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',

      // VU pool. k6 allocates from here as needed; if it runs out, it warns —
      // which is itself a signal that responses are slower than budgeted.
      preAllocatedVUs: 200,
      maxVUs: 2000,

      stages: [
        // WARM-UP — not optional. The JVM starts interpreted and JIT-compiles
        // hot paths over the first thousands of invocations. Measure a cold JVM
        // and your p99 is dominated by compilation, not your code. Connection
        // pools, Kafka metadata and Cassandra's prepared-statement cache also
        // need to fill.
//        { target: 1000,  duration: '1m' },
//
//        { target: 10000, duration: '3m' },   // ramp
//        { target: 10000, duration: '5m' },   // HOLD — the measurement window
//        { target: 25000, duration: '3m' },   // STRESS — find the ceiling
//        { target: 0,     duration: '1m' },   // ramp down

           { target: 1000, duration: '1m' },
           { target: 1000, duration: '3m' },
           { target: 0,    duration: '30s' },

      ],
    },
  },

  // Thresholds are our SLOs, asserted. k6 EXITS NON-ZERO if they fail, which
  // makes this runnable in CI as a regression gate rather than a manual ritual.
  thresholds: {
    'http_req_duration{expected_response:true}': ['p(99)<50'],
    'accept_rate': ['rate>0.99'],
    'http_req_failed': ['rate<0.01'],
  },
};

/**
 * Each VU keeps its own driver, created once and then moved.
 *
 * __VU is the virtual user number; deriving the driver id from it means a VU
 * always drives the same driver, so movement is continuous rather than
 * teleporting between unrelated positions.
 */
let driver = null;
let lastPingTime = 0;

export default function () {
  if (driver === null) {
    const id = `drv-${(__VU % FLEET_SIZE).toString().padStart(6, '0')}`;
    driver = createDriver(id, __VU);
    lastPingTime = Date.now();
  }

  // Advance by however long since this VU's last ping — so movement tracks
  // real elapsed time rather than iteration count.
  const now = Date.now();
  const elapsed = Math.max(0.5, (now - lastPingTime) / 1000);
  lastPingTime = now;
  moveDriver(driver, elapsed);

  const res = http.post(`${BASE}/v1/locations`, JSON.stringify(buildPing(driver)), {
    headers: { 'Content-Type': 'application/json' },
    // Tag so this shows separately if we later add other request types.
    tags: { endpoint: 'ingest_single' },
  });

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
  });

  ingestLatency.add(res.timings.duration);
  acceptRate.add(ok);
  if (ok) accepted.add(1); else failed.add(1);
}

export function handleSummary(data) {
  // Write a JSON summary so runs are comparable and archivable — a screenshot
  // of a terminal is not a measurement you can diff.
  return {
    'loadtest/results/summary.json': JSON.stringify(data, null, 2),
    stdout: textSummary(data),
  };
}

// k6's built-in summary renderer.
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';