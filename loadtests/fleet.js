// Fleet simulation: drivers that exist, cluster, and move.
//
// WHY NOT RANDOM COORDINATES: a uniform random lat/lng in a bounding box
// produces perfectly even partition load — which is exactly the condition
// under which the hot-cell problem is INVISIBLE. You'd be testing a system
// you don't have. It also makes every ping cross a cell boundary, exercising
// the SREM/ZADD move path on every write, which is not the common case.

// Mumbai demand centres, weighted by how much supply concentrates there.
// The weights are what create the SKEW the system was designed for — the
// airport is deliberately over-represented so a hot cell actually forms.
const HOTSPOTS = [
  { name: 'airport',  lat: 19.0988, lng: 72.8744, weight: 30, spread: 0.004 },
  { name: 'bkc',      lat: 19.0657, lng: 72.8685, weight: 20, spread: 0.010 },
  { name: 'bandra',   lat: 19.0544, lng: 72.8402, weight: 15, spread: 0.012 },
  { name: 'andheri',  lat: 19.1197, lng: 72.8464, weight: 12, spread: 0.015 },
  { name: 'parel',    lat: 19.0068, lng: 72.8303, weight: 10, spread: 0.012 },
  { name: 'colaba',   lat: 18.9220, lng: 72.8347, weight:  6, spread: 0.010 },
  { name: 'borivali', lat: 19.2307, lng: 72.8567, weight:  4, spread: 0.020 },
  { name: 'mulund',   lat: 19.1760, lng: 72.9515, weight:  3, spread: 0.020 },
];

const TOTAL_WEIGHT = HOTSPOTS.reduce((s, h) => s + h.weight, 0);

/**
 * Deterministic PRNG, seeded per driver.
 *
 * Determinism matters for a load test: the same driver id must always produce
 * the same starting position and heading, so a run is REPRODUCIBLE. Comparing
 * two runs is meaningless if the fleet moved differently each time.
 */
function seededRandom(seed) {
  let x = seed;
  return function () {
    x = (x * 1103515245 + 12345) & 0x7fffffff;
    return x / 0x7fffffff;
  };
}

/**
 * Build one driver's initial state — position drawn from a weighted hotspot,
 * plus a heading and speed they'll keep.
 */
export function createDriver(driverId, index) {
  const rand = seededRandom(index * 7919 + 13);

  // Weighted pick: more drivers near the airport than near Mulund.
  let pick = rand() * TOTAL_WEIGHT;
  let spot = HOTSPOTS[0];
  for (const h of HOTSPOTS) {
    pick -= h.weight;
    if (pick <= 0) { spot = h; break; }
  }

  // Gaussian-ish scatter around the centre (sum of two uniforms), so density
  // falls off from the middle rather than being a flat disc.
  const jitter = () => ((rand() - 0.5) + (rand() - 0.5)) * spot.spread;

  return {
    driverId,
    lat: spot.lat + jitter(),
    lng: spot.lng + jitter(),
    heading: rand() * 360,
    speedMps: 3 + rand() * 12,   // 11–54 km/h, urban plausible
    hotspot: spot.name,
  };
}

/**
 * Advance a driver by `seconds` along its heading.
 *
 * Real drivers WALK A PATH — successive pings are metres apart, usually in the
 * same cell, occasionally crossing a boundary. That ratio is what the Redis
 * write path is actually optimised for, and random coordinates would destroy it.
 */
export function moveDriver(d, seconds) {
  // Small random heading change: turns, not teleports.
  d.heading = (d.heading + (Math.random() - 0.5) * 20 + 360) % 360;

  const metres = d.speedMps * seconds;
  const rad = (d.heading * Math.PI) / 180;

  // ~111,320 m per degree of latitude; longitude shrinks by cos(lat).
  d.lat += (metres * Math.cos(rad)) / 111320;
  d.lng += (metres * Math.sin(rad)) / (111320 * Math.cos((d.lat * Math.PI) / 180));

  return d;
}

/**
 * The ping payload. Timestamp is ALWAYS current wall-clock:
 *  - the stale-write guard rejects anything older than what's stored
 *  - the dedup key is driverId+timestamp, so a repeated timestamp is skipped
 * Either would silently reduce the load actually reaching the write path, and
 * you'd be measuring rejection rather than throughput.
 */
export function buildPing(d) {
  return {
    driverId: d.driverId,
    lat: Number(d.lat.toFixed(6)),
    lng: Number(d.lng.toFixed(6)),
    timestamp: Date.now(),
    speed: Number(d.speedMps.toFixed(2)),
    heading: Number(d.heading.toFixed(1)),
    // 2–12m: realistic urban GPS, comfortably under the 100m quality gate so
    // we're not accidentally testing the drop path.
    accuracy: Number((2 + Math.random() * 10).toFixed(1)),
  };
}