// seed.js
//
// One-shot backfill of the 4 raw/context topics with a realistic, backdated
// batch (200 telemetry, 60 weather, 25 traffic, 200 parcel-volume records),
// so the console and the Flink jobs have history to work with the moment
// the stack comes up. telemetry-simulator.js keeps fresh data flowing after
// that.
//
// Runs as the `seed` service in docker-compose.yml, before fleet-app starts.
// Skips itself if vehicle.telemetry already has data (so repeated
// `docker compose up` runs don't keep appending backfills); pass --force to
// seed anyway.
//
//   KAFKA_BOOTSTRAP=localhost:9094 node seed.js [--force]

import 'dotenv/config';
import { getKafka, produceRecords } from './kafka-client.js';

const GEOZONES = ['tullamarine-linehaul', 'docklands-cbd', 'essendon-fields',
  'westgate-linehaul', 'dandenong-south', 'melbourne-cbd'];
const VEHICLES = Array.from({ length: 12 }, (_, i) => `VH-${1000 + i}`);

// Seeded PRNG (mulberry32) so every seed run produces the same batch.
let seedState = 7;
function random() {
  seedState = (seedState + 0x6D2B79F5) | 0;
  let t = seedState;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const uniform = (lo, hi) => lo + random() * (hi - lo);
const randint = (lo, hi) => Math.floor(uniform(lo, hi + 1));
const choice = (arr) => arr[Math.floor(random() * arr.length)];
const round = (v, digits = 0) => Number(v.toFixed(digits));
function weightedChoice(options, weights) {
  let r = random() * weights.reduce((a, b) => a + b, 0);
  for (let i = 0; i < options.length; i++) {
    r -= weights[i];
    if (r < 0) return options[i];
  }
  return options[options.length - 1];
}

const startedAt = Date.now();
const isoAgo = (seconds) => new Date(startedAt - seconds * 1000).toISOString().replace(/\.\d+Z$/, '.000Z');

// Each generator builds rows oldest-first: Flink's event-time watermark on
// vehicle.telemetry would otherwise treat everything after the newest row
// as late data.
function oldestFirst(n, spacingSeconds, build) {
  return Array.from({ length: n }, (_, i) => build(isoAgo((n - 1 - i) * spacingSeconds)));
}

function telemetry(n = 200) {
  return oldestFirst(n, 3, (ts) => {
    const engineTemp = round(uniform(75, 108), 1);
    return {
      object_id: choice(VEHICLES),
      datetime: ts,
      ignition_status: choice(['ON', 'ON', 'ON', 'OFF']),
      trip_type: choice(['BUSINESS', 'UNKNOWN']),
      position: {
        altitude: round(uniform(0, 120), 2),
        longitude: round(144.9 + uniform(-0.3, 0.3), 6),
        latitude: round(-37.8 + uniform(-0.3, 0.3), 6),
        direction: round(uniform(0, 360), 1),
        satellites_count: randint(6, 14),
        speed: round(uniform(0, 100), 1),
      },
      inputs: {
        other: { country_code_geonames: 2077456, virtual_gps_odometer: round(uniform(1000, 200000), 2) },
        calculated_inputs: {
          fuel_consumption: round(uniform(0.1, 1.0), 3),
          fuel_level: round(uniform(5, 100), 1),
          mileage: round(uniform(1000, 200000), 2),
          rpm: round(uniform(600, 3000)),
          temperature: engineTemp,
          weight: round(uniform(500, 4000), 1),
        },
        device_inputs: {
          priority: choice(['LOW', 'HIGH']),
          movement: 'MOVING',
          hdop: String(round(uniform(0.3, 2.0), 1)),
          power_supply_voltage: round(uniform(12, 14.5), 2),
          battery_voltage: round(uniform(11.5, 12.8), 2),
          gps_speed: round(uniform(0, 100), 1),
          gsm_signal_strength: round(uniform(0.2, 1.0), 2),
          operator: round(random(), 2),
          engine_rpm: round(uniform(600, 3000)),
          canbus_engine_coolant_temperature: engineTemp,
          fuel_level_can: round(uniform(5, 100), 1),
          speed_wheel: round(uniform(0, 100), 1),
          overspeeding_events: random() < 0.15 ? 'OVERSPEED' : 'NO_OVERSPEED',
          ecodrive_braking_events: round(random(), 2),
          ecodrive_harsh_acceleration: round(random(), 2),
          ecodrive_idling_time: round(random(), 2),
          lcv_driver_doors: choice(['OPEN', 'CLOSE']),
          lcv_left_back_doors: choice(['OPEN', 'CLOSE']),
          lcv_right_back_doors: choice(['OPEN', 'CLOSE']),
        },
        tires: [1, 2, 3, 4].map((t) => ({
          tire_id: `tire0${t}`,
          tire_pressure: round(uniform(85, 100), 1),
          tire_temperature: round(uniform(20, 45), 1),
          tire_air_leakage_rate: round(uniform(0, 0.3), 3),
          tire_pressure_threshold_detection: randint(0, 1),
          tire_status: randint(0, 2),
          tire_sensor_electrical_fault: randint(0, 1),
          tire_sensor_enable_status: randint(0, 1),
          tire_location: t,
          tire_extended_tire_pressure_support: randint(0, 1),
        })),
      },
      geozone_ids: [choice(GEOZONES)],
    };
  });
}

function weather(n = 60) {
  return oldestFirst(n, 30, (ts) => ({
    geozone: choice(GEOZONES),
    condition: weightedChoice(['clear', 'light rain', 'heavy rain', 'fog'], [70, 15, 10, 5]),
    temp_c: round(uniform(10, 32), 1),
    wind_kph: round(uniform(5, 40), 1),
    updated_at: ts,
  }));
}

function trafficIncidents(n = 25) {
  return oldestFirst(n, 90, (ts) => ({
    geozone: choice(GEOZONES),
    type: choice(['congestion', 'road closure', 'collision reported', 'roadworks']),
    severity: choice(['low', 'medium', 'high']),
    timestamp: ts,
  }));
}

function parcelVolume(n = 200) {
  return oldestFirst(n, 15, (ts) => ({
    vehicle_id: choice(VEHICLES),
    geozone: choice(GEOZONES),
    parcels_onboard: randint(0, 90),
    parcels_delivered_last_hour: randint(0, 20),
    timestamp: ts,
  }));
}

async function topicHasData(topic) {
  const admin = getKafka().admin();
  await admin.connect();
  try {
    const offsets = await admin.fetchTopicOffsets(topic);
    return offsets.some(({ high, low }) => Number(high) > Number(low));
  } finally {
    await admin.disconnect();
  }
}

async function main() {
  if (!process.argv.includes('--force') && await topicHasData('vehicle.telemetry')) {
    console.error('[seed] vehicle.telemetry already has data - skipping (pass --force to seed anyway)');
    return;
  }
  const datasets = [
    ['vehicle.telemetry', telemetry()],
    ['weather.conditions', weather()],
    ['traffic.incidents', trafficIncidents()],
    ['parcel.volume', parcelVolume()],
  ];
  for (const [topic, rows] of datasets) {
    await produceRecords(topic, rows);
    console.error(`[seed] produced ${rows.length} records -> ${topic}`);
  }
}

main().then(() => process.exit(0), (err) => {
  console.error('[seed] failed:', err.message);
  process.exit(1);
});
