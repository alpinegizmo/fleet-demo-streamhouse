// telemetry-simulator.js
//
// Keeps the real Kafka pipeline continuously fed with fresh, realistic
// telemetry, instead of leaving it as the one-shot 485-record seed batch
// (see seed.js). Without
// this, the Flink jobs' watermarks stop advancing a few seconds after
// startup and the console has nothing new to show between demo-button
// clicks.
//
// Produces REAL records via kafka-client.js's produceRecord() - same
// mechanism the HTTP API's /api/inject-incident endpoint uses - so every
// number downstream (Flink-derived events, AI recommendations) is genuinely
// computed from these, not faked in this process's memory.
//
// Vehicle positions stay anchored near their geozone's real coordinate
// (same 6 anchors the console plots on the map) so the map shows coherent
// local drift instead of vehicles teleporting across the whole metro area.

import { produceRecord, produceRecords } from './kafka-client.js';

const GEOZONE_COORDS = {
  'melbourne-cbd': [-37.8136, 144.9631],
  'docklands-cbd': [-37.8142, 144.9490],
  'tullamarine-linehaul': [-37.6996, 144.8410],
  'essendon-fields': [-37.7280, 144.9020],
  'westgate-linehaul': [-37.8168, 144.8830],
  'dandenong-south': [-37.9930, 145.2150],
};
const GEOZONES = Object.keys(GEOZONE_COORDS);

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
// Compass bearing (0=N, 90=E) from a lat/lng step vector - used so the
// console's directional vehicle-arrow icon points the way a vehicle is
// actually moving, not a random heading unrelated to its real position deltas.
function bearingFromDelta(dLat, dLng) {
  return (Math.atan2(dLng, dLat) * 180 / Math.PI + 360) % 360;
}

function makeVehicleState(i) {
  const geozone = GEOZONES[i % GEOZONES.length];
  const [homeLat, homeLng] = GEOZONE_COORDS[geozone];
  return {
    id: 'VH-' + String(1000 + i),
    geozone,
    homeLat, homeLng,
    lat: homeLat + (Math.random() - 0.5) * 0.02,
    lng: homeLng + (Math.random() - 0.5) * 0.02,
    dLat: (Math.random() - 0.5) * 0.003,
    dLng: (Math.random() - 0.5) * 0.003,
    speed: 30 + Math.random() * 50,
    engineTemp: 78 + Math.random() * 12,
    fuel: 40 + Math.random() * 50,
    tyrePressure: 92 + Math.random() * 10,
    parcelsOnboard: Math.round(20 + Math.random() * 60),
  };
}

function step(v) {
  v.lat += v.dLat;
  v.lng += v.dLng;
  if (Math.abs(v.lat - v.homeLat) > 0.02) v.dLat *= -1;
  if (Math.abs(v.lng - v.homeLng) > 0.02) v.dLng *= -1;
  v.speed = clamp(v.speed + (Math.random() - 0.5) * 10, 0, 110);
  // Mean-reverting around a normal operating temperature, so running hot
  // (over the 100 C maintenance threshold) is occasional. A plain random walk
  // clamped at 108 C drifted up and stuck there: a quarter of all readings
  // raised a maintenance alert.
  v.engineTemp = clamp(v.engineTemp + (92 - v.engineTemp) * 0.03 + (Math.random() - 0.5) * 2.5, 70, 110);
  v.fuel = clamp(v.fuel - Math.random() * 0.3, 5, 100);
  v.tyrePressure = clamp(v.tyrePressure + (Math.random() - 0.5) * 0.6, 80, 102);
}

// Most braking is gentle; a harsh stop (score over the 0.7 safety threshold)
// is rare, and likelier at speed. A uniform random score made ~30% of all
// readings a harsh-braking safety event.
function brakingScore(v) {
  const harsh = Math.random() < (v.speed > 70 ? 0.04 : 0.01);
  return Number((harsh ? 0.72 + Math.random() * 0.28 : Math.random() * 0.4).toFixed(2));
}

function telemetryRecord(v) {
  const overspeed = v.speed > 95 ? Math.random() < 0.7 : Math.random() < 0.005;
  return {
    object_id: v.id,
    datetime: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
    ignition_status: v.speed > 2 ? 'ON' : pick(['ON', 'OFF']),
    trip_type: 'BUSINESS',
    position: {
      altitude: Math.round(20 + Math.random() * 80),
      longitude: Number(v.lng.toFixed(6)),
      latitude: Number(v.lat.toFixed(6)),
      direction: Math.round(bearingFromDelta(v.dLat, v.dLng)),
      satellites_count: 6 + Math.floor(Math.random() * 8),
      speed: Number(v.speed.toFixed(1)),
    },
    inputs: {
      other: { country_code_geonames: 2077456, virtual_gps_odometer: 100000 },
      calculated_inputs: {
        fuel_consumption: Number((0.1 + Math.random() * 0.9).toFixed(3)),
        fuel_level: Number(v.fuel.toFixed(1)),
        mileage: 100000,
        rpm: Math.round(600 + Math.random() * 2400),
        temperature: Number(v.engineTemp.toFixed(1)),
        weight: Math.round(500 + Math.random() * 3500),
      },
      device_inputs: {
        priority: overspeed ? 'HIGH' : 'LOW',
        movement: v.speed > 2 ? 'MOVING' : 'STATIONARY',
        hdop: (0.3 + Math.random() * 1.7).toFixed(1),
        power_supply_voltage: Number((12 + Math.random() * 2.5).toFixed(2)),
        battery_voltage: Number((11.5 + Math.random() * 1.3).toFixed(2)),
        gps_speed: Number(v.speed.toFixed(1)),
        gsm_signal_strength: Number((0.2 + Math.random() * 0.8).toFixed(2)),
        operator: Number(Math.random().toFixed(2)),
        engine_rpm: Math.round(600 + Math.random() * 2400),
        canbus_engine_coolant_temperature: Number(v.engineTemp.toFixed(1)),
        fuel_level_can: Number(v.fuel.toFixed(1)),
        speed_wheel: Number(v.speed.toFixed(1)),
        overspeeding_events: overspeed ? 'OVERSPEED' : 'NO_OVERSPEED',
        ecodrive_braking_events: brakingScore(v),
        ecodrive_harsh_acceleration: Number(Math.random().toFixed(2)),
        ecodrive_idling_time: Number(Math.random().toFixed(2)),
        lcv_driver_doors: 'CLOSE',
        lcv_left_back_doors: 'CLOSE',
        lcv_right_back_doors: 'CLOSE',
      },
      tires: [1, 2, 3, 4].map((n) => ({
        tire_id: `tire0${n}`,
        tire_pressure: Number((v.tyrePressure + (Math.random() - 0.5) * 2).toFixed(1)),
        tire_temperature: Number((20 + Math.random() * 25).toFixed(1)),
        tire_air_leakage_rate: Number((Math.random() * 0.3).toFixed(3)),
        tire_pressure_threshold_detection: 0,
        tire_status: 0,
        tire_sensor_electrical_fault: 0,
        tire_sensor_enable_status: 1,
        tire_location: n,
        tire_extended_tire_pressure_support: 1,
      })),
    },
    geozone_ids: [v.geozone],
  };
}

export function startTelemetrySimulator() {
  if (process.env.AUTO_TELEMETRY === 'false') {
    console.error('[fleet-telemetry-simulator] disabled via AUTO_TELEMETRY=false');
    return;
  }

  const vehicles = Array.from({ length: 12 }, (_, i) => makeVehicleState(i));

  // Moves every vehicle each tick, as one batched produce call. Previously
  // only 1-2 of the 12
  // vehicles moved per tick, so any given marker sat frozen on the map for
  // ~25s on average between updates - moving all of them every tick is what
  // actually makes movement visible, not a bigger step size.
  setInterval(async () => {
    try {
      await produceRecords('vehicle.telemetry', vehicles.map((v) => { step(v); return telemetryRecord(v); }));
    } catch (err) {
      console.error('[fleet-telemetry-simulator] vehicle.telemetry produce failed:', err.message);
    }
  }, 3000);

  setInterval(async () => {
    try {
      const geozone = pick(GEOZONES);
      await produceRecord('weather.conditions', {
        geozone,
        condition: pick(['clear', 'clear', 'clear', 'light rain', 'heavy rain', 'fog']),
        temp_c: Number((10 + Math.random() * 20).toFixed(1)),
        wind_kph: Number((5 + Math.random() * 35).toFixed(1)),
        updated_at: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      });
    } catch (err) {
      console.error('[fleet-telemetry-simulator] weather.conditions produce failed:', err.message);
    }
  }, 20000);

  setInterval(async () => {
    try {
      // 1% chance per 3s tick - averages one new incident every ~5 minutes.
      // Was 4% (~75s average): with only 6 geozones and 12 vehicles, that
      // rate kept most of the fleet sitting in a "recently incident-hit"
      // zone at any given moment, which read as implausibly saturated on
      // the Network Conditions table and the impacted-deliveries KPI.
      if (Math.random() >= 0.01) return;
      const geozone = pick(GEOZONES);
      await produceRecord('traffic.incidents', {
        geozone,
        type: pick(['congestion', 'road closure', 'collision reported', 'roadworks']),
        severity: pick(['low', 'medium', 'high']),
        timestamp: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      });
      console.error(`[fleet-telemetry-simulator] produced -> traffic.incidents (${geozone})`);
    } catch (err) {
      console.error('[fleet-telemetry-simulator] traffic.incidents produce failed:', err.message);
    }
  }, 3000);

  setInterval(async () => {
    try {
      const v = pick(vehicles);
      v.parcelsOnboard = clamp(v.parcelsOnboard + Math.round((Math.random() - 0.5) * 10), 0, 100);
      await produceRecord('parcel.volume', {
        vehicle_id: v.id,
        geozone: v.geozone,
        parcels_onboard: v.parcelsOnboard,
        parcels_delivered_last_hour: Math.round(Math.random() * 20),
        timestamp: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      });
    } catch (err) {
      console.error('[fleet-telemetry-simulator] parcel.volume produce failed:', err.message);
    }
  }, 8000);

  console.error('[fleet-telemetry-simulator] started - producing continuous real telemetry/weather/traffic/parcel data (AUTO_TELEMETRY=false to disable)');
}
