// topic-schemas.js
//
// The shape of every topic, for the MCP tools (mcp-server.js) and the
// queryable copy of recent records behind them (query-store.js). There's no
// schema registry, so this mirrors the Flink table definitions in
// local/flink/sql/00-tables.sql - change both together.
//
// Each schema is a DuckDB JSON "structure": a column maps to a DuckDB type,
// a nested object to a STRUCT, and a one-element array to a LIST. query-store
// passes it straight to DuckDB's from_json() to type the loaded records.
//
// injection_id is set only on records the console's inject buttons produced
// (and everything Flink derived from them); NULL otherwise.
//
// Every topic also gets kafka_timestamp (when the record was written to
// Kafka), added by query-store.js - so questions like "in the last 5
// minutes" can be answered for topics whose own time field is a string.

const TIRE = {
  tire_id: 'VARCHAR',
  tire_pressure: 'DOUBLE',
  tire_temperature: 'DOUBLE',
  tire_air_leakage_rate: 'DOUBLE',
  tire_pressure_threshold_detection: 'INTEGER',
  tire_status: 'INTEGER',
  tire_sensor_electrical_fault: 'INTEGER',
  tire_sensor_enable_status: 'INTEGER',
  tire_location: 'INTEGER',
  tire_extended_tire_pressure_support: 'INTEGER',
};

export const TOPICS = {
  'vehicle.telemetry': {
    description: 'Raw per-vehicle GPS/engine/sensor telemetry, one record per vehicle every ~3 seconds. Speed is position.speed (km/h); engine temperature is inputs.calculated_inputs.temperature (C).',
    schema: {
      object_id: 'VARCHAR',
      datetime: 'VARCHAR',
      ignition_status: 'VARCHAR',
      trip_type: 'VARCHAR',
      position: {
        altitude: 'DOUBLE',
        longitude: 'DOUBLE',
        latitude: 'DOUBLE',
        direction: 'DOUBLE',
        satellites_count: 'INTEGER',
        speed: 'DOUBLE',
      },
      inputs: {
        other: { country_code_geonames: 'DOUBLE', virtual_gps_odometer: 'DOUBLE' },
        calculated_inputs: {
          fuel_consumption: 'DOUBLE',
          fuel_level: 'DOUBLE',
          mileage: 'DOUBLE',
          rpm: 'DOUBLE',
          temperature: 'DOUBLE',
          weight: 'DOUBLE',
        },
        device_inputs: {
          priority: 'VARCHAR',
          movement: 'VARCHAR',
          hdop: 'VARCHAR',
          power_supply_voltage: 'DOUBLE',
          battery_voltage: 'DOUBLE',
          gps_speed: 'DOUBLE',
          gsm_signal_strength: 'DOUBLE',
          operator: 'DOUBLE',
          engine_rpm: 'DOUBLE',
          canbus_engine_coolant_temperature: 'DOUBLE',
          fuel_level_can: 'DOUBLE',
          speed_wheel: 'DOUBLE',
          overspeeding_events: 'VARCHAR',
          ecodrive_braking_events: 'DOUBLE',
          ecodrive_harsh_acceleration: 'DOUBLE',
          ecodrive_idling_time: 'DOUBLE',
          lcv_driver_doors: 'VARCHAR',
          lcv_left_back_doors: 'VARCHAR',
          lcv_right_back_doors: 'VARCHAR',
        },
        tires: [TIRE],
      },
      geozone_ids: ['VARCHAR'],
      injection_id: 'VARCHAR',
    },
  },
  'weather.conditions': {
    description: 'Weather per geozone, updated every ~20 seconds.',
    schema: { geozone: 'VARCHAR', condition: 'VARCHAR', temp_c: 'DOUBLE', wind_kph: 'DOUBLE', updated_at: 'VARCHAR' },
  },
  'traffic.incidents': {
    description: 'Traffic incidents per geozone (congestion, road closure, collision reported, roadworks) with severity low/medium/high.',
    schema: { geozone: 'VARCHAR', type: 'VARCHAR', severity: 'VARCHAR', timestamp: 'VARCHAR' },
  },
  'parcel.volume': {
    description: 'Per-vehicle parcel load: parcels currently onboard and delivered in the last hour.',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', parcels_onboard: 'INTEGER',
      parcels_delivered_last_hour: 'INTEGER', timestamp: 'VARCHAR',
    },
  },
  'maintenance.alerts': {
    description: 'Derived by Flink: a vehicle whose engine temperature exceeded 100C (severity high above 105C).',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', engine_temp_c: 'DOUBLE',
      severity: 'VARCHAR', message: 'VARCHAR', event_time: 'VARCHAR', injection_id: 'VARCHAR',
    },
  },
  'driver.risk.events': {
    description: 'Derived by Flink: driver-risk events from a fixed rule - the device flagged OVERSPEED, or harsh braking score above 0.7 (risk_level high if both).',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', speed_kmh: 'DOUBLE', overspeed_status: 'VARCHAR',
      harsh_braking_score: 'DOUBLE', risk_level: 'VARCHAR', event_time: 'VARCHAR', injection_id: 'VARCHAR',
    },
  },
  'driver.risk.events.anomaly': {
    description: "Derived by Flink: speed readings more than 3 standard deviations from that vehicle's own previous 30 readings (forecast_speed_kmh is the baseline mean; lower/upper_bound are mean ± 3σ).",
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', speed_kmh: 'DOUBLE', forecast_speed_kmh: 'DOUBLE',
      lower_bound: 'DOUBLE', upper_bound: 'DOUBLE', risk_level: 'VARCHAR', event_time: 'VARCHAR', injection_id: 'VARCHAR',
    },
  },
  'delivery.performance.events': {
    description: 'Derived by Flink: vehicles behind schedule (over 70 parcels onboard, under 5 delivered last hour) or ahead of schedule (15+ delivered last hour).',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', parcels_onboard: 'INTEGER',
      parcels_delivered_last_hour: 'INTEGER', performance_status: 'VARCHAR', event_time: 'VARCHAR',
    },
  },
  'ai.maintenance.recommendations': {
    description: 'Generated by Flink with a local LLM: one recommended maintenance action per maintenance alert.',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', severity: 'VARCHAR',
      trigger_event: 'VARCHAR', recommendation: 'VARCHAR', injection_id: 'VARCHAR',
    },
  },
  'ai.safety.recommendations': {
    description: 'Generated by Flink with a local LLM: one recommended safety action per medium/high driver-risk event.',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', risk_level: 'VARCHAR',
      speed_kmh: 'DOUBLE', recommendation: 'VARCHAR', injection_id: 'VARCHAR',
    },
  },
  'ai.delivery.recommendations': {
    description: 'Generated by Flink with a local LLM: one recommended delivery-ops action per delivery performance event.',
    schema: {
      vehicle_id: 'VARCHAR', geozone: 'VARCHAR', performance_status: 'VARCHAR',
      parcels_onboard: 'INTEGER', recommendation: 'VARCHAR',
    },
  },
};

// Renders a schema as DuckDB-style column types, e.g.
// { position: 'STRUCT(speed DOUBLE, ...)', geozone_ids: 'VARCHAR[]' }.
export function describeColumns(schema) {
  const typeOf = (t) => {
    if (Array.isArray(t)) return `${typeOf(t[0])}[]`;
    if (typeof t === 'object') return `STRUCT(${Object.entries(t).map(([k, v]) => `${k} ${typeOf(v)}`).join(', ')})`;
    return t;
  };
  return Object.fromEntries(Object.entries(schema).map(([name, t]) => [name, typeOf(t)]));
}
