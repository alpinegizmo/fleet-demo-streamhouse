// context-engine.js
//
// Reads the REAL fleet state from the local Kafka + Flink + LLM pipeline
// (docker-compose.yml), instead of simulating it in memory.
//
// A background Kafka consumer (kafkajs) tails all 11 real topics from the
// beginning and continuously rebuilds the same in-memory shape the
// original simulator used (vehicles Map, weather Map, rolling logs, etc.),
// so every public method here is a synchronous, instant read from memory -
// same as before. Running Flink SQL pull queries per tool call was tried
// first and is too slow/contends with the pipeline's own compute pool
// capacity for an interactive demo (each ad-hoc query is a real Flink job
// submission, 10-60+s depending on pool load) - tailing Kafka directly in
// the background is the right way to serve a real-time read model.
//
// Every topic carries plain JSON (no schema registry), so each message
// value is parsed directly.
//
// If KAFKA_BOOTSTRAP isn't set, this module falls back to the original
// in-memory simulator so the console still works with a plain `npm start`
// and no Kafka at all.

import { ALL_TOPICS, getKafka, produceRecord, purgeAllTopics } from './kafka-client.js';
import { remember } from './query-store.js';

const GEOZONES = [
  'tullamarine-linehaul',
  'docklands-cbd',
  'essendon-fields',
  'westgate-linehaul',
  'dandenong-south',
  'melbourne-cbd',
];

// Real-world anchor coordinates for each geozone - only used by the
// SIMULATED fallback engine below, so its vehicles still plot at coherent
// Melbourne metro positions on the console's map (LIVE mode gets real
// lat/lng straight from vehicle.telemetry's position field instead).
const GEOZONE_COORDS = {
  'melbourne-cbd': [-37.8136, 144.9631],
  'docklands-cbd': [-37.8142, 144.9490],
  'tullamarine-linehaul': [-37.6996, 144.8410],
  'essendon-fields': [-37.7280, 144.9020],
  'westgate-linehaul': [-37.8168, 144.8830],
  'dandenong-south': [-37.9930, 145.2150],
};

// Tags a demo injection so the pipeline carries it through to the
// recommendations it causes (the injection_id column in
// local/flink/sql/00-tables.sql) and the console can mark them.
function newInjectionId() {
  return `inj-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

export function isLiveModeConfigured() {
  return Boolean(process.env.KAFKA_BOOTSTRAP);
}

function decodeValue(buffer) {
  return JSON.parse(buffer.toString('utf8'));
}

// How close in time (via receivedAt) a heuristic overspeed event and an AI
// anomaly for the same vehicle need to be to count as corroborating each
// other. vehicle.telemetry ticks every 3s, so this covers several ticks
// either side (allowing for the two Flink jobs' independent processing
// latency) without being so loose that almost any recent anomaly for a busy
// vehicle counts - a wide window (e.g. minutes) made "confirmed" trivially
// true for most overspeed events, which isn't a meaningful comparison.
const RISK_MATCH_WINDOW_MS = 20 * 1000;

// A vehicle's maintenance / driver risk comes from its most recent alert of
// that kind, and nothing lowers it while the vehicle drives normally - so
// without an expiry, one overheat kept a vehicle red on the map (and counted
// as an open work order) indefinitely. Risk now lapses this long after the
// alert that set it, unless a newer alert renews it. Measured from the
// alert's Kafka timestamp, so history replayed at startup is already stale.
const RISK_HOLD_MS = 2 * 60 * 1000;

function severityToRisk(severity) {
  if (severity === 'high') return 90;
  if (severity === 'medium') return 65;
  if (severity === 'low') return 30;
  return 0;
}

// ---------------------------------------------------------------------------
// "Behind the Scenes" pipeline visualization (fleet-intelligence-console.html
// Behind the Scenes tab). Each hop below is a (node, edge) pair in that tab's
// diagram - a topic arrival fires every hop in its list, in order, so the
// animation can show the real journey: raw telemetry -> Kafka -> Flink
// business rules -> Flink+LLM -> the served in-memory model -> its real
// consumers (this console and MCP agents). Business-topic arrivals only
// render the first hop (Kafka -> Flink) since their onward journey to
// serve/app isn't separately observable until the AI recommendation for them
// arrives.
//
// The diagram's Iceberg and DuckDB nodes (still keyed 'tableflow' and
// 'snowflake', after the Confluent Cloud services they stand in for) aren't
// driven by topic arrivals at all: they pulse when Flink actually commits a
// new Iceberg snapshot - see _watchIceberg.
const AI_HOPS = [
  { node: 'flinkAi', edge: 'flinkBusiness-flinkAi' },
  { node: 'serve', edge: 'flinkAi-serve' },
  { node: 'app', edge: 'serve-app' },
];

// The Iceberg tables written by local/flink/sql/30-lakehouse.sql.
const LAKE_TABLES = [
  'maintenance_alerts', 'driver_risk_events', 'driver_risk_events_anomaly', 'delivery_performance_events',
  'ai_maintenance_recommendations', 'ai_safety_recommendations', 'ai_delivery_recommendations',
];
const TOPIC_PULSE_HOPS = {
  'vehicle.telemetry': [{ node: 'kafka', edge: 'source-kafka' }],
  'weather.conditions': [{ node: 'kafka', edge: 'source-kafka' }],
  'traffic.incidents': [{ node: 'kafka', edge: 'source-kafka' }],
  'parcel.volume': [{ node: 'kafka', edge: 'source-kafka' }],
  'maintenance.alerts': [{ node: 'flinkBusiness', edge: 'kafka-flinkBusiness' }],
  'driver.risk.events': [{ node: 'flinkBusiness', edge: 'kafka-flinkBusiness' }],
  'driver.risk.events.anomaly': [{ node: 'flinkBusiness', edge: 'kafka-flinkBusiness' }],
  'delivery.performance.events': [{ node: 'flinkBusiness', edge: 'kafka-flinkBusiness' }],
  'ai.maintenance.recommendations': AI_HOPS,
  'ai.safety.recommendations': AI_HOPS,
  'ai.delivery.recommendations': AI_HOPS,
};

function summarizePulseDetail(topic, r) {
  switch (topic) {
    case 'vehicle.telemetry': return `${r.object_id} · ${r.geozone_ids?.[0] || ''}`;
    case 'weather.conditions': return `${r.geozone} · ${r.condition}`;
    case 'traffic.incidents': return `${r.geozone} · ${r.type}`;
    case 'parcel.volume': return `${r.vehicle_id} · ${r.parcels_onboard} onboard`;
    case 'maintenance.alerts': return `${r.vehicle_id} · ${r.severity}`;
    case 'driver.risk.events': return `${r.vehicle_id} · ${r.risk_level}`;
    case 'driver.risk.events.anomaly': return `${r.vehicle_id} · ${r.speed_kmh}km/h vs forecast ${r.forecast_speed_kmh}`;
    case 'delivery.performance.events': return `${r.vehicle_id} · ${r.performance_status}`;
    case 'ai.maintenance.recommendations':
    case 'ai.safety.recommendations':
    case 'ai.delivery.recommendations': {
      const text = r.recommendation || '';
      return `${r.vehicle_id} · "${text.slice(0, 50)}${text.length > 50 ? '…' : ''}"`;
    }
    default: return '';
  }
}

class LiveContextEngine {
  constructor() {
    this.vehicles = new Map();
    this.weather = new Map();
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    this.driverRiskAnomalyEvents = [];
    this.deliveryPerformanceEvents = [];
    this.maintenanceAlerts = [];
    this._recentMsgTimestamps = [];
    this.ready = false;

    // Behind the Scenes tab state - see TOPIC_PULSE_HOPS above.
    this.pipelineEvents = [];
    this._pipelineSeq = 0;
    this.pipelineStats = { kafka: 0, flinkBusiness: 0, flinkAi: 0, serve: 0, app: 0, agent: 0, tableflow: 0, snowflake: 0 };

    this.kafka = getKafka();

    this._consuming = this._startConsumer();
    this._watchIceberg();
  }

  // Polls the Iceberg REST catalog (ICEBERG_REST_URL) for each lakehouse
  // table's latest snapshot. A new snapshot means Flink just committed rows
  // that DuckDB (or any Iceberg engine) can now query - that's what the
  // Behind the Scenes tab's Iceberg/DuckDB nodes show. Snapshots are
  // compared by sequence number, not snapshot-id: ids are 64-bit and lose
  // precision as JavaScript numbers.
  _watchIceberg() {
    const base = process.env.ICEBERG_REST_URL;
    if (!base) return;
    const lastSeq = new Map();
    const totalRows = new Map();
    setInterval(async () => {
      await Promise.all(LAKE_TABLES.map(async (table) => {
        try {
          const resp = await fetch(`${base}/v1/namespaces/fleet/tables/${table}`);
          if (!resp.ok) return;
          const { metadata } = await resp.json();
          const seq = metadata['last-sequence-number'];
          const snapshot = metadata.snapshots?.find((s) => s['sequence-number'] === seq);
          if (!snapshot) return;
          totalRows.set(table, Number(snapshot.summary?.['total-records'] ?? 0));
          const previous = lastSeq.get(table);
          lastSeq.set(table, seq);
          // First sighting is just the baseline, not a commit seen live.
          if (previous === undefined || previous === seq) return;
          const added = Number(snapshot.summary?.['added-records'] ?? 0);
          this._pushPulseHop('tableflow', 'serve-tableflow', `Iceberg commit · ${table} +${added} rows`);
          this._pushPulseHop('snowflake', 'tableflow-snowflake', `queryable in DuckDB · lake.fleet.${table}`);
        } catch {
          // catalog not up yet, or briefly unreachable - try again next tick
        }
      }));
      // 'snowflake' is a gauge (rows queryable now), not a hop counter.
      this.pipelineStats.snowflake = [...totalRows.values()].reduce((a, b) => a + b, 0);
    }, 5000);
  }

  async _startConsumer() {
    const admin = this.kafka.admin();
    await admin.connect();

    const topics = ALL_TOPICS;

    const consumer = this.kafka.consumer({ groupId: `fleet-context-engine-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topics, fromBeginning: true });

    // Behind the Scenes tab: this consumer always replays every topic from
    // offset 0 on every process start (fresh random groupId above, so every
    // container restart re-reads full history) - great for
    // instantly rebuilding the in-memory model the other tabs read, but
    // without this, that same full-history replay would also fire a
    // pipeline-event pulse per historical message, making the Behind the
    // Scenes animation look like a firehose right after every restart
    // instead of real-time pace. Snapshotting each topic-partition's
    // high-water-mark offset before consuming starts - same technique
    // river-svof-demo's bridge server uses for its compacted topics - lets
    // _recordPipelinePulse (below) tell "existed before this process
    // started" apart from "genuinely arrived just now", without touching
    // _applyRecord's own instant full-history rebuild.
    const pulseCatchupTarget = new Map(); // `${topic}:${partition}` -> offset
    (await Promise.all(topics.map((topic) => admin.fetchTopicOffsets(topic)))).forEach((offsets, i) => {
      offsets.forEach(({ partition, offset }) => pulseCatchupTarget.set(`${topics[i]}:${partition}`, Number(offset)));
    });
    await admin.disconnect();

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;
        let record;
        try {
          record = decodeValue(message.value);
        } catch {
          return; // skip anything that isn't valid JSON
        }
        this._applyRecord(topic, record, Number(message.timestamp));
        remember(topic, record, message.timestamp); // for SQL via MCP - see query-store.js

        // Everything below is about live traffic only, not the history this
        // consumer replays on startup (see pulseCatchupTarget above).
        const target = pulseCatchupTarget.get(`${topic}:${partition}`) ?? 0;
        if (Number(message.offset) < target) return;
        this._recordPipelinePulse(topic, record);
        // Real basis for the "events/sec" KPI: every message that arrived
        // live, across all 11 topics, timestamped and trimmed to a 10s window.
        const now = Date.now();
        this._recentMsgTimestamps.push(now);
        this._recentMsgTimestamps = this._recentMsgTimestamps.filter((t) => now - t < 10000);
      },
    });

    // kafkajs has no built-in "caught up to the point where consumption
    // started" signal for a simple run() loop; a short grace period after
    // connecting is good enough for this demo's fixed-size seed batches.
    setTimeout(() => { this.ready = true; }, 8000);
  }

  _applyRecord(topic, r, kafkaTs = Date.now()) {
    switch (topic) {
      case 'vehicle.telemetry': {
        const geozone = r.geozone_ids?.[0];
        const existing = this.vehicles.get(r.object_id);
        if (existing && existing.datetime >= r.datetime) break;
        const tires = r.inputs?.tires;
        const avgTyrePressurePsi = tires?.length
          ? tires.reduce((s, t) => s + (t.tire_pressure || 0), 0) / tires.length
          : existing?.avgTyrePressurePsi;
        this.vehicles.set(r.object_id, {
          ...existing,
          id: r.object_id,
          datetime: r.datetime,
          geozone,
          lat: r.position?.latitude ?? existing?.lat,
          lng: r.position?.longitude ?? existing?.lng,
          direction: r.position?.direction ?? existing?.direction,
          ignitionStatus: r.ignition_status,
          tripType: r.trip_type,
          speed: r.position?.speed,
          engineTemp: r.inputs?.calculated_inputs?.temperature,
          fuel: r.inputs?.calculated_inputs?.fuel_level,
          overspeed: r.inputs?.device_inputs?.overspeeding_events === 'OVERSPEED',
          harshBrakingEvents: r.inputs?.device_inputs?.ecodrive_braking_events,
          avgTyrePressurePsi: avgTyrePressurePsi != null ? Number(avgTyrePressurePsi.toFixed(1)) : undefined,
          maintRisk: existing?.maintRisk ?? 0,
          driverRisk: existing?.driverRisk ?? 0,
          parcelsOnboard: existing?.parcelsOnboard ?? 0,
        });
        break;
      }
      case 'weather.conditions': {
        const existing = this.weather.get(r.geozone);
        if (existing && existing.updatedAt >= r.updated_at) break;
        this.weather.set(r.geozone, { geozone: r.geozone, condition: r.condition, tempC: r.temp_c, windKph: r.wind_kph, updatedAt: r.updated_at });
        break;
      }
      case 'traffic.incidents': {
        this.trafficIncidents.unshift({ geozone: r.geozone, type: r.type, severity: r.severity, timestamp: r.timestamp });
        this.trafficIncidents = this.trafficIncidents.slice(0, 50);
        break;
      }
      case 'parcel.volume': {
        this.parcelVolumeLog.unshift({ vehicleId: r.vehicle_id, geozone: r.geozone, parcelsOnboard: r.parcels_onboard, parcelsDeliveredLastHour: r.parcels_delivered_last_hour, timestamp: r.timestamp });
        this.parcelVolumeLog = this.parcelVolumeLog.slice(0, 100);
        const v = this.vehicles.get(r.vehicle_id);
        if (v) v.parcelsOnboard = r.parcels_onboard;
        break;
      }
      case 'maintenance.alerts': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) {
          v.maintRisk = severityToRisk(r.severity);
          v.maintRiskAt = kafkaTs;
        }
        // receivedAt (wall-clock consumption time), not r.event_time, is what
        // the trace-strip UI correlates against an injection's timestamp -
        // event_time is the Flink-derived business event's own clock, not
        // when this process actually saw it.
        this.maintenanceAlerts.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          engineTempC: r.engine_temp_c,
          severity: r.severity,
          message: r.message,
          eventTime: r.event_time,
          injectionId: r.injection_id ?? null,
          receivedAt: Date.now(),
        });
        this.maintenanceAlerts = this.maintenanceAlerts.slice(0, 200);
        break;
      }
      case 'driver.risk.events': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) {
          v.driverRisk = severityToRisk(r.risk_level);
          v.driverRiskAt = kafkaTs;
          v.overspeed = r.overspeed_status === 'OVERSPEED';
          v.harshBrakingEvents = r.harsh_braking_score;
        }
        this.driverRiskEvents.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          speedKmh: r.speed_kmh,
          overspeedStatus: r.overspeed_status,
          harshBrakingScore: r.harsh_braking_score,
          riskLevel: r.risk_level,
          eventTime: r.event_time,
          receivedAt: Date.now(),
        });
        this.driverRiskEvents = this.driverRiskEvents.slice(0, 200);
        break;
      }
      // The per-vehicle z-score baseline (local/flink/sql/10-business.sql) -
      // deliberately kept
      // separate from the 'driver.risk.events' case above rather than
      // merging into the same vehicle.driverRisk/overspeed fields, so the
      // existing Safety tab keeps showing today's heuristic unchanged and
      // this feed can be shown alongside it for a side-by-side comparison.
      case 'driver.risk.events.anomaly': {
        this.driverRiskAnomalyEvents.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          speedKmh: r.speed_kmh,
          forecastSpeedKmh: r.forecast_speed_kmh,
          lowerBound: r.lower_bound,
          upperBound: r.upper_bound,
          riskLevel: r.risk_level,
          eventTime: r.event_time,
          receivedAt: Date.now(),
        });
        this.driverRiskAnomalyEvents = this.driverRiskAnomalyEvents.slice(0, 200);
        break;
      }
      case 'delivery.performance.events': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) {
          v.performanceStatus = r.performance_status;
          v.parcelsDeliveredLastHour = r.parcels_delivered_last_hour;
        }
        this.deliveryPerformanceEvents.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          parcelsOnboard: r.parcels_onboard,
          parcelsDeliveredLastHour: r.parcels_delivered_last_hour,
          performanceStatus: r.performance_status,
          eventTime: r.event_time,
          receivedAt: Date.now(),
        });
        this.deliveryPerformanceEvents = this.deliveryPerformanceEvents.slice(0, 200);
        break;
      }
      case 'ai.maintenance.recommendations':
        this._pushRecommendation('maintenance', r.vehicle_id, r.recommendation, r.severity, r.injection_id);
        break;
      case 'ai.safety.recommendations':
        this._pushRecommendation('safety', r.vehicle_id, r.recommendation, r.risk_level, r.injection_id);
        break;
      case 'ai.delivery.recommendations':
        this._pushRecommendation('delivery', r.vehicle_id, r.recommendation, r.performance_status);
        break;
      default:
        break;
    }
  }

  // Keeps the newest 200 per domain, not 200 overall: safety events arrive
  // ~30x as often as maintenance alerts, so a single shared cap pushed every
  // maintenance recommendation out within seconds.
  _pushRecommendation(domain, vehicleId, text, severity, injectionId = null) {
    this.recommendations.unshift({ domain, vehicleId, text, severity, injectionId: injectionId ?? null, timestamp: new Date().toISOString() });
    const perDomain = {};
    this.recommendations = this.recommendations.filter((r) => (perDomain[r.domain] = (perDomain[r.domain] || 0) + 1) <= 200);
  }

  _pushPulseHop(node, edge, detail) {
    this.pipelineEvents.push({ id: ++this._pipelineSeq, node, edge, detail, ts: Date.now() });
    this.pipelineEvents = this.pipelineEvents.slice(-500);
    if (this.pipelineStats[node] != null) this.pipelineStats[node] += 1;
  }

  _recordPipelinePulse(topic, record) {
    const hops = TOPIC_PULSE_HOPS[topic];
    if (!hops) return;
    const detail = summarizePulseDetail(topic, record);
    hops.forEach((hop) => this._pushPulseHop(hop.node, hop.edge, detail));
  }

  // Real, on-demand signal for the Behind the Scenes tab's "Agent" node -
  // fired for every SQL query an agent runs through the queryData tool,
  // whether from an external MCP client (Claude, Copilot) or this console's
  // own chat widget (see mcp-server.js). Unlike the continuous Kafka-driven
  // hops above, this is a genuine on-demand pull, so it only pulses when
  // actually queried.
  recordAgentQuery(sql) {
    const text = (sql || '').replace(/\s+/g, ' ').trim();
    this._pushPulseHop('agent', 'serve-agent', `MCP query · ${text.slice(0, 60)}${text.length > 60 ? '…' : ''}`);
  }

  async getPipelineEvents(sinceId = 0) {
    const events = this.pipelineEvents.filter((e) => e.id > sinceId).slice(-200);
    return {
      events,
      stats: this.pipelineStats,
      latestId: this.pipelineEvents.length ? this.pipelineEvents[this.pipelineEvents.length - 1].id : sinceId,
      live: true,
    };
  }

  // --- public interface matching the original simulator's shape --------

  _expireRisks() {
    const now = Date.now();
    for (const v of this.vehicles.values()) {
      if (v.maintRisk && now - (v.maintRiskAt ?? 0) > RISK_HOLD_MS) v.maintRisk = 0;
      if (v.driverRisk && now - (v.driverRiskAt ?? 0) > RISK_HOLD_MS) v.driverRisk = 0;
    }
  }

  async getFleetSummary() {
    this._expireRisks();
    const list = [...this.vehicles.values()];
    const avg = (fn) => (list.length ? list.reduce((s, v) => s + (fn(v) || 0), 0) / list.length : 0);
    const now = Date.now();
    const recentDriverEvents = (windowMs) => this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const recentDeliveryEvents = this.deliveryPerformanceEvents.slice(0, 30);
    const onTimeCount = recentDeliveryEvents.filter((e) => e.performanceStatus !== 'behind_schedule').length;

    return {
      vehiclesOnline: list.length,
      avgSpeedKmh: Number(avg((v) => v.speed).toFixed(1)),
      avgEngineTempC: Number(avg((v) => v.engineTemp).toFixed(1)),
      avgFuelLevelPct: Number(avg((v) => v.fuel).toFixed(1)),
      avgTyrePressurePsi: Number(avg((v) => v.avgTyrePressurePsi).toFixed(1)),
      totalParcelsOnboard: list.reduce((s, v) => s + (v.parcelsOnboard || 0), 0),
      highDriverRiskCount: list.filter((v) => v.driverRisk > 70).length,
      highMaintenanceRiskCount: list.filter((v) => v.maintRisk > 70).length,
      // "Open work order" = a vehicle whose most recent maintenance.alerts
      // message put it over the risk threshold and nothing since has
      // cleared it. There's no separate open/closed work-order state in
      // this topic's schema, so a vehicle currently over threshold IS the
      // open work order - same underlying number as highMaintenanceRiskCount,
      // exposed under its business name for the console's Maintenance view.
      openWorkOrders: list.filter((v) => v.maintRisk > 70).length,
      activeTrafficIncidents: this.trafficIncidents.length,
      openRecommendations: this.recommendations.length,
      recommendationCounts: {
        safety: this.recommendations.filter((r) => r.domain === 'safety').length,
        delivery: this.recommendations.filter((r) => r.domain === 'delivery').length,
        maintenance: this.recommendations.filter((r) => r.domain === 'maintenance').length,
      },
      // Real measured throughput - count of Kafka messages (any topic)
      // consumed in the trailing 10s window, not a fabricated number.
      eventsPerSecond: Number((this._recentMsgTimestamps.length / 10).toFixed(1)),
      // harsh_braking_score > 0.7 is the same threshold the real Flink job
      // (derive_driver_risk_events) uses to flag a harsh-braking event.
      harshBrakingEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.harshBrakingScore > 0.7).length,
      overspeedEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.overspeedStatus === 'OVERSPEED').length,
      // Replaces the old "Fatigue flags" tile, which had no backing data
      // anywhere in the real schema (no duty-hour/fatigue field exists).
      highRiskEventsRecent: recentDriverEvents(15 * 60 * 1000).filter((e) => e.riskLevel === 'high').length,
      onTimeRatePct: recentDeliveryEvents.length ? Number(((onTimeCount / recentDeliveryEvents.length) * 100).toFixed(0)) : null,
      // Replaces the old "Avg delivery ETA drift" tile, which had no real
      // scheduled/actual-time field to compute drift from.
      avgParcelsDeliveredPerHour: recentDeliveryEvents.length
        ? Number((recentDeliveryEvents.reduce((s, e) => s + (e.parcelsDeliveredLastHour || 0), 0) / recentDeliveryEvents.length).toFixed(1))
        : null,
      routesDisruptedCount: list.filter((v) => v.performanceStatus === 'behind_schedule').length + this.trafficIncidents.length,
      generatedAt: new Date().toISOString(),
      source: 'local-kafka',
      llmModel: process.env.LLM_MODEL || null,
      consumerCaughtUp: this.ready,
    };
  }

  async getVehicle(id) {
    this._expireRisks();
    const v = this.vehicles.get(id);
    if (!v) return null;
    return { ...v, currentWeather: this.weather.get(v.geozone) ?? null };
  }

  async listVehicles() {
    this._expireRisks();
    return [...this.vehicles.values()].map((v) => ({ ...v, currentWeather: this.weather.get(v.geozone) ?? null }));
  }

  async listHighRisk(domain) {
    const list = await this.listVehicles();
    if (domain === 'safety') return list.filter((v) => v.driverRisk > 70);
    if (domain === 'maintenance') return list.filter((v) => v.maintRisk > 70);
    return list.filter((v) => v.driverRisk > 70 || v.maintRisk > 70);
  }

  async getRecentRecommendations(domain, limit = 10, vehicleId) {
    const filtered = this.recommendations.filter((r) => (!domain || r.domain === domain) && (!vehicleId || r.vehicleId === vehicleId));
    return filtered.slice(0, limit);
  }

  async getWeather(geozone) {
    if (geozone) return this.weather.get(geozone) ?? null;
    return [...this.weather.values()];
  }

  async getTrafficIncidents(geozone, limit = 10) {
    const filtered = geozone ? this.trafficIncidents.filter((i) => i.geozone === geozone) : this.trafficIncidents;
    return filtered.slice(0, limit);
  }

  async getParcelVolume(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.parcelVolumeLog.filter((p) => p.vehicleId === vehicleId) : this.parcelVolumeLog;
    return filtered.slice(0, limit);
  }

  // Used by the console's trace-strip UI to detect the moment Flink's
  // derive_maintenance_alerts statement has actually processed an injected
  // vehicle.telemetry record - receivedAt lets the client tell "this alert
  // is newer than my injection" apart from a pre-existing one.
  async getMaintenanceAlerts(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.maintenanceAlerts.filter((a) => a.vehicleId === vehicleId) : this.maintenanceAlerts;
    return filtered.slice(0, limit);
  }

  // Whether the per-vehicle baseline corroborates a single heuristic event -
  // exposed per-event (via getDriverRiskEvents below) rather than only as an
  // aggregate, so the verdict on any one card is independently checkable
  // instead of something the console just asserts.
  //
  // Matched on receivedAt (this process's own Date.now() at consumption
  // time, for BOTH feeds), not the business event_time - the two jobs
  // format event_time differently (driver.risk.events passes vehicle.
  // telemetry's ISO `datetime` through untouched; driver.risk.events.anomaly
  // is Flink's CAST(TIMESTAMP AS STRING), which renders in the Flink
  // session's local time zone rather than UTC) so comparing those strings
  // directly was silently comparing timestamps hours apart. receivedAt is
  // the same clock, same process, for both feeds, so it has no such
  // zone-formatting hazard, and it's still a reasonable proxy for "arrived
  // at roughly the same moment" since both jobs read the same
  // near-real-time vehicle.telemetry stream.
  //
  // Only the OVERSPEED half of the heuristic is compared at all - the other
  // half (harsh_braking_score > 0.7) flags on a signal this AI job was never
  // given (the baseline job only watches speed_kmh), so there is no
  // fair verdict to render for those; they're labeled 'not-evaluated'
  // instead of being silently counted as either confirmed or noise.
  _riskVerdict(e, anomalyEvents, matchWindowMs) {
    if (e.overspeedStatus !== 'OVERSPEED') return 'not-evaluated';
    const corroborated = anomalyEvents.some((a) => a.vehicleId === e.vehicleId && Math.abs(a.receivedAt - e.receivedAt) < matchWindowMs);
    return corroborated ? 'confirmed' : 'normal-for-vehicle';
  }

  // Raw driver.risk.events feed (today's fixed-threshold heuristic),
  // enriched with aiVerdict (see _riskVerdict) - and its per-vehicle
  // baseline counterpart - exposed side by side for the Safety tab's before/after
  // comparison panel.
  async getDriverRiskEvents(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.driverRiskEvents.filter((e) => e.vehicleId === vehicleId) : this.driverRiskEvents;
    return filtered.slice(0, limit).map((e) => ({ ...e, aiVerdict: this._riskVerdict(e, this.driverRiskAnomalyEvents, RISK_MATCH_WINDOW_MS) }));
  }

  async getDriverRiskAnomalyEvents(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.driverRiskAnomalyEvents.filter((e) => e.vehicleId === vehicleId) : this.driverRiskAnomalyEvents;
    return filtered.slice(0, limit);
  }

  // Aggregate version of the same per-event verdict above, for the Safety
  // tab's KPI tiles. Every count here is directly reproducible from what
  // getDriverRiskEvents/getDriverRiskAnomalyEvents themselves return -
  // there's no aggregate-only signal being asserted without the underlying
  // per-event evidence also being visible in the feeds.
  async getRiskComparison(windowMs = 15 * 60 * 1000) {
    const now = Date.now();
    const heuristic = this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const anomaly = this.driverRiskAnomalyEvents.filter((e) => now - e.receivedAt < windowMs);

    const overspeedHeuristic = heuristic.filter((e) => e.overspeedStatus === 'OVERSPEED');
    const overspeedConfirmed = overspeedHeuristic.filter((e) => this._riskVerdict(e, anomaly, RISK_MATCH_WINDOW_MS) === 'confirmed').length;

    // Mirrors the speed > 95 km/h half of derive_driver_risk_events' own
    // threshold, applied to the same speed_kmh column the anomaly feed
    // carries - "would the heuristic also flag this instant" on the one
    // signal both jobs actually share.
    const anomalyCaughtByHeuristic = anomaly.filter((e) => e.speedKmh > 95).length;

    return {
      windowMinutes: Math.round(windowMs / 60000),
      brakingOnlyHeuristicCount: heuristic.length - overspeedHeuristic.length,
      overspeedHeuristicCount: overspeedHeuristic.length,
      overspeedNoiseCount: overspeedHeuristic.length - overspeedConfirmed,
      anomalyCount: anomaly.length,
      anomalyMissedByHeuristicCount: anomaly.length - anomalyCaughtByHeuristic,
    };
  }

  // Demo control: produces a REAL record to vehicle.telemetry
  // with a spiked engine temperature, so the real Flink pipeline derives a
  // real maintenance alert and the real local LLM generates a real
  // recommendation - the live-data equivalent of the "Inject engine spike"
  // button in the HTML console. Reflects back into this process's own
  // in-memory state once its own consumer reads the message back.
  async injectIncident(vehicleId) {
    const ids = [...this.vehicles.keys()];
    const id = vehicleId ?? ids[Math.floor(Math.random() * ids.length)];
    const base = this.vehicles.get(id);
    const geozone = base?.geozone ?? GEOZONES[Math.floor(Math.random() * GEOZONES.length)];
    const record = {
      object_id: id,
      datetime: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      ignition_status: 'ON',
      trip_type: 'BUSINESS',
      position: { altitude: 50, longitude: 144.9, latitude: -37.8, direction: 180, satellites_count: 10, speed: 40 },
      inputs: {
        other: { country_code_geonames: 2077456, virtual_gps_odometer: 100000 },
        calculated_inputs: { fuel_consumption: 0.5, fuel_level: 50, mileage: 100000, rpm: 2000, temperature: 104, weight: 2000 },
        device_inputs: {
          priority: 'HIGH', movement: 'MOVING', hdop: '0.5',
          power_supply_voltage: 13, battery_voltage: 12, gps_speed: 40, gsm_signal_strength: 0.8, operator: 0.5,
          engine_rpm: 2000, canbus_engine_coolant_temperature: 104, fuel_level_can: 50, speed_wheel: 40,
          overspeeding_events: 'OVERSPEED', ecodrive_braking_events: 0.9, ecodrive_harsh_acceleration: 0.5, ecodrive_idling_time: 0.1,
          lcv_driver_doors: 'CLOSE', lcv_left_back_doors: 'CLOSE', lcv_right_back_doors: 'CLOSE',
        },
        tires: [1, 2, 3, 4].map((n) => ({
          tire_id: `tire0${n}`, tire_pressure: 95, tire_temperature: 30,
          tire_air_leakage_rate: 0, tire_pressure_threshold_detection: 0, tire_status: 0,
          tire_sensor_electrical_fault: 0, tire_sensor_enable_status: 1, tire_location: n, tire_extended_tire_pressure_support: 1,
        })),
      },
      geozone_ids: [geozone],
      injection_id: newInjectionId(),
    };
    await produceRecord('vehicle.telemetry', record);
    return { id, geozone, injectionId: record.injection_id, injected: 'engine_spike', note: 'Real record produced to vehicle.telemetry - the real Flink pipeline will derive a maintenance alert and AI recommendation within a few seconds.' };
  }

  // Demo control: produces a REAL high-severity traffic incident record.
  async injectTrafficIncident(geozone) {
    const g = geozone ?? GEOZONES[Math.floor(Math.random() * GEOZONES.length)];
    const record = {
      geozone: g,
      type: 'collision reported',
      severity: 'high',
      timestamp: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
    };
    await produceRecord('traffic.incidents', record);
    return { ...record, note: 'Real record produced to traffic.incidents.' };
  }

  // Demo control: produces a REAL vehicle.telemetry record with speed far
  // outside the simulator's normal 0-110 km/h range, so the real
  // per-vehicle baseline job (driver.risk.events.anomaly) flags a
  // genuine anomaly within a few seconds,
  // instead of waiting for the random walk to happen to drift there -
  // useful for recording a demo without an open-ended wait. The heuristic
  // (driver.risk.events) will also fire on this via its own OVERSPEED
  // threshold, so it shows up in both feeds - a real side-by-side moment,
  // not a fabricated one.
  async injectSpeedAnomaly(vehicleId) {
    const ids = [...this.vehicles.keys()];
    const id = vehicleId ?? ids[Math.floor(Math.random() * ids.length)];
    const base = this.vehicles.get(id);
    const geozone = base?.geozone ?? GEOZONES[Math.floor(Math.random() * GEOZONES.length)];
    const spikeSpeed = 150;
    const record = {
      object_id: id,
      datetime: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      ignition_status: 'ON',
      trip_type: 'BUSINESS',
      position: { altitude: 50, longitude: 144.9, latitude: -37.8, direction: 180, satellites_count: 10, speed: spikeSpeed },
      inputs: {
        other: { country_code_geonames: 2077456, virtual_gps_odometer: 100000 },
        calculated_inputs: { fuel_consumption: 0.5, fuel_level: 50, mileage: 100000, rpm: 4200, temperature: 90, weight: 2000 },
        device_inputs: {
          priority: 'HIGH', movement: 'MOVING', hdop: '0.5',
          power_supply_voltage: 13, battery_voltage: 12, gps_speed: spikeSpeed, gsm_signal_strength: 0.8, operator: 0.5,
          engine_rpm: 4200, canbus_engine_coolant_temperature: 90, fuel_level_can: 50, speed_wheel: spikeSpeed,
          overspeeding_events: 'OVERSPEED', ecodrive_braking_events: 0.2, ecodrive_harsh_acceleration: 0.9, ecodrive_idling_time: 0,
          lcv_driver_doors: 'CLOSE', lcv_left_back_doors: 'CLOSE', lcv_right_back_doors: 'CLOSE',
        },
        tires: [1, 2, 3, 4].map((n) => ({
          tire_id: `tire0${n}`, tire_pressure: 95, tire_temperature: 30,
          tire_air_leakage_rate: 0, tire_pressure_threshold_detection: 0, tire_status: 0,
          tire_sensor_electrical_fault: 0, tire_sensor_enable_status: 1, tire_location: n, tire_extended_tire_pressure_support: 1,
        })),
      },
      geozone_ids: [geozone],
      injection_id: newInjectionId(),
    };
    await produceRecord('vehicle.telemetry', record);
    return { id, geozone, speedKmh: spikeSpeed, injectionId: record.injection_id, injected: 'speed_anomaly', note: 'Real record produced to vehicle.telemetry with speed far outside baseline. Both the heuristic (OVERSPEED) and the per-vehicle baseline should flag it in driver.risk.events / driver.risk.events.anomaly within a few seconds.' };
  }

  // Demo control: HARD reset. Purges all 11 real topics (see
  // kafka-client.js's purgeAllTopics - deletes records, not topics, so the
  // running Flink jobs are untouched) and clears this process's own in-memory view. This is
  // destructive and irreversible - the real seeded data and every
  // real AI recommendation generated so far are actually deleted.
  async resetPipeline() {
    const result = await purgeAllTopics();
    this.vehicles.clear();
    this.weather.clear();
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    this.driverRiskAnomalyEvents = [];
    this.deliveryPerformanceEvents = [];
    this._recentMsgTimestamps = [];
    return { ...result, note: 'Real topics purged and in-memory state cleared. The Flink jobs were not touched - new data will flow through normally.' };
  }
}

// --- fallback: original in-memory simulator, used only when KAFKA_BOOTSTRAP
// isn't configured, so the server still runs
// out of the box without any setup. ---

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
// Compass bearing (0=N, 90=E) from a lat/lng step vector - same formula as
// telemetry-simulator.js's LIVE-mode equivalent, so the console's
// directional vehicle-arrow icon works identically in both modes.
function bearingFromDelta(dLat, dLng) {
  return (Math.atan2(dLng, dLat) * 180 / Math.PI + 360) % 360;
}

function makeVehicle(i) {
  const geozone = pick(GEOZONES);
  const [homeLat, homeLng] = GEOZONE_COORDS[geozone];
  return {
    id: 'VH-' + String(1000 + i),
    tripType: pick(['BUSINESS', 'UNKNOWN']),
    ignitionStatus: 'ON',
    geozone,
    homeLat, homeLng,
    lat: homeLat + (Math.random() - 0.5) * 0.02,
    lng: homeLng + (Math.random() - 0.5) * 0.02,
    dLat: (Math.random() - 0.5) * 0.0016,
    dLng: (Math.random() - 0.5) * 0.0016,
    direction: Math.round(Math.random() * 360),
    speed: 30 + Math.random() * 50,
    engineTemp: 78 + Math.random() * 12,
    fuel: 40 + Math.random() * 50,
    avgTyrePressurePsi: 92 + Math.random() * 10,
    driverRisk: Math.random() * 35,
    maintRisk: Math.random() * 35,
    overspeed: false,
    harshBrakingEvents: 0,
    performanceStatus: 'on_schedule',
    parcelsDeliveredLastHour: Math.round(Math.random() * 10),
    parcelsOnboard: Math.round(20 + Math.random() * 60),
    spiked: false,
    lastUpdated: new Date().toISOString(),
  };
}

function makeWeather(geozone) {
  return {
    geozone,
    condition: pick(['clear', 'clear', 'clear', 'light rain', 'heavy rain', 'fog']),
    tempC: Math.round(10 + Math.random() * 20),
    windKph: Math.round(5 + Math.random() * 35),
    updatedAt: new Date().toISOString(),
  };
}

class SimulatedContextEngine {
  constructor() {
    this.vehicles = new Map(
      Array.from({ length: 12 }, (_, i) => makeVehicle(i)).map((v) => [v.id, v])
    );
    this.weather = new Map(GEOZONES.map((g) => [g, makeWeather(g)]));
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    // No simulated equivalent of driver.risk.events.anomaly - the baseline
    // only runs in the real Flink pipeline.
    // Left permanently empty rather than fabricated, per getDriverRiskAnomalyEvents below.
    this.driverRiskAnomalyEvents = [];
    this.deliveryPerformanceEvents = [];
    this.maintenanceAlerts = [];
    this._recentMsgTimestamps = [];
    this._interval = setInterval(() => this.tick(), 1000);
  }

  _recordEvent() {
    const now = Date.now();
    this._recentMsgTimestamps.push(now);
    this._recentMsgTimestamps = this._recentMsgTimestamps.filter((t) => now - t < 10000);
  }

  stop() { clearInterval(this._interval); }

  tick() {
    if (Math.random() < 0.1) {
      const g = pick(GEOZONES);
      this.weather.set(g, makeWeather(g));
      this._recordEvent();
    }
    if (Math.random() < 0.03) {
      const g = pick(GEOZONES);
      this.trafficIncidents.unshift({
        geozone: g,
        type: pick(['congestion', 'road closure', 'collision reported', 'roadworks']),
        severity: pick(['low', 'medium', 'high']),
        timestamp: new Date().toISOString(),
      });
      this.trafficIncidents = this.trafficIncidents.slice(0, 50);
      this._recordEvent();
    }
    for (const v of this.vehicles.values()) {
      const w = this.weather.get(v.geozone);
      const wetRoad = w.condition === 'heavy rain' || w.condition === 'light rain';
      const activeIncidentHere = this.trafficIncidents.find((i) => i.geozone === v.geozone);

      v.lat += v.dLat; v.lng += v.dLng;
      if (Math.abs(v.lat - v.homeLat) > 0.015) v.dLat *= -1;
      if (Math.abs(v.lng - v.homeLng) > 0.015) v.dLng *= -1;
      v.direction = Math.round(bearingFromDelta(v.dLat, v.dLng));

      v.speed = Math.max(0, v.speed + (Math.random() - 0.5) * 8 - (activeIncidentHere ? 4 : 0));
      if (!v.spiked) v.engineTemp = Math.max(70, Math.min(99, v.engineTemp + (Math.random() - 0.5) * 2));
      v.fuel = Math.max(5, v.fuel - Math.random() * 0.15);
      v.avgTyrePressurePsi = Math.max(80, v.avgTyrePressurePsi + (Math.random() - 0.5) * 0.5);
      v.overspeed = v.speed > 95;

      const harshBrakeChance = wetRoad ? 0.12 : 0.04;
      const harshBrakingScore = Math.random() < harshBrakeChance ? 0.7 + Math.random() * 0.3 : Math.random() * 0.6;
      if (harshBrakingScore > 0.7) v.harshBrakingEvents += 1;

      v.driverRisk = Math.max(0, Math.min(100,
        v.driverRisk
        + (v.overspeed ? Math.random() * 15 : -Math.random() * 3)
        + (wetRoad ? Math.random() * 4 : 0)
      ));
      v.maintRisk = v.engineTemp > 95 ? Math.min(100, v.maintRisk + 6) : Math.max(0, v.maintRisk - 1);
      this._recordEvent();

      // Mirrors the real driver.risk.events schema/thresholds
      // (local/flink/sql/10-business.sql), so
      // getFleetSummary()'s rolling-window stats work identically in both
      // modes.
      const riskLevel = (v.overspeed && harshBrakingScore > 0.7) ? 'high' : (v.overspeed || harshBrakingScore > 0.7) ? 'medium' : 'low';
      this.driverRiskEvents.unshift({
        vehicleId: v.id, geozone: v.geozone, harshBrakingScore,
        overspeedStatus: v.overspeed ? 'OVERSPEED' : 'NO_OVERSPEED',
        riskLevel, receivedAt: Date.now(),
      });
      this.driverRiskEvents = this.driverRiskEvents.slice(0, 200);

      if (Math.random() < 0.15) {
        v.parcelsOnboard = Math.max(0, v.parcelsOnboard + Math.round((Math.random() - 0.6) * 6));
        v.parcelsDeliveredLastHour = Math.round(Math.random() * 20);
        v.performanceStatus = (v.parcelsOnboard > 70 && v.parcelsDeliveredLastHour < 5) ? 'behind_schedule'
          : (v.parcelsDeliveredLastHour >= 15) ? 'ahead_of_schedule' : 'on_schedule';
        this.parcelVolumeLog.unshift({ vehicleId: v.id, geozone: v.geozone, parcelsOnboard: v.parcelsOnboard, timestamp: new Date().toISOString() });
        this.parcelVolumeLog = this.parcelVolumeLog.slice(0, 100);
        this.deliveryPerformanceEvents.unshift({
          vehicleId: v.id, geozone: v.geozone, parcelsOnboard: v.parcelsOnboard,
          parcelsDeliveredLastHour: v.parcelsDeliveredLastHour, performanceStatus: v.performanceStatus,
          receivedAt: Date.now(),
        });
        this.deliveryPerformanceEvents = this.deliveryPerformanceEvents.slice(0, 200);
        this._recordEvent();
      }

      v.lastUpdated = new Date().toISOString();

      if (v.maintRisk > 70 && Math.random() < 0.08) {
        this.pushRecommendation('maintenance', v.id,
          `Vehicle ${v.id} shows signs of cooling-system degradation near ${v.geozone}. Inspect within 48 hours.`,
          v.maintRisk > 85 ? 'high' : 'medium');
      }
      if (v.driverRisk > 70 && Math.random() < 0.06) {
        const weatherNote = wetRoad ? ` (road conditions in ${v.geozone} are currently ${w.condition})` : '';
        this.pushRecommendation('safety', v.id,
          `Driver on ${v.id} flagged for a harsh-braking / overspeed pattern${weatherNote} - recommend a coaching check-in.`,
          v.driverRisk > 85 ? 'high' : 'medium');
      }
      if (activeIncidentHere && Math.random() < 0.1) {
        this.pushRecommendation('delivery', v.id,
          `${v.id} approaching a ${activeIncidentHere.severity}-severity ${activeIncidentHere.type} in ${v.geozone} - recommend rerouting remaining stops.`,
          activeIncidentHere.severity === 'high' ? 'high' : 'medium');
      }
    }
  }

  pushRecommendation(domain, vehicleId, text, severity, injectionId = null) {
    this.recommendations.unshift({
      domain, vehicleId, text, severity, injectionId,
      confidence: Math.round(85 + Math.random() * 10),
      timestamp: new Date().toISOString(),
    });
    this.recommendations = this.recommendations.slice(0, 200);
  }

  async getFleetSummary() {
    const list = [...this.vehicles.values()];
    const avg = (fn) => list.reduce((s, v) => s + fn(v), 0) / list.length;
    const now = Date.now();
    const recentDriverEvents = (windowMs) => this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const recentDeliveryEvents = this.deliveryPerformanceEvents.slice(0, 30);
    const onTimeCount = recentDeliveryEvents.filter((e) => e.performanceStatus !== 'behind_schedule').length;

    return {
      vehiclesOnline: list.length,
      avgSpeedKmh: Number(avg((v) => v.speed).toFixed(1)),
      avgEngineTempC: Number(avg((v) => v.engineTemp).toFixed(1)),
      avgTyrePressurePsi: Number(avg((v) => v.avgTyrePressurePsi).toFixed(1)),
      totalParcelsOnboard: list.reduce((s, v) => s + v.parcelsOnboard, 0),
      highDriverRiskCount: list.filter((v) => v.driverRisk > 70).length,
      highMaintenanceRiskCount: list.filter((v) => v.maintRisk > 70).length,
      openWorkOrders: list.filter((v) => v.maintRisk > 70).length,
      activeTrafficIncidents: this.trafficIncidents.length,
      openRecommendations: this.recommendations.length,
      recommendationCounts: {
        safety: this.recommendations.filter((r) => r.domain === 'safety').length,
        delivery: this.recommendations.filter((r) => r.domain === 'delivery').length,
        maintenance: this.recommendations.filter((r) => r.domain === 'maintenance').length,
      },
      eventsPerSecond: Number((this._recentMsgTimestamps.length / 10).toFixed(1)),
      harshBrakingEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.harshBrakingScore > 0.7).length,
      overspeedEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.overspeedStatus === 'OVERSPEED').length,
      highRiskEventsRecent: recentDriverEvents(15 * 60 * 1000).filter((e) => e.riskLevel === 'high').length,
      onTimeRatePct: recentDeliveryEvents.length ? Number(((onTimeCount / recentDeliveryEvents.length) * 100).toFixed(0)) : null,
      avgParcelsDeliveredPerHour: recentDeliveryEvents.length
        ? Number((recentDeliveryEvents.reduce((s, e) => s + (e.parcelsDeliveredLastHour || 0), 0) / recentDeliveryEvents.length).toFixed(1))
        : null,
      routesDisruptedCount: list.filter((v) => v.performanceStatus === 'behind_schedule').length + this.trafficIncidents.length,
      generatedAt: new Date().toISOString(),
      source: 'in-memory-simulator',
    };
  }

  async getVehicle(id) {
    const v = this.vehicles.get(id);
    if (!v) return null;
    return { ...v, currentWeather: this.weather.get(v.geozone) };
  }

  async listVehicles() {
    return [...this.vehicles.values()].map((v) => ({ ...v, currentWeather: this.weather.get(v.geozone) }));
  }

  async listHighRisk(domain) {
    const list = [...this.vehicles.values()];
    if (domain === 'safety') return list.filter((v) => v.driverRisk > 70);
    if (domain === 'maintenance') return list.filter((v) => v.maintRisk > 70);
    return list.filter((v) => v.driverRisk > 70 || v.maintRisk > 70);
  }

  async getRecentRecommendations(domain, limit = 10, vehicleId) {
    const filtered = this.recommendations.filter((r) => (!domain || r.domain === domain) && (!vehicleId || r.vehicleId === vehicleId));
    return filtered.slice(0, limit);
  }

  async getWeather(geozone) {
    if (geozone) return this.weather.get(geozone) ?? null;
    return [...this.weather.values()];
  }

  async getTrafficIncidents(geozone, limit = 10) {
    const filtered = geozone ? this.trafficIncidents.filter((i) => i.geozone === geozone) : this.trafficIncidents;
    return filtered.slice(0, limit);
  }

  async getParcelVolume(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.parcelVolumeLog.filter((p) => p.vehicleId === vehicleId) : this.parcelVolumeLog;
    return filtered.slice(0, limit);
  }

  async getMaintenanceAlerts(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.maintenanceAlerts.filter((a) => a.vehicleId === vehicleId) : this.maintenanceAlerts;
    return filtered.slice(0, limit);
  }

  // aiVerdict always null - no simulated anomaly feed to compare against
  // (see getDriverRiskAnomalyEvents below), so there's no verdict to render.
  async getDriverRiskEvents(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.driverRiskEvents.filter((e) => e.vehicleId === vehicleId) : this.driverRiskEvents;
    return filtered.slice(0, limit).map((e) => ({ ...e, aiVerdict: null }));
  }

  // Always empty - see this.driverRiskAnomalyEvents in the constructor.
  async getDriverRiskAnomalyEvents() {
    return [];
  }

  // No simulated anomaly feed to compare against - see getRiskComparison on
  // LiveContextEngine for the real comparison this mirrors the shape of.
  async getRiskComparison(windowMs = 15 * 60 * 1000) {
    const now = Date.now();
    const recent = this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const overspeedHeuristicCount = recent.filter((e) => e.overspeedStatus === 'OVERSPEED').length;
    return {
      windowMinutes: Math.round(windowMs / 60000),
      brakingOnlyHeuristicCount: recent.length - overspeedHeuristicCount,
      overspeedHeuristicCount,
      overspeedNoiseCount: 0,
      anomalyCount: 0,
      anomalyMissedByHeuristicCount: 0,
    };
  }

  async injectIncident(vehicleId) {
    const v = vehicleId ? this.vehicles.get(vehicleId) : [...this.vehicles.values()][Math.floor(Math.random() * this.vehicles.size)];
    if (!v) return null;
    v.spiked = true;
    v.engineTemp = 104;
    v.maintRisk = 92;
    v.driverRisk = Math.max(v.driverRisk, 72);
    const injectionId = newInjectionId();
    this.maintenanceAlerts.unshift({
      vehicleId: v.id, geozone: v.geozone, engineTempC: v.engineTemp, severity: 'high',
      message: `Vehicle ${v.id} engine temperature ${v.engineTemp}C near ${v.geozone}`,
      eventTime: new Date().toISOString(), injectionId, receivedAt: Date.now(),
    });
    this.maintenanceAlerts = this.maintenanceAlerts.slice(0, 200);
    this.pushRecommendation('maintenance', v.id, `Vehicle ${v.id} shows signs of cooling-system degradation near ${v.geozone}. Inspect the vehicle within 48 hours.`, 'high', injectionId);
    this.pushRecommendation('safety', v.id, `Elevated engine heat on ${v.id} correlated with harsh-braking pattern - recommend a driver check-in.`, 'medium', injectionId);
    this.pushRecommendation('delivery', v.id, `${v.id} flagged for depot diversion in ${v.geozone} - reassigning remaining stops to nearest available vehicle.`, 'high', injectionId);
    setTimeout(() => { v.spiked = false; }, 15000);
    return { ...v, injectionId };
  }

  async injectTrafficIncident(geozone) {
    const g = geozone ?? pick(GEOZONES);
    const incident = { geozone: g, type: 'collision reported', severity: 'high', timestamp: new Date().toISOString() };
    this.trafficIncidents.unshift(incident);
    this.trafficIncidents = this.trafficIncidents.slice(0, 50);
    return incident;
  }

  // No real baseline job to trigger in SIMULATED mode - just bumps
  // the vehicle's own (already-simulated) speed and driverRisk fields, same
  // as the rest of tick()'s heuristic, rather than fabricating an anomaly
  // verdict this mode has no real pipeline to back up.
  async injectSpeedAnomaly(vehicleId) {
    const v = vehicleId ? this.vehicles.get(vehicleId) : [...this.vehicles.values()][Math.floor(Math.random() * this.vehicles.size)];
    if (!v) return null;
    v.speed = 150;
    v.overspeed = true;
    v.driverRisk = Math.max(v.driverRisk, 80);
    return { id: v.id, geozone: v.geozone, speedKmh: v.speed, injected: 'speed_anomaly', note: 'SIMULATED mode - no real baseline feed to confirm this against.' };
  }

  // The Behind the Scenes tab's diagram is only meaningful against the real
  // pipeline (real topic arrivals, real Iceberg commits) - in
  // SIMULATED mode there's nothing genuine to animate, so this reports
  // "not live" instead of fabricating pulses, matching this file's own rule
  // that the browser never gets to see a made-up number.
  recordAgentQuery() {}

  async getPipelineEvents() {
    return { events: [], stats: null, latestId: 0, live: false };
  }
}

export const isLive = isLiveModeConfigured();
export const contextEngine = isLive ? new LiveContextEngine() : new SimulatedContextEngine();
export { GEOZONES };
