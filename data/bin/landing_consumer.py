#!/usr/bin/env python3
import os
import sys
import json
import time
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

from kafka import KafkaConsumer, TopicPartition


# ----------------------------
# Defaults
# ----------------------------
LATE_THRESHOLD_SEC_DEFAULT = 300  # 5 minutes

# Impact: simple proxy (retro minute bucket density)
IMPACT_THRESHOLD_DEFAULT = 0.95  # you can tune; higher = fewer quarantines by impact
IMPACT_WINDOW_MINUTES_DEFAULT = 120

# Ops health: rolling late-rate / late-count (GLOBAL)
OPS_LATE_WINDOW_MINUTES_DEFAULT = 5
OPS_LATE_RATE_THRESHOLD_DEFAULT = 0.20
OPS_LATE_COUNT_THRESHOLD_DEFAULT = 200

# Warm-up guard: do not allow impact-based quarantine unless bucket already has volume
MIN_BUCKET_COUNT_DEFAULT = 10


# ============================================================
# Time helpers
# ============================================================
def parse_event_dt(payload: Dict[str, Any], kafka_ts_ms: Optional[int]) -> datetime:
    """
    Prefer payload["ts"] if present (ISO8601). Fall back to Kafka message timestamp, then now().
    Returns timezone-aware datetime in UTC.
    """
    ts = payload.get("ts")
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    if kafka_ts_ms is not None:
        return datetime.fromtimestamp(kafka_ts_ms / 1000.0, tz=timezone.utc)

    return datetime.now(tz=timezone.utc)


def floor_to_minute(dt_utc: datetime) -> datetime:
    return dt_utc.replace(second=0, microsecond=0)


def iso_minute(dt_utc: datetime) -> str:
    return floor_to_minute(dt_utc).isoformat().replace("+00:00", "Z")


# ============================================================
# Filesystem helpers (durability)
# ============================================================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, obj: Any) -> None:
    """
    Atomic write: write tmp -> fsync -> replace.
    """
    ensure_dir(os.path.dirname(path))
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def open_append(path: str):
    ensure_dir(os.path.dirname(path))
    return open(path, "a", encoding="utf-8")


# ============================================================
# Append-only partitioned JSONL writers
# ============================================================
class PartitionedWriter:
    """
    Writes JSONL partitioned by dt/hr:
      root/topic/dt=YYYY-MM-DD/hr=HH/topic-YYYYMMDD-HH.jsonl
    Keeps open file handles per path. Flush+fsync each record (durable).
    """
    def __init__(self, root: str, topic: str):
        self.root = root
        self.topic = topic
        self._open: Dict[str, Any] = {}

    def _path_for_dt(self, dt_utc: datetime) -> str:
        d = dt_utc.strftime("%Y-%m-%d")
        h = dt_utc.strftime("%H")
        ymdh = dt_utc.strftime("%Y%m%d-%H")
        return os.path.join(
            self.root,
            self.topic,
            f"dt={d}",
            f"hr={h}",
            f"{self.topic}-{ymdh}.jsonl",
        )

    def write_jsonl(self, dt_utc: datetime, record: Dict[str, Any]) -> str:
        path = self._path_for_dt(dt_utc)
        f = self._open.get(path)
        if f is None:
            f = open_append(path)
            self._open[path] = f

        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        f.write(line)
        f.flush()
        os.fsync(f.fileno())
        return path

    def close_all(self):
        for f in self._open.values():
            try:
                f.close()
            except Exception:
                pass
        self._open.clear()


# ============================================================
# Checkpoint offsets (durable)
# ============================================================
def load_checkpoint(path: str) -> Dict[str, Dict[str, int]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    out: Dict[str, Dict[str, int]] = {}
    for topic, parts in (data or {}).items():
        out[topic] = {str(p): int(o) for p, o in (parts or {}).items()}
    return out


# ============================================================
# Ops Health (GLOBAL): rolling late-rate / late-count
# ============================================================
def rolling_window_keys(now_utc: datetime, window_minutes: int) -> List[str]:
    now_min = floor_to_minute(now_utc)
    return [
        iso_minute(now_min - timedelta(minutes=i))
        for i in range(max(1, window_minutes))
    ]


def ops_anomaly_rolling(
        now_utc: datetime,
        minute_total: Dict[str, int],
        minute_late: Dict[str, int],
        window_minutes: int,
        rate_threshold: float,
        count_threshold: int,
) -> Dict[str, Any]:
    keys = rolling_window_keys(now_utc, window_minutes)

    total = sum(int(minute_total.get(k, 0)) for k in keys)
    late = sum(int(minute_late.get(k, 0)) for k in keys)

    rate = (late / total) if total > 0 else 0.0

    status = (rate > rate_threshold) or (late > count_threshold)
    if rate > rate_threshold:
        reason = "late_rate_high"
    elif late > count_threshold:
        reason = "late_count_high"
    else:
        reason = "ok"

    return {
        "status_anomaly": bool(status),
        "reason": reason,
        "window_minutes": window_minutes,
        "late_rate": rate,
        "late_count": late,
        "total_count": total,
        "thresholds": {
            "late_rate_threshold": rate_threshold,
            "late_count_threshold": count_threshold,
        },
        "keys_tail": keys[:3],  # tiny hint of window (most recent 3)
    }


# ============================================================
# Impact: simple per-event-minute density score
# ============================================================
def impact_score_for(event_dt_utc: datetime, minute_event_counts: Dict[str, int]) -> Dict[str, Any]:
    minute_key = iso_minute(event_dt_utc)
    before = int(minute_event_counts.get(minute_key, 0))
    score = 1.0 / (1.0 + float(before))  # higher when bucket is sparse
    return {
        "bucket_minute_utc": minute_key,
        "bucket_count_before": before,
        "impact_score": score,
    }


# ============================================================
# Main
# ============================================================
def main() -> int:
    # ----------------------------
    # Config
    # ----------------------------
    topic = os.environ.get("TOPIC", "signals")
    bootstrap = os.environ.get(
        "BOOTSTRAP_SERVERS",
        "kafka-controller-0.kafka-controller-headless.infra.svc.cluster.local:9092",
    )

    landing_root = os.environ.get("LANDING_ROOT", os.path.expanduser("~/landing-zone/landing"))
    delayed_root = os.environ.get("DELAYED_ROOT", os.path.expanduser("~/landing-zone/delayed"))
    quarantine_root = os.environ.get("QUARANTINE_ROOT", os.path.expanduser("~/landing-zone/quarantine"))

    checkpoint_path = os.environ.get(
        "CHECKPOINT_PATH",
        os.path.expanduser(f"~/landing-zone/checkpoints/{topic}/offsets.json"),
    )
    state_path = os.environ.get("STATE_PATH", os.path.expanduser("~/landing-zone/state/late_state.json"))
    alerts_path = os.environ.get("ALERTS_PATH", os.path.expanduser("~/landing-zone/alerts/late_alerts.jsonl"))
    log_path = os.environ.get("LOG_PATH", os.path.expanduser("~/landing-zone/logs/landing-consumer.log"))

    late_threshold_sec = int(os.environ.get("LATE_THRESHOLD_SEC", str(LATE_THRESHOLD_SEC_DEFAULT)))

    impact_threshold = float(os.environ.get("IMPACT_THRESHOLD", str(IMPACT_THRESHOLD_DEFAULT)))
    impact_window_minutes = int(os.environ.get("IMPACT_WINDOW_MINUTES", str(IMPACT_WINDOW_MINUTES_DEFAULT)))
    min_bucket_count = int(os.environ.get("MIN_BUCKET_COUNT", str(MIN_BUCKET_COUNT_DEFAULT)))

    ops_late_window_minutes = int(os.environ.get("OPS_LATE_WINDOW_MINUTES", str(OPS_LATE_WINDOW_MINUTES_DEFAULT)))
    ops_late_rate_threshold = float(os.environ.get("OPS_LATE_RATE_THRESHOLD", str(OPS_LATE_RATE_THRESHOLD_DEFAULT)))
    ops_late_count_threshold = int(os.environ.get("OPS_LATE_COUNT_THRESHOLD", str(OPS_LATE_COUNT_THRESHOLD_DEFAULT)))

    # Ensure dirs
    ensure_dir(os.path.dirname(checkpoint_path))
    ensure_dir(os.path.dirname(state_path))
    ensure_dir(os.path.dirname(alerts_path))
    ensure_dir(os.path.dirname(log_path))
    ensure_dir(landing_root)
    ensure_dir(delayed_root)
    ensure_dir(quarantine_root)

    # ----------------------------
    # Logging
    # ----------------------------
    def log(msg: str):
        ts = datetime.now(tz=timezone.utc).isoformat()
        line = f"{ts} {msg}"
        print(line, flush=True)
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(line + "\n")

    def write_alert(alert_obj: Dict[str, Any]) -> None:
        with open(alerts_path, "a", encoding="utf-8") as af:
            af.write(json.dumps(alert_obj, separators=(",", ":"), ensure_ascii=False) + "\n")
            af.flush()
            os.fsync(af.fileno())

    # ----------------------------
    # Startup config log
    # ----------------------------
    log("========== Landing Consumer Configuration ==========")
    log(f"topic={topic}")
    log(f"bootstrap={bootstrap}")

    log("---- Late Policy ----")
    log(f"late_threshold_sec={late_threshold_sec}")

    log("---- Impact Policy ----")
    log(f"impact_threshold={impact_threshold}")
    log(f"impact_window_minutes={impact_window_minutes}")
    log(f"min_bucket_count={min_bucket_count}")

    log("---- Ops Health Policy (GLOBAL rolling window) ----")
    log(f"ops_late_window_minutes={ops_late_window_minutes}")
    log(f"ops_late_rate_threshold={ops_late_rate_threshold}")
    log(f"ops_late_count_threshold={ops_late_count_threshold}")

    log("---- Paths ----")
    log(f"landing_root={landing_root}")
    log(f"delayed_root={delayed_root}")
    log(f"quarantine_root={quarantine_root}")
    log(f"checkpoint_path={checkpoint_path}")
    log(f"state_path={state_path}")
    log(f"alerts_path={alerts_path}")
    log("=====================================================")

    # ----------------------------
    # Load checkpoint + state
    # ----------------------------
    ckpt = load_checkpoint(checkpoint_path)
    last_offsets = ckpt.get(topic, {})  # partition(str) -> last_offset(int)

    state = load_json(state_path, default={})

    # Impact state: event-minute counts (retro bucket density)
    minute_event_counts: Dict[str, int] = state.get("minute_event_counts", {}) or {}

    # Ops state: ingest-minute totals and late totals (rolling health)
    minute_ingest_total: Dict[str, int] = state.get("minute_ingest_total", {}) or {}
    minute_ingest_late: Dict[str, int] = state.get("minute_ingest_late", {}) or {}

    def prune_state(now_utc: datetime) -> None:
        # Keep enough for both impact + ops; impact usually larger window.
        keep_minutes = max(impact_window_minutes, ops_late_window_minutes) + 2
        cutoff_ts = floor_to_minute(now_utc).timestamp() - (keep_minutes * 60)

        def prune_map(dct: Dict[str, int]):
            to_del = []
            for k in list(dct.keys()):
                try:
                    dt = datetime.fromisoformat(k.replace("Z", "+00:00"))
                    if dt.timestamp() < cutoff_ts:
                        to_del.append(k)
                except Exception:
                    to_del.append(k)
            for k in to_del:
                dct.pop(k, None)

        prune_map(minute_event_counts)
        prune_map(minute_ingest_total)
        prune_map(minute_ingest_late)

    # ----------------------------
    # Writers
    # ----------------------------
    w_landing = PartitionedWriter(landing_root, topic)
    w_delayed = PartitionedWriter(delayed_root, topic)
    w_quarantine = PartitionedWriter(quarantine_root, topic)

    # ----------------------------
    # Shutdown handling
    # ----------------------------
    running = True

    def handle_stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    # ----------------------------
    # EXTRACT: Kafka consumer (manual assign; no group)
    # ----------------------------
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
        consumer_timeout_ms=1000,
        api_version_auto_timeout_ms=10000,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
        max_poll_records=500,
    )

    # Discover partitions and assign
    parts = None
    while running and not parts:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            log(f"Waiting for topic metadata: {topic}")
            time.sleep(0.5)

    if not parts:
        log("No partitions discovered. Exiting.")
        consumer.close()
        return 2

    tps = [TopicPartition(topic, p) for p in sorted(parts)]
    consumer.assign(tps)
    log(f"Assigned partitions: {sorted(parts)}")

    # Seek from checkpoint
    for tp in tps:
        p = str(tp.partition)
        if p in last_offsets:
            seek_to = last_offsets[p] + 1
            consumer.seek(tp, seek_to)
            log(f"Seek {topic}[{p}] -> {seek_to} (from checkpoint)")
        else:
            consumer.seek_to_beginning(tp)
            log(f"Seek {topic}[{p}] -> beginning (no checkpoint)")

    log("Entering consume loop...")

    # ----------------------------
    # Main loop
    # ----------------------------
    processed = 0
    while running:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            continue

        for tp, msgs in batch.items():
            p = str(tp.partition)
            last = int(last_offsets.get(p, -1))

            for m in msgs:
                # idempotent on restart
                if m.offset <= last:
                    continue

                # Parse message
                raw = m.value
                try:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        payload = {"value": payload}
                except Exception:
                    payload = {"raw": raw}

                # Times
                event_dt_utc = parse_event_dt(payload, m.timestamp)
                ingest_dt_utc = datetime.now(tz=timezone.utc)

                lateness_sec = int((ingest_dt_utc - event_dt_utc).total_seconds())
                if lateness_sec < 0:
                    lateness_sec = 0

                is_late = lateness_sec > late_threshold_sec
                source = str(payload.get("source", "unknown"))

                # ---- Ops health (GLOBAL rolling)
                ops_info = ops_anomaly_rolling(
                    now_utc=ingest_dt_utc,
                    minute_total=minute_ingest_total,
                    minute_late=minute_ingest_late,
                    window_minutes=ops_late_window_minutes,
                    rate_threshold=ops_late_rate_threshold,
                    count_threshold=ops_late_count_threshold,
                )

                # ---- Impact only matters for late records (retro correction)
                if is_late:
                    impact_info = impact_score_for(event_dt_utc, minute_event_counts)
                else:
                    impact_info = {
                        "bucket_minute_utc": iso_minute(event_dt_utc),
                        "bucket_count_before": int(minute_event_counts.get(iso_minute(event_dt_utc), 0)),
                        "impact_score": 0.0,
                    }

                # ----------------------------
                # AI DECISION: routing
                # ----------------------------
                if not is_late:
                    route = "landing"
                    reason = "on_time_or_within_threshold"
                else:
                    # Quarantine if rolling ops says pipeline unhealthy (late-rate burst),
                    # OR if impact says this late record meaningfully changes a sparse bucket.
                    status_anom = bool(ops_info.get("status_anomaly", False))
                    impact_score = float(impact_info.get("impact_score", 0.0))
                    bucket_before = int(impact_info.get("bucket_count_before", 0))

                    high_impact_ok = (bucket_before >= min_bucket_count) and (impact_score >= impact_threshold)

                    if status_anom:
                        route = "quarantine"
                        reason = "late_ops_health_anomaly"
                    elif high_impact_ok:
                        route = "quarantine"
                        reason = "late_high_impact"
                    else:
                        route = "delayed"
                        reason = "late_low_impact_and_ops_ok"

                # Record enrichment
                record = dict(payload)
                record["_kafka"] = {
                    "topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "timestamp_ms": m.timestamp,
                }
                record["_decision"] = {
                    "route": route,
                    "reason": reason,
                    "late_threshold_sec": late_threshold_sec,
                    "lateness_sec": lateness_sec,
                    "ingest_time_utc": ingest_dt_utc.isoformat(),
                    "event_time_utc": event_dt_utc.isoformat(),
                    "ops": ops_info,
                    "impact": impact_info,
                }

                # ----------------------------
                # LOAD: write output
                # ----------------------------
                if route == "landing":
                    out_path = w_landing.write_jsonl(event_dt_utc, record)
                elif route == "delayed":
                    out_path = w_delayed.write_jsonl(event_dt_utc, record)
                else:
                    out_path = w_quarantine.write_jsonl(event_dt_utc, record)

                # ----------------------------
                # Update state (atomic)
                # ----------------------------
                ingest_min = iso_minute(ingest_dt_utc)
                event_min = iso_minute(event_dt_utc)

                # ops rolling counters (by ingest minute)
                minute_ingest_total[ingest_min] = int(minute_ingest_total.get(ingest_min, 0)) + 1
                if is_late:
                    minute_ingest_late[ingest_min] = int(minute_ingest_late.get(ingest_min, 0)) + 1

                # impact counters (by event minute)
                minute_event_counts[event_min] = int(minute_event_counts.get(event_min, 0)) + 1

                prune_state(ingest_dt_utc)

                atomic_write_json(
                    state_path,
                    {
                        "minute_event_counts": minute_event_counts,
                        "minute_ingest_total": minute_ingest_total,
                        "minute_ingest_late": minute_ingest_late,
                    },
                )

                # ----------------------------
                # Checkpoint AFTER durable write
                # ----------------------------
                last_offsets[p] = m.offset
                ckpt[topic] = last_offsets
                atomic_write_json(checkpoint_path, ckpt)

                # ----------------------------
                # Alerts (only quarantine)
                # ----------------------------
                if route == "quarantine":
                    alert = {
                        "ts_utc": ingest_dt_utc.isoformat(),
                        "topic": topic,
                        "source": source,
                        "event_time_utc": event_dt_utc.isoformat(),
                        "lateness_sec": lateness_sec,
                        "reason": reason,
                        "ops": ops_info,
                        "impact": impact_info,
                        "kafka": {"partition": m.partition, "offset": m.offset},
                        "out_path": out_path,
                    }
                    write_alert(alert)

                processed += 1
                if processed == 1 or processed % 50 == 0:
                    log(
                        f"Processed {processed} route={route} lateness={lateness_sec}s "
                        f"ops_anom={ops_info.get('status_anomaly')} late_rate={ops_info.get('late_rate',0):.3f} "
                        f"impact={impact_info.get('impact_score',0):.3f} wrote={out_path}"
                    )

    log("Shutting down...")
    w_landing.close_all()
    w_delayed.close_all()
    w_quarantine.close_all()
    consumer.close()
    log(f"Exited cleanly. Total processed: {processed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())