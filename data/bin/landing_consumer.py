#!/usr/bin/env python3
import os
import sys
import json
import time
import signal
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

from kafka import KafkaConsumer, TopicPartition


# ----------------------------
# CONFIG DEFAULTS
# ----------------------------
LATE_THRESHOLD_SEC_DEFAULT = 300           # 5 minutes
IMPACT_THRESHOLD_DEFAULT = 0.33            # quarantine if >= this (you set to 0.95 via systemd env)
OPS_IQR_K_DEFAULT = 3.0                    # ops anomaly if lateness > median + k*IQR
OPS_MIN_SAMPLES_DEFAULT = 20               # need at least N samples per source
OPS_WINDOW_DEFAULT = 200                   # keep last N lateness samples per source
IMPACT_WINDOW_MINUTES_DEFAULT = 60         # keep last N minutes of per-minute counts
OPS_ZERO_IQR_FLOOR_SEC_DEFAULT = 60        # when IQR=0, require at least this many seconds to call it anomalous


# ============================================================
# TRANSFORM HELPERS: time parsing
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


# ============================================================
# DURABILITY HELPERS: directories + atomic JSON
# ============================================================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, obj: Any) -> None:
    """
    Atomic write on same filesystem:
      write tmp -> fsync -> rename/replace
    """
    tmp = f"{path}.tmp"
    ensure_dir(os.path.dirname(path))
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
# LOAD: append-only, partitioned JSONL writer
# ============================================================
class PartitionedWriter:
    """
    Append-only JSONL with dt/hr partitioning.
    Keeps open handles per path; flush+fsync each write for durability.
    """
    def __init__(self, root: str, topic: str):
        self.root = root
        self.topic = topic
        self._open_files: Dict[str, Any] = {}

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
        f = self._open_files.get(path)
        if f is None:
            f = open_append(path)
            self._open_files[path] = f

        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        f.write(line)
        f.flush()
        os.fsync(f.fileno())
        return path

    def close_all(self):
        for f in self._open_files.values():
            try:
                f.close()
            except Exception:
                pass
        self._open_files.clear()


# ============================================================
# TRANSFORM HELPERS: robust stats (median/IQR)
# ============================================================
def _median(sorted_vals: List[float]) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return float(sorted_vals[mid])
    return (float(sorted_vals[mid - 1]) + float(sorted_vals[mid])) / 2.0


def _quantile(sorted_vals: List[float], q: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return float(sorted_vals[lo]) * (1.0 - frac) + float(sorted_vals[hi]) * frac


def median_iqr(values: List[float]) -> Tuple[float, float, float, float]:
    """
    Returns (median, q1, q3, iqr)
    """
    s = sorted(values)
    med = _median(s)
    q1 = _quantile(s, 0.25)
    q3 = _quantile(s, 0.75)
    iqr = max(0.0, q3 - q1)
    return med, q1, q3, iqr


# ============================================================
# CHECKPOINT: offsets.json
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


def floor_to_minute(dt_utc: datetime) -> datetime:
    return dt_utc.replace(second=0, microsecond=0)


def iso_minute(dt_utc: datetime) -> str:
    return floor_to_minute(dt_utc).isoformat().replace("+00:00", "Z")


# ============================================================
# MAIN
# ============================================================
def main() -> int:
    # ----------------------------
    # CONFIG (env-driven)
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
    log_path = os.environ.get("LOG_PATH", os.path.expanduser("~/landing-zone/logs/landing-consumer.log"))

    alerts_path = os.environ.get("ALERTS_PATH", os.path.expanduser("~/landing-zone/alerts/late_alerts.jsonl"))
    state_path = os.environ.get("STATE_PATH", os.path.expanduser("~/landing-zone/state/late_state.json"))

    late_threshold_sec = int(os.environ.get("LATE_THRESHOLD_SEC", str(LATE_THRESHOLD_SEC_DEFAULT)))
    impact_threshold = float(os.environ.get("IMPACT_THRESHOLD", str(IMPACT_THRESHOLD_DEFAULT)))

    ops_iqr_k = float(os.environ.get("OPS_IQR_K", str(OPS_IQR_K_DEFAULT)))
    ops_min_samples = int(os.environ.get("OPS_MIN_SAMPLES", str(OPS_MIN_SAMPLES_DEFAULT)))
    ops_window = int(os.environ.get("OPS_WINDOW", str(OPS_WINDOW_DEFAULT)))
    ops_zero_iqr_floor_sec = int(os.environ.get("OPS_ZERO_IQR_FLOOR_SEC", str(OPS_ZERO_IQR_FLOOR_SEC_DEFAULT)))

    impact_window_minutes = int(os.environ.get("IMPACT_WINDOW_MINUTES", str(IMPACT_WINDOW_MINUTES_DEFAULT)))

    # Ensure dirs exist
    ensure_dir(os.path.dirname(log_path))
    ensure_dir(os.path.dirname(checkpoint_path))
    ensure_dir(os.path.dirname(alerts_path))
    ensure_dir(os.path.dirname(state_path))
    ensure_dir(landing_root)
    ensure_dir(delayed_root)
    ensure_dir(quarantine_root)

    # ----------------------------
    # Logging helper
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
    # STARTUP CONFIG LOG (single place to see everything)
    # ----------------------------
    log("========== Landing Consumer Configuration ==========")
    log(f"topic={topic}")
    log(f"bootstrap={bootstrap}")

    log("---- Late Policy ----")
    log(f"late_threshold_sec={late_threshold_sec}")

    log("---- Impact Policy ----")
    log(f"impact_threshold={impact_threshold}")
    log(f"impact_window_minutes={impact_window_minutes}")

    log("---- Ops (Pipeline Health) Policy ----")
    log(f"ops_iqr_k={ops_iqr_k}")
    log(f"ops_min_samples={ops_min_samples}")
    log(f"ops_window={ops_window}")
    log(f"ops_zero_iqr_floor_sec={ops_zero_iqr_floor_sec}")

    log("---- Paths ----")
    log(f"landing_root={landing_root}")
    log(f"delayed_root={delayed_root}")
    log(f"quarantine_root={quarantine_root}")
    log(f"checkpoint_path={checkpoint_path}")
    log(f"state_path={state_path}")
    log(f"alerts_path={alerts_path}")
    log("=====================================================")

    # ----------------------------
    # DURABILITY/CHECKPOINT: load offsets + state
    # ----------------------------
    ckpt = load_checkpoint(checkpoint_path)
    last_offsets = ckpt.get(topic, {})  # partition(str) -> last_offset(int)

    # state tracks:
    #   - lateness distribution per source (ops lane)
    #   - per-minute counts (impact lane)
    state = load_json(state_path, default={})
    source_lateness: Dict[str, List[int]] = state.get("source_lateness", {}) or {}
    minute_counts: Dict[str, int] = state.get("minute_counts", {}) or {}

    # ----------------------------
    # LOAD writers (3 routes)
    # ----------------------------
    writer_landing = PartitionedWriter(root=landing_root, topic=topic)
    writer_delayed = PartitionedWriter(root=delayed_root, topic=topic)
    writer_quarantine = PartitionedWriter(root=quarantine_root, topic=topic)

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
    # State helpers
    # ----------------------------
    def prune_minute_counts(now_utc: datetime) -> None:
        cutoff = floor_to_minute(now_utc).timestamp() - (impact_window_minutes * 60)
        to_del = []
        for k in list(minute_counts.keys()):
            try:
                dt = datetime.fromisoformat(k.replace("Z", "+00:00"))
                if dt.timestamp() < cutoff:
                    to_del.append(k)
            except Exception:
                to_del.append(k)
        for k in to_del:
            minute_counts.pop(k, None)

    # ============================================================
    # OPS LANE: lateness shift anomaly per source (median + IQR)
    # ============================================================
    def ops_anomaly_for(source: str, lateness_sec: int) -> Dict[str, Any]:
        vals = source_lateness.get(source, [])
        if len(vals) < ops_min_samples:
            return {
                "status_anomaly": False,
                "reason": "insufficient_history",
                "samples": len(vals),
                "median": None,
                "iqr": None,
                "upper_bound": None,
            }

        med, q1, q3, iqr = median_iqr([float(x) for x in vals])

        # Important fix: if baseline has IQR=0 (often all zeros),
        # require a minimum floor so any tiny non-zero delay doesn't look "anomalous".
        if iqr > 0:
            upper = med + (ops_iqr_k * iqr)
        else:
            upper = med + float(ops_zero_iqr_floor_sec)

        status_anom = float(lateness_sec) > upper

        return {
            "status_anomaly": bool(status_anom),
            "reason": "lateness_shift",
            "samples": len(vals),
            "median": med,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "upper_bound": upper,
        }

    # ============================================================
    # IMPACT LANE: global per-minute counts (simple "current situation" proxy)
    # ============================================================
    def impact_score_for(event_dt_utc: datetime) -> Dict[str, Any]:
        minute_key = iso_minute(event_dt_utc)
        count_before = int(minute_counts.get(minute_key, 0))
        score = 1.0 / (1.0 + float(count_before))
        return {
            "bucket_minute_utc": minute_key,
            "bucket_count_before": count_before,
            "impact_score": score,
        }

    # ============================================================
    # EXTRACT: Kafka consumer (manual partition assignment; no group)
    # ============================================================
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

    # Discover partitions + assign
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
        part_str = str(tp.partition)
        if part_str in last_offsets:
            seek_to = last_offsets[part_str] + 1
            consumer.seek(tp, seek_to)
            log(f"Seek {topic}[{part_str}] -> {seek_to} (from checkpoint)")
        else:
            consumer.seek_to_beginning(tp)
            log(f"Seek {topic}[{part_str}] -> beginning (no checkpoint)")

    log("Entering consume loop...")

    # ============================================================
    # MAIN LOOP: EXTRACT -> TRANSFORM -> AI DECISION -> LOAD -> CHECKPOINT
    # ============================================================
    processed = 0
    while running:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            continue

        for tp, msgs in records.items():
            part_str = str(tp.partition)
            last = int(last_offsets.get(part_str, -1))

            for m in msgs:
                # Idempotency on restart
                if m.offset <= last:
                    continue

                # Parse payload JSON
                raw = m.value
                try:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        payload = {"value": payload}
                except Exception:
                    payload = {"raw": raw}

                # Compute event time + lateness
                event_dt_utc = parse_event_dt(payload, m.timestamp)
                ingest_dt_utc = datetime.now(tz=timezone.utc)
                lateness_sec = int((ingest_dt_utc - event_dt_utc).total_seconds())
                if lateness_sec < 0:
                    lateness_sec = 0

                source = str(payload.get("source", "unknown"))
                is_late = lateness_sec > late_threshold_sec

                # Compute lane signals (only meaningful for late)
                ops_info = ops_anomaly_for(source, lateness_sec) if is_late else {
                    "status_anomaly": False,
                    "reason": "not_late",
                    "samples": len(source_lateness.get(source, [])),
                    "median": None,
                    "iqr": None,
                    "upper_bound": None,
                }
                impact_info = impact_score_for(event_dt_utc) if is_late else {
                    "bucket_minute_utc": iso_minute(event_dt_utc),
                    "bucket_count_before": int(minute_counts.get(iso_minute(event_dt_utc), 0)),
                    "impact_score": 0.0,
                }

                # ----------------------------
                # AI DECISION: routing
                # ----------------------------
                if not is_late:
                    route = "landing"
                    reason = "on_time_or_within_threshold"
                else:
                    status_anom = bool(ops_info.get("status_anomaly", False))
                    impact_score = float(impact_info.get("impact_score", 0.0))
                    if status_anom or impact_score >= impact_threshold:
                        route = "quarantine"
                        reason = "late_ops_anomaly" if status_anom else "late_high_impact"
                    else:
                        route = "delayed"
                        reason = "late_low_impact_and_stable_pipeline"

                # Enrich record
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
                # LOAD: write routed output
                # ----------------------------
                if route == "landing":
                    out_path = writer_landing.write_jsonl(event_dt_utc, record)
                elif route == "delayed":
                    out_path = writer_delayed.write_jsonl(event_dt_utc, record)
                else:
                    out_path = writer_quarantine.write_jsonl(event_dt_utc, record)

                # ----------------------------
                # UPDATE STATE (atomic)
                # ----------------------------
                mk = iso_minute(event_dt_utc)
                minute_counts[mk] = int(minute_counts.get(mk, 0)) + 1
                prune_minute_counts(ingest_dt_utc)

                vals = source_lateness.get(source, [])
                vals.append(lateness_sec)
                if len(vals) > ops_window:
                    vals = vals[-ops_window:]
                source_lateness[source] = vals

                atomic_write_json(state_path, {"source_lateness": source_lateness, "minute_counts": minute_counts})

                # ----------------------------
                # CHECKPOINT AFTER durable write
                # ----------------------------
                last_offsets[part_str] = m.offset
                ckpt[topic] = last_offsets
                atomic_write_json(checkpoint_path, ckpt)

                # Alerts on quarantine
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
                        f"ops_anom={ops_info.get('status_anomaly')} impact={impact_info.get('impact_score'):.3f} wrote={out_path}"
                    )

    log("Shutting down...")
    writer_landing.close_all()
    writer_delayed.close_all()
    writer_quarantine.close_all()
    consumer.close()
    log(f"Exited cleanly. Total processed: {processed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())