#!/usr/bin/env python3
import os
import sys
import json
import time
import signal
import errno
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from kafka import KafkaConsumer, TopicPartition


def parse_event_dt(payload: Dict[str, Any], kafka_ts_ms: Optional[int]) -> datetime:
    """
    Prefer payload["ts"] if present (ISO8601). Fall back to Kafka message timestamp, then now().
    Returns timezone-aware datetime in local UTC.
    """
    ts = payload.get("ts")
    if isinstance(ts, str):
        try:
            # Handles "2026-02-15T16:05:33-07:00" and "2026-02-15T23:05:33+00:00"
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    if kafka_ts_ms is not None:
        return datetime.fromtimestamp(kafka_ts_ms / 1000.0, tz=timezone.utc)

    return datetime.now(tz=timezone.utc)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, obj: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # atomic on same filesystem


def load_checkpoint(path: str) -> Dict[str, Dict[str, int]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # normalize to ints
    out: Dict[str, Dict[str, int]] = {}
    for topic, parts in (data or {}).items():
        out[topic] = {str(p): int(o) for p, o in (parts or {}).items()}
    return out


def open_append(path: str):
    # line-buffered append; we still fsync after writes for durability
    ensure_dir(os.path.dirname(path))
    return open(path, "a", encoding="utf-8")


class LandingWriter:
    def __init__(self, landing_root: str, topic: str):
        self.landing_root = landing_root
        self.topic = topic
        self._open_files: Dict[str, Any] = {}  # path -> file handle

    def _path_for_dt(self, dt_utc: datetime) -> str:
        # Partition by UTC date/hour (change to local if you want)
        d = dt_utc.strftime("%Y-%m-%d")
        h = dt_utc.strftime("%H")
        ymdh = dt_utc.strftime("%Y%m%d-%H")
        return os.path.join(
            self.landing_root,
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


def main():
    topic = os.environ.get("TOPIC", "signals")
    group_id = os.environ.get("GROUP_ID", "landing-popos-signals-v1")

    bootstrap = os.environ.get(
        "BOOTSTRAP_SERVERS",
        "kafka-controller-0.kafka-controller-headless.infra.svc.cluster.local:9092",
    )

    landing_root = os.environ.get("LANDING_ROOT", os.path.expanduser("~/landing-zone/landing"))
    checkpoint_path = os.environ.get(
        "CHECKPOINT_PATH",
        os.path.expanduser(f"~/landing-zone/checkpoints/{topic}/offsets.json"),
    )

    log_path = os.environ.get("LOG_PATH", os.path.expanduser("~/landing-zone/logs/landing-consumer.log"))
    ensure_dir(os.path.dirname(log_path))
    ensure_dir(os.path.dirname(checkpoint_path))

    ckpt = load_checkpoint(checkpoint_path)
    last_offsets = ckpt.get(topic, {})  # partition(str) -> last_offset(int)

    writer = LandingWriter(landing_root=landing_root, topic=topic)

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    def log(msg: str):
        ts = datetime.now(tz=timezone.utc).isoformat()
        line = f"{ts} {msg}"
        print(line, flush=True)
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(line + "\n")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,      # we control checkpointing
        auto_offset_reset="earliest",  # used only if no checkpoint exists
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
        consumer_timeout_ms=1000,
        api_version_auto_timeout_ms=10000,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
        max_poll_records=500,
    )

    log(f"Started landing consumer: topic={topic} group_id={group_id} bootstrap={bootstrap}")
    log(f"Landing root: {landing_root}")
    log(f"Checkpoint: {checkpoint_path}")

    # Wait for partition assignment
    while running and not consumer.assignment():
        consumer.poll(timeout_ms=500)

    assigned = list(consumer.assignment())
    if not assigned:
        log("No partitions assigned (yet). Exiting.")
        return 2

    # Seek to checkpoint+1 for each assigned partition if present
    for tp in assigned:
        p = str(tp.partition)
        if p in last_offsets:
            seek_to = last_offsets[p] + 1
            consumer.seek(tp, seek_to)
            log(f"Seek {topic}[{p}] -> {seek_to} (from checkpoint)")
        else:
            log(f"{topic}[{p}] no checkpoint, using auto_offset_reset policy")

    # Consume loop
    processed = 0
    while running:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            continue

        for tp, msgs in records.items():
            p = str(tp.partition)
            last = int(last_offsets.get(p, -1))

            for m in msgs:
                if m.offset <= last:
                    continue  # idempotent on restart

                raw = m.value
                payload: Dict[str, Any]
                try:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        payload = {"value": payload}
                except Exception:
                    payload = {"raw": raw}

                dt_utc = parse_event_dt(payload, m.timestamp)
                record = dict(payload)
                record["_kafka"] = {
                    "topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "timestamp_ms": m.timestamp,
                }

                out_path = writer.write_jsonl(dt_utc, record)

                # checkpoint AFTER durable write
                last_offsets[p] = m.offset
                ckpt[topic] = last_offsets
                atomic_write_json(checkpoint_path, ckpt)

                processed += 1
                if processed % 500 == 0:
                    log(f"Processed {processed} records (latest wrote: {out_path})")

    log("Shutting down...")
    writer.close_all()
    consumer.close()
    log(f"Exited cleanly. Total processed: {processed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
