#!/usr/bin/env python3
import os
import json
import time
import random
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from kafka import KafkaProducer


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def iso_minutes_ago(minutes: int) -> str:
    return (datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)).isoformat()


def build_event(seq: int, source: str, event: str, v: int, ts_iso: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    d = {
        "ts": ts_iso,
        "source": source,
        "event": event,
        "v": v,
        "seq": seq,
    }
    d.update(extra)
    return d


def main() -> int:
    # ---- Config (env or defaults)
    topic = os.environ.get("TOPIC", "signals")
    # IMPORTANT: produce directly to NodePort (bypasses the advertised DNS mess)
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "192.168.0.146:30992")

    source = os.environ.get("SOURCE", "pop-os-producer")
    base_rate_eps = float(os.environ.get("RATE_EPS", "5"))   # events per second
    jitter = float(os.environ.get("JITTER", "0.30"))         # +/- % on sleep time

    # Late-data patterns
    late_every = int(os.environ.get("LATE_EVERY", "50"))      # every N events send 1 late
    late_min = int(os.environ.get("LATE_MIN", "6"))           # minutes ago
    late_max = int(os.environ.get("LATE_MAX", "12"))          # minutes ago

    burst_every = int(os.environ.get("BURST_EVERY", "500"))   # every N events trigger a burst
    burst_size = int(os.environ.get("BURST_SIZE", "20"))
    burst_late_min = int(os.environ.get("BURST_LATE_MIN", "10"))
    burst_late_max = int(os.environ.get("BURST_LATE_MAX", "25"))

    seed = os.environ.get("SEED")
    if seed is not None:
        random.seed(int(seed))

    events = [e.strip() for e in os.environ.get("EVENT_TYPES", "click,view,login,logout,error,heartbeat").split(",") if e.strip()]

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        linger_ms=10,
        retries=10,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
    )

    print(f"{utc_now_iso()} Producer starting topic={topic} bootstrap={bootstrap}", flush=True)
    print(f"{utc_now_iso()} rate={base_rate_eps} eps jitter={jitter}", flush=True)
    print(f"{utc_now_iso()} late_every={late_every} late_range={late_min}-{late_max} min", flush=True)
    print(f"{utc_now_iso()} burst_every={burst_every} burst_size={burst_size} burst_range={burst_late_min}-{burst_late_max} min", flush=True)

    seq = 0
    sent = 0

    def send_record(rec: Dict[str, Any]):
        nonlocal sent
        producer.send(topic, rec)
        sent += 1

    # helper: sleep with jitter
    base_sleep = 1.0 / max(base_rate_eps, 0.001)

    while running:
        seq += 1

        # ---- Pattern C: burst anomaly (many late at once)
        if burst_every > 0 and (seq % burst_every == 0):
            for i in range(burst_size):
                minutes_ago = random.randint(burst_late_min, burst_late_max)
                ev = random.choice(events)
                rec = build_event(
                    seq=seq,
                    source=source,
                    event=f"burst_{ev}",
                    v=random.randint(1, 10),
                    ts_iso=iso_minutes_ago(minutes_ago),
                    extra={"pattern": "burst_late", "minutes_ago": minutes_ago},
                )
                send_record(rec)
            producer.flush()
            print(f"{utc_now_iso()} sent burst of {burst_size} late events at seq={seq}", flush=True)
            # small pause after burst
            time.sleep(min(1.0, base_sleep * 5))
            continue

        # ---- Pattern B: occasional single late event
        if late_every > 0 and (seq % late_every == 0):
            minutes_ago = random.randint(late_min, late_max)
            ev = random.choice(events)
            rec = build_event(
                seq=seq,
                source=source,
                event=f"late_{ev}",
                v=random.randint(1, 10),
                ts_iso=iso_minutes_ago(minutes_ago),
                extra={"pattern": "single_late", "minutes_ago": minutes_ago},
            )
            send_record(rec)
            producer.flush()
            # continue to also send a normal event this tick (optional)
            # fall through

        # ---- Pattern A: normal on-time event
        ev = random.choice(events)
        rec = build_event(
            seq=seq,
            source=source,
            event=ev,
            v=random.randint(1, 10),
            ts_iso=utc_now_iso(),
            extra={"pattern": "normal"},
        )
        send_record(rec)

        # flush periodically to keep latency reasonable
        if seq % 25 == 0:
            producer.flush()

        # sleep at target rate with jitter
        # jitter applies multiplicatively: sleep * (1 +/- jitter)
        j = 1.0 + random.uniform(-jitter, jitter)
        time.sleep(max(0.0, base_sleep * j))

    producer.flush()
    producer.close()
    print(f"{utc_now_iso()} Producer stopped. sent={sent} last_seq={seq}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
