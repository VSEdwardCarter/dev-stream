[//]: # (# LAN Streaming Ingestion Platform)

[//]: # (**Pop!_OS + k3s + Kafka + Append-Only Landing Zone**)

[//]: # ()
[//]: # (- Last Updated: 2026-02-15)

[//]: # (- Owner: Edward J. Carter)

[//]: # ()
[//]: # (## Strategic View)

[//]: # ()
[//]: # (This project establishes a LAN-based streaming data ingestion platform designed to simulate production-grade event-driven architecture within a home lab environment.)

[//]: # ()
[//]: # (**The goal is to:**)

[//]: # ()
[//]: # (- Build a real message bus &#40;Kafka on k3s VM&#41;)

[//]: # (- Consume events from a distributed system)

[//]: # (- Persist raw, append-only landing data)

[//]: # (- Ensure restart safety and idempotency)

[//]: # (- Operate services in a boot-safe, systemd-managed environment)

[//]: # (- Lay the foundation for streaming transformations and analytics)

[//]: # (- This is not a demo script — it is a functional ingestion layer.)

[//]: # ()
[//]: # (## Current Architecture)

[//]: # (### Infrastructure Node &#40;k3s VM — 192.168.0.146&#41;)

[//]: # (- Kafka &#40;NodePort: 30992&#41;)

[//]: # (- Postgres &#40;NodePort: 30432&#41;)

[//]: # (- Topic: signals &#40;1 partition&#41;)

[//]: # ()
[//]: # (### Pop!_OS &#40;Landing Host&#41;)

[//]: # (- socat bridge ``localhost:9092 → 192.168.0.146:30992``)

[//]: # (- Kafka consumer service &#40;systemd user service&#41;)

[//]: # (- Append-only JSONL landing zone)

[//]: # (- Local offset checkpoint persistence)

[//]: # (- Boot-safe services &#40;loginctl enable-linger&#41;)

[//]: # ()
[//]: # (```css)

[//]: # (landing-zone/)

[//]: # (  landing/)

[//]: # (    signals/)

[//]: # (      dt=YYYY-MM-DD/)

[//]: # (        hr=HH/)

[//]: # (          signals-YYYYMMDD-HH.jsonl)

[//]: # (  checkpoints/)

[//]: # (    signals/)

[//]: # (      offsets.json)

[//]: # (  logs/)

[//]: # (  ```)

[//]: # ()
[//]: # (### Characteristics)

[//]: # (- Append-only JSONL)

[//]: # (- Partitioned by UTC date/hour)

[//]: # (- _kafka metadata embedded per record)

[//]: # (- Idempotent on restart)

[//]: # (- Atomic checkpoint writes)

[//]: # (- fsync durability)

[//]: # (- systemd managed)

[//]: # (- LAN distributed topology)

[//]: # ()
[//]: # (## Lessons Learned)

[//]: # (### Kafka Advertised Listener Matters)

[//]: # (Kafka inside Kubernetes advertises internal DNS. \)

[//]: # (When accessed externally via NodePort, clients may fail unless:)

[//]: # (- Advertised listeners are properly configured or)

[//]: # (- A local DNS + socat workaround is used.)

[//]: # ()
[//]: # (Takeaway: Distributed systems assume correct network topology.)

[//]: # ()
[//]: # (### Local Checkpointing > Blind Auto-Commit)

[//]: # ()
[//]: # (Relying only on Kafka consumer groups is insufficient when:)

[//]: # (- You want local durability guarantees)

[//]: # (- You want file-write-first semantics)

[//]: # ()
[//]: # (`Writing → fsync → checkpoint → continue`)

[//]: # ()
[//]: # (creates deterministic recovery behavior.)

[//]: # ()
[//]: # ()
[//]: # ()
[//]: # (### Append-Only Simplicity Scales)

[//]: # ()
[//]: # (Starting with:)

[//]: # ()
[//]: # (- JSONL)

[//]: # ()
[//]: # (- Partitioned directories)

[//]: # ()
[//]: # (- Immutable writes)

[//]: # ()
[//]: # (- keeps the system transparent and debuggable.)

[//]: # ()
[//]: # (- Premature complexity &#40;Spark, Flink, etc.&#41; is unnecessary at this stage.)

[//]: # ()
[//]: # ()
[//]: # (## Current System Maturity)

[//]: # ()
[//]: # (**What is solid:**)

[//]: # ()
[//]: # (- Message bus operational)

[//]: # ()
[//]: # (- Restart-safe ingestion)

[//]: # ()
[//]: # (- Partitioned landing)

[//]: # ()
[//]: # (- Boot-safe services)

[//]: # ()
[//]: # (- Local durability guarantees)

[//]: # ()
[//]: # (- Clean folder structure)

[//]: # ()
[//]: # (- Scripted activation &#40;devmode landing&#41;)

[//]: # ()
[//]: # (**What is not yet implemented:**)

[//]: # ()
[//]: # (- Metrics / observability)

[//]: # ()
[//]: # (- Dead-letter queue)

[//]: # ()
[//]: # (- Multi-partition scaling)

[//]: # ()
[//]: # (- Schema enforcement)

[//]: # ()
[//]: # (- Retention management)

[//]: # ()
[//]: # (- Backpressure testing)

[//]: # ()
[//]: # (- Streaming transforms)

[//]: # ()
[//]: # (## Path Ahead)

[//]: # (### Phase 1 – Hardening)

[//]: # ()
[//]: # (- Add metrics &#40;records/sec, lag&#41;)

[//]: # ()
[//]: # (- Add consumer lag inspection)

[//]: # ()
[//]: # (- Add dead-letter handling for invalid JSON)

[//]: # ()
[//]: # (- Move from single partition → 3 partitions)

[//]: # ()
[//]: # (- Improve Kafka advertised listeners &#40;remove socat workaround&#41;)

[//]: # ()
[//]: # (### Phase 2 – Streaming Transform Layer)

[//]: # ()
[//]: # (Options:)

[//]: # ()
[//]: # (- Bronze/Silver streaming transforms &#40;Python&#41;)

[//]: # ()
[//]: # (- Spark Structured Streaming)

[//]: # ()
[//]: # (- Lightweight validation engine)

[//]: # ()
[//]: # (- Feature engineering on ingest)

[//]: # ()
[//]: # (- Online model scoring)

[//]: # ()
[//]: # (### Phase 3 – Producer Expansion)

[//]: # ()
[//]: # (- Deploy React click-stream app on second desktop)

[//]: # ()
[//]: # (- Emit structured event schema)

[//]: # ()
[//]: # (- Simulate real user telemetry)

[//]: # ()
[//]: # (- Introduce versioned event contracts)

[//]: # ()
[//]: # (### Phase 4 – Observability)

[//]: # ()
[//]: # (- Prometheus)

[//]: # ()
[//]: # (- Grafana)

[//]: # ()
[//]: # (- Kafka exporter)

[//]: # ()
[//]: # (- Disk growth monitoring)

[//]: # ()
[//]: # (- Landing file integrity checks)

[//]: # ()
[//]: # (## Strategic Direction)

[//]: # ()
[//]: # (This project is evolving into:)

[//]: # ()
[//]: # (A self-hosted distributed data engineering platform for streaming analytics experimentation.)

[//]: # ()
[//]: # (It is positioned to support:)

[//]: # ()
[//]: # (Clickstream ingestion)

[//]: # ()
[//]: # (Model drift detection)

[//]: # ()
[//]: # (Real-time feature pipelines)

[//]: # ()
[//]: # (RAG ingestion)

[//]: # ()
[//]: # (ISR-style event streaming simulations)

[//]: # ()
[//]: # (Production-style architecture experimentation)

[//]: # ()
[//]: # (## Summary)

[//]: # ()
[//]: # (As of 2026-02-15, the core ingestion foundation is complete:)

[//]: # ()
[//]: # (Kafka on LAN)

[//]: # ()
[//]: # (Durable append-only landing)

[//]: # ()
[//]: # (Checkpoint-safe consumer)

[//]: # ()
[//]: # (Boot-safe services)

[//]: # ()
[//]: # (Distributed topology validated)

[//]: # ()
[//]: # (The system is stable and ready for the next architectural layer.)

#  AI-Aware Streaming Pipeline (LAN Lab)

- Last Updated: 2026-02-15
- Owner: Edward J. Carter

## Strategic End State

The objective of this project is to build an AI/ML-integrated streaming pipeline that evaluates incoming data in real time and determines how to handle late, anomalous, or drift-inducing events based on measurable model impact.

All current work (Kafka, landing zone, boot-safe services, LAN topology) is foundational infrastructure to support intelligent, model-driven decision logic inside the pipeline.

## Current State (Infrastructure Phase)
- Message Bus

- Kafka running on k3s VM (LAN)

- Topic: signals

- NodePort exposure verified

- External connectivity via socat bridge

## Landing Zone

- Append-only JSONL

- Partitioned by date/hour

- Idempotent consumer

- Local offset checkpointing

- fsync durability

- Boot-safe via systemd user services

## Operational Characteristics

- Restart-safe ingestion

- Deterministic recovery

- Fully self-hosted on LAN

- Dev-mode activation (devmode landing)

## Why This Exists

Before AI-driven decision logic can exist in a streaming system, the pipeline must guarantee:

- Deterministic ingestion

- Durable landing

- Replay capability

- Offset control

- Boot-safe operation

- Controlled distributed topology

The current phase establishes those guarantees.
 
# Lessons Learned

- Kafka advertised listeners must match real network topology.

- Local checkpoint control provides stronger guarantees than blind auto-commit.

- systemd user services + linger turn a workstation into a data appliance.

- Append-only raw ingestion keeps the system transparent and debuggable.

- Even on a LAN, distributed systems behave like distributed systems.

## Path Ahead (AI Integration Phase)
**Phase 1 – Streaming Evaluation Layer**

- Introduce model scoring inside consumer

- Measure model delta impact per event

- Detect late data beyond window threshold

- Track concept drift signals

**Phase 2 – Decision Logic**

- Based on model impact:

- Accept into primary dataset

- Route to delayed processing

- Trigger model retrain

- Flag for investigation

- Drop or quarantine

**Phase 3 – Feedback Loop**

- Measure performance degradation

- Adaptive window tuning

- Online evaluation metrics

- Model-aware pipeline branching

# Summary

As of 2026-02-15, the infrastructure foundation is complete.

The next milestone is integrating AI/ML into the streaming layer so the pipeline can reason about data quality, lateness, and model impact — and act autonomously.