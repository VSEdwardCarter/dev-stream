# 🚀 Dev Stream  
# A Local-First Streaming Ingestion Platform

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black)
![Architecture](https://img.shields.io/badge/Architecture-Event%20Driven-blueviolet)
![Storage](https://img.shields.io/badge/Storage-JSONL-orange)
![Deployment](https://img.shields.io/badge/Deployment-On--Prem-lightgrey)
![Orchestration](https://img.shields.io/badge/Orchestration-systemd-green)

---

## 📌 Overview

Dev Stream is a **local-first streaming ingestion platform** designed to bridge real-time event streams into a durable, replayable data lake.

The system consumes events from Kafka and materializes them into a **partitioned landing zone**, enabling downstream processing, analytics, and data platform workflows.

This project emphasizes:
- Data durability
- Replayability
- Decoupling of ingestion from processing

---

## 🎯 Problem Statement

In many environments, especially hybrid or regulated systems:

- Raw event streams cannot be directly exposed downstream
- Processing frameworks (Spark, Flink, etc.) should not own ingestion durability
- Pipelines require **replayable, audit-ready data sources**

Dev Stream addresses this by acting as a **persistent ingestion layer**, converting transient streams into durable datasets.

---

## 🧠 Key Concepts

### 1. Landing Zone as a First-Class Layer
Instead of processing directly from Kafka, data is written to a structured landing zone:

landing/<topic>/dt=YYYY-MM-DD/hr=HH/*.jsonl

This enables:
- Replayability
- Partitioned processing
- Time-based querying

---

### 2. Immutable, Append-Only Storage

- JSONL format (one record per line)
- Append-only writes
- No mutation of historical data

This ensures:
- Auditability
- Simplified downstream processing
- Compatibility with batch and streaming frameworks

---

### 3. Offset-Based Ingestion

The system tracks Kafka offsets to ensure:

- Controlled consumption
- Restart-safe processing
- Consistent ingestion behavior

Offsets are persisted locally to support recovery.

---

### 4. Local Orchestration (systemd)

Services are managed using systemd (user-level), enabling:

- Automatic restarts
- Dependency management
- Long-running ingestion processes

---

## 🏗️ Architecture

        ┌──────────────────────┐
        │     Producers        │
        │   (Event Sources)    │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │        Kafka         │
        │   Topic: signals     │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   Ingestion Service  │
        │ (Kafka Consumer)     │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │    Landing Zone      │
        │   (Partitioned JSONL)│
        └──────────────────────┘

---

## 🔄 Data Flow

1. Producers emit events to Kafka (`signals` topic)  
2. Consumer reads messages from Kafka  
3. Events are written to JSONL files  
4. Files are partitioned by date and hour  
5. Offsets are checkpointed for recovery  

---

## 📂 Storage Model

### Landing Zone Structure

landing/
└── signals/
    ├── dt=2026-03-01/
    │   ├── hr=10/
    │   │   └── signals-20260301-10.jsonl
    │   └── hr=11/
    └── dt=2026-03-02/

---

## ⚙️ System Components

### Kafka Consumer

Responsibilities:
- Subscribe to topic(s)
- Deserialize messages
- Handle timestamps
- Write to disk
- Commit offsets

---

### Checkpointing

Location:
- ~/landing-zone/checkpoints/<topic>/offsets.json

Purpose:
- Persist consumer progress
- Enable restart-safe ingestion

---

### Logging

Location:
- ~/landing-zone/logs/

Tracks:
- Consumer activity
- Errors and failures

---

### systemd Services

Example services:
- kafka-socat.service (network bridging)
- landing-signals-consumer.service

Responsibilities:
- Ensure continuous operation
- Manage dependencies
- Restart on failure

---

## ⚙️ Execution

### Start Ingestion

Managed via systemd:

systemctl --user start landing-signals-consumer.service

---

### Check Status

systemctl --user status landing-signals-consumer.service

---

### Logs

journalctl --user -u landing-signals-consumer.service -f

---

## ⚠️ Design Decisions

### Why Not Process Directly from Kafka?

- Decouples ingestion from processing
- Enables replayable pipelines
- Supports multiple downstream consumers

---

### Why JSONL?

- Stream-friendly format
- Easy to append
- Compatible with Spark, Python, and CLI tools

---

### Why Local Storage?

- Supports on-prem / air-gapped environments
- Reduces dependency on cloud infrastructure
- Aligns with hybrid data platform patterns

---

## ⚖️ Guarantees

- At-least-once delivery (offset-based ingestion)
- Partition-level ordering (Kafka guarantees)
- Append-only durability

---

## 🚧 Future Enhancements

- Schema validation layer
- Compression (e.g., gzip or parquet conversion)
- Multi-topic ingestion
- Centralized offset store
- Cloud landing zone integration (S3-compatible)

---

## 🧑‍💻 Relationship to Other Systems

This project is designed to integrate with downstream platforms such as:

- Hybrid Metrics Platform (metrics aggregation and API layer)
- PySpark pipelines (medallion architecture)
- Data observability systems

---

## 📎 Summary

Dev Stream provides:

- A durable ingestion layer for event streams
- A replayable, partitioned data source
- A foundation for building scalable data platforms

It represents the **ingestion backbone** of a larger hybrid data architecture.
