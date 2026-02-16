#!/usr/bin/env bash
set -euo pipefail

BASE="${HOME}/landing-zone"
mkdir -p "${BASE}/"{landing,checkpoints,logs}

# Optional: activate venv if you use one
# source "${BASE}/.venv/bin/activate"

export TOPIC="${TOPIC:-signals}"
export GROUP_ID="${GROUP_ID:-landing-popos-signals-v1}"

# IMPORTANT: uses your /etc/hosts + socat workaround
export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-controller-0.kafka-controller-headless.infra.svc.cluster.local:9092}"

export LANDING_ROOT="${LANDING_ROOT:-${BASE}/landing}"
export CHECKPOINT_PATH="${CHECKPOINT_PATH:-${BASE}/checkpoints/${TOPIC}/offsets.json}"
export LOG_PATH="${LOG_PATH:-${BASE}/logs/landing-consumer.log}"

mkdir -p "$(dirname "${CHECKPOINT_PATH}")"

python3 "${BASE}/bin/landing_consumer.py"
