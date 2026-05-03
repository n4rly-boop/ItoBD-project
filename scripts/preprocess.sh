#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data output models notebooks

if [ ! -d "venv" ]; then
  echo "[preprocess] Creating venv..."
  python3 -m venv venv
fi

# shellcheck disable=SC1091
source venv/bin/activate

if [ -f "requirements.txt" ]; then
  pip install --quiet --upgrade pip
  pip install --quiet -r requirements.txt
fi

if [ ! -f "secrets/.psql.pass" ] || [ ! -f "secrets/.hive.pass" ]; then
  echo "ERROR: secrets/.psql.pass and secrets/.hive.pass are required." >&2
  exit 1
fi

for tool in hdfs sqoop beeline; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "WARN: $tool not on PATH; pipeline expects cluster environment." >&2
  fi
done

echo "[preprocess] Environment ready."
