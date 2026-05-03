#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
else
  echo "ERROR: venv not found at $ROOT_DIR/venv" >&2
  exit 1
fi

if [ ! -f "secrets/.psql.pass" ]; then
  echo "ERROR: secrets/.psql.pass not found" >&2
  exit 1
fi

PASSWORD="$(tr -d '\n' < secrets/.psql.pass)"

echo "[1/5] Downloading dataset..."
bash scripts/data_collection.sh

echo "[2/5] Building PostgreSQL database..."
python scripts/build_projectdb.py

echo "[3/5] Cleaning HDFS warehouse..."
hdfs dfs -rm -r -f project/warehouse || true
hdfs dfs -mkdir -p project/warehouse

echo "[4/5] Importing PostgreSQL tables to HDFS via Sqoop (AVRO + Snappy)..."
rm -f output/*.avsc output/*.java
rm -f ./*.avsc ./*.java

sqoop import-all-tables \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team22_projectdb \
  --username team22 \
  --password "$PASSWORD" \
  --compression-codec=snappy \
  --compress \
  --as-avrodatafile \
  --warehouse-dir=project/warehouse \
  --outdir output \
  -m 1

find . -maxdepth 1 \( -name "*.avsc" -o -name "*.java" \) -exec mv -f {} output/ \; || true

echo "[5/5] Copying AVSC schemas to HDFS for Stage II..."
hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put -f output/*.avsc project/warehouse/avsc

echo "Stage I completed successfully."
echo "----- HDFS warehouse -----"
hdfs dfs -ls project/warehouse
echo "----- HDFS avsc -----"
hdfs dfs -ls project/warehouse/avsc
echo "----- output artifacts -----"
ls -lh output/*.avsc output/*.java
