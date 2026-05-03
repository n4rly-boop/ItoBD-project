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

if [ ! -f "secrets/.hive.pass" ]; then
  echo "ERROR: secrets/.hive.pass not found" >&2
  exit 1
fi

if [ ! -f "output/flights_2024_raw.avsc" ]; then
  echo "ERROR: output/flights_2024_raw.avsc not found. Run stage1.sh first." >&2
  exit 1
fi

HIVE_PASSWORD="$(tr -d '\n' < secrets/.hive.pass)"

echo "[1/6] Copying AVSC files to HDFS..."
hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put -f output/*.avsc project/warehouse/avsc

echo "[2/6] Cleaning Hive warehouse path..."
hdfs dfs -rm -r -f project/hive/warehouse || true

echo "[3/6] Building Hive raw/external table..."
beeline \
  -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
  -n team22 \
  -p "$HIVE_PASSWORD" \
  -f sql/db.hql \
  > output/stage2_db.txt 2> /dev/null

echo "[4/6] Building optimized partitioned/bucketed table..."
beeline \
  -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
  -n team22 \
  -p "$HIVE_PASSWORD" \
  -f sql/optimize_flights.hql \
  > output/stage2_optimize.txt 2> /dev/null

echo "[5/6] Removing raw table and finalizing..."
beeline \
  -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
  -n team22 \
  -p "$HIVE_PASSWORD" \
  -f sql/finalize_stage2.hql \
  > output/stage2_finalize.txt 2> /dev/null

echo "[6/6] Building feature-engineered table flights_2024_features..."
beeline \
  -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
  -n team22 \
  -p "$HIVE_PASSWORD" \
  -f sql/build_features.hql \
  > output/stage2_features.txt 2> /dev/null

echo "Stage II infrastructure completed (features table ready)."

echo "Running EDA queries (q1..q10) and exporting CSV..."

for i in $(seq 1 10); do
  echo "Processing q$i..."
  beeline \
    -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
    -n team22 \
    -p "$HIVE_PASSWORD" \
    -f sql/q$i.hql > /dev/null 2>&1
  echo "Exporting q$i.csv..."
  beeline \
    -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
    -n team22 \
    -p "$HIVE_PASSWORD" \
    --outputformat=csv2 \
    -e "USE team22_projectdb; SELECT * FROM q${i}_results;" \
    > output/q$i.csv
done

echo "Stage II EDA completed successfully."
