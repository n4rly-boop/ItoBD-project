#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

if ! command -v kaggle >/dev/null 2>&1; then
  echo "ERROR: kaggle CLI is not available." >&2
  echo "Activate venv and install kaggle first." >&2
  exit 1
fi

mkdir -p data
TMP_DIR="data/_tmp_kaggle_download"

rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

kaggle datasets download -d hrishitpatil/flight-data-2024 -p "$TMP_DIR" --force
unzip -o "$TMP_DIR/flight-data-2024.zip" -d "$TMP_DIR"

cp -f "$TMP_DIR/flight_data_2024.csv" data/
cp -f "$TMP_DIR/flight_data_2024_data_dictionary.csv" data/
cp -f "$TMP_DIR/flight_data_2024_sample.csv" data/

rm -rf "$TMP_DIR"

ls -lh \
  data/flight_data_2024.csv \
  data/flight_data_2024_data_dictionary.csv \
  data/flight_data_2024_sample.csv
