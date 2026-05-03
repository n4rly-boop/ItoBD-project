#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

echo "[postprocess] Pipeline artefact summary"
echo "----------------------------------------"

EXPECTED=(
  "output/flights_2024_raw.avsc"
  "output/pylint_stage1_stage2.txt"
  "output/q1.csv"
  "output/q10.csv"
  "output/q1.jpg"
  "output/q10.jpg"
  "output/evaluation.csv"
  "output/sample_prediction.csv"
  "output/model1_predictions.csv"
  "output/model2_predictions.csv"
  "output/stage3_pylint.txt"
)

missing=0
for f in "${EXPECTED[@]}"; do
  if [ -e "$f" ]; then
    printf "  OK    %s (%s)\n" "$f" "$(du -h "$f" | cut -f1)"
  else
    printf "  MISS  %s\n" "$f"
    missing=$((missing + 1))
  fi
done

echo "----------------------------------------"
echo "[postprocess] output/ size: $(du -sh output 2>/dev/null | cut -f1)"
echo "[postprocess] models/ size: $(du -sh models 2>/dev/null | cut -f1)"

if [ -f "output/evaluation.csv" ]; then
  echo "[postprocess] Stage 3 evaluation:"
  cat output/evaluation.csv
fi

if [ "$missing" -gt 0 ]; then
  echo "[postprocess] $missing expected artefact(s) missing." >&2
fi

echo "[postprocess] Done."
