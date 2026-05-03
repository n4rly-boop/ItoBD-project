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

mkdir -p data output models

echo "[Stage III] Running pylint..."
pylint scripts/model.py > output/stage3_pylint.txt || true

echo "[Stage III] Running Spark ML pipeline on YARN..."

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf

SPARK_SUBMIT="/usr/lib/spark3/bin/spark-submit"
PYTHON_EXEC="$ROOT_DIR/venv/bin/python"

"$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode client \
  --conf spark.io.compression.codec=snappy \
  --conf spark.sql.catalogImplementation=hive \
  --conf "spark.pyspark.python=$PYTHON_EXEC" \
  --conf "spark.pyspark.driver.python=$PYTHON_EXEC" \
  scripts/model.py \
  > output/stage3_model.log 2>&1

echo "[Stage III] Spark job completed."

echo "[Stage III] Exporting train/test JSON from HDFS..."
rm -f data/train.json data/test.json

hdfs dfs -cat project/data/train/*.json > data/train.json
hdfs dfs -cat project/data/test/*.json > data/test.json

echo "[Stage III] Exporting predictions from HDFS..."
rm -f output/model1_predictions.csv output/model2_predictions.csv

hdfs dfs -cat project/output/model1_predictions/*.csv > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions/*.csv > output/model2_predictions.csv

echo "[Stage III] Exporting evaluation.csv from HDFS..."
rm -f output/evaluation.csv

hdfs dfs -cat project/output/evaluation/*.csv > output/evaluation.csv

echo "[Stage III] Exporting sample_prediction.csv from HDFS..."
rm -f output/sample_prediction.csv

hdfs dfs -cat project/output/sample_prediction/*.csv > output/sample_prediction.csv

echo "[Stage III] Copying trained models from HDFS to local models/..."
rm -rf models/model1 models/model2

hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2

echo "[Stage III] Evaluation results:"
cat output/evaluation.csv

echo "[Stage III] Sample prediction:"
cat output/sample_prediction.csv

echo "[Stage III] Completed successfully."
