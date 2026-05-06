#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data output models

echo "[Stage III] Running pylint..."
if command -v pylint >/dev/null 2>&1; then
  pylint scripts/model.py > output/stage3_pylint.txt || true
else
  echo "pylint is not installed or not available in PATH" > output/stage3_pylint.txt
fi

echo "[Stage III] Setting Hadoop/Spark environment..."

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf

echo "[Stage III] Checking cluster endpoints..."
echo "[Stage III] HiveServer2:"
nc -vz hadoop-03.uni.innopolis.ru 10001 || true

echo "[Stage III] Hive Metastore:"
nc -vz hadoop-02.uni.innopolis.ru 9883 || true

echo "[Stage III] YARN ResourceManager:"
nc -vz hadoop-03.uni.innopolis.ru 8032 || true
nc -vz hadoop-03.uni.innopolis.ru 8088 || true

echo "[Stage III] Running Spark ML pipeline on YARN..."

SPARK_SUBMIT="/usr/lib/spark3/bin/spark-submit"
PYTHON_EXEC="/usr/bin/python3"
ANTLR_JAR="/usr/lib/spark3/jars/antlr4-runtime-4.8.jar"

"$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode client \
  --jars "$ANTLR_JAR" \
  --driver-class-path "$ANTLR_JAR" \
  --conf "spark.pyspark.python=$PYTHON_EXEC" \
  --conf "spark.pyspark.driver.python=$PYTHON_EXEC" \
  --conf "spark.driver.extraClassPath=$ANTLR_JAR" \
  --conf "spark.executor.extraClassPath=$ANTLR_JAR" \
  --conf "spark.driver.userClassPathFirst=true" \
  --conf "spark.executor.userClassPathFirst=true" \
  --conf "spark.io.compression.codec=snappy" \
  --conf "spark.sql.avro.compression.codec=snappy" \
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

echo "[Stage III] Exporting sample_prediction.csv from HDFS if exists..."
rm -f output/sample_prediction.csv

if hdfs dfs -test -e project/output/sample_prediction; then
  hdfs dfs -cat project/output/sample_prediction/*.csv > output/sample_prediction.csv
fi

echo "[Stage III] Copying trained models from HDFS to local models/..."
rm -rf models/model1 models/model2

hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2

echo "[Stage III] Evaluation results:"
cat output/evaluation.csv

echo "[Stage III] Completed successfully."