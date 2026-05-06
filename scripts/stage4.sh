#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Виртуальное окружение (если есть) – для единообразия
if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

if [ ! -f "secrets/.hive.pass" ]; then
  echo "ERROR: secrets/.hive.pass not found" >&2
  exit 1
fi

HIVE_PASSWORD="$(tr -d '\n' < secrets/.hive.pass)"
BEELINE="beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team22 -p $HIVE_PASSWORD"

echo "[Stage IV] Checking HDFS output directories from Stage III..."

# Проверяем наличие основных результатов
if ! hdfs dfs -test -d project/output/evaluation; then
  echo "ERROR: HDFS directory project/output/evaluation not found. Run stage3.sh first." >&2
  exit 1
fi

if ! hdfs dfs -test -d project/output/model1_predictions; then
  echo "ERROR: HDFS directory project/output/model1_predictions not found. Run stage3.sh first." >&2
  exit 1
fi

if ! hdfs dfs -test -d project/output/model2_predictions; then
  echo "ERROR: HDFS directory project/output/model2_predictions not found. Run stage3.sh first." >&2
  exit 1
fi

SAMPLE_PRED_EXISTS=false
if hdfs dfs -test -d project/output/sample_prediction; then
  SAMPLE_PRED_EXISTS=true
fi

echo "[Stage IV] Creating Hive tables for ML results..."

# Таблица с метриками моделей
$BEELINE -e "
USE team22_projectdb;
DROP TABLE IF EXISTS ml_evaluation;
CREATE EXTERNAL TABLE ml_evaluation (
    model STRING,
    numFolds INT,
    numGrids INT,
    numParams INT,
    trainingTime DOUBLE,
    accuracy DOUBLE,
    f1 DOUBLE,
    areaUnderROC DOUBLE,
    areaUnderPR DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/evaluation'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# Предсказания первой модели
$BEELINE -e "
USE team22_projectdb;
DROP TABLE IF EXISTS ml_model1_preds;
CREATE EXTERNAL TABLE ml_model1_preds (
    label INT,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# Предсказания второй модели
$BEELINE -e "
USE team22_projectdb;
DROP TABLE IF EXISTS ml_model2_preds;
CREATE EXTERNAL TABLE ml_model2_preds (
    label INT,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# Если есть sample_prediction – создаём и для него
if [ "$SAMPLE_PRED_EXISTS" = true ]; then
  $BEELINE -e "
  USE team22_projectdb;
  DROP TABLE IF EXISTS ml_sample_preds;
  CREATE EXTERNAL TABLE ml_sample_preds (
      label INT,
      prediction DOUBLE
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'project/output/sample_prediction'
  TBLPROPERTIES ('skip.header.line.count'='1');
  "
  echo "[Stage IV] Additional table ml_sample_preds created."
fi

echo "[Stage IV] Hive tables for ML results created successfully."
echo "Now use Apache Superset to create datasets and build charts for the ML section."