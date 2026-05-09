#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p output

if [ ! -f "secrets/.hive.pass" ]; then
  echo "ERROR: secrets/.hive.pass not found" >&2
  exit 1
fi

HIVE_PASSWORD="$(tr -d '\n' < secrets/.hive.pass)"

BEELINE_CMD=(
  beeline
  -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001
  -n team22
  -p "$HIVE_PASSWORD"
)

echo "[Stage IV Hive] Checking required HDFS outputs..."

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

if hdfs dfs -test -d project/output/sample_prediction; then
  echo "[Stage IV Hive] sample_prediction exists."
else
  echo "[Stage IV Hive] WARNING: sample_prediction does not exist. Continuing."
fi

echo "[Stage IV Hive] Creating feature summary CSV..."

cat > output/stage3_feature_summary.csv <<'EOF'
feature_group,feature_count,description
categorical_features,3,"op_unique_carrier; origin; dest encoded with StringIndexer and OneHotEncoder"
direct_numerical_features,3,"crs_elapsed_time; distance; is_weekend"
cyclical_encoded_features,10,"scheduled departure hour; scheduled arrival hour; day of week; month; day of month encoded with sin/cos"
models,2,"Logistic Regression and Random Forest"
hyperparameter_combinations_per_model,6,"6 grid-search combinations per model"
cross_validation_folds,3,"3-fold cross-validation"
class_balancing,1,"class_weight computed from train set and passed through weightCol"
scaling,1,"StandardScaler applied to assembled feature vectors"
EOF

echo "[Stage IV Hive] Creating hyperparameter summary CSV..."

cat > output/stage3_hyperparameter_summary.csv <<'EOF'
model,hyperparameter_1,values_1,hyperparameter_2,values_2,paramCombinations,cvFolds,cvFits,optimization_metric
model1_logistic_regression,regParam,"0.001;0.01;0.1",elasticNetParam,"0.0;0.5",6,3,18,areaUnderROC
model2_random_forest,numTrees,"20;50;100",maxDepth,"5;10",6,3,18,areaUnderROC
EOF

echo "[Stage IV Hive] Uploading summary CSV files to HDFS..."

hdfs dfs -rm -r -f project/output/stage3_feature_summary || true
hdfs dfs -rm -r -f project/output/stage3_hyperparameter_summary || true

hdfs dfs -mkdir -p project/output/stage3_feature_summary
hdfs dfs -mkdir -p project/output/stage3_hyperparameter_summary

hdfs dfs -put -f output/stage3_feature_summary.csv project/output/stage3_feature_summary/data.csv
hdfs dfs -put -f output/stage3_hyperparameter_summary.csv project/output/stage3_hyperparameter_summary/data.csv

echo "[Stage IV Hive] Creating external Hive tables and views with existing names..."

"${BEELINE_CMD[@]}" \
  -f sql/create_stage3_hive_tables.hql \
  > output/create_stage3_hive_tables.txt 2>&1

echo "[Stage IV Hive] Checking created tables..."

"${BEELINE_CMD[@]}" \
  -e "
  USE team22_projectdb;

  SHOW TABLES LIKE 'stage3*';

  SELECT * FROM stage3_evaluation LIMIT 5;
  SELECT * FROM stage3_feature_summary;
  SELECT * FROM stage3_hyperparameter_summary;
  SELECT * FROM stage3_hyperparameter_optimization;
  SELECT * FROM stage3_model_metrics_long LIMIT 20;
  SELECT * FROM stage3_model_comparison;
  SELECT * FROM stage3_prediction_counts;
  SELECT * FROM stage3_confusion_matrix_long;
  SELECT * FROM stage3_sample_prediction LIMIT 5;
  " \
  > output/check_stage3_hive_tables.txt 2>&1

echo "[Stage IV Hive] Done."
echo
echo "Generated files:"
echo "  output/stage3_feature_summary.csv"
echo "  output/stage3_hyperparameter_summary.csv"
echo "  output/create_stage3_hive_tables.txt"
echo "  output/check_stage3_hive_tables.txt"