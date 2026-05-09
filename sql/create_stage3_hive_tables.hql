USE team22_projectdb;

DROP VIEW IF EXISTS stage3_model_metrics_long;
DROP VIEW IF EXISTS stage3_confusion_matrix_long;
DROP VIEW IF EXISTS stage3_prediction_counts;
DROP VIEW IF EXISTS stage3_hyperparameter_optimization;
DROP VIEW IF EXISTS stage3_model_comparison;

DROP TABLE IF EXISTS stage3_evaluation;
DROP TABLE IF EXISTS stage3_model1_predictions;
DROP TABLE IF EXISTS stage3_model2_predictions;
DROP TABLE IF EXISTS stage3_sample_prediction;
DROP TABLE IF EXISTS stage3_feature_summary;
DROP TABLE IF EXISTS stage3_hyperparameter_summary;

CREATE EXTERNAL TABLE stage3_evaluation (
    model STRING,
    paramCombinations INT,
    cvFolds INT,
    cvFits INT,
    trainingTimeSeconds DOUBLE,
    predictionTimeSeconds DOUBLE,
    evaluationTimeSeconds DOUBLE,
    areaUnderROC DOUBLE,
    areaUnderPR DOUBLE,
    accuracy DOUBLE,
    weightedPrecision DOUBLE,
    weightedRecall DOUBLE,
    weightedF1 DOUBLE,
    tn BIGINT,
    fp BIGINT,
    fn BIGINT,
    tp BIGINT,
    precision_0 DOUBLE,
    recall_0 DOUBLE,
    f1_0 DOUBLE,
    precision_1 DOUBLE,
    recall_1 DOUBLE,
    f1_1 DOUBLE,
    macroPrecision DOUBLE,
    macroRecall DOUBLE,
    macroF1 DOUBLE,
    model_path STRING,
    predictions_path STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/evaluation'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE stage3_model1_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/model1_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE stage3_model2_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/model2_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE stage3_sample_prediction (
    actual_label DOUBLE,
    prediction DOUBLE,
    probability_class_0 DOUBLE,
    probability_class_1 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/sample_prediction'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE stage3_feature_summary (
    feature_group STRING,
    feature_count INT,
    description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/stage3_feature_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE stage3_hyperparameter_summary (
    model STRING,
    hyperparameter_1 STRING,
    values_1 STRING,
    hyperparameter_2 STRING,
    values_2 STRING,
    paramCombinations INT,
    cvFolds INT,
    cvFits INT,
    optimization_metric STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team22/project/output/stage3_hyperparameter_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE VIEW stage3_model_metrics_long AS
SELECT model, 'areaUnderROC' AS metric_name, areaUnderROC AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'areaUnderPR' AS metric_name, areaUnderPR AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'accuracy' AS metric_name, accuracy AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'weightedPrecision' AS metric_name, weightedPrecision AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'weightedRecall' AS metric_name, weightedRecall AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'weightedF1' AS metric_name, weightedF1 AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'macroPrecision' AS metric_name, macroPrecision AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'macroRecall' AS metric_name, macroRecall AS metric_value FROM stage3_evaluation
UNION ALL
SELECT model, 'macroF1' AS metric_name, macroF1 AS metric_value FROM stage3_evaluation;

CREATE VIEW stage3_confusion_matrix_long AS
SELECT model, '0_actual' AS actual_label, '0_predicted' AS predicted_label, tn AS count_value
FROM stage3_evaluation
UNION ALL
SELECT model, '0_actual' AS actual_label, '1_predicted' AS predicted_label, fp AS count_value
FROM stage3_evaluation
UNION ALL
SELECT model, '1_actual' AS actual_label, '0_predicted' AS predicted_label, fn AS count_value
FROM stage3_evaluation
UNION ALL
SELECT model, '1_actual' AS actual_label, '1_predicted' AS predicted_label, tp AS count_value
FROM stage3_evaluation;

CREATE VIEW stage3_prediction_counts AS
SELECT
    'model1_logistic_regression' AS model,
    prediction,
    COUNT(*) AS prediction_count
FROM stage3_model1_predictions
GROUP BY prediction
UNION ALL
SELECT
    'model2_random_forest' AS model,
    prediction,
    COUNT(*) AS prediction_count
FROM stage3_model2_predictions
GROUP BY prediction;

CREATE VIEW stage3_hyperparameter_optimization AS
SELECT
    h.model,
    h.hyperparameter_1,
    h.values_1,
    h.hyperparameter_2,
    h.values_2,
    h.paramCombinations,
    h.cvFolds,
    h.cvFits,
    h.optimization_metric,
    e.areaUnderROC,
    e.areaUnderPR,
    e.accuracy,
    e.weightedF1,
    e.macroF1
FROM stage3_hyperparameter_summary h
JOIN stage3_evaluation e
ON h.model = e.model;

CREATE VIEW stage3_model_comparison AS
SELECT
    model,
    paramCombinations,
    cvFolds,
    cvFits,
    trainingTimeSeconds,
    trainingTimeSeconds / 60.0 AS trainingTimeMinutes,
    predictionTimeSeconds,
    evaluationTimeSeconds,
    areaUnderROC,
    areaUnderPR,
    accuracy,
    weightedPrecision,
    weightedRecall,
    weightedF1,
    macroPrecision,
    macroRecall,
    macroF1
FROM stage3_evaluation;
