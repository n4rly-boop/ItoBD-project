"""Stage III predictive analytics with Spark ML.

Trains Logistic Regression and Random Forest classifiers on the
flights_2024_features Hive table with 3-fold cross-validation.
Cyclic temporal variables are sin/cos encoded; features are scaled
with StandardScaler. Saves trained pipelines, predictions, evaluation
metrics, and a sample prediction to HDFS.
"""

import math
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, lit, sin, when


DB_NAME = "team22_projectdb"
TABLE_NAME = "flights_2024_features"
SOURCE_PARQUET_PATH = (
    "hdfs:///user/team22/project/hive/warehouse/flights_2024_features"
)

LABEL_COL = "label"
WEIGHT_COL = "class_weight"

HDFS_TRAIN_PATH = "project/data/train"
HDFS_TEST_PATH = "project/data/test"

HDFS_MODEL1_PATH = "project/models/model1"
HDFS_MODEL2_PATH = "project/models/model2"

HDFS_MODEL1_PREDICTIONS_PATH = "project/output/model1_predictions"
HDFS_MODEL2_PREDICTIONS_PATH = "project/output/model2_predictions"

HDFS_EVALUATION_PATH = "project/output/evaluation"
HDFS_SAMPLE_PREDICTION_PATH = "project/output/sample_prediction"

RANDOM_SEED = 42
TWO_PI = 2.0 * math.pi


def log(message):
    """Print log message immediately to stage3_model.log."""
    print(message, flush=True)


def remove_hdfs_path(spark, path):
    """Remove HDFS path if it exists."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    filesystem = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem.delete(hdfs_path, True)


def build_spark_session():
    """Create Spark session on YARN."""
    return (
        SparkSession.builder
        .appName("team22_stage3_predictive_analytics")
        .master("yarn")
        .config("spark.io.compression.codec", "snappy")
        .config("spark.sql.avro.compression.codec", "snappy")
        .getOrCreate()
    )


def add_cyclical_features(df):
    """Add sin/cos columns for hour, day-of-week, month, day-of-month."""
    return (
        df
        .withColumn("dep_hour_sin", sin(col("scheduled_dep_hour") * (TWO_PI / 24)))
        .withColumn("dep_hour_cos", cos(col("scheduled_dep_hour") * (TWO_PI / 24)))
        .withColumn("arr_hour_sin", sin(col("scheduled_arr_hour") * (TWO_PI / 24)))
        .withColumn("arr_hour_cos", cos(col("scheduled_arr_hour") * (TWO_PI / 24)))
        .withColumn("dow_sin", sin(col("day_of_week") * (TWO_PI / 7)))
        .withColumn("dow_cos", cos(col("day_of_week") * (TWO_PI / 7)))
        .withColumn("month_sin", sin(col("month") * (TWO_PI / 12)))
        .withColumn("month_cos", cos(col("month") * (TWO_PI / 12)))
        .withColumn("dom_sin", sin(col("day_of_month") * (TWO_PI / 31)))
        .withColumn("dom_cos", cos(col("day_of_month") * (TWO_PI / 31)))
    )


def compute_class_weight_map(train_df):
    """Compute balanced class weights using only the training set."""
    counts = {
        float(r[LABEL_COL]): int(r["count"])
        for r in train_df.groupBy(LABEL_COL).count().collect()
    }

    total = sum(counts.values())
    num_classes = len(counts)

    weight_for = {
        label: total / (num_classes * count)
        for label, count in counts.items()
    }

    log(f"Class counts from train: {counts}")
    log(f"Class weight map from train: {weight_for}")

    return weight_for


def apply_class_weights(df, weight_for):
    """Apply previously computed class weights to a dataframe."""
    return df.withColumn(
        WEIGHT_COL,
        when(col(LABEL_COL) == 0.0, lit(float(weight_for.get(0.0, 1.0))))
        .otherwise(lit(float(weight_for.get(1.0, 1.0)))),
    )


def prepare_dataset(spark):
    """Read Hive warehouse parquet and build clean ML-ready dataframe."""
    source_df = spark.read.parquet(SOURCE_PARQUET_PATH)
    log(f"=== Source parquet path: {SOURCE_PARQUET_PATH} ===")

    log("=== Source schema ===")
    source_df.printSchema()

    categorical_cols = [
        "op_unique_carrier",
        "origin",
        "dest",
    ]

    # Raw temporal columns are needed only to create cyclical sin/cos features.
    # They are NOT passed directly to the model as ordinary numerical features,
    # because hour/day/month are cyclic values.
    raw_temporal_cols = [
        "day_of_month",
        "day_of_week",
        "scheduled_dep_hour",
        "scheduled_arr_hour",
        "month",
    ]

    # Non-cyclical numerical features used directly by the model.
    # crs_dep_time and crs_arr_time are excluded because they encode time and are
    # represented through scheduled_dep_hour/scheduled_arr_hour sin-cos features.
    base_numeric_cols = [
        "crs_elapsed_time",
        "distance",
        "is_weekend",
    ]

    cyclical_cols = [
        "dep_hour_sin",
        "dep_hour_cos",
        "arr_hour_sin",
        "arr_hour_cos",
        "dow_sin",
        "dow_cos",
        "month_sin",
        "month_cos",
        "dom_sin",
        "dom_cos",
    ]

    required_raw_cols = (
        categorical_cols
        + base_numeric_cols
        + raw_temporal_cols
        + ["is_arrival_delayed"]
    )

    # Clean prediction setting:
    # We predict arrival delay before the flight starts.
    # Therefore, cancelled/diverted flights are excluded and actual/post-flight
    # columns are not used to avoid target leakage.
    df = (
        source_df
        .filter((col("cancelled") == 0) & (col("diverted") == 0))
        .select(*required_raw_cols)
        .na.drop(subset=required_raw_cols)
        .withColumnRenamed("is_arrival_delayed", LABEL_COL)
        .withColumn(LABEL_COL, col(LABEL_COL).cast("double"))
    )

    df = add_cyclical_features(df)
    numeric_cols = base_numeric_cols + cyclical_cols

    log("=== Feature columns used by the model ===")
    log(f"Categorical columns: {categorical_cols}")
    log(f"Direct numerical columns: {base_numeric_cols}")
    log(f"Raw temporal columns used only for sin/cos encoding: {raw_temporal_cols}")
    log(f"Cyclical encoded columns: {cyclical_cols}")
    log(f"Final numerical columns passed to VectorAssembler: {numeric_cols}")

    log("=== Prepared dataset ===")
    log(f"Rows: {df.count()}")

    log("=== Label distribution ===")
    df.groupBy(LABEL_COL).count().orderBy(LABEL_COL).show()

    log("=== Cleaned dataset sample before train/test split ===")
    df.select(
        "op_unique_carrier",
        "origin",
        "dest",
        "day_of_month",
        "day_of_week",
        "crs_elapsed_time",
        "distance",
        "scheduled_dep_hour",
        "scheduled_arr_hour",
        "is_weekend",
        "month",
        "dep_hour_sin",
        "dep_hour_cos",
        "arr_hour_sin",
        "arr_hour_cos",
        "dow_sin",
        "dow_cos",
        "month_sin",
        "month_cos",
        "dom_sin",
        "dom_cos",
        LABEL_COL,
    ).show(20, truncate=False)

    log("=== Null counts after cleaning ===")
    df.select([
        col(c).isNull().cast("int").alias(c)
        for c in df.columns
    ]).groupBy().sum().show(truncate=False)

    return df, categorical_cols, numeric_cols


def build_feature_stages(categorical_cols, numeric_cols):
    """Build Spark ML feature extraction stages."""
    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep",
        )
        for c in categorical_cols
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in categorical_cols],
        outputCols=[f"{c}_ohe" for c in categorical_cols],
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=numeric_cols + [f"{c}_ohe" for c in categorical_cols],
        outputCol="raw_features",
        handleInvalid="keep",
    )

    # Scaling is applied after assembling all numeric and encoded categorical
    # features. withMean=False preserves sparse vectors after OneHotEncoder.
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,
    )

    return indexers + [encoder, assembler, scaler]


def debug_features(feature_stages, train_df):
    """Print sample rows before and after ML feature transformations."""
    log("=== Cleaned train sample before ML transformations ===")
    train_df.select(
        "op_unique_carrier",
        "origin",
        "dest",
        "day_of_month",
        "day_of_week",
        "crs_elapsed_time",
        "distance",
        "scheduled_dep_hour",
        "scheduled_arr_hour",
        "is_weekend",
        "month",
        "dep_hour_sin",
        "dep_hour_cos",
        "arr_hour_sin",
        "arr_hour_cos",
        "dow_sin",
        "dow_cos",
        "month_sin",
        "month_cos",
        "dom_sin",
        "dom_cos",
        LABEL_COL,
        WEIGHT_COL,
    ).show(20, truncate=False)

    log("=== Transformed feature sample before model fitting ===")

    debug_pipeline = Pipeline(stages=feature_stages)
    debug_model = debug_pipeline.fit(train_df)
    debug_df = debug_model.transform(train_df.limit(20))

    debug_df.select(
        "op_unique_carrier",
        "op_unique_carrier_idx",
        "op_unique_carrier_ohe",
        "origin",
        "origin_idx",
        "origin_ohe",
        "dest",
        "dest_idx",
        "dest_ohe",
        "raw_features",
        "features",
        LABEL_COL,
        WEIGHT_COL,
    ).show(20, truncate=False)


def save_transformed_train_test(feature_stages, train_df, test_df):
    """Save transformed train/test datasets with features and label to HDFS."""
    log("=== Fitting feature pipeline for train/test export ===")
    start_time = time.time()

    feature_pipeline = Pipeline(stages=feature_stages)
    feature_model = feature_pipeline.fit(train_df)

    train_features = feature_model.transform(train_df)
    test_features = feature_model.transform(test_df)

    log("=== Writing transformed train set to HDFS ===")
    (
        train_features
        .select("features", LABEL_COL)
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(HDFS_TRAIN_PATH)
    )

    log("=== Writing transformed test set to HDFS ===")
    (
        test_features
        .select("features", LABEL_COL)
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(HDFS_TEST_PATH)
    )

    elapsed = time.time() - start_time
    log(f"Feature export time seconds: {elapsed:.2f}")
    log(f"Feature export time minutes: {elapsed / 60.0:.2f}")

    log("=== Saved transformed train/test with features and label ===")
    train_features.select("features", LABEL_COL).show(10, truncate=False)
    test_features.select("features", LABEL_COL).show(10, truncate=False)


def compute_confusion_metrics(predictions):
    """Compute confusion matrix and per-class/macro metrics for binary classification."""
    rows = (
        predictions
        .groupBy(LABEL_COL, "prediction")
        .count()
        .collect()
    )

    cm = {
        (float(r[LABEL_COL]), float(r["prediction"])): int(r["count"])
        for r in rows
    }

    tn = cm.get((0.0, 0.0), 0)
    fp = cm.get((0.0, 1.0), 0)
    fn = cm.get((1.0, 0.0), 0)
    tp = cm.get((1.0, 1.0), 0)

    precision_0 = tn / (tn + fn) if (tn + fn) > 0 else 0.0
    recall_0 = tn / (tn + fp) if (tn + fp) > 0 else 0.0
    f1_0 = (
        2.0 * precision_0 * recall_0 / (precision_0 + recall_0)
        if (precision_0 + recall_0) > 0
        else 0.0
    )

    precision_1 = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall_1 = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1_1 = (
        2.0 * precision_1 * recall_1 / (precision_1 + recall_1)
        if (precision_1 + recall_1) > 0
        else 0.0
    )

    macro_precision = (precision_0 + precision_1) / 2.0
    macro_recall = (recall_0 + recall_1) / 2.0
    macro_f1 = (f1_0 + f1_1) / 2.0

    return {
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
        "precision_0": float(precision_0),
        "recall_0": float(recall_0),
        "f1_0": float(f1_0),
        "precision_1": float(precision_1),
        "recall_1": float(recall_1),
        "f1_1": float(f1_1),
        "macroPrecision": float(macro_precision),
        "macroRecall": float(macro_recall),
        "macroF1": float(macro_f1),
    }


def save_predictions(predictions, predictions_path):
    """Save predictions to HDFS as CSV with only label and prediction columns."""
    (
        predictions
        .select(
            col(LABEL_COL).alias("label"),
            col("prediction"),
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(predictions_path)
    )


def train_model(
    model_name,
    estimator,
    param_grid,
    feature_stages,
    train_df,
    test_df,
    model_path,
    predictions_path,
):
    """Train one model using cross-validation and save predictions/model."""
    n_combos = len(param_grid)
    cv_folds = 3
    total_fits = n_combos * cv_folds

    log("=" * 80)
    log(f"=== Training {model_name} ===")
    log(f"Parameter combinations: {n_combos}")
    log(f"Cross-validation folds: {cv_folds}")
    log(f"Total model fits: {total_fits}")
    log("Optimization metric: areaUnderROC")
    log("Parameter grid:")

    for idx, param_map in enumerate(param_grid, start=1):
        readable_params = {
            param.name: value
            for param, value in param_map.items()
        }
        log(f"  Combination {idx}/{n_combos}: {readable_params}")

    evaluator_roc = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )

    evaluator_pr = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )

    evaluator_accuracy = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="accuracy",
    )

    evaluator_weighted_precision = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="weightedPrecision",
    )

    evaluator_weighted_recall = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="weightedRecall",
    )

    evaluator_weighted_f1 = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="f1",
    )

    pipeline = Pipeline(stages=feature_stages + [estimator])

    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_roc,
        numFolds=cv_folds,
        parallelism=2,
        seed=RANDOM_SEED,
    )

    log(f"=== {model_name}: starting CrossValidator.fit(train_df) ===")
    fit_start = time.time()

    fitted_model = cross_validator.fit(train_df)

    fit_elapsed = time.time() - fit_start
    log(f"=== {model_name}: CrossValidator finished ===")
    log(f"Training time seconds: {fit_elapsed:.2f}")
    log(f"Training time minutes: {fit_elapsed / 60.0:.2f}")

    best_model = fitted_model.bestModel
    best_stage = best_model.stages[-1]
    best_param_map = {
        param.name: value
        for param, value in best_stage.extractParamMap().items()
        if param.parent == best_stage.uid
    }

    log(f"=== {model_name}: best model parameters ===")
    for key, value in sorted(best_param_map.items()):
        log(f"{key}: {value}")

    log(f"=== {model_name}: generating predictions on test set ===")
    pred_start = time.time()

    predictions = fitted_model.transform(test_df)
    predictions.cache()

    prediction_count = predictions.count()
    pred_elapsed = time.time() - pred_start

    log(f"{model_name}: prediction rows = {prediction_count}")
    log(f"{model_name}: prediction time seconds = {pred_elapsed:.2f}")

    log(f"=== {model_name}: evaluating predictions ===")
    eval_start = time.time()

    area_under_roc = evaluator_roc.evaluate(predictions)
    area_under_pr = evaluator_pr.evaluate(predictions)

    accuracy = evaluator_accuracy.evaluate(predictions)
    weighted_precision = evaluator_weighted_precision.evaluate(predictions)
    weighted_recall = evaluator_weighted_recall.evaluate(predictions)
    weighted_f1 = evaluator_weighted_f1.evaluate(predictions)

    confusion_metrics = compute_confusion_metrics(predictions)

    eval_elapsed = time.time() - eval_start
    log(f"{model_name}: evaluation time seconds = {eval_elapsed:.2f}")

    log(f"{model_name} areaUnderROC      = {area_under_roc:.6f}")
    log(f"{model_name} areaUnderPR       = {area_under_pr:.6f}")
    log(f"{model_name} accuracy          = {accuracy:.6f}")
    log(f"{model_name} weightedPrecision = {weighted_precision:.6f}")
    log(f"{model_name} weightedRecall    = {weighted_recall:.6f}")
    log(f"{model_name} weightedF1        = {weighted_f1:.6f}")

    log(f"=== {model_name} confusion matrix ===")
    predictions.groupBy(LABEL_COL, "prediction").count().orderBy(
        LABEL_COL,
        "prediction",
    ).show()

    log(f"=== {model_name} per-class and macro metrics ===")
    for key, value in confusion_metrics.items():
        log(f"{key}: {value}")

    log(f"=== {model_name}: saving fitted model to {model_path} ===")
    fitted_model.write().overwrite().save(model_path)

    log(f"=== {model_name}: saving predictions to {predictions_path} ===")
    save_predictions(predictions, predictions_path)

    predictions.unpersist()

    metrics = {
        "model": model_name,
        "paramCombinations": int(n_combos),
        "cvFolds": int(cv_folds),
        "cvFits": int(total_fits),
        "trainingTimeSeconds": float(fit_elapsed),
        "predictionTimeSeconds": float(pred_elapsed),
        "evaluationTimeSeconds": float(eval_elapsed),
        "areaUnderROC": float(area_under_roc),
        "areaUnderPR": float(area_under_pr),
        "accuracy": float(accuracy),
        "weightedPrecision": float(weighted_precision),
        "weightedRecall": float(weighted_recall),
        "weightedF1": float(weighted_f1),
        "model_path": model_path,
        "predictions_path": predictions_path,
    }

    metrics.update(confusion_metrics)

    log(f"=== {model_name}: completed ===")
    log("=" * 80)

    return metrics, fitted_model


def save_sample_prediction(fitted_model, test_df):
    """Run the best model on one test row and save label vs prediction to HDFS."""
    sample_df = test_df.limit(1)
    pred_df = fitted_model.transform(sample_df)

    pred_df = (
        pred_df
        .withColumn("probability_array", vector_to_array(col("probability")))
        .withColumn("probability_class_0", col("probability_array")[0])
        .withColumn("probability_class_1", col("probability_array")[1])
    )

    log("=== Sample Prediction ===")
    pred_df.select(
        col(LABEL_COL).alias("actual_label"),
        col("prediction"),
        col("probability_class_0"),
        col("probability_class_1"),
    ).show(truncate=False)

    (
        pred_df
        .select(
            col(LABEL_COL).alias("actual_label"),
            col("prediction"),
            col("probability_class_0"),
            col("probability_class_1"),
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(HDFS_SAMPLE_PREDICTION_PATH)
    )


def main():
    """Run Stage III ML pipeline."""
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log("=== Cleaning previous Stage III HDFS outputs ===")
    for path in [
        HDFS_TRAIN_PATH,
        HDFS_TEST_PATH,
        HDFS_MODEL1_PATH,
        HDFS_MODEL2_PATH,
        HDFS_MODEL1_PREDICTIONS_PATH,
        HDFS_MODEL2_PREDICTIONS_PATH,
        HDFS_EVALUATION_PATH,
        HDFS_SAMPLE_PREDICTION_PATH,
    ]:
        remove_hdfs_path(spark, path)

    df, categorical_cols, numeric_cols = prepare_dataset(spark)

    log("=== Splitting train/test (70/30) ===")
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=RANDOM_SEED)

    log(f"Train rows before weights: {train_df.count()}")
    log(f"Test rows before weights:  {test_df.count()}")

    log("=== Computing class weights from train only ===")
    class_weight_map = compute_class_weight_map(train_df)

    train_df = apply_class_weights(train_df, class_weight_map)
    test_df = apply_class_weights(test_df, class_weight_map)

    log("=== Train label/weight distribution ===")
    train_df.groupBy(LABEL_COL, WEIGHT_COL).count().orderBy(LABEL_COL).show()

    log("=== Test label/weight distribution ===")
    test_df.groupBy(LABEL_COL, WEIGHT_COL).count().orderBy(LABEL_COL).show()

    feature_stages = build_feature_stages(categorical_cols, numeric_cols)

    log("=== Saving transformed train/test JSON to HDFS ===")
    save_transformed_train_test(feature_stages, train_df, test_df)

    # Debug output to verify preprocessing before model training.
    debug_features(feature_stages, train_df)

    results = []

    # Model 1: Logistic Regression
    # Hyperparameters: regParam x elasticNetParam = 3 x 2 = 6 combinations.
    logistic_regression = LogisticRegression(
        labelCol=LABEL_COL,
        featuresCol="features",
        weightCol=WEIGHT_COL,
    )

    logistic_grid = (
    ParamGridBuilder()
    .addGrid(logistic_regression.regParam, [0.001, 0.01, 0.1])
    .addGrid(logistic_regression.elasticNetParam, [0.0, 0.5])
    .build()
    )

    lr_result, lr_model = train_model(
        model_name="model1_logistic_regression",
        estimator=logistic_regression,
        param_grid=logistic_grid,
        feature_stages=feature_stages,
        train_df=train_df,
        test_df=test_df,
        model_path=HDFS_MODEL1_PATH,
        predictions_path=HDFS_MODEL1_PREDICTIONS_PATH,
    )

    results.append(lr_result)

    # Model 2: Random Forest
    # Hyperparameters: numTrees x maxDepth = 3 x 2 = 6 combinations.
    random_forest = RandomForestClassifier(
        labelCol=LABEL_COL,
        featuresCol="features",
        weightCol=WEIGHT_COL,
        seed=RANDOM_SEED,
    )

    random_forest_grid = (
        ParamGridBuilder()
        .addGrid(random_forest.numTrees, [20, 50, 100])
        .addGrid(random_forest.maxDepth, [5, 10])
        .build()
    )
    rf_result, rf_model = train_model(
        model_name="model2_random_forest",
        estimator=random_forest,
        param_grid=random_forest_grid,
        feature_stages=feature_stages,
        train_df=train_df,
        test_df=test_df,
        model_path=HDFS_MODEL2_PATH,
        predictions_path=HDFS_MODEL2_PREDICTIONS_PATH,
    )

    results.append(rf_result)

    log("=== Saving evaluation results ===")
    evaluation_df = spark.createDataFrame(results)

    (
        evaluation_df
        .select(
            "model",
            "paramCombinations",
            "cvFolds",
            "cvFits",
            "trainingTimeSeconds",
            "predictionTimeSeconds",
            "evaluationTimeSeconds",
            "areaUnderROC",
            "areaUnderPR",
            "accuracy",
            "weightedPrecision",
            "weightedRecall",
            "weightedF1",
            "tn",
            "fp",
            "fn",
            "tp",
            "precision_0",
            "recall_0",
            "f1_0",
            "precision_1",
            "recall_1",
            "f1_1",
            "macroPrecision",
            "macroRecall",
            "macroF1",
            "model_path",
            "predictions_path",
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(HDFS_EVALUATION_PATH)
    )

    evaluation_df.show(truncate=False)

    # Sample prediction with the best model by ROC-AUC
    best = max(results, key=lambda r: r["areaUnderROC"])
    log(f"=== Best model: {best['model']} (AUC-ROC={best['areaUnderROC']:.4f}) ===")

    best_fitted = lr_model if best["model"] == lr_result["model"] else rf_model
    save_sample_prediction(best_fitted, test_df)

    log("=== Stage III Spark job completed successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()