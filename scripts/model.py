"""Stage III: Predictive Data Analytics with Spark ML.

Two binary classification models (Logistic Regression, Random Forest)
with cross-validated hyperparameter grid search (>=9 combinations each),
cyclical sin/cos feature encoding, class-weight balancing,
and a sample prediction output.
"""

import math

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, lit, sin, when

DB_NAME = "team22_projectdb"
TABLE_NAME = "flights_2024_features"

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


def remove_hdfs_path(spark, path):
    """Remove HDFS path if it exists."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    filesystem = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem.delete(hdfs_path, True)


def build_spark_session():
    """Create Spark session with Hive support."""
    return (
        SparkSession.builder
        .appName("team22_stage3_predictive_analytics")
        .master("yarn")
        # Use snappy everywhere to avoid lz4-java version conflict on the cluster
        .config("spark.io.compression.codec", "snappy")
        .config("spark.sql.avro.compression.codec", "snappy")
        .getOrCreate()
    )


def add_cyclical_features(df):
    """Encode cyclical temporal features with sin/cos to preserve periodicity."""
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


def add_class_weights(df):
    """Add balanced class weights to address label imbalance."""
    counts = {r[LABEL_COL]: r["count"] for r in df.groupBy(LABEL_COL).count().collect()}
    total = sum(counts.values())
    num_classes = len(counts)
    weight_for = {label: total / (num_classes * cnt) for label, cnt in counts.items()}
    print(f"Class weight map: {weight_for}")
    return df.withColumn(
        WEIGHT_COL,
        when(col(LABEL_COL) == 0.0, lit(weight_for.get(0.0, 1.0)))
        .otherwise(lit(weight_for.get(1.0, 1.0))),
    )


def prepare_dataset(spark):
    """Read Hive table and build clean ML-ready dataframe with engineered features."""
    source_df = spark.read.parquet(
        "hdfs:///user/team22/project/hive/warehouse/flights_2024_features"
    )
    print("=== Source schema ===")
    source_df.printSchema()

    categorical_cols = ["op_unique_carrier", "origin", "dest"]

    base_numeric_cols = [
        "day_of_month",
        "day_of_week",
        "crs_dep_time",
        "crs_arr_time",
        "crs_elapsed_time",
        "distance",
        "scheduled_dep_hour",
        "scheduled_arr_hour",
        "is_weekend",
        "month",
    ]

    cyclical_cols = [
        "dep_hour_sin", "dep_hour_cos",
        "arr_hour_sin", "arr_hour_cos",
        "dow_sin", "dow_cos",
        "month_sin", "month_cos",
        "dom_sin", "dom_cos",
    ]

    required_raw = categorical_cols + base_numeric_cols + ["is_arrival_delayed"]

    df = (
        source_df
        .filter((col("cancelled") == 0) & (col("diverted") == 0))
        .select(*required_raw)
        .na.drop(subset=required_raw)
        .withColumnRenamed("is_arrival_delayed", LABEL_COL)
        .withColumn(LABEL_COL, col(LABEL_COL).cast("double"))
    )

    df = add_cyclical_features(df)

    numeric_cols = base_numeric_cols + cyclical_cols

    print("=== Prepared dataset ===")
    print(f"Rows: {df.count()}")
    df.groupBy(LABEL_COL).count().show()

    return df, categorical_cols, numeric_cols


def build_feature_stages(categorical_cols, numeric_cols):
    """Assemble Spark ML feature extraction pipeline stages."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
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

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False,
    )

    return indexers + [encoder, assembler, scaler]


def save_train_test(train_df, test_df):
    """Persist train/test splits to HDFS as JSON."""
    train_df.coalesce(1).write.mode("overwrite").json(HDFS_TRAIN_PATH)
    test_df.coalesce(1).write.mode("overwrite").json(HDFS_TEST_PATH)


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
    """Train one model via cross-validation; return metrics dict and fitted model."""
    n_combos = len(param_grid)
    print(f"=== Training {model_name} ({n_combos} param combinations, 3-fold CV) ===")

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

    pipeline = Pipeline(stages=feature_stages + [estimator])

    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_roc,
        numFolds=3,
        parallelism=2,
        seed=RANDOM_SEED,
    )

    fitted_model = cross_validator.fit(train_df)
    predictions = fitted_model.transform(test_df)

    area_under_roc = evaluator_roc.evaluate(predictions)
    area_under_pr = evaluator_pr.evaluate(predictions)

    print(f"{model_name} areaUnderROC = {area_under_roc:.6f}")
    print(f"{model_name} areaUnderPR  = {area_under_pr:.6f}")

    fitted_model.write().overwrite().save(model_path)

    (
        predictions
        .select(col(LABEL_COL).alias("label"), col("prediction"))
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(predictions_path)
    )

    metrics = {
        "model": model_name,
        "areaUnderROC": float(area_under_roc),
        "areaUnderPR": float(area_under_pr),
        "model_path": model_path,
        "predictions_path": predictions_path,
    }
    return metrics, fitted_model


def save_sample_prediction(fitted_model, test_df):
    """Run the best model on one test row and save label vs prediction to HDFS."""
    sample_df = test_df.limit(1)
    pred_df = fitted_model.transform(sample_df)

    print("=== Sample Prediction ===")
    pred_df.select(
        col(LABEL_COL).alias("actual_label"),
        col("prediction"),
        col("probability"),
    ).show(truncate=False)

    (
        pred_df
        .select(
            col(LABEL_COL).alias("actual_label"),
            col("prediction"),
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

    print("=== Cleaning previous Stage III HDFS outputs ===")
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
    df = add_class_weights(df)

    print("=== Splitting train/test (70/30) ===")
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=RANDOM_SEED)
    print(f"Train rows: {train_df.count()}")
    print(f"Test rows:  {test_df.count()}")

    print("=== Saving train/test JSON to HDFS ===")
    save_train_test(train_df, test_df)

    feature_stages = build_feature_stages(categorical_cols, numeric_cols)

    # ── Model 1: Logistic Regression ──────────────────────────────────────────
    # Hyperparameters: regParam × elasticNetParam × tol  →  3³ = 27 combinations
    logistic_regression = LogisticRegression(
        labelCol=LABEL_COL,
        featuresCol="features",
        weightCol=WEIGHT_COL,
    )
    logistic_grid = (
        ParamGridBuilder()
        .addGrid(logistic_regression.regParam, [0.001, 0.01, 0.1])
        .addGrid(logistic_regression.elasticNetParam, [0.0, 0.3, 0.7])
        .addGrid(logistic_regression.tol, [1e-6, 1e-5, 1e-4])
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

    # ── Model 2: Random Forest ─────────────────────────────────────────────────
    # Hyperparameters: numTrees × maxDepth × minInstancesPerNode  →  3³ = 27
    random_forest = RandomForestClassifier(
        labelCol=LABEL_COL,
        featuresCol="features",
        weightCol=WEIGHT_COL,
        seed=RANDOM_SEED,
    )
    random_forest_grid = (
        ParamGridBuilder()
        .addGrid(random_forest.numTrees, [20, 50, 100])
        .addGrid(random_forest.maxDepth, [5, 8, 10])
        .addGrid(random_forest.minInstancesPerNode, [1, 5, 10])
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

    results = [lr_result, rf_result]

    print("=== Saving evaluation results ===")
    evaluation_df = spark.createDataFrame(results)
    (
        evaluation_df
        .select("model", "areaUnderROC", "areaUnderPR", "model_path", "predictions_path")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(HDFS_EVALUATION_PATH)
    )
    evaluation_df.show(truncate=False)

    # ── Sample prediction with the best model (highest AUC-ROC) ───────────────
    best = max(results, key=lambda r: r["areaUnderROC"])
    print(f"=== Best model: {best['model']} (AUC-ROC={best['areaUnderROC']:.4f}) ===")

    best_fitted = lr_model if best["model"] == lr_result["model"] else rf_model
    save_sample_prediction(best_fitted, test_df)

    print("=== Stage III Spark job completed successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()
