#!/usr/bin/env python3
"""Predict concrete strength for previously unseen mix recipes; Spark 3.0 / YARN.

Input is a Hive managed table loaded from NiFi-ingested HDFS data, never a CSV
read in this script. A recipe is the seven ingredient quantities, not curing
age or measured strength. Identical recipes stay in the same holdout split.
This is an educational benchmark, not a structural-engineering approval tool.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import re
from urllib.parse import urlparse

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F


INGREDIENTS = [
    "cement_kg_m3", "slag_kg_m3", "fly_ash_kg_m3", "water_kg_m3",
    "superplasticizer_kg_m3", "coarse_aggregate_kg_m3", "fine_aggregate_kg_m3",
]
FEATURES = INGREDIENTS + ["age_days"]
LABEL = "strength_mpa"
COLUMNS = FEATURES + [LABEL]
EXPECTED_ROWS = 1030
SEED = 650


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="dsc650_final")
    parser.add_argument("--table", default="final_concrete_strength")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dataset-sha256", required=True,
                        help="SHA-256 of the exact normalized CSV ingested by NiFi")
    parser.add_argument("--output-dir", required=True,
                        help="New HDFS run directory; must not already exist")
    parser.add_argument("--hbase-host", default="master")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-table", default="concrete_model_metrics_v1")
    args = parser.parse_args()
    for value in (args.database, args.table, args.hbase_table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            parser.error("Database/table identifiers must contain only letters, digits, underscores")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", args.run_id):
        parser.error("Run ID must be 1-80 letters, digits, underscores, or hyphens")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", args.dataset_sha256):
        parser.error("Dataset SHA-256 must contain exactly 64 hexadecimal characters")
    uri = urlparse(args.output_dir)
    if uri.scheme != "hdfs" or not uri.netloc or uri.path in ("", "/"):
        parser.error("Output directory must be an explicit non-root hdfs://host:port/path URI")
    if uri.query or uri.fragment or ".." in uri.path.split("/"):
        parser.error("Output directory must not contain a query, fragment, or parent traversal")
    if not 1 <= args.hbase_port <= 65535:
        parser.error("HBase port must be between 1 and 65535")
    args.output_dir = args.output_dir.rstrip("/")
    args.dataset_sha256 = args.dataset_sha256.lower()
    return args


def log_event(name, value):
    print("PROJECT_EVIDENCE " + name + " " + json.dumps(value, sort_keys=True,
                                                        allow_nan=False), flush=True)


def hbase_connection(host, port):
    # I use a timeout so an unavailable Thrift service does not hang the job.
    import happybase
    return happybase.Connection(host=host, port=port, timeout=15000,
                                compat="0.98", transport="buffered", protocol="binary")


def check_hbase(args, row_key):
    connection = hbase_connection(args.hbase_host, args.hbase_port)
    try:
        if args.hbase_table.encode("utf-8") not in connection.tables():
            raise RuntimeError("Required HBase table does not exist; create and empty-scan it first")
        table = connection.table(args.hbase_table)
        if not {b"metrics", b"run"}.issubset(set(table.families())):
            raise RuntimeError("HBase table must already contain metrics and run column families")
        if table.row(row_key):
            raise RuntimeError("Run ID already exists in HBase; use a new run ID, never overwrite")
    finally:
        connection.close()


def write_hbase_partition(rows, host, port, table_name):
    # I open the connection on the executor instead of trying to share a live socket.
    connection = hbase_connection(host, port)
    try:
        table = connection.table(table_name)
        for row_key, cells in rows:
            existing = table.row(row_key)
            # I allow an identical task retry, but I reject different values for this run.
            if existing and existing != cells:
                raise RuntimeError("Refusing to overwrite a conflicting HBase metrics row")
            if not existing:
                table.put(row_key, cells, wal=True)
            if table.row(row_key) != cells:
                raise RuntimeError("Executor HBase read-back did not match the submitted metrics")
    finally:
        connection.close()


def validate_input(data):
    expected_types = {column: "double" for column in COLUMNS}
    expected_types["age_days"] = "int"
    if dict(data.dtypes) != expected_types:
        raise RuntimeError("Hive schema differs from the required DOUBLE / INT schema: "
                           + repr(data.dtypes))
    row_count = data.count()
    if row_count != EXPECTED_ROWS:
        raise RuntimeError("Expected 1030 preserved source rows; observed {}".format(row_count))
    invalid = F.lit(False)
    for name in COLUMNS:
        value = F.col(name)
        invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
    for name in INGREDIENTS:
        invalid = invalid | (F.col(name) < 0)
    for name in ("cement_kg_m3", "water_kg_m3", "coarse_aggregate_kg_m3",
                 "fine_aggregate_kg_m3", "age_days", LABEL):
        invalid = invalid | (F.col(name) <= 0)
    invalid_count = data.filter(invalid).count()
    if invalid_count:
        raise RuntimeError("Invalid/null/nonfinite/domain-violating rows: {}".format(invalid_count))
    # I check ingredient precision before grouping so different recipes are not combined.
    excess_precision = F.lit(False)
    for name in INGREDIENTS:
        excess_precision = excess_precision | (F.abs(F.col(name) - F.round(name, 3)) > 1e-8)
    if data.filter(excess_precision).limit(1).count():
        raise RuntimeError("Ingredient precision exceeds the three-decimal grouping contract")
    return row_count, row_count - data.dropDuplicates(COLUMNS).count()


def assign_splits(data):
    # I keep repeated recipes together so the test set contains unseen mixtures.
    recipe_text = F.concat_ws("|", *[F.format_string("%.3f", F.col(c)) for c in INGREDIENTS])
    grouped = data.withColumn("recipe_id", F.sha2(recipe_text, 256))
    # I use positive hash buckets with operators available in Spark 3.0.
    bucket = F.conv(F.substring("recipe_id", 1, 8), 16, 10).cast("long") % 100
    return grouped.withColumn("split", F.when(bucket < 70, "train")
                              .when(bucket < 85, "validation").otherwise("test"))


def make_model(max_depth):
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="error")
    forest = RandomForestRegressor(featuresCol="features", labelCol=LABEL,
                                   predictionCol="prediction", numTrees=32,
                                   maxDepth=max_depth, maxBins=32, seed=SEED,
                                   featureSubsetStrategy="onethird")
    return Pipeline(stages=[assembler, forest])


def validation_rmse(predictions):
    value = predictions.agg(F.sqrt(F.avg(F.pow(F.col(LABEL) - F.col("prediction"), 2)))
                            .alias("rmse")).first()["rmse"]
    if value is None or not math.isfinite(value):
        raise RuntimeError("Validation RMSE is missing or nonfinite")
    return float(value)


def test_metrics(predictions, baseline_mean):
    # I compare both models on the same untouched test records.
    result = predictions.agg(
        F.count("*").alias("n"), F.sum(LABEL).alias("sum_y"),
        F.sum(F.pow(F.col(LABEL), 2)).alias("sum_y2"),
        F.sum(F.pow(F.col(LABEL) - F.col("prediction"), 2)).alias("model_sse"),
        F.sum(F.abs(F.col(LABEL) - F.col("prediction"))).alias("model_sae"),
        F.sum(F.pow(F.col(LABEL) - F.lit(baseline_mean), 2)).alias("baseline_sse"),
        F.sum(F.abs(F.col(LABEL) - F.lit(baseline_mean))).alias("baseline_sae"),
    ).first().asDict()
    return scores_from_aggregate(result)


def scores_from_aggregate(result):
    if result["n"] <= 0:
        raise RuntimeError("Cannot evaluate an empty test split")
    if not all(math.isfinite(value) for value in result.values()):
        raise RuntimeError("Test aggregates contain a nonfinite value")
    total_variation = result["sum_y2"] - result["sum_y"] ** 2 / result["n"]
    if total_variation <= 0:
        raise RuntimeError("The test target is constant; R-squared would be undefined")
    scores = {}
    for model_name in ("model", "baseline"):
        sse, sae = result[model_name + "_sse"], result[model_name + "_sae"]
        scores[model_name] = {"rmse_mpa": math.sqrt(sse / result["n"]),
                              "mae_mpa": sae / result["n"],
                              "r2": 1.0 - sse / total_variation}
    if not all(math.isfinite(value) for score in scores.values() for value in score.values()):
        raise RuntimeError("At least one test metric is nonfinite")
    return scores


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("DSC650_Concrete_" + args.run_id).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    row_key = ("run:" + args.run_id).encode("utf-8")
    try:
        if spark.sparkContext.master != "yarn":
            raise RuntimeError("This final-project job must run through spark-submit --master yarn")
        managed_name = args.database + "." + args.table
        matching_tables = [table for table in spark.catalog.listTables(args.database)
                           if table.name == args.table and not table.isTemporary]
        if len(matching_tables) != 1 or matching_tables[0].tableType.upper() != "MANAGED":
            raise RuntimeError("Input must be a Hive MANAGED table, not a view or external table")
        output_path = spark._jvm.org.apache.hadoop.fs.Path(args.output_dir)
        output_fs = output_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if output_fs.exists(output_path):
            raise RuntimeError("Output directory already exists; use a new run ID and directory")
        check_hbase(args, row_key)

        data = spark.table(managed_name).cache()
        row_count, exact_duplicate_rows = validate_input(data)
        split_data = assign_splits(data).cache()
        recipe_count = split_data.select("recipe_id").distinct().count()
        if recipe_count != data.select(*INGREDIENTS).distinct().count():
            raise RuntimeError("Canonical recipe hashing merged distinct ingredient combinations")
        if split_data.groupBy("recipe_id").agg(F.countDistinct("split").alias("n")) \
                .filter(F.col("n") != 1).limit(1).count():
            raise RuntimeError("A recipe appeared in more than one split")
        split_counts = {row["split"]: {"rows": row["rows"], "recipes": row["recipes"]}
                        for row in split_data.groupBy("split").agg(
                            F.count("*").alias("rows"), F.countDistinct("recipe_id").alias("recipes")
                        ).collect()}
        if set(split_counts) != {"train", "validation", "test"}:
            raise RuntimeError("All three recipe-grouped splits must be nonempty")
        if sum(value["rows"] for value in split_counts.values()) != row_count:
            raise RuntimeError("Split row counts do not reconcile to the input")
        log_event("input_audit", {"hive_table": managed_name, "rows": row_count,
                                 "exact_duplicate_rows_preserved": exact_duplicate_rows,
                                 "recipes": recipe_count, "splits": split_counts,
                                 "invalid_rows": 0, "cross_split_recipe_overlap": 0})

        train = split_data.filter(F.col("split") == "train")
        validation = split_data.filter(F.col("split") == "validation")
        candidates = []
        # I choose depth using validation RMSE and save the test set for the final check.
        for depth in (5, 8):
            fitted = make_model(depth).fit(train)
            candidates.append({"max_depth": depth, "num_trees": 32,
                               "validation_rmse_mpa": validation_rmse(fitted.transform(validation))})
        selected = min(candidates, key=lambda item: (item["validation_rmse_mpa"], item["max_depth"]))
        log_event("validation_selection", {"candidates": candidates, "selected": selected})
        development = split_data.filter(F.col("split") != "test")
        final_model = make_model(selected["max_depth"]).fit(development)
        baseline_mean = float(development.agg(F.avg(LABEL).alias("mean")).first()["mean"])
        test = split_data.filter(F.col("split") == "test")
        predictions = final_model.transform(test).cache()
        scores = test_metrics(predictions, baseline_mean)
        metrics = {
            "run_id": args.run_id, "application_id": spark.sparkContext.applicationId,
            "created_utc": datetime.now(timezone.utc).isoformat(), "spark_version": spark.version,
            "hive_table": managed_name, "dataset_sha256": args.dataset_sha256,
            "input_rows": row_count, "exact_duplicate_rows_preserved": exact_duplicate_rows,
            "recipe_count": recipe_count, "split_counts": split_counts, "seed": SEED,
            "split_method": "SHA256 of seven ingredients formatted to 3 decimals; first 8 hex digits modulo 100; 0-69 train,70-84 validation,85-99 test",
            "algorithm": "Spark MLlib RandomForestRegressor", "features": FEATURES,
            "validation_candidates": candidates, "selected_parameters": selected,
            "final_fit_rows": split_counts["train"]["rows"] + split_counts["validation"]["rows"],
            "baseline": "Mean strength of train plus validation; no test fitting",
            "baseline_mean_mpa": baseline_mean, "test_metrics": scores,
            "feature_importances": dict(zip(FEATURES, [float(x) for x in final_model.stages[-1].featureImportances])),
            "hbase_table": args.hbase_table, "hbase_row_key": row_key.decode("utf-8"),
            "output_dir": args.output_dir,
        }
        log_event("final_metrics", metrics)
        log_event("feature_importances", metrics["feature_importances"])
        # I print five sorted examples so the predictions are easy to inspect.
        sample_predictions = predictions.orderBy("recipe_id", "age_days", LABEL) \
            .select("recipe_id", "age_days", LABEL, "prediction") \
            .withColumn("baseline_prediction", F.lit(baseline_mean)).limit(5).collect()
        log_event("test_prediction_examples", [row.asDict() for row in sample_predictions])

        # I save each run in a new folder so earlier results stay unchanged.
        if not output_fs.mkdirs(output_path):
            raise RuntimeError("Could not create the new HDFS output directory")
        final_model.write().save(args.output_dir + "/model")
        predictions.select(*(COLUMNS + ["recipe_id", "split", "prediction"])) \
            .withColumn("baseline_prediction", F.lit(baseline_mean)) \
            .write.mode("errorifexists").json(args.output_dir + "/test_predictions")
        metrics_json = json.dumps(metrics, sort_keys=True, allow_nan=False)
        spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(args.output_dir + "/metrics")
        cells = {
            b"run:run_id": args.run_id.encode("utf-8"),
            b"run:application_id": metrics["application_id"].encode("utf-8"),
            b"run:dataset_sha256": args.dataset_sha256.encode("ascii"),
            b"run:input_rows": str(row_count).encode("ascii"),
            b"run:split_counts": json.dumps(split_counts, sort_keys=True).encode("utf-8"),
            b"run:algorithm": metrics["algorithm"].encode("utf-8"),
            b"run:output_dir": args.output_dir.encode("utf-8"),
            b"metrics:full_json": metrics_json.encode("utf-8"),
        }
        for model_name, score in scores.items():
            for metric_name, value in score.items():
                cells[("metrics:" + model_name + "_" + metric_name).encode("ascii")] = repr(value).encode("ascii")
        host, port, table_name = args.hbase_host, args.hbase_port, args.hbase_table
        spark.sparkContext.parallelize([(row_key, cells)], 1).foreachPartition(
            lambda rows: write_hbase_partition(rows, host, port, table_name))
        connection = hbase_connection(host, port)
        try:
            if connection.table(table_name).row(row_key) != cells:
                raise RuntimeError("Driver read-back failed to reconcile the HBase metrics row")
        finally:
            connection.close()
        receipt = {"run_id": args.run_id, "application_id": metrics["application_id"],
                   "hbase_row_key": row_key.decode("utf-8"), "hbase_cells_verified": len(cells),
                   "status": "HDFS artifacts saved and HBase metrics read-back matched"}
        spark.sparkContext.parallelize([json.dumps(receipt, sort_keys=True)], 1) \
            .saveAsTextFile(args.output_dir + "/verification")
        log_event("SUCCESS", receipt)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
