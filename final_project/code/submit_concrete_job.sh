#!/usr/bin/env bash
set -euo pipefail

# I use a new run ID so a rerun cannot replace an earlier result.
project_run_id="${1:?Pass a unique run ID, such as concrete_20261113_01}"
[[ "$project_run_id" =~ ^[A-Za-z0-9_-]{1,80}$ ]] || exit 2
project_output="hdfs://master:9000/user/violettakalinicheva/dsc650_final/runs/${project_run_id}"

# I keep the driver in the master container and let YARN run the executors.
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "Violetta_Concrete_Strength_${project_run_id}" \
  --driver-memory 1g \
  --executor-memory 1g \
  --num-executors 2 \
  --executor-cores 1 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=2 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  concrete_strength_ml.py \
  --run-id "$project_run_id" \
  --dataset-sha256 5a128e9b3691404f10718d636a0d55db242e7d0dac572d538d127e2cd39459b9 \
  --output-dir "$project_output"
