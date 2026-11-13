#!/usr/bin/env python3
"""Read-only independent audit of the completed concrete-strength YARN run.

Optional --fetch reads existing HDFS, HBase, and YARN outputs over verified SSH.
It never submits jobs, trains a model, or changes remote data.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
import subprocess


ROOT = Path(__file__).resolve().parents[1]
VERIFY = ROOT / "verification"
DATA = ROOT / "data" if (ROOT / "data").is_dir() else ROOT / "repository" / "final_project" / "data"
RUN_ID = "concrete_20261113_01"
APP_ID = "application_1794609888896_0008"
HDFS_RUN = "/user/violettakalinicheva/dsc650_final/runs/" + RUN_ID
MASTER = "hadoop-hive-spark-hbase-master-1"
SSH = None
COLUMNS = ["cement_kg_m3", "slag_kg_m3", "fly_ash_kg_m3", "water_kg_m3",
           "superplasticizer_kg_m3", "coarse_aggregate_kg_m3", "fine_aggregate_kg_m3",
           "age_days", "strength_mpa"]


def remote(arguments, stem, check=True):
    result = subprocess.run(SSH + [" ".join(shlex.quote(value) for value in arguments)], capture_output=True,
                            text=True, timeout=60)
    (VERIFY / (stem + ".stderr.log")).write_text(result.stderr, encoding="utf-8")
    if check and result.returncode:
        raise RuntimeError(stem + " failed: " + result.stderr[-1500:])
    return result


def fetch():
    for directory, filename in [("metrics", "actual_metrics.json"),
                                ("verification", "actual_verification.json"),
                                ("test_predictions", "actual_test_predictions.jsonl")]:
        result = remote(["docker", "exec", MASTER, "hdfs", "dfs", "-cat",
                         HDFS_RUN + "/" + directory + "/part-*"], "actual_" + directory)
        (VERIFY / filename).write_text(result.stdout, encoding="utf-8")
    hbase_code = (
        "import happybase,json; "
        "c=happybase.Connection('master',port=9090,timeout=15000,compat='0.98',"
        "transport='buffered',protocol='binary'); "
        "r=c.table('concrete_model_metrics_v1').row(" + repr(("run:" + RUN_ID).encode()) + "); "
        "print(json.dumps({k.decode('utf-8'):v.decode('utf-8') for k,v in r.items()},sort_keys=True)); "
        "c.close()"
    )
    result = remote(["docker", "exec", MASTER, "python3", "-c", hbase_code], "actual_hbase")
    (VERIFY / "actual_hbase_cells.json").write_text(result.stdout, encoding="utf-8")
    yarn_code = ("import urllib.request; print(urllib.request.urlopen("
                 "'http://master:8088/ws/v1/cluster/apps/" + APP_ID +
                 "',timeout=15).read().decode('utf-8'))")
    result = remote(["docker", "exec", MASTER, "python3", "-c", yarn_code], "actual_yarn")
    (VERIFY / "actual_yarn_application.json").write_text(result.stdout, encoding="utf-8")
    result = remote(["docker", "exec", MASTER, "yarn", "logs", "-applicationId", APP_ID],
                    "actual_yarn_logs", check=False)
    (VERIFY / "actual_yarn_logs.log").write_text(result.stdout, encoding="utf-8")
    (VERIFY / "actual_yarn_logs_status.json").write_text(
        json.dumps({"returncode": result.returncode}), encoding="utf-8")


def recipe_id(values):
    return hashlib.sha256("|".join("%.3f" % value for value in values[:7]).encode()).hexdigest()


def calculate_scores(rows, predicted):
    actual = [row["strength_mpa"] for row in rows]
    errors = [truth - row[predicted] for truth, row in zip(actual, rows)]
    mean = math.fsum(actual) / len(actual)
    sse = math.fsum(error * error for error in errors)
    return {"rmse_mpa": math.sqrt(sse / len(rows)),
            "mae_mpa": math.fsum(abs(error) for error in errors) / len(rows),
            "r2": 1 - sse / math.fsum((truth - mean) ** 2 for truth in actual)}


def audit():
    metrics = json.loads((VERIFY / "actual_metrics.json").read_text())
    receipt = json.loads((VERIFY / "actual_verification.json").read_text())
    cells = json.loads((VERIFY / "actual_hbase_cells.json").read_text())
    yarn = json.loads((VERIFY / "actual_yarn_application.json").read_text())["app"]
    predictions = [json.loads(line) for line in (VERIFY / "actual_test_predictions.jsonl")
                   .read_text().splitlines() if line.strip()]
    raw = (DATA / "concrete_strength.csv").read_bytes()
    source = [[float(value) for value in row] for row in csv.reader(raw.decode().splitlines())]
    assert hashlib.sha256(raw).hexdigest() == metrics["dataset_sha256"]
    assert len(source) == metrics["input_rows"] == 1030
    split_names = lambda digest: "train" if int(digest[:8], 16) % 100 < 70 else (
        "validation" if int(digest[:8], 16) % 100 < 85 else "test")
    recipe_rows = Counter(recipe_id(row) for row in source)
    source_splits = {split: [row for row in source if split_names(recipe_id(row)) == split]
                     for split in ("train", "validation", "test")}
    oracle = {row["recipe_sha256"]: row for row in
              csv.DictReader((DATA / "recipe_split_oracle.csv").open())}
    assert set(recipe_rows) == set(oracle) and len(recipe_rows) == 427
    for digest, count in recipe_rows.items():
        assert count == int(oracle[digest]["row_count"])
        assert split_names(digest) == oracle[digest]["partition"]
        assert hashlib.sha256(oracle[digest]["recipe_text"].encode()).hexdigest() == digest
    split_counts = {split: {"rows": len(rows), "recipes": len({recipe_id(row) for row in rows})}
                    for split, rows in source_splits.items()}
    assert split_counts == metrics["split_counts"]
    assert Counter(tuple(row[column] for column in COLUMNS) for row in predictions) == \
        Counter(map(tuple, source_splits["test"]))
    assert len(predictions) == split_counts["test"]["rows"] == 125
    for row in predictions:
        assert row["split"] == "test" and row["recipe_id"] == recipe_id([row[c] for c in COLUMNS])
        assert all(math.isfinite(row[key]) for key in COLUMNS + ["prediction", "baseline_prediction"])
    development = source_splits["train"] + source_splits["validation"]
    baseline_mean = math.fsum(row[-1] for row in development) / len(development)
    assert len(development) == metrics["final_fit_rows"] == 905
    assert math.isclose(baseline_mean, metrics["baseline_mean_mpa"], abs_tol=1e-10)
    assert all(math.isclose(row["baseline_prediction"], baseline_mean, abs_tol=1e-10) for row in predictions)
    recomputed = {"model": calculate_scores(predictions, "prediction"),
                  "baseline": calculate_scores(predictions, "baseline_prediction")}
    for name, scores in recomputed.items():
        for metric, value in scores.items():
            assert math.isclose(value, metrics["test_metrics"][name][metric], rel_tol=1e-10, abs_tol=1e-10)
            assert float(cells["metrics:" + name + "_" + metric]) == metrics["test_metrics"][name][metric]
    assert json.loads(cells["metrics:full_json"]) == metrics
    assert json.loads(cells["run:split_counts"]) == split_counts
    assert cells["run:run_id"] == metrics["run_id"] == receipt["run_id"] == RUN_ID
    assert cells["run:application_id"] == metrics["application_id"] == receipt["application_id"] == APP_ID
    assert cells["run:dataset_sha256"] == metrics["dataset_sha256"]
    assert int(cells["run:input_rows"]) == 1030
    assert cells["run:algorithm"] == metrics["algorithm"]
    assert cells["run:output_dir"] == metrics["output_dir"]
    assert receipt["hbase_row_key"] == metrics["hbase_row_key"] == "run:" + RUN_ID
    assert len(cells) == receipt["hbase_cells_verified"] == 14
    assert yarn["id"] == APP_ID and yarn["state"] == "FINISHED" and yarn["finalStatus"] == "SUCCEEDED"
    assert metrics["features"] == COLUMNS[:8] and "strength_mpa" not in metrics["features"]
    assert metrics["spark_version"] == "3.0.0"
    assert metrics["selected_parameters"] == min(metrics["validation_candidates"],
        key=lambda value: (value["validation_rmse_mpa"], value["max_depth"]))
    assert set(metrics["feature_importances"]) == set(COLUMNS[:8])
    assert all(math.isfinite(value) and value >= 0 for value in metrics["feature_importances"].values())
    assert math.isclose(math.fsum(metrics["feature_importances"].values()), 1, abs_tol=1e-10)
    driver_log = (VERIFY / ("05-spark-yarn-" + RUN_ID + ".log")).read_text()
    evidence = {}
    for line in driver_log.splitlines():
        if line.startswith("PROJECT_EVIDENCE "):
            _, event, payload = line.split(" ", 2)
            evidence[event] = json.loads(payload)
    assert evidence["final_metrics"] == metrics and evidence["SUCCESS"] == receipt
    yarn_logs = (VERIFY / "actual_yarn_logs.log").read_text()
    log_status = json.loads((VERIFY / "actual_yarn_logs_status.json").read_text())
    failure_pattern = r"(?im)^.*(?:\bERROR\b|TaskSetManager.*Lost task|Task .* failed|Traceback \(most recent|Caused by:).*$"
    failure_lines = re.findall(failure_pattern, yarn_logs)
    containers = sorted(set(re.findall(r"Container: (container_\S+) on (\S+)", yarn_logs)))
    finished_tasks = re.findall(r"Finished task (\d+\.\d+) in stage (\d+\.\d+) \(TID (\d+)\)", yarn_logs)
    audit_result = {
        "verified_utc": datetime.now(timezone.utc).isoformat(), "run_id": RUN_ID,
        "application_id": APP_ID, "status": "PASS", "dataset_sha256": metrics["dataset_sha256"],
        "source_rows": len(source), "exact_duplicate_extra_rows_preserved": len(source)-len(set(map(tuple, source))),
        "recipe_count": len(recipe_rows), "split_counts": split_counts,
        "test_prediction_rows": len(predictions), "test_raw_multiset_matches": True,
        "cross_split_recipe_overlap": 0, "baseline_fit_rows": len(development),
        "baseline_mean_mpa": baseline_mean, "independently_recomputed_test_metrics": recomputed,
        "hbase_cells_matched": len(cells), "hbase_full_metrics_match": True,
        "yarn_state": yarn["state"], "yarn_final_status": yarn["finalStatus"],
        "aggregated_log_fetch_returncode": log_status["returncode"],
        "aggregated_log_bytes": len(yarn_logs.encode()), "executor_failure_matching_lines": failure_lines[:30],
        "yarn_log_containers": [{"container_id": container, "node": node} for container, node in containers],
        "executor_finished_task_records": len(finished_tasks),
        "executor_finished_tasks_with_nonzero_attempt_number": sum(float(task.split(".")[1]) != 0 for task, stage, tid in finished_tasks),
        "limitations": ["This audit verifies the saved predictions and metrics; it does not independently retrain the model.",
                        "Recipe grouping prevents exact repeated-mixture leakage, not all similarity between related mixtures."]
    }
    if log_status["returncode"]:
        audit_result["limitations"].append("Aggregated YARN logs were unavailable; executor log review requires node-local logs.")
    (VERIFY / "actual_independent_audit.json").write_text(json.dumps(audit_result, indent=2), encoding="utf-8")
    print(json.dumps(audit_result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--data-dir", type=Path, default=DATA)
    parser.add_argument("--verification-dir", type=Path, default=VERIFY)
    parser.add_argument("--run-id", default=RUN_ID)
    parser.add_argument("--application-id", default=APP_ID)
    parser.add_argument("--ssh-host", help="Verified SSH destination, such as user@host; required with --fetch")
    parser.add_argument("--ssh-key", type=Path, default=Path.home() / ".ssh" / "id_ed25519")
    parser.add_argument("--master-container", default=MASTER)
    arguments = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", arguments.run_id):
        parser.error("Run ID must contain 1-80 letters, digits, underscores, or hyphens")
    if not re.fullmatch(r"application_[0-9]+_[0-9]+", arguments.application_id):
        parser.error("Application ID must follow application_<timestamp>_<number>")
    if arguments.fetch and not arguments.ssh_host:
        parser.error("--ssh-host is required with --fetch; verify the VM endpoint before connecting")
    if arguments.ssh_host and not re.fullmatch(r"(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9_.-]+", arguments.ssh_host):
        parser.error("SSH destination must be a hostname or user@hostname")
    DATA, VERIFY = arguments.data_dir.expanduser(), arguments.verification_dir.expanduser()
    RUN_ID, APP_ID, MASTER = arguments.run_id, arguments.application_id, arguments.master_container
    HDFS_RUN = "/user/violettakalinicheva/dsc650_final/runs/" + RUN_ID
    if arguments.fetch:
        VERIFY.mkdir(parents=True, exist_ok=True)
        SSH = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-i",
               str(arguments.ssh_key.expanduser()), arguments.ssh_host]
        fetch()
    audit()
