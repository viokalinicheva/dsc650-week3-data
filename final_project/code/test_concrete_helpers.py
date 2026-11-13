"""Local standard-library tests only; these do not claim Spark/YARN execution."""

import ast
import argparse
import contextlib
import io
import math
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlparse


# I test the non-Spark helpers locally without needing the cluster.
source_path = Path(__file__).with_name("concrete_strength_ml.py")
source_tree = ast.parse(source_path.read_text(encoding="utf-8"))
helper_names = {"parse_args", "scores_from_aggregate", "write_hbase_partition"}
helper_tree = ast.Module(body=[node for node in source_tree.body
                             if isinstance(node, ast.FunctionDef) and node.name in helper_names],
                         type_ignores=[])
namespace = dict(argparse=argparse, math=math, re=re, urlparse=urlparse)
exec(compile(ast.fix_missing_locations(helper_tree), str(source_path), "exec"), namespace)


class ConcreteHelperTests(unittest.TestCase):
    def test_metric_formulas(self):
        scores = namespace["scores_from_aggregate"]({"n": 3, "sum_y": 6, "sum_y2": 14,
                    "model_sse": 0.5, "model_sae": 1.0, "baseline_sse": 2, "baseline_sae": 2})
        self.assertAlmostEqual(scores["model"]["rmse_mpa"], math.sqrt(0.5 / 3))
        self.assertAlmostEqual(scores["model"]["mae_mpa"], 1 / 3)
        self.assertAlmostEqual(scores["model"]["r2"], 0.75)
        self.assertAlmostEqual(scores["baseline"]["r2"], 0)

    def test_constant_target_rejected(self):
        with self.assertRaises(RuntimeError):
            namespace["scores_from_aggregate"]({"n": 2, "sum_y": 4, "sum_y2": 8})

    def test_empty_and_nonfinite_aggregates_rejected(self):
        with self.assertRaises(RuntimeError):
            namespace["scores_from_aggregate"]({"n": 0})
        valid = {"n": 3, "sum_y": 6, "sum_y2": 14, "model_sse": 0.5,
                 "model_sae": 1, "baseline_sse": 2, "baseline_sae": 2}
        for bad in (float("nan"), float("inf"), float("-inf")):
            for name in ("sum_y", "sum_y2", "model_sse", "baseline_sae"):
                with self.subTest(value=bad, field=name), self.assertRaises(RuntimeError):
                    namespace["scores_from_aggregate"](dict(valid, **{name: bad}))

    def test_argument_contract(self):
        valid = ["job", "--run-id", "proof_01", "--dataset-sha256", "a" * 64,
                 "--output-dir", "hdfs://master:9000/user/violetta/final/proof_01"]
        with patch.object(sys, "argv", valid):
            self.assertEqual(namespace["parse_args"]().table, "final_concrete_strength")
        for bad in ("/", "file:///tmp/run", "hdfs://master:9000/", "hdfs://master:9000/x/../y"):
            with patch.object(sys, "argv", valid[:-1] + [bad]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    namespace["parse_args"]()

    def test_hbase_write_is_byte_safe_and_retry_idempotent(self):
        cells, stored, calls = {b"metrics:rmse": b"2.5", b"run:run_id": b"proof_01"}, {}, []
        class FakeTable:
            def row(self, key):
                return stored.get(key, {})
            def put(self, key, value, wal=True):
                calls.append((key, value, wal))
                stored[key] = value
        class FakeConnection:
            def table(self, name):
                return FakeTable()
            def close(self):
                pass
        namespace["hbase_connection"] = lambda host, port: FakeConnection()
        writer = namespace["write_hbase_partition"]
        writer(iter([(b"run:proof_01", cells)]), "master", 9090, "metrics")
        writer(iter([(b"run:proof_01", cells)]), "master", 9090, "metrics")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0][2])
        with self.assertRaises(RuntimeError):
            writer(iter([(b"run:proof_01", {b"metrics:rmse": b"9"})]), "master", 9090, "metrics")


if __name__ == "__main__":
    unittest.main(verbosity=2)
