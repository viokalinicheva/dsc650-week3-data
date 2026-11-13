#!/usr/bin/env python3
"""Prepare an auditable, headerless Concrete Strength CSV for the DSC 650 lab.

This local preparation does not load HDFS. NiFi must perform the first HDFS
ingestion. The original source bytes and every original CSV cell are preserved.
Only the header and record terminators differ in the pipeline-ready CSV.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import io
import json
from pathlib import Path
import shutil


EXPECTED_SOURCE_SHA256 = (
    "8d4b15b6fc68cd932d745cbd663d5ceae66dd54422e99c1e4865f2936ab7e2af"
)
SOURCE_URL = "https://archive.ics.uci.edu/static/public/165/data.csv"
LANDING_URL = "https://archive.ics.uci.edu/dataset/165/concrete-compressive-strength"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
DATASET_DOI = "https://doi.org/10.24432/C5PK67"
SOURCE_HEADERS = [
    "Cement", "Blast Furnace Slag", "Fly Ash", "Water", "Superplasticizer",
    "Coarse Aggregate", "Fine Aggregate", "Age", "Concrete compressive strength",
]
COLUMN_NAMES = [
    "cement_kg_m3", "slag_kg_m3", "fly_ash_kg_m3", "water_kg_m3",
    "superplasticizer_kg_m3", "coarse_aggregate_kg_m3", "fine_aggregate_kg_m3",
    "age_days", "strength_mpa",
]
AGE_BANDS = ["01_age_le_7", "02_age_8_to_28", "03_age_29_to_90", "04_age_gt_90"]
SPLITS = ["train", "validation", "test"]


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def decimal_text(value: Decimal) -> str:
    """Return a human-readable decimal without altering source cell strings."""
    return format(value, "f")


def rounded_mean(values: list[Decimal]) -> str:
    with localcontext() as context:
        context.prec = 40
        return decimal_text((sum(values) / len(values)).quantize(Decimal("0.000001")))


def age_band(age: Decimal) -> str:
    if age <= 7:
        return AGE_BANDS[0]
    if age <= 28:
        return AGE_BANDS[1]
    if age <= 90:
        return AGE_BANDS[2]
    return AGE_BANDS[3]


def recipe_identity(row: tuple[Decimal, ...]) -> tuple[str, str, int, str]:
    """Match the ML contract: seven fixed-3-decimal ingredients, pipe-delimited."""
    recipe_text = "|".join(format(value, ".3f") for value in row[:7])
    recipe_sha256 = sha256_bytes(recipe_text.encode("utf-8"))
    bucket = int(recipe_sha256[:8], 16) % 100
    partition = "train" if bucket < 70 else "validation" if bucket < 85 else "test"
    return recipe_text, recipe_sha256, bucket, partition


def summarize_rows(rows: list[tuple[Decimal, ...]]) -> dict:
    if not rows:
        return {"row_count": 0}
    strength = [row[8] for row in rows]
    return {
        "row_count": len(rows),
        "age_min_days": int(min(row[7] for row in rows)),
        "age_max_days": int(max(row[7] for row in rows)),
        "strength_sum_mpa": decimal_text(sum(strength)),
        "strength_mean_mpa_6dp": rounded_mean(strength),
        "strength_min_mpa": decimal_text(min(strength)),
        "strength_max_mpa": decimal_text(max(strength)),
        "cement_mean_kg_m3_6dp": rounded_mean([row[0] for row in rows]),
        "water_mean_kg_m3_6dp": rounded_mean([row[3] for row in rows]),
    }


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target, lineterminator="\n")
        writer.writerows(rows)


def prepare(source_path: Path, repository_path: Path) -> dict:
    # I am validating the immutable source so I cannot accidentally ingest example data.
    original_bytes = source_path.read_bytes()
    source_sha256 = sha256_bytes(original_bytes)
    assert source_sha256 == EXPECTED_SOURCE_SHA256, "Unexpected source SHA-256."
    reader = csv.reader(io.StringIO(original_bytes.decode("utf-8-sig")), strict=True)
    source_headers = next(reader)
    assert source_headers == SOURCE_HEADERS, "Unexpected source column order."
    source_rows = list(reader)
    assert len(source_rows) == 1030, "Unexpected source row count."
    assert all(len(row) == 9 for row in source_rows), "Malformed CSV row."
    assert all(cell.strip() for row in source_rows for cell in row), "Blank source cell."
    assert all("\r" not in cell and "\n" not in cell for row in source_rows for cell in row)
    numeric_rows = [tuple(Decimal(cell) for cell in row) for row in source_rows]
    assert all(value.is_finite() for row in numeric_rows for value in row)
    assert all(value >= 0 for row in numeric_rows for value in row[:7])
    assert all(row[7] > 0 and row[7] == row[7].to_integral_value() for row in numeric_rows)
    assert all(row[8] > 0 for row in numeric_rows)
    assert all(value == value.quantize(Decimal("0.001")) for row in numeric_rows for value in row[:7]), (
        "A recipe value exceeds the fixed-three-decimal split contract."
    )

    data_path = repository_path / "data"
    raw_path = data_path / "raw"
    raw_path.mkdir(parents=True, exist_ok=True)
    preserved_path = raw_path / "concrete_uci_original.csv"
    shutil.copy2(source_path, preserved_path)
    canonical_path = data_path / "concrete_strength.csv"
    # I am preserving all original strings and rows so preparation cannot hide data-quality issues.
    write_csv(canonical_path, source_rows)
    canonical_bytes = canonical_path.read_bytes()
    with canonical_path.open(encoding="utf-8", newline="") as stream:
        canonical_rows = list(csv.reader(stream, strict=True))
    assert canonical_rows == source_rows, "Source-cell identity check failed."
    assert preserved_path.read_bytes() == original_bytes, "Raw-byte preservation failed."
    assert b"\r" not in canonical_bytes and canonical_bytes.endswith(b"\n")
    assert canonical_bytes.count(b"\n") == len(source_rows)

    exact_counts = Counter(numeric_rows)
    predictor_counts = Counter(row[:8] for row in numeric_rows)
    target_values = defaultdict(set)
    recipes = {}
    split_rows = {partition: [] for partition in SPLITS}
    for row in numeric_rows:
        target_values[row[:8]].add(row[8])
        recipe_text, recipe_sha256, bucket, partition = recipe_identity(row)
        if recipe_sha256 not in recipes:
            recipes[recipe_sha256] = {
                "recipe_text": recipe_text, "recipe_sha256": recipe_sha256,
                "bucket": bucket, "partition": partition, "row_count": 0,
            }
        assert recipes[recipe_sha256]["recipe_text"] == recipe_text, "Recipe hash collision."
        recipes[recipe_sha256]["row_count"] += 1
        split_rows[partition].append(row)
    # I am checking partitions independently so repeated recipes cannot cross the holdout boundary.
    recipe_sets = {
        partition: {recipe_identity(row)[1] for row in rows}
        for partition, rows in split_rows.items()
    }
    overlaps = {
        f"{left}_{right}": len(recipe_sets[left] & recipe_sets[right])
        for index, left in enumerate(SPLITS) for right in SPLITS[index + 1:]
    }
    assert all(value == 0 for value in overlaps.values())
    assert sum(len(rows) for rows in split_rows.values()) == len(numeric_rows)
    assert sum(len(values) for values in recipe_sets.values()) == len(recipes)

    columns = []
    for index, name in enumerate(COLUMN_NAMES):
        values = [row[index] for row in numeric_rows]
        columns.append({
            "position_1_based": index + 1, "column_name": name,
            "source_header": SOURCE_HEADERS[index], "count": len(values),
            "min": decimal_text(min(values)), "max": decimal_text(max(values)),
            "sum": decimal_text(sum(values)), "mean_6dp": rounded_mean(values),
            "zero_count": sum(value == 0 for value in values),
            "noninteger_count": sum(value != value.to_integral_value() for value in values),
        })
    profile = {
        "dataset": "UCI Concrete Compressive Strength", "source_sha256": source_sha256,
        "row_count": len(numeric_rows), "column_count": len(COLUMN_NAMES),
        "source_cell_count": sum(map(len, source_rows)),
        "canonical_cell_identity_verified": canonical_rows == source_rows,
        "missing_cell_count": 0, "nonfinite_cell_count": 0,
        "exact_duplicate_extra_rows": sum(count - 1 for count in exact_counts.values()),
        "exact_duplicate_extra_rows_preserved": True,
        "distinct_full_numeric_rows": len(exact_counts),
        "distinct_predictor_vectors": len(predictor_counts),
        "predictor_vectors_with_different_targets": sum(len(values) > 1 for values in target_values.values()),
        "distinct_ingredient_recipes": len(recipes),
        "ingredient_recipes_with_multiple_rows": sum(recipe["row_count"] > 1 for recipe in recipes.values()),
        "column_profile": columns,
        "overall_reconciliation": summarize_rows(numeric_rows),
        "age_band_definitions": {
            AGE_BANDS[0]: "age_days <= 7", AGE_BANDS[1]: "7 < age_days <= 28",
            AGE_BANDS[2]: "28 < age_days <= 90", AGE_BANDS[3]: "age_days > 90",
        },
        "age_band_reconciliation": {
            band: summarize_rows([row for row in numeric_rows if age_band(row[7]) == band])
            for band in AGE_BANDS
        },
        "split_contract": {
            "ingredient_columns": COLUMN_NAMES[:7],
            "recipe_text": "|".join(f"{{{name}:.3f}}" for name in COLUMN_NAMES[:7]),
            "hash": "SHA-256 of UTF-8 recipe_text, lowercase hexadecimal",
            "bucket": "int(recipe_sha256[:8], 16) % 100",
            "partitions": {"train": "0..69", "validation": "70..84", "test": "85..99"},
            "expected_allocation": "70/15/15 by hash bucket; realized row proportions may differ",
            "overlapping_recipe_counts": overlaps,
            "partitions_profile": {
                partition: {
                    **summarize_rows(rows), "recipe_count": len(recipe_sets[partition]),
                    "age_bands": {
                        band: summarize_rows([row for row in rows if age_band(row[7]) == band])
                        for band in AGE_BANDS
                    },
                }
                for partition, rows in split_rows.items()
            },
        },
    }
    assert sum(item["row_count"] for item in profile["age_band_reconciliation"].values()) == len(numeric_rows)
    write_json(data_path / "dataset_profile.json", profile)
    write_csv(data_path / "recipe_split_oracle.csv", [
        ["recipe_sha256", "bucket", "partition", "row_count", "recipe_text"],
        *[[recipe["recipe_sha256"], recipe["bucket"], recipe["partition"], recipe["row_count"], recipe["recipe_text"]]
          for recipe in sorted(recipes.values(), key=lambda item: item["recipe_sha256"])],
    ])
    dictionary_rows = [["position", "column_name", "source_header", "hive_type", "role", "units", "definition"]]
    for index, name in enumerate(COLUMN_NAMES):
        units = "kg/m^3" if index < 7 else "days" if index == 7 else "MPa"
        definition = (
            f"Quantity of {SOURCE_HEADERS[index].lower()} per cubic metre of mixture."
            if index < 7 else "Curing age when compressive strength was measured."
            if index == 7 else "Observed laboratory concrete compressive strength; regression target."
        )
        dictionary_rows.append([
            index + 1, name, SOURCE_HEADERS[index], "INT" if index == 7 else "DOUBLE",
            "target" if index == 8 else "predictor", units, definition,
        ])
    write_csv(data_path / "data_dictionary.csv", dictionary_rows)

    prepared_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    provenance = {
        "dataset": "Concrete Compressive Strength", "creator": "I-Cheng Yeh",
        "citation": "Yeh, I. (1998). Concrete Compressive Strength [Dataset]. UCI Machine Learning Repository.",
        "doi": DATASET_DOI, "landing_url": LANDING_URL, "source_csv_url": SOURCE_URL,
        "license": "CC BY 4.0", "license_url": LICENSE_URL,
        "source_retrieval_date": "2026-11-13", "prepared_at_utc": prepared_at,
        "source_file": "data/raw/concrete_uci_original.csv", "source_sha256": source_sha256,
        "source_bytes": len(original_bytes), "pipeline_file": "data/concrete_strength.csv",
        "pipeline_sha256": sha256_bytes(canonical_bytes), "pipeline_bytes": len(canonical_bytes),
        "row_count": len(source_rows), "column_count": len(COLUMN_NAMES),
        "column_order": COLUMN_NAMES,
        "transformations": [
            "Preserved source CSV unchanged under data/raw/.",
            "Removed only the header row from the pipeline-ready CSV.",
            "Normalized record terminators to LF in the pipeline-ready CSV.",
            "Preserved all 1,030 data rows, original order, and all 9,270 string cell values.",
            "Defined normalized column names in metadata; the pipeline-ready CSV is headerless.",
            "Computed a recipe-group split oracle without changing, dropping, or appending dataset rows.",
        ],
        "verification": {
            "source_byte_identity": preserved_path.read_bytes() == original_bytes,
            "all_source_cells_preserved": canonical_rows == source_rows,
            "source_cells_compared": sum(map(len, source_rows)),
            "canonical_lf_record_terminators": b"\r" not in canonical_bytes,
            "canonical_record_count": canonical_bytes.count(b"\n"),
            "all_duplicate_rows_preserved": Counter(tuple(row) for row in canonical_rows) == Counter(tuple(row) for row in source_rows),
        },
        "limitations": [
            "This historical laboratory benchmark does not justify safety certification, field deployment, or causal recipe optimization.",
            "Ingredient-based grouping reduces repeated-recipe leakage but cannot establish independence of undocumented laboratory batches.",
            "The small dataset demonstrates distributed-pipeline correctness, not a necessity for big-data-scale computation.",
        ],
        "git_hub_publication": "Not performed by this preparation script; the final report must use a verified direct GitHub URL.",
        "hdfs_ingestion": "Not performed by this preparation script; NiFi must ingest the pipeline-ready file first.",
    }
    write_json(data_path / "provenance.json", provenance)
    split_summary = "; ".join(
        f"{partition}: {len(split_rows[partition])} rows / {len(recipe_sets[partition])} recipes"
        for partition in SPLITS
    )
    readme = f"""# Concrete compressive strength dataset

Source: I-Cheng Yeh's Concrete Compressive Strength dataset, hosted by the UCI Machine Learning Repository.

Citation: Yeh, I. (1998). *Concrete Compressive Strength* [Dataset]. UCI Machine Learning Repository. {DATASET_DOI}

Official page: {LANDING_URL}

Official source CSV: {SOURCE_URL}

License: Creative Commons Attribution 4.0 International (CC BY 4.0), {LICENSE_URL}

Source retrieved: 2026-11-13. Preparation verified at {prepared_at}.

## Files and explicitly disclosed changes

- `raw/concrete_uci_original.csv`: original source bytes, including the original header and CRLF record terminators; unmodified.
- `concrete_strength.csv`: pipeline-ready, headerless CSV; only the header was removed and record terminators changed to LF. All 1,030 data rows, original row order, and 9,270 string cells are identical to the source. All 25 extra exact-duplicate rows are preserved.
- `data_dictionary.csv`: normalized metadata names, source headers, field order, units, roles, and Hive types. No identifiers are used as model predictors.
- `dataset_profile.json`: actual numeric profiles, duplicate audit, age-band SQL reconciliation, and deterministic recipe-group partition counts.
- `recipe_split_oracle.csv`: independently computed split membership for all 427 ingredient recipes; not part of the ingested dataset.
- `provenance.json`: source, checksums, transformations, assertions, and limitations.

Original SHA-256: `{source_sha256}`

Pipeline-ready SHA-256: `{sha256_bytes(canonical_bytes)}`

## Analytical contract

Question: How accurately can ingredient composition and curing age predict compressive strength for previously unseen ingredient recipes?

Each row is a laboratory mix-and-age observation. Seven ingredient amounts are measured in kg/m^3; age is in days; the target is measured in MPa. The primary-source variable documentation supplies these units. All material quantities are represented as DOUBLE because the actual data include fractional values, including slag. Age is integer-valued.

The raw data have 1,030 observations, no blank or nonfinite numeric cells, 992 distinct predictor vectors, and 427 distinct seven-ingredient recipes. Nine predictor vectors have differing observed strengths; these observations are retained rather than silently averaged.

## Leakage-resistant split oracle

Format each of the seven ingredient values to exactly three decimal places in CSV column order, join with `|`, SHA-256 the UTF-8 string, and calculate `int(hash[:8], 16) % 100`. Buckets 0–69 are training, 70–84 validation, and 85–99 test. Age and target are excluded from recipe identity. Every repeated recipe stays in one partition. The nominal allocation is 70/15/15 by hash bucket, not a guarantee of exact row proportions.

Verified realized allocation: {split_summary}. Recipe overlap between every pair of partitions is zero.

## Scope and limitations

This small historical benchmark supports an instructional pipeline and predictive evaluation. It does not establish causal effects, laboratory-batch independence, structural safety, production suitability, or a need for distributed computation at this data volume. Model metrics must come from the actual Spark/YARN execution, not from this preparation script.

This script does not publish a GitHub repository and does not load HDFS. NiFi must perform the first HDFS ingestion. The final report must include the verified direct GitHub URL after publication.
"""
    (data_path / "README.md").write_text(readme, encoding="utf-8")
    return {
        "source_sha256": source_sha256, "pipeline_sha256": sha256_bytes(canonical_bytes),
        "row_count": len(source_rows), "column_count": len(COLUMN_NAMES),
        "all_9270_source_cells_identical": canonical_rows == source_rows,
        "raw_bytes_identical": preserved_path.read_bytes() == original_bytes,
        "duplicates_preserved": profile["exact_duplicate_extra_rows"],
        "recipe_count": len(recipes), "split_summary": split_summary,
        "age_bands": profile["age_band_reconciliation"],
        "overall_reconciliation": profile["overall_reconciliation"],
        "repository_data_directory": str(data_path.resolve()),
    }


def main() -> None:
    project_path = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=project_path / "source" / "concrete_uci_original.csv")
    parser.add_argument("--repository-dir", type=Path, default=project_path / "repository" / "final_project")
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.repository_dir), indent=2))


if __name__ == "__main__":
    main()
