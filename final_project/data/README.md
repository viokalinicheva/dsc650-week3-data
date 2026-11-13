# Concrete compressive strength dataset

Source: I-Cheng Yeh's Concrete Compressive Strength dataset, hosted by the UCI Machine Learning Repository.

Citation: Yeh, I. (1998). *Concrete Compressive Strength* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5PK67

Official page: https://archive.ics.uci.edu/dataset/165/concrete-compressive-strength

Official source CSV: https://archive.ics.uci.edu/static/public/165/data.csv

License: Creative Commons Attribution 4.0 International (CC BY 4.0), https://creativecommons.org/licenses/by/4.0/

Source retrieved: 2026-11-13. Preparation verified at 2026-11-13T22:47:18+00:00.

## Files and explicitly disclosed changes

- `raw/concrete_uci_original.csv`: original source bytes, including the original header and CRLF record terminators; unmodified.
- `concrete_strength.csv`: pipeline-ready, headerless CSV; only the header was removed and record terminators changed to LF. All 1,030 data rows, original row order, and 9,270 string cells are identical to the source. All 25 extra exact-duplicate rows are preserved.
- `data_dictionary.csv`: normalized metadata names, source headers, field order, units, roles, and Hive types. No identifiers are used as model predictors.
- `dataset_profile.json`: actual numeric profiles, duplicate audit, age-band SQL reconciliation, and deterministic recipe-group partition counts.
- `recipe_split_oracle.csv`: independently computed split membership for all 427 ingredient recipes; not part of the ingested dataset.
- `provenance.json`: source, checksums, transformations, assertions, and limitations.

Original SHA-256: `8d4b15b6fc68cd932d745cbd663d5ceae66dd54422e99c1e4865f2936ab7e2af`

Pipeline-ready SHA-256: `5a128e9b3691404f10718d636a0d55db242e7d0dac572d538d127e2cd39459b9`

## Analytical contract

Question: How accurately can ingredient composition and curing age predict compressive strength for previously unseen ingredient recipes?

Each row is a laboratory mix-and-age observation. Seven ingredient amounts are measured in kg/m^3; age is in days; the target is measured in MPa. The primary-source variable documentation supplies these units. All material quantities are represented as DOUBLE because the actual data include fractional values, including slag. Age is integer-valued.

The raw data have 1,030 observations, no blank or nonfinite numeric cells, 992 distinct predictor vectors, and 427 distinct seven-ingredient recipes. Nine predictor vectors have differing observed strengths; these observations are retained rather than silently averaged.

## Leakage-resistant split oracle

Format each of the seven ingredient values to exactly three decimal places in CSV column order, join with `|`, SHA-256 the UTF-8 string, and calculate `int(hash[:8], 16) % 100`. Buckets 0–69 are training, 70–84 validation, and 85–99 test. Age and target are excluded from recipe identity. Every repeated recipe stays in one partition. The nominal allocation is 70/15/15 by hash bucket, not a guarantee of exact row proportions.

Verified realized allocation: train: 754 rows / 317 recipes; validation: 151 rows / 58 recipes; test: 125 rows / 52 recipes. Recipe overlap between every pair of partitions is zero.

## Scope and limitations

This small historical benchmark supports an instructional pipeline and predictive evaluation. It does not establish causal effects, laboratory-batch independence, structural safety, production suitability, or a need for distributed computation at this data volume. Model metrics must come from the actual Spark/YARN execution, not from this preparation script.

This script does not publish a GitHub repository and does not load HDFS. NiFi must perform the first HDFS ingestion. The final report must include the verified direct GitHub URL after publication.
