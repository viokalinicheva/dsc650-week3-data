# Concrete strength prediction with the Hadoop ecosystem

Violetta Kalinicheva · DSC 650 Final Project

## Question

How accurately can ingredient composition and curing age predict concrete compressive strength for an ingredient recipe that was not used to train the model?

I use a new public dataset for this final project. This folder is separate from the earlier penguin and sentence-processing assignments in the same repository. The small data volume is intentional: the project demonstrates a connected pipeline, not a claim that 40 KB of data requires distributed computing.

## Pipeline

1. The required instructor NiFi flow downloads the direct GitHub CSV URL, sets its filename, and writes it to HDFS.
2. After checking the queued content and the HDFS file, I stop NiFi to release memory.
3. Hive loads the HDFS file into a new managed table. Counts, numeric totals, and an age-group aggregation are checked against the source profile.
4. NumPy and HappyBase are verified on master and both workers. HBase Thrift runs on master.
5. An HBase table is created with `metrics` and `run` column families. Its empty scan must be captured before Spark writes anything.
6. Spark MLlib runs on YARN, reads only the managed Hive table, selects a random-forest depth using validation data, and evaluates the selected model against a mean baseline on untouched test recipes.
7. The Spark job saves its model, predictions, and metrics in a unique HDFS run directory and writes one complete metrics row to HBase. The stored values are read back and compared with the job output.

## Dataset and preparation

Source: Yeh, I. (1998). *Concrete Compressive Strength* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5PK67

License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Dataset and changes are attributed in `data/README.md` and `data/provenance.json`.

The source has 1,030 records and nine columns. I preserve all original data cells and all duplicate rows. The pipeline CSV removes the header and changes record endings to LF; it makes no numerical changes. The original source file is retained unchanged.

Verified direct NiFi download URL:

`https://raw.githubusercontent.com/viokalinicheva/dsc650-week3-data/main/final_project/data/concrete_strength.csv`

Expected CSV SHA-256: `5a128e9b3691404f10718d636a0d55db242e7d0dac572d538d127e2cd39459b9`

The seven ingredients are in kg/m³, age is in days, and the outcome is strength in MPa. Each repeated ingredient recipe stays in one split, even when its age differs. The independently calculated split contains 754 training records, 151 validation records, and 125 test records, with no recipe overlap.

## Files

- `data/`: source, pipeline-ready CSV, dictionary, provenance, profile, and independent split oracle.
- `nifi/Final_Project_Concrete_Strength.json`: parameterized copy of the required instructor template; the three processors and four connections are preserved.
- `code/prepare_dataset.py`: reproducible local preparation and validation.
- `code/hive_concrete_strength.hql`: managed-table creation, load, and reconciliation queries.
- `code/create_hbase_metrics.hbase`: table creation and the pre-job empty scan.
- `code/install_dependencies.sh`: pinned installations on master, worker1, and worker2.
- `code/start_hbase_thrift.sh`: starts Thrift only if its port is unused and checks readiness.
- `code/concrete_strength_ml.py`: Spark MLlib workflow and HBase persistence.
- `code/submit_concrete_job.sh`: bounded YARN client-mode submission.
- `code/test_concrete_helpers.py`: local tests for evaluation and HBase write logic; these do not substitute for an actual YARN run.
- `code/verify_hbase_metrics.hbase`: populated metrics scan and row count after Spark.
- `code/verify_concrete_outputs.py`: independent reconciliation of saved predictions, source rows, HBase cells, and YARN status.

## Verified execution on November 13 2026

The real YARN run `concrete_20261113_01` completed as application `application_1794609888896_0008`, with state **FINISHED** and final status **SUCCEEDED**. Both workers executed tasks. The job read all 1,030 records from the managed Hive table, selected depth 8 from validation RMSE, and refit 32 trees on the 905 development records before evaluating the 125 held-out records.

| Held-out metric | Random forest | Development-mean baseline |
| --- | ---: | ---: |
| RMSE in MPa | 6.2703 | 15.4498 |
| MAE in MPa | 4.8020 | 12.8675 |
| R squared | 0.8349 | -0.0024 |

I checked every saved test record against the original held-out data and recalculated the metrics independently. The job's HBase read-back and the independent audit both matched all 14 stored cells. The populated HBase scan contains one run row. The empty scan was captured before the job. Screenshots and the complete execution logs are included in the submitted report package.

These are results for one recorded recipe-grouped split, not estimates of performance on every future concrete mixture. The validation candidates had RMSE 10.0944 MPa at depth 5 and 8.4541 MPa at depth 8. The test set was not used to choose the depth.

## Running the project

The paths in the course setup scripts match my existing VM. Run them from that VM, after starting its existing Hadoop Compose stack. Install libraries on each node with `bash code/install_dependencies.sh master`, then repeat for `worker1` and `worker2`. Start Thrift with `bash code/start_hbase_thrift.sh`.

Import `nifi/Final_Project_Concrete_Strength.json` into NiFi as a new process group. Check the four parameters against the VM owner and desired new HDFS directory. Run the downloader **once**, inspect its queued CSV, and run the two downstream processors. Verify the HDFS file size, row count, and checksum, then stop NiFi. Run the HQL file in the master container and create the HBase table using the supplied HBase shell file. Do not rerun table creation against an existing table or load the same data twice.

Copy the ML and submission scripts into the same directory inside master, change into that directory, and run `bash submit_concrete_job.sh NEW_UNIQUE_RUN_ID`. The wrapper uses `spark-submit --master yarn --deploy-mode client`. It expects the local Python filename in its current directory. Scan HBase after completion and retain the complete log and unique HDFS run output. The full report includes the exact commands used for the recorded run.

## Execution safeguards

Do not run the Hive load before NiFi has successfully ingested the data. Do not replace the ingestion step with `hdfs dfs -put`. The Hive load moves the source file into managed storage, so capture the raw HDFS evidence first. Use a unique run ID and HDFS output directory; the job refuses to overwrite existing results. Keep credentials outside source control and screenshots.

The inherited course environment uses Spark 3.0.0 and Python 3.7.10. The verified dependency set is NumPy 1.16.4, HappyBase 1.2.0, thriftpy2 0.4.20, Cython 0.29.10, six 1.16.0, and ply 3.11. These older versions match this isolated course environment; they are not a production deployment recommendation. Local helper tests and syntax checks are separate from the recorded distributed execution evidence.

## Interpretation limits

This historical laboratory benchmark is not a structural-safety certification tool. Ingredient grouping reduces repeated-recipe leakage, but the source does not identify every laboratory batch or establish independence across batches. Model importance describes how the fitted model uses variables; it does not establish causal effects. The small dataset demonstrates integration, not distributed speedup or production readiness.
