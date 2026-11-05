# TLC Big Data Project

## 1. Overview

Big Data infrastructure using Docker Compose, Hadoop HDFS and Apache Spark.

**Architecture:**
- Hadoop NameNode
- Hadoop DataNode
- Spark Master
- Spark Worker

## 2. Requirements

- Docker Desktop
- Docker Compose

## 3. Start & Stop

Start on this Windows machine (Docker Desktop directory sharing is unavailable):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_windows.ps1
```

For environments where host-directory sharing works, start with the original Compose file:

```powershell
docker compose up -d
```

Check containers:

```powershell
docker compose ps
```

Stop infrastructure:

```powershell
docker compose stop
```

Use `docker compose start` to resume stopped containers. `docker compose down` removes containers and the network while preserving named volumes.

## 4. Data Ingestion

Upload a Parquet file to HDFS:

```powershell
docker exec namenode bash /scripts/upload_to_hdfs.sh `
  /local-data/raw/yellow_tripdata_2023-01.parquet `
  /user/tlc/raw
```

Check HDFS files:

```powershell
docker exec namenode hdfs dfs -ls /user/tlc/raw
```

## 5. Spark Test

Run Spark test:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/work-dir/test_spark.py
```

The test reads Parquet data from HDFS and displays its schema, sample records and row count.

## 6. Web UI

| Service | URL |
|---|---|
| Hadoop NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |

**HDFS path:** `/user/tlc/raw/`

**Note:** Start the infrastructure before running ingestion or Spark tests.

## 7. Source

Phase 1 is based on [sangvominh/TLC](https://github.com/sangvominh/TLC/tree/708b15f246edea4684598bf87954aeb6c5c8542b), commit `708b15f246edea4684598bf87954aeb6c5c8542b`.

## 8. Windows: directory sharing unavailable

Requires Docker Compose 2.24.4 or newer for the override file.

If Docker Desktop reports `user declined directory sharing`, use the Windows helper instead of `docker compose up`:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_windows.ps1
```

The helper uses `compose.windows.yml` to remove host bind mounts, creates the containers, copies configuration/source/data with `docker cp`, and starts the same Hadoop/Spark services. HDFS named volumes are preserved. Docker Desktop must already be running.

Then run the ingestion and Spark test commands in sections 4 and 5. The helper recreates containers when run again, so run it between jobs.

After editing Python source, copy the updated files before submitting another job:

```powershell
docker --context desktop-linux cp .\src\. spark-master:/opt/spark/work-dir
```

Files written inside containers are not automatically synced back to Windows in this mode; use HDFS for pipeline outputs or `docker cp` to export local results.

References: [Compose overrides](https://docs.docker.com/reference/compose-file/merge/), [Docker cp](https://docs.docker.com/reference/cli/docker/container/cp/).

## 9. Runtime verification

Verified on 03/11/2026: Spark 4.1.4 read the HDFS Parquet file, printed 19 columns and 5 sample rows, and counted 3,066,766 rows. The same test passed after recreating containers with the Windows helper. See [runtime results](RUNTIME_RESULT.md).

## 10. Phase 2 — Cleaning

With the cluster running and the original Parquet uploaded, run from the project directory:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1
```

The runner copies the current Python source to Spark, checks 25 synthetic cases, uploads the zone lookup, runs the real distributed cleaning job, and exports the report. It does not recreate containers or overwrite original data. If the cluster is stopped, start it first using section 3.

Outputs:

- Clean Parquet: `/user/tlc/cleaned/year=2023/month=01/run_id=<run_id>/`.
- Rejected Parquet with original values and `rejection_reasons`: `/user/tlc/rejected/year=2023/month=01/run_id=<run_id>/`.
- HDFS JSON report: `/user/tlc/quality/year=2023/month=01/run_id=<run_id>/quality_report.json`.
- Local report and logs: `reports/phase2/<run_id>/`.
- Before/after counts per rule: `reports/phase2/<run_id>/rule-counts.csv` (also printed in the terminal).
- Latest successful report: `reports/phase2/latest.json`.

Each run has its own output directories. Phase 3 should read **one exact cleaned path from the successful report**, rather than scanning all run versions together.

### Cleaning policy

The job rejects missing/unparseable pickup or dropoff time, nonpositive duration, pickup outside the requested month, invalid passenger count, nonpositive/nonfinite distance, and missing/invalid/negative fare or total. Passenger count must be a positive integer fitting int32. A nonnull recorded tip must also be finite, nonnegative and representable as money.

A missing tip remains null and is flagged; it is never filled with zero. Unknown zones/payment types are warnings and do not remove an otherwise eligible trip. Zones 264 and 265 are also marked unknown for spatial interpretation. Optional money values that cannot be normalized become null with an explicit warning. Optional surcharges are not forced to be nonnegative.

These are eligibility rules for the analysis scope. Negative financial records may represent adjustments; exclusion does not prove the record is false. Identical rows are retained because a guaranteed unique trip key is unavailable.

Money columns use `decimal(18,2)`; the conversion rounds to cents using Spark's decimal cast. Source timestamps stay `timestamp_ntz`. Added fields are `trip_duration_min`, `pickup_date`, `pickup_hour` and `is_weekend`. Duration follows source wall-clock time; no UTC conversion is applied to the original timestamps.

### Verification

The job asserts `input_rows = cleaned_rows + rejected_rows`, reads back both Parquet outputs, checks their counts/schema, and re-applies the rules to written clean rows. Every rejected row must carry a reason. Error/warning counts in the input profile may overlap; their sum is not the rejected-row count.

Report version 2 adds `quality.sequential_rule_counts`: ordered steps with `input_rows`, `removed_rows` and `output_rows`. Rules run in the reported `rule_order` (timestamps, month, passengers, distance, money). A row contributes to removals at its first failed rule only; all its reasons remain in rejected Parquet. Each step's output equals the next step's input, all removals sum to `rejected_rows`, and the final output equals `cleaned_rows`. These counts are computed in the same distributed aggregate used for profiling. `rejection_counts` still records every violation across the original input, so it may overlap.

Only tiny lookup metadata, aggregate statistics and five sample rows are collected to the driver. The complete dataset stays distributed; caching uses disk to reduce RAM pressure. Timings include profiling, writing and output verification, and are not a Spark-versus-Pandas benchmark.

For a repeat run with unchanged code after the synthetic tests have passed:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1 -SkipTests
```

Main files: `src/clean_data.py` (rules and job), `src/test_cleaning_rules.py` (hand-checked synthetic cases), `scripts/run_phase2.ps1` (execution and report export).

Updated and verified on 04/11/2026: 3,066,766 input rows = 2,884,463 clean rows + 182,303 rejected rows (5.94%). The latest successful output uses `trip_duration_min`; report version 2 includes sequential before/after counts and a CSV. Counts remain unchanged from the original runs. See [Phase 2 results and explanation (Vietnamese)](PHASE2_RESULT.md), [latest report](reports/phase2/latest.json) and [updated contract checks](reports/phase2/p2-202301-20261104T053321000Z-69e307/contract-verification.json). [Original reproducibility checks dated 03/11](reports/phase2/reproducibility.json) describe the historical runs and their source hashes.

AI disclosure: Codex substantially assisted with Phase 2 implementation, tests, execution checks and documentation. The team must review and understand the submitted work and acknowledge this assistance in its coursework report.
