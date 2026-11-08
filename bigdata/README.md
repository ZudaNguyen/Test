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

## 10. Phase 2 - TLC guide v2

With the Phase 1 cluster running and January 2023 Parquet already on HDFS:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1
```

The runner copies source into Spark, executes 8 tests on 47 hand-counted synthetic rows, uploads the zone CSV, runs cleaning on the standalone worker, exports the quality report and copies clean Parquet to Windows. It preserves original data and creates a separate output per run.

### Hard filters

- Parseable pickup/dropoff; dropoff later than pickup; duration at most 360 minutes.
- Pickup year 2023. The month parameter identifies the input-file batch (default January), not an additional pickup-month filter. Eligible pickups in other months of 2023 remain, and dropoff may cross a month boundary.
- Finite distance in `(0,100]` miles and speed at most 80 mph. Speed uses `try_divide` to remain safe on zero duration.
- Finite fare strictly greater than zero, also positive after cent rounding.
- Both pickup and dropoff LocationID must normalize to nonnull integers. Nonnull unknown IDs, including 264/265, are warnings.

### Soft transformations

Passenger count equal to 0 or greater than 6 becomes null, with the row retained if its hard filters pass. Existing null remains null. Negative, fractional, nonfinite and unparseable passenger values also become null defensively; valid integers 1-6 remain. Never fill passenger count with 1 or replace missing tip with zero.

`RatecodeID` becomes integer; 99 is retained for KPI-specific decisions in Phase 3. Invalid RatecodeID becomes null with a flag. Total/tip sign is not a row filter in guide v2: negative recorded values remain with warnings. Invalid optional money becomes null with flags. Money is normalized to `decimal(18,2)`; this rounds to cents. Timestamps preserve source `timestamp_ntz` wall-clock values. Identical rows remain because there is no guaranteed unique trip key.

Added features: `trip_duration_min`, `pickup_date`, `pickup_hour`, `pickup_month`, `is_weekend`. Original column names remain, alongside `source_file`, `warning_flags` and `transformation_flags`.

These are analysis eligibility rules, not proof that excluded records are fraudulent or physically impossible. Defensive casts and cent rounding are documented extensions of the guide's conditions. Output `year/month` folders identify the source-file batch; `pickup_month` stores the actual event month used for KPI grouping.

### Outputs and evidence

- HDFS clean: `/user/tlc/cleaned/year=2023/month=01/run_id=<run_id>/`.
- HDFS rejected original values plus all rejection/repair reasons: `/user/tlc/rejected/year=2023/month=01/run_id=<run_id>/`.
- HDFS quality JSON: `/user/tlc/quality/year=2023/month=01/run_id=<run_id>/quality_report.json`.
- Local clean copy: `data/cleaned/year=2023/month=01/run_id=<run_id>/`.
- Report, logs and CSV: `reports/phase2/<run_id>/`.
- Latest successful report: `reports/phase2/latest.json`; latest local export: `reports/phase2/latest-export.json`.
- `rule-counts.csv`: sequential input / removed / output counts; a rejected row is counted at its first failed rule only.
- `transformation-counts.csv`: soft repairs counted separately across input, retained and rejected rows. These counts can overlap and are not row removals.
- `cleaned-explain.txt`: Spark's actual physical plan, including supported Parquet `PushedFilters`.
- `local-output-manifest.json`: file sizes and SHA-256 of the exported Parquet copy, with `_SUCCESS` and header/footer checks.

Phase 3 must read one exact successful `outputs.cleaned` path, or its corresponding local export. Scanning multiple historical run IDs would double-count trips. Historical `tlc-cleaning-v1` reports and old ZIPs do not describe guide-v2 results.

### Verification and optimization

The job reconciles input = clean + rejected, validates sequential counts, reads persisted Parquet back, checks count/schema/derived fields and re-applies the hard rules. It checks the expected retained passenger-null count, demonstrating that soft repairs do not remove the row.

The cleaned branch applies one combined filter directly to raw Parquet, before repairs or cache. A separate audit branch scans every input row for complete before/after counts. Only supported predicates push into the Parquet reader; HDFS does not execute SQL row filters. Both branches may scan input, and cache uses `DISK_ONLY`. Aggregate statistics, the small zone lookup and at most 5 rows per sample category are collected; full TLC data stays distributed. There is no Python UDF in cleaning.

Job timings include profiling, output writing and readback checks; they exclude fixture tests, CSV upload and local export. These are not Spark-versus-Pandas benchmark results.

Repeat unchanged code without the synthetic suite:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1 -SkipTests
```

Main files: `src/cleaning_policy_v2.py` (policy, projection, counters), `src/clean_data.py` (job, persistence and validation), `src/test_cleaning_v2.py` (47 fixture rows), `src/test_cleaning_rules.py` (test entry point), `scripts/run_phase2.ps1` (run/export).

See [Phase 2 results (Vietnamese)](PHASE2_RESULT.md) for the verified run, counts and handoff paths.

AI disclosure: Codex substantially assisted with implementation, tests, execution checks and documentation. The team must review and understand the submitted work and acknowledge this assistance in its coursework report.

Verified guide v2 on 08/11/2026: 3,066,766 input = 2,993,943 clean + 72,823 rejected (2.37%). The retained dataset includes 49,797 repaired passenger counts and 113,936 null passenger counts in total. Both HDFS readback and the local Parquet export passed; see PHASE2_RESULT.md and reports/phase2/latest.json.
