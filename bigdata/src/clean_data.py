"""Phase 2: auditable batch cleaning of original TLC Parquet with Spark SQL."""

import argparse
from contextlib import redirect_stdout
import io
import json
import math
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F


from cleaning_policy_v2 import (ERRORS, POLICY_VERSION, REPORT_VERSION, RULE_ORDER,
                                PICKUP, DROPOFF, mark_rows, clean_projection, profile_rows,
                                month_bounds, early_cleaned_source)


def load_zones(spark, path):
    frame = spark.read.option("header", True).csv(path)
    if not {"LocationID", "Borough", "Zone", "service_zone"}.issubset(frame.columns):
        raise ValueError("Zone CSV does not have the TLC lookup columns")
    rows = frame.select(F.col("LocationID").try_cast("int").alias("id")).limit(10001).collect()
    ids = [row.id for row in rows]
    if not ids or len(ids) > 10000 or None in ids or len(set(ids)) != len(ids):
        raise ValueError("Zone lookup must be small, nonempty and have unique nonnull integer IDs")
    return set(ids)


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    return value


def write_hdfs_json(spark, path, value):
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.mkdirs(target.getParent())
    stream = fs.create(target, False)
    try:
        # ensure_ascii makes writeBytes produce valid UTF-8/ASCII JSON without JVM byte-array conversions.
        stream.writeBytes(json.dumps(json_safe(value), ensure_ascii=True, indent=2, allow_nan=False) + "\n")
    finally:
        stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2023-01")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input-path")
    parser.add_argument("--zones-path", default="hdfs://namenode:8020/user/tlc/reference/taxi_zone_lookup.csv")
    parser.add_argument("--output-root", default="hdfs://namenode:8020/user/tlc")
    parser.add_argument("--report-dir", required=True)
    args = parser.parse_args()
    month_bounds(args.month)
    if not args.month.startswith("2023-"):
        parser.error("Guide v2 requires pickup year 2023; select a 2023 batch month")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id):
        parser.error("Unsafe run ID")
    input_path = args.input_path or f"hdfs://namenode:8020/user/tlc/raw/yellow_tripdata_{args.month}.parquet"
    base = args.output_root.rstrip("/")
    parsed = urlparse(base)
    if parsed.scheme != "hdfs" or parsed.netloc != "namenode:8020" or parsed.path not in ("/user/tlc", "/user/tlc/tests"):
        parser.error("Output root must be hdfs://namenode:8020/user/tlc or /user/tlc/tests")
    year, month = args.month.split("-")
    suffix = f"year={year}/month={month}/run_id={args.run_id}"
    outputs = {"cleaned": f"{base}/cleaned/{suffix}", "rejected": f"{base}/rejected/{suffix}",
               "quality_report": f"{base}/quality/{suffix}/quality_report.json"}
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    if (report_dir / "quality_report.json").exists():
        parser.error("A report already exists for this run; use a new run ID")
    started = time.perf_counter()
    report = {"status": "running", "policy_version": POLICY_VERSION, "report_version": REPORT_VERSION,
              "rule_order": list(RULE_ORDER), "run_id": args.run_id,
              "month": args.month, "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "input_path": input_path, "zones_path": args.zones_path, "outputs": outputs,
              "policy": ERRORS,
              "policy_notes": ["Guide v2 pages 3-5: duration (0,360] minutes, distance (0,100] miles, speed <=80 mph, fare >0, pickup year 2023.",
                               "Month identifies the input-file batch and output folder, not a pickup-month filter. All eligible pickups in 2023 remain; dropoff may cross a month boundary.",
                               "Passenger zero or >6 becomes null; missing passengers stay null. No row is rejected for passenger count.",
                               "Negative, fractional, nonfinite or unparseable passenger values also become null defensively, without row removal.",
                               "RatecodeID is int; 99 is retained. Invalid casts become null with a repair flag.",
                               "Either null/unparseable/noninteger LocationID rejects the row; nonnull unknown zones including 264/265 are warnings.",
                               "Total/tip sign is not a hard filter in v2. Negative values remain with warnings; invalid optional money becomes null.",
                               "Money uses decimal(18,2); optional money parse failures become null with a warning.",
                               "Fare must remain positive after cent rounding, so tiny positive values rounding to zero are excluded.",
                               "TimestampNTZ source wall-clock values are preserved; duration is a naive time difference.",
                               "Identical rows are retained; the source has no guaranteed unique trip key."]}
    spark = (SparkSession.builder.appName("TLC-Phase2-" + args.run_id).config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", "200").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    marked = None
    clean = None
    try:
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise ValueError("The real job must run on the standalone Spark cluster")
        hosts = spark.sparkContext.parallelize([1], 1).map(lambda _: socket.gethostname()).collect()
        if "spark-worker" not in hosts:
            raise ValueError("No task executed on spark-worker")
        report.update({"spark_version": spark.version, "application_id": spark.sparkContext.applicationId,
                       "executor_hosts": sorted(set(hosts)), "driver_host": socket.gethostname(),
                       "cache_storage_level": "DISK_ONLY"})
        zones = load_zones(spark, args.zones_path)
        raw = spark.read.parquet(input_path)
        source_columns = raw.columns
        report["input_schema"] = json.loads(raw.schema.json())
        report["zone_lookup_rows"] = len(zones)
        report["input_files"] = raw.inputFiles()
        marked, warning_codes = mark_rows(raw, args.month, zones)
        marked.persist(StorageLevel.DISK_ONLY)
        step = time.perf_counter()
        report["quality"] = profile_rows(marked, source_columns, warning_codes)
        report["timings_seconds"] = {"profile_and_materialize": round(time.perf_counter() - step, 3)}
        quality = report["quality"]
        if quality["input_rows"] == 0:
            raise ValueError("Input is empty")
        if quality["input_rows"] != quality["cleaned_rows"] + quality["rejected_rows"]:
            raise ValueError("Input row conservation failed")
        print("PROFILE " + json.dumps(quality), flush=True)
        # This branch reads raw Parquet with one combined early filter. The audit
        # branch above scans all rows so before/after statistics remain complete.
        early = early_cleaned_source(raw, args.month)
        clean_marked, _ = mark_rows(early, args.month, zones)
        clean = clean_projection(clean_marked, source_columns, already_eligible=True)
        rejected = marked.filter(F.size("rejection_reasons") > 0).select(*source_columns, "source_file", "rejection_reasons", "warning_flags", "transformation_flags")
        plan_buffer = io.StringIO()
        with redirect_stdout(plan_buffer):
            clean.explain(mode="formatted")
        plan = plan_buffer.getvalue()
        (report_dir / "cleaned-explain.txt").write_text(plan, encoding="utf-8")
        report["optimization"] = {
            "cleaned_branch_filter_before_repairs_and_cache": True,
            "audit_branch_scans_all_rows_for_complete_counts": True,
            "uses_python_udf": False,
            "safe_speed_division": "pyspark.sql.functions.try_divide",
            "explain_file": "cleaned-explain.txt",
            "pushed_filter_lines": [line.strip() for line in plan.splitlines() if "PushedFilters" in line],
            "note": "Only supported predicates push into the Parquet reader. HDFS itself does not evaluate row filters. Audit and early-cleaned branches may scan input separately.",
        }
        clean.persist(StorageLevel.DISK_ONLY)
        report["cleaned_schema"] = json.loads(clean.schema.json())
        report["samples"] = {
            "cleaned": [json.loads(row) for row in clean.limit(5).toJSON().collect()],
            "rejected": [json.loads(row) for row in rejected.limit(5).toJSON().collect()],
            "cleaned_with_repairs": [json.loads(row) for row in clean.filter(F.size("transformation_flags") > 0).limit(5).toJSON().collect()],
        }
        step = time.perf_counter()
        clean.coalesce(2).write.mode("errorifexists").parquet(outputs["cleaned"])
        report["timings_seconds"]["write_cleaned"] = round(time.perf_counter() - step, 3)
        print("WROTE " + outputs["cleaned"], flush=True)
        step = time.perf_counter()
        rejected.coalesce(1).write.mode("errorifexists").parquet(outputs["rejected"])
        report["timings_seconds"]["write_rejected"] = round(time.perf_counter() - step, 3)
        step = time.perf_counter()
        clean_back = spark.read.parquet(outputs["cleaned"])
        rejected_back = spark.read.parquet(outputs["rejected"])
        clean_rows = clean_back.count()
        rejected_rows = rejected_back.count()
        checked, _ = mark_rows(clean_back.select(*source_columns), args.month, zones)
        invalid_clean = checked.filter(F.size("rejection_reasons") > 0).limit(1).count()
        bad_rejected = rejected_back.filter(F.size("rejection_reasons") == 0).limit(1).count()
        duration = F.timestamp_diff("MICROSECOND", F.col(PICKUP), F.col(DROPOFF)) / 60_000_000.0
        bad_features = clean_back.filter(
            ~F.col("pickup_date").eqNullSafe(F.to_date(PICKUP)) |
            ~F.col("pickup_hour").eqNullSafe(F.hour(PICKUP)) |
            ~F.col("pickup_month").eqNullSafe(F.month(PICKUP)) |
            ~F.col("is_weekend").eqNullSafe(F.dayofweek(PICKUP).isin(1, 7)) |
            ~F.col("trip_duration_min").eqNullSafe(duration) |
            (F.col("passenger_count").isNotNull() & ~F.col("passenger_count").between(1, 6))
        ).limit(1).count()
        null_passengers = clean_back.filter(F.col("passenger_count").isNull()).count()
        if (clean_rows != quality["cleaned_rows"] or rejected_rows != quality["rejected_rows"] or
                invalid_clean or bad_rejected or bad_features or null_passengers != quality["cleaned_null_passenger_count"] or
                clean_back.schema.simpleString() != clean.schema.simpleString()):
            raise ValueError("Written output validation failed")
        report["validation"] = {"input_equals_cleaned_plus_rejected": True, "written_row_counts_match": True,
                                "sequential_rule_counts_reconcile": True,
                                "derived_features_match_source": True,
                                "passenger_soft_repairs_preserved": True,
                                "cleaned_null_passenger_count": null_passengers,
                                "cleaned_rows_pass_rules": True, "rejected_rows_have_reasons": True,
                                "written_schema_matches": True,
                                "cleaned_rows_read_back": clean_rows, "rejected_rows_read_back": rejected_rows}
        report["timings_seconds"]["read_back_and_validate"] = round(time.perf_counter() - step, 3)
        report["timings_seconds"]["job_until_report"] = round(time.perf_counter() - started, 3)
        report["input_rows_per_second"] = round(quality["input_rows"] / report["timings_seconds"]["job_until_report"], 3)
        report["status"] = "passed"
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report = json_safe(report)
        write_hdfs_json(spark, outputs["quality_report"], report)
        (report_dir / "quality_report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        print(f"PHASE2 PASSED input={quality['input_rows']} cleaned={clean_rows} rejected={rejected_rows}", flush=True)
        print("REPORT " + outputs["quality_report"], flush=True)
    except Exception as error:
        report.update({"status": "failed", "error": str(error), "failed_at_utc": datetime.now(timezone.utc).isoformat()})
        (report_dir / "failure.json").write_text(json.dumps(json_safe(report), indent=2, default=str) + "\n", encoding="utf-8")
        raise
    finally:
        if clean is not None:
            clean.unpersist()
        if marked is not None:
            marked.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
