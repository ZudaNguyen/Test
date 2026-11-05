"""Phase 2: auditable batch cleaning of original TLC Parquet with Spark SQL."""

import argparse
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


POLICY_VERSION = "tlc-cleaning-v1"
REPORT_VERSION = 2
PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"
REQUIRED = {PICKUP, DROPOFF, "passenger_count", "trip_distance", "fare_amount",
            "tip_amount", "total_amount", "PULocationID", "DOLocationID", "payment_type"}
MONEY = ("fare_amount", "tip_amount", "total_amount", "extra", "mta_tax",
         "tolls_amount", "improvement_surcharge", "congestion_surcharge",
         "airport_fee", "cbd_congestion_fee")
ERRORS = {
    "invalid_pickup_time": "Pickup time is missing or cannot be parsed.",
    "invalid_dropoff_time": "Dropoff time is missing or cannot be parsed.",
    "nonpositive_duration": "Dropoff is not later than pickup.",
    "pickup_outside_month": "Pickup is outside the requested calendar month.",
    "invalid_passenger_count": "Passenger count must be finite, positive, integral and fit int32.",
    "invalid_trip_distance": "Distance must be finite and strictly positive.",
    "invalid_fare_amount": "Fare must be finite, nonnegative and fit decimal(18,2).",
    "invalid_total_amount": "Total must be finite, nonnegative and fit decimal(18,2).",
    "invalid_tip_amount": "A recorded nonnull tip must be finite, nonnegative and fit decimal(18,2).",
}
# Fixed reporting order: a row is removed only at its first failed rule.
RULE_ORDER = tuple(ERRORS)


def month_bounds(month):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("Month must be YYYY-MM")
    start = datetime.strptime(month, "%Y-%m")
    end = datetime(start.year + 1, 1, 1) if start.month == 12 else datetime(start.year, start.month + 1, 1)
    return start.isoformat(), end.isoformat()


def finite(column):
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) != F.lit(float("inf")))


def flag_array(conditions):
    return F.filter(F.array(*[F.when(condition, F.lit(code)) for code, condition in conditions.items()]),
                    lambda item: item.isNotNull())


def mark_rows(raw, month, zone_ids):
    """Preserve original columns; add normalized values and explicit error/warning arrays."""
    missing = sorted(REQUIRED - set(raw.columns))
    if missing:
        raise ValueError("Missing source columns: " + ", ".join(missing))
    reserved = {"_n", "rejection_reasons", "warning_flags", "source_file",
                "pickup_date", "pickup_hour", "is_weekend", "trip_duration_min", "trip_duration_minutes"}
    if reserved & set(raw.columns):
        raise ValueError("Input already contains Phase 2 columns")
    start, end = month_bounds(month)
    expressions = [F.col(PICKUP).try_cast("timestamp_ntz").alias("pickup"),
                   F.col(DROPOFF).try_cast("timestamp_ntz").alias("dropoff"),
                   F.col("passenger_count").try_cast("double").alias("passengers_num"),
                   F.col("passenger_count").try_cast("double").try_cast("int").alias("passengers"),
                   F.col("trip_distance").try_cast("double").alias("distance")]
    for name in MONEY:
        if name in raw.columns:
            expressions += [F.col(name).try_cast("double").alias(name + "_num"),
                            F.col(name).try_cast("decimal(18,2)").alias(name)]
    for name in ("PULocationID", "DOLocationID", "payment_type"):
        number = F.col(name).try_cast("double")
        integer = number.try_cast("int")
        safe_integer = F.when(finite(number) & (number == integer.cast("double")), integer).otherwise(F.lit(None).cast("int"))
        expressions += [number.alias(name + "_num"), safe_integer.alias(name)]
    frame = raw.withColumn("source_file", F.input_file_name()).withColumn("_n", F.struct(*expressions))
    n = lambda name: F.col("_n." + name)
    errors = {
        "invalid_pickup_time": n("pickup").isNull(),
        "invalid_dropoff_time": n("dropoff").isNull(),
        "nonpositive_duration": n("pickup").isNotNull() & n("dropoff").isNotNull() & (n("dropoff") <= n("pickup")),
        "pickup_outside_month": n("pickup").isNotNull() & ((n("pickup") < F.lit(start).cast("timestamp_ntz")) |
                                                                     (n("pickup") >= F.lit(end).cast("timestamp_ntz"))),
        "invalid_passenger_count": ~(finite(n("passengers_num")) & (n("passengers_num") > 0) &
                                      n("passengers").isNotNull() &
                                      (n("passengers_num") == n("passengers").cast("double"))),
        "invalid_trip_distance": ~(finite(n("distance")) & (n("distance") > 0)),
    }
    for name in ("fare_amount", "total_amount"):
        errors["invalid_" + name] = ~(finite(n(name + "_num")) & (n(name + "_num") >= 0) & n(name).isNotNull())
    errors["invalid_tip_amount"] = F.col("tip_amount").isNotNull() & ~(
        finite(n("tip_amount_num")) & (n("tip_amount_num") >= 0) & n("tip_amount").isNotNull())
    # Explicit coalesce prevents SQL NULL from silently escaping a rejection rule.
    errors = {code: F.coalesce(condition, F.lit(True)) for code, condition in errors.items()}
    warnings = {"missing_tip_amount": F.col("tip_amount").isNull()}
    for name, label in (("PULocationID", "pickup"), ("DOLocationID", "dropoff")):
        valid_id = (finite(n(name + "_num")) & n(name).isNotNull() &
                    (n(name + "_num") == n(name).cast("double")) & n(name).isin(sorted(zone_ids)))
        warnings["unknown_" + label + "_zone"] = ~F.coalesce(valid_id, F.lit(False)) | n(name).isin(264, 265)
    valid_payment = (finite(n("payment_type_num")) & n("payment_type").isNotNull() &
                     (n("payment_type_num") == n("payment_type").cast("double")) & n("payment_type").isin(list(range(7))))
    warnings["unknown_payment_type"] = ~F.coalesce(valid_payment, F.lit(False))
    for name in MONEY:
        if name in raw.columns and name not in ("fare_amount", "total_amount", "tip_amount"):
            warnings["invalid_" + name] = F.col(name).isNotNull() & ~(
                finite(n(name + "_num")) & n(name).isNotNull())
    frame = frame.withColumn("rejection_reasons", flag_array(errors)).withColumn("warning_flags", flag_array(warnings))
    return frame, list(warnings)


def clean_projection(marked, source_columns):
    replacements = {PICKUP: "pickup", DROPOFF: "dropoff", "passenger_count": "passengers",
                    "trip_distance": "distance", "PULocationID": "PULocationID",
                    "DOLocationID": "DOLocationID", "payment_type": "payment_type"}
    replacements.update({name: name for name in MONEY if name in source_columns})
    fields = [(F.col("_n." + replacements[name]).alias(name) if name in replacements else F.col(name))
              for name in source_columns]
    clean = marked.filter(F.size("rejection_reasons") == 0).select(*fields, "source_file", "warning_flags")
    return (clean.withColumn("pickup_date", F.to_date(PICKUP))
            .withColumn("pickup_hour", F.hour(PICKUP))
            .withColumn("is_weekend", F.dayofweek(PICKUP).isin(1, 7))
            .withColumn("trip_duration_min", F.timestamp_diff("MICROSECOND", F.col(PICKUP), F.col(DROPOFF)) / F.lit(60_000_000.0)))


def sum_flag(condition, alias):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long").alias(alias)


def profile_rows(marked, source_columns, warning_codes):
    eligible = F.size("rejection_reasons") == 0
    expressions = [F.count(F.lit(1)).alias("input_rows"), sum_flag(eligible, "cleaned_rows"),
                   sum_flag(~eligible, "rejected_rows")]
    expressions += [sum_flag(F.array_contains("rejection_reasons", code), "error__" + code) for code in ERRORS]
    surviving = F.lit(True)
    for code in RULE_ORDER:
        failed = F.array_contains("rejection_reasons", code)
        expressions.append(sum_flag(surviving & failed, "removed__" + code))
        surviving = surviving & ~failed
    expressions += [sum_flag(F.array_contains("warning_flags", code), "warning__" + code) for code in warning_codes]
    expressions += [sum_flag(F.col(name).isNull(), "null__" + name) for name in source_columns]
    numeric_names = [name for name in MONEY if name in source_columns]
    for name in numeric_names:
        bad = F.col(name).isNotNull() & (~finite(F.col("_n." + name + "_num")) | F.col("_n." + name).isNull())
        expressions.append(sum_flag(bad, "numeric__" + name))
    values = marked.agg(*expressions).first().asDict()
    rows = values["input_rows"]
    remaining = rows
    steps = []
    for index, code in enumerate(RULE_ORDER, start=1):
        removed = values["removed__" + code]
        steps.append({"step": index, "rule": code, "input_rows": remaining,
                      "removed_rows": removed, "output_rows": remaining - removed})
        if removed < 0 or removed > remaining:
            raise ValueError("Invalid sequential rule counts")
        remaining -= removed
    if remaining != values["cleaned_rows"] or sum(step["removed_rows"] for step in steps) != values["rejected_rows"]:
        raise ValueError("Sequential rule counts do not reconcile with final outputs")
    return {
        "input_rows": rows, "cleaned_rows": values["cleaned_rows"], "rejected_rows": values["rejected_rows"],
        "rejected_fraction": values["rejected_rows"] / rows if rows else None,
        "rejection_counts": {code: values["error__" + code] for code in ERRORS},
        "sequential_rule_counts": steps,
        "warning_counts": {code: values["warning__" + code] for code in warning_codes},
        "source_null_counts": {name: values["null__" + name] for name in source_columns},
        "invalid_money_parse_or_range_counts": {name: values["numeric__" + name] for name in numeric_names},
        "counting_note": "rejection_counts overlap across all input rows; sequential_rule_counts remove each row only at its first failed rule in RULE_ORDER, so removed_rows sum to rejected_rows.",
    }


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
              "policy_notes": ["Zero fare/total are allowed; distance and passenger count must be positive.",
                               "Missing tips are retained as null with a warning, never replaced by zero.",
                               "Unknown zones/payment types are warnings, not row rejection rules.",
                               "Negative recorded fare/total/tip are excluded by policy, not declared fraudulent.",
                               "Money uses decimal(18,2); optional money parse failures become null with a warning.",
                               "TimestampNTZ source wall-clock values are preserved; duration is a naive time difference.",
                               "Identical rows are retained; the source has no guaranteed unique trip key."]}
    spark = SparkSession.builder.appName("TLC-Phase2-" + args.run_id).config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    marked = None
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
        clean = clean_projection(marked, source_columns)
        rejected = marked.filter(F.size("rejection_reasons") > 0).select(*source_columns, "source_file", "rejection_reasons", "warning_flags")
        report["cleaned_schema"] = json.loads(clean.schema.json())
        report["samples"] = {
            "cleaned": [json.loads(row) for row in clean.limit(5).toJSON().collect()],
            "rejected": [json.loads(row) for row in rejected.limit(5).toJSON().collect()],
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
        if (clean_rows != quality["cleaned_rows"] or rejected_rows != quality["rejected_rows"] or
                invalid_clean or bad_rejected or clean_back.schema.simpleString() != clean.schema.simpleString()):
            raise ValueError("Written output validation failed")
        report["validation"] = {"input_equals_cleaned_plus_rejected": True, "written_row_counts_match": True,
                                "sequential_rule_counts_reconcile": True,
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
        if marked is not None:
            marked.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
