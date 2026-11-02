"""Step 1 only: prove that a standalone Spark executor can read original HDFS inputs."""

import argparse
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    datetime.strptime(args.month, "%Y-%m")
    if not args.run_id or any(char not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_" for char in args.run_id):
        parser.error("Invalid run ID")
    year, month = args.month.split("-")
    trip_path = f"hdfs://namenode:9000/taxi/raw/yellow/year={year}/month={month}/yellow_tripdata_{args.month}.parquet"
    lookup_path = "hdfs://namenode:9000/taxi/reference/taxi_zone_lookup.csv"
    result_dir = Path("/opt/project/logs") / args.run_id
    result_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    spark = SparkSession.builder.appName(f"step1-read-{args.month}-{args.run_id}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        master = spark.sparkContext.master
        if master != "spark://spark-master:7077":
            raise RuntimeError(f"Expected standalone cluster, got {master}")
        executor_hosts = spark.sparkContext.parallelize([1], 1).map(lambda _: socket.gethostname()).collect()
        if "spark-worker" not in executor_hosts:
            raise RuntimeError(f"No task ran on spark-worker: {executor_hosts}")
        trips = spark.read.parquet(trip_path)
        required = {
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount", "total_amount",
        }
        missing = sorted(required - set(trips.columns))
        if missing:
            raise RuntimeError(f"Missing required source columns: {missing}")
        lookup = spark.read.option("header", True).csv(lookup_path)
        lookup_required = {"LocationID", "Borough", "Zone", "service_zone"}
        if not lookup_required.issubset(lookup.columns):
            raise RuntimeError(f"Unexpected lookup schema: {lookup.columns}")
        trip_rows = trips.count()
        lookup_rows = lookup.count()
        if trip_rows <= 0 or lookup_rows <= 0:
            raise RuntimeError("An input is empty")
        trip_sample = [json.loads(row) for row in trips.limit(5).toJSON().collect()]
        lookup_sample = [row.asDict() for row in lookup.limit(5).collect()]
        metrics = {
            "status": "passed", "scope": "step1_infrastructure_and_original_input_only",
            "run_id": args.run_id, "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "month": args.month, "spark_version": spark.version,
            "application_id": spark.sparkContext.applicationId, "master": master,
            "driver_host": socket.gethostname(), "executor_hosts": sorted(set(executor_hosts)),
            "trip_path": trip_path, "lookup_path": lookup_path,
            "trip_rows": trip_rows, "lookup_rows": lookup_rows,
            "trip_columns": len(trips.columns), "input_partitions": trips.rdd.getNumPartitions(),
            "verification_seconds_including_spark_startup": round(time.perf_counter() - started, 3),
            "timing_note": "Read verification, not a processing benchmark; count may use Parquet metadata.",
        }
        artifacts = {
            "spark-read.json": metrics,
            "trip-schema.json": json.loads(trips.schema.json()),
            "sample-trips.json": trip_sample,
            "sample-zones.json": lookup_sample,
        }
        for name, value in artifacts.items():
            (result_dir / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("\nORIGINAL PARQUET SCHEMA")
        trips.printSchema()
        print("\nINPUT VERIFICATION")
        print(json.dumps(metrics, indent=2))
        print("\nORIGINAL INPUT SAMPLE (NO CLEANING)")
        print(json.dumps(trip_sample, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
