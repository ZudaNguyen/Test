"""TLC guide v2, pages 3-5: hard filters, soft repairs and audit counts."""

from datetime import datetime
import re

from pyspark.sql import functions as F


POLICY_VERSION = "tlc-cleaning-v2"
REPORT_VERSION = 3
PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"
REQUIRED = {PICKUP, DROPOFF, "passenger_count", "trip_distance", "fare_amount",
            "tip_amount", "total_amount", "PULocationID", "DOLocationID", "payment_type", "RatecodeID"}
MONEY = ("fare_amount", "tip_amount", "total_amount", "extra", "mta_tax", "tolls_amount",
         "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee")
ERRORS = {
    "invalid_pickup_time": "Pickup time must be present and parseable.",
    "invalid_dropoff_time": "Dropoff time must be present and parseable.",
    "nonpositive_duration": "Dropoff must be later than pickup.",
    "duration_exceeds_6_hours": "Duration must not exceed 360 minutes (inclusive).",
    "pickup_outside_year_2023": "Pickup year must be 2023.",
    "invalid_trip_distance": "Distance must be finite and strictly positive.",
    "distance_exceeds_100_miles": "Distance must not exceed 100 miles (inclusive).",
    "speed_exceeds_80_mph": "Distance / duration in hours must not exceed 80 mph.",
    "invalid_fare_amount": "Fare must be finite, strictly positive and positive after decimal(18,2) normalization.",
    "invalid_pickup_location": "Pickup LocationID must normalize to a nonnull integer.",
    "invalid_dropoff_location": "Dropoff LocationID must normalize to a nonnull integer.",
}
RULE_ORDER = tuple(ERRORS)


def month_bounds(month):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("Month must be YYYY-MM")
    start = datetime.strptime(month, "%Y-%m")
    end = datetime(start.year + 1, 1, 1) if start.month == 12 else datetime(start.year, start.month + 1, 1)
    return start.isoformat(), end.isoformat()


def finite(column):
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) != F.lit(float("inf")))


def safe_integer(column):
    number = column.try_cast("double")
    integer = number.try_cast("int")
    return F.when(finite(number) & (number == integer.cast("double")), integer).otherwise(F.lit(None).cast("int"))


def flag_array(conditions):
    return F.filter(F.array(*[F.when(condition, F.lit(code)) for code, condition in conditions.items()]),
                    lambda value: value.isNotNull())


def normalized_values(raw):
    missing = sorted(REQUIRED - set(raw.columns))
    if missing:
        raise ValueError("Missing source columns: " + ", ".join(missing))
    values = {
        "pickup": F.col(PICKUP) if raw.schema[PICKUP].dataType.simpleString() == "timestamp_ntz" else F.col(PICKUP).try_cast("timestamp_ntz"),
        "dropoff": F.col(DROPOFF) if raw.schema[DROPOFF].dataType.simpleString() == "timestamp_ntz" else F.col(DROPOFF).try_cast("timestamp_ntz"),
        "passengers_num": F.col("passenger_count").try_cast("double"),
        "distance": F.col("trip_distance").try_cast("double"),
    }
    passengers = safe_integer(F.col("passenger_count"))
    values["passengers"] = F.when(passengers.between(1, 6), passengers).otherwise(F.lit(None).cast("int"))
    for name in MONEY:
        if name in raw.columns:
            values[name + "_num"] = F.col(name).try_cast("double")
            values[name] = F.col(name).try_cast("decimal(18,2)")
    for name in ("PULocationID", "DOLocationID", "payment_type", "RatecodeID"):
        values[name + "_num"] = F.col(name).try_cast("double")
        values[name] = safe_integer(F.col(name))
    values["duration_us"] = F.timestamp_diff("MICROSECOND", values["pickup"], values["dropoff"])
    # try_divide remains safe even if Catalyst evaluates division before duration filtering.
    values["speed_mph"] = F.try_divide(values["distance"] * F.lit(3_600_000_000.0), values["duration_us"])
    return values


def rejection_conditions(n, month):
    month_bounds(month)
    duration = n["duration_us"]
    valid_distance = finite(n["distance"])
    conditions = {
        "invalid_pickup_time": n["pickup"].isNull(),
        "invalid_dropoff_time": n["dropoff"].isNull(),
        "nonpositive_duration": duration.isNotNull() & (duration <= 0),
        "duration_exceeds_6_hours": duration.isNotNull() & (duration > 21_600_000_000),
        "pickup_outside_year_2023": n["pickup"].isNotNull() & (F.year(n["pickup"]) != 2023),
        "invalid_trip_distance": ~(valid_distance & (n["distance"] > 0)),
        "distance_exceeds_100_miles": valid_distance & (n["distance"] > 100),
        "speed_exceeds_80_mph": (duration > 0) & valid_distance & (n["speed_mph"] > 80),
        "invalid_fare_amount": ~(finite(n["fare_amount_num"]) & (n["fare_amount_num"] > 0) &
                                  n["fare_amount"].isNotNull() & (n["fare_amount"] > 0)),
        "invalid_pickup_location": n["PULocationID"].isNull(),
        "invalid_dropoff_location": n["DOLocationID"].isNull(),
    }
    # Missing times/distance have their own reasons; comparisons depending on them
    # must not invent additional failures just because SQL evaluates to NULL.
    return {code: F.coalesce(condition, F.lit(False)) for code, condition in conditions.items()}


def mark_rows(raw, month, zone_ids):
    reserved = {"_n", "source_file", "rejection_reasons", "warning_flags", "transformation_flags",
                "pickup_date", "pickup_hour", "pickup_month", "is_weekend", "trip_duration_min", "trip_duration_minutes"}
    if reserved & set(raw.columns):
        raise ValueError("Input already contains Phase 2 columns")
    values = normalized_values(raw)
    frame = raw.withColumn("source_file", F.input_file_name()).withColumn("_n", F.struct(*[value.alias(name) for name, value in values.items()]))
    n = {name: F.col("_n." + name) for name in values}
    errors = rejection_conditions(n, month)
    p = n["passengers_num"]
    repairs = {
        "passenger_zero_to_null": finite(p) & (p == 0),
        "passenger_above_6_to_null": finite(p) & (p > 6),
        "passenger_invalid_to_null": F.col("passenger_count").isNotNull() & n["passengers"].isNull() &
                                     ~F.coalesce(finite(p) & ((p == 0) | (p > 6)), F.lit(False)),
        "invalid_ratecode_to_null": F.col("RatecodeID").isNotNull() & n["RatecodeID"].isNull(),
    }
    warnings = {
        "missing_passenger_count": F.col("passenger_count").isNull(),
        "missing_tip_amount": F.col("tip_amount").isNull(),
        "negative_tip_amount": n["tip_amount_num"] < 0,
        "negative_total_amount": n["total_amount_num"] < 0,
    }
    for name, label in (("PULocationID", "pickup"), ("DOLocationID", "dropoff")):
        known = n[name].isNotNull() & n[name].isin(sorted(zone_ids)) & ~n[name].isin(264, 265)
        warnings["unknown_" + label + "_zone"] = ~F.coalesce(known, F.lit(False))
    warnings["unknown_payment_type"] = ~F.coalesce(n["payment_type"].isin(list(range(7))), F.lit(False))
    for name in MONEY:
        if name in raw.columns and name != "fare_amount":
            invalid = F.col(name).isNotNull() & ~(finite(n[name + "_num"]) & n[name].isNotNull())
            repairs["invalid_" + name + "_to_null"] = invalid
    warnings.update(repairs)
    frame = (frame.withColumn("rejection_reasons", flag_array(errors))
             .withColumn("warning_flags", flag_array(warnings))
             .withColumn("transformation_flags", flag_array(repairs)))
    return frame, list(warnings)


def early_cleaned_source(raw, month):
    """One combined filter directly on the raw Parquet branch, before repairs/cache."""
    n = normalized_values(raw)
    month_bounds(month)
    # Direct source predicates also expose simple Parquet pushdown opportunities.
    condition = (F.col(PICKUP).isNotNull() & F.col(DROPOFF).isNotNull() &
                 F.col("PULocationID").isNotNull() & F.col("DOLocationID").isNotNull() &
                 (n["pickup"] >= F.lit("2023-01-01").cast("timestamp_ntz")) &
                 (n["pickup"] < F.lit("2024-01-01").cast("timestamp_ntz")) &
                 (n["distance"] > 0) & (n["distance"] <= 100) & (n["fare_amount_num"] > 0))
    for failed in rejection_conditions(n, month).values():
        condition = condition & ~failed
    return raw.filter(condition)


def clean_projection(marked, source_columns, already_eligible=False):
    replacements = {PICKUP: "pickup", DROPOFF: "dropoff", "passenger_count": "passengers",
                    "trip_distance": "distance", "PULocationID": "PULocationID", "DOLocationID": "DOLocationID",
                    "payment_type": "payment_type", "RatecodeID": "RatecodeID"}
    replacements.update({name: name for name in MONEY if name in source_columns})
    fields = [(F.col("_n." + replacements[name]).alias(name) if name in replacements else F.col(name)) for name in source_columns]
    source = marked if already_eligible else marked.filter(F.size("rejection_reasons") == 0)
    clean = source.select(*fields, "source_file", "warning_flags", "transformation_flags")
    return (clean.withColumn("pickup_date", F.to_date(PICKUP)).withColumn("pickup_hour", F.hour(PICKUP))
            .withColumn("pickup_month", F.month(PICKUP)).withColumn("is_weekend", F.dayofweek(PICKUP).isin(1, 7))
            .withColumn("trip_duration_min", F.timestamp_diff("MICROSECOND", F.col(PICKUP), F.col(DROPOFF)) / 60_000_000.0))


def sum_flag(condition, alias):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long").alias(alias)


def profile_rows(marked, source_columns, warning_codes):
    eligible = F.size("rejection_reasons") == 0
    repair_codes = [code for code in warning_codes if code.endswith("_to_null")]
    expressions = [F.count(F.lit(1)).alias("input_rows"), sum_flag(eligible, "cleaned_rows"), sum_flag(~eligible, "rejected_rows")]
    expressions += [sum_flag(F.array_contains("rejection_reasons", code), "error__" + code) for code in RULE_ORDER]
    surviving = F.lit(True)
    for code in RULE_ORDER:
        failed = F.array_contains("rejection_reasons", code)
        expressions.append(sum_flag(surviving & failed, "removed__" + code))
        surviving = surviving & ~failed
    expressions += [sum_flag(F.array_contains("warning_flags", code), "warning__" + code) for code in warning_codes]
    expressions += [sum_flag(F.col(name).isNull(), "null__" + name) for name in source_columns]
    for code in repair_codes:
        affected = F.array_contains("transformation_flags", code)
        expressions += [sum_flag(affected, "repair_input__" + code), sum_flag(affected & eligible, "repair_cleaned__" + code)]
    expressions += [sum_flag(F.size("transformation_flags") > 0, "input_rows_with_repairs"),
                    sum_flag((F.size("transformation_flags") > 0) & eligible, "cleaned_rows_with_repairs"),
                    sum_flag(F.col("_n.passengers").isNull() & eligible, "cleaned_null_passenger_count")]
    values = marked.agg(*expressions).first().asDict()
    rows, remaining, steps = values["input_rows"], values["input_rows"], []
    for index, code in enumerate(RULE_ORDER, start=1):
        removed = values["removed__" + code]
        if removed < 0 or removed > remaining:
            raise ValueError("Invalid sequential rule counts")
        steps.append({"step": index, "rule": code, "input_rows": remaining, "removed_rows": removed, "output_rows": remaining - removed})
        remaining -= removed
    if remaining != values["cleaned_rows"] or sum(step["removed_rows"] for step in steps) != values["rejected_rows"]:
        raise ValueError("Sequential counts do not reconcile")
    repairs = [{"rule": code, "input_affected_rows": values["repair_input__" + code],
                "cleaned_affected_rows": values["repair_cleaned__" + code],
                "rejected_affected_rows": values["repair_input__" + code] - values["repair_cleaned__" + code]} for code in repair_codes]
    return {"input_rows": rows, "cleaned_rows": values["cleaned_rows"], "rejected_rows": values["rejected_rows"],
            "rejected_fraction": values["rejected_rows"] / rows if rows else None,
            "rejection_counts": {code: values["error__" + code] for code in RULE_ORDER},
            "sequential_rule_counts": steps, "transformation_counts": repairs,
            "input_rows_with_repairs": values["input_rows_with_repairs"],
            "cleaned_rows_with_repairs": values["cleaned_rows_with_repairs"],
            "cleaned_null_passenger_count": values["cleaned_null_passenger_count"],
            "warning_counts": {code: values["warning__" + code] for code in warning_codes},
            "source_null_counts": {name: values["null__" + name] for name in source_columns},
            "counting_note": "Independent rejection_counts overlap. Sequential removed_rows count each rejected row once. Repairs never remove a row and may overlap each other; repair counts exclude ordinary casts/rounding and already-null values."}
