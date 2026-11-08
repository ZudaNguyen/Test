"""Hand-counted guide-v2 boundaries and repairs; synthetic rows never enter TLC output."""

from collections import Counter
from decimal import Decimal
import sys
import unittest

from pyspark.sql import SparkSession, functions as F

from cleaning_policy_v2 import (RULE_ORDER, clean_projection, early_cleaned_source,
                                mark_rows, month_bounds, profile_rows)


class GuideV2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TLC-Guide-V2-Synthetic-Test").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.columns = ["case_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                       "trip_distance", "fare_amount", "tip_amount", "total_amount", "PULocationID",
                       "DOLocationID", "payment_type", "RatecodeID", "extra"]
        base = dict(zip(cls.columns, ["normal", "2023-01-02 08:00:00", "2023-01-02 08:10:00",
                                     "1", "2.5", "10.005", "2.50", "15.005", "161", "237", "1", "1.0", "0.50"]))
        cases = [base.copy(), base.copy()]

        def add(name, **changes):
            cases.append(dict(base, case_id=name, **changes))

        add("passenger_zero", passenger_count="0")
        add("passenger_high", passenger_count="7")
        add("passenger_missing", passenger_count=None)
        add("passenger_negative", passenger_count="-1")
        add("passenger_fractional", passenger_count="1.5")
        add("passenger_nan", passenger_count="NaN")
        add("passenger_infinite", passenger_count="Infinity")
        add("passenger_bad", passenger_count="bad")
        add("rate_99", RatecodeID="99.0")
        add("rate_bad", RatecodeID="1.5")
        add("rate_missing", RatecodeID=None)
        add("unknown_zone", PULocationID="999", DOLocationID="264")
        add("missing_money", tip_amount=None, total_amount=None)
        add("negative_money", tip_amount="-0.01", total_amount="-1")
        add("bad_optional_money", extra="bad", tip_amount="NaN", total_amount="Infinity")
        add("six_hours", trip_distance="100", tpep_dropoff_datetime="2023-01-02 14:00:00")
        add("hundred_miles", trip_distance="100", tpep_dropoff_datetime="2023-01-02 09:15:00")
        add("eighty_mph", trip_distance="20", tpep_dropoff_datetime="2023-01-02 08:15:00")
        add("month_boundary", tpep_pickup_datetime="2023-01-31 23:55:00", tpep_dropoff_datetime="2023-02-01 00:05:00")
        add("small_positive_fare", fare_amount="0.005")
        add("microsecond_duration", trip_distance="0.000000001", tpep_dropoff_datetime="2023-01-02 08:00:00.000001")
        add("missing_pickup", tpep_pickup_datetime=None)
        add("bad_dropoff", tpep_dropoff_datetime="garbage")
        add("backwards", tpep_dropoff_datetime="2023-01-02 07:59:00")
        add("zero_duration", tpep_dropoff_datetime="2023-01-02 08:00:00")
        add("over_six_hours", tpep_dropoff_datetime="2023-01-02 14:00:00.000001")
        add("outside_year", tpep_pickup_datetime="2022-01-02 08:00:00", tpep_dropoff_datetime="2022-01-02 08:10:00")
        add("outside_month", tpep_pickup_datetime="2023-02-02 08:00:00", tpep_dropoff_datetime="2023-02-02 08:10:00")
        add("zero_distance", trip_distance="0")
        add("negative_distance", trip_distance="-1")
        add("nan_distance", trip_distance="NaN")
        add("infinite_distance", trip_distance="Infinity")
        add("over_hundred_miles", trip_distance="100.0001", tpep_dropoff_datetime="2023-01-02 10:00:00")
        add("over_eighty_mph", trip_distance="20.000001", tpep_dropoff_datetime="2023-01-02 08:15:00")
        add("zero_fare", fare_amount="0")
        add("negative_fare", fare_amount="-0.004")
        add("bad_fare", fare_amount="bad")
        add("overflow_fare", fare_amount="100000000000000000000")
        add("nan_fare", fare_amount="NaN")
        add("infinite_fare", fare_amount="Infinity")
        add("fare_rounds_to_zero", fare_amount="0.004")
        add("missing_pickup_zone", PULocationID=None)
        add("missing_dropoff_zone", DOLocationID=None)
        add("fractional_dropoff_zone", DOLocationID="1.5")
        add("multiple", passenger_count="0", trip_distance="0", fare_amount="0", tpep_dropoff_datetime="2023-01-02 08:00:00")
        cls.raw = cls.spark.createDataFrame([tuple(row[name] for name in cls.columns) for row in cases],
                                           ", ".join(name + " string" for name in cls.columns))
        cls.zones = {161, 237, 264, 265}
        cls.marked, cls.warning_codes = mark_rows(cls.raw, "2023-01", cls.zones)
        cls.marked.cache()
        cls.quality = profile_rows(cls.marked, cls.columns, cls.warning_codes)
        cls.clean = clean_projection(cls.marked, cls.columns)
        cls.clean_rows = cls.clean.collect()
        cls.by_case = {row.case_id: row for row in cls.clean_rows}
        cls.reasons = {row.case_id: row.rejection_reasons for row in cls.marked.select("case_id", "rejection_reasons").collect()}

    @classmethod
    def tearDownClass(cls):
        cls.marked.unpersist()
        cls.spark.stop()

    def test_conservation_and_duplicates(self):
        self.assertEqual((self.quality["input_rows"], self.quality["cleaned_rows"], self.quality["rejected_rows"]), (47, 24, 23))
        self.assertEqual(sum(row.case_id == "normal" for row in self.clean_rows), 2)
        self.assertNotIn("invalid_passenger_count", self.quality["rejection_counts"])

    def test_soft_passengers_keep_rows_and_revenue(self):
        for name in ("zero", "high", "missing", "negative", "fractional", "nan", "infinite", "bad"):
            row = self.by_case["passenger_" + name]
            self.assertIsNone(row.passenger_count)
            self.assertEqual(row.fare_amount, Decimal("10.01"))
        self.assertEqual(self.quality["cleaned_null_passenger_count"], 8)
        repairs = {row["rule"]: row for row in self.quality["transformation_counts"]}
        self.assertEqual(repairs["passenger_zero_to_null"]["input_affected_rows"], 2)
        self.assertEqual(repairs["passenger_zero_to_null"]["cleaned_affected_rows"], 1)
        self.assertEqual(repairs["passenger_above_6_to_null"]["cleaned_affected_rows"], 1)
        self.assertEqual(repairs["passenger_invalid_to_null"]["cleaned_affected_rows"], 5)
        self.assertEqual(self.quality["cleaned_rows_with_repairs"], 9)
        for row in repairs.values():
            self.assertEqual(row["input_affected_rows"], row["cleaned_affected_rows"] + row["rejected_affected_rows"])

    def test_time_distance_speed_inclusive_boundaries(self):
        self.assertEqual(self.by_case["six_hours"].trip_duration_min, 360.0)
        self.assertEqual(self.by_case["hundred_miles"].trip_distance, 100.0)
        self.assertEqual(self.by_case["eighty_mph"].trip_duration_min, 15.0)
        self.assertAlmostEqual(self.by_case["microsecond_duration"].trip_duration_min, 1 / 60_000_000)
        for case, reason in {"over_six_hours": "duration_exceeds_6_hours", "over_hundred_miles": "distance_exceeds_100_miles",
                             "over_eighty_mph": "speed_exceeds_80_mph", "outside_year": "pickup_outside_year_2023",
                             "zero_duration": "nonpositive_duration"}.items():
            self.assertIn(reason, self.reasons[case])

    def test_financial_policy_and_rate_99(self):
        self.assertEqual(self.by_case["rate_99"].RatecodeID, 99)
        self.assertIsNone(self.by_case["rate_bad"].RatecodeID)
        self.assertEqual(self.by_case["negative_money"].tip_amount, Decimal("-0.01"))
        self.assertEqual(self.by_case["negative_money"].total_amount, Decimal("-1.00"))
        self.assertIsNone(self.by_case["bad_optional_money"].tip_amount)
        self.assertIsNone(self.by_case["bad_optional_money"].total_amount)
        self.assertIsNone(self.by_case["bad_optional_money"].extra)
        for name in ("zero_fare", "fare_rounds_to_zero", "nan_fare", "infinite_fare"):
            self.assertIn("invalid_fare_amount", self.reasons[name])
        self.assertEqual(self.by_case["small_positive_fare"].fare_amount, Decimal("0.01"))

    def test_features_schema_and_locations(self):
        self.assertEqual(self.by_case["normal"].fare_amount, Decimal("10.01"))
        self.assertEqual(self.by_case["normal"].pickup_month, 1)
        self.assertFalse(self.by_case["normal"].is_weekend)
        self.assertEqual(self.by_case["month_boundary"].trip_duration_min, 10.0)
        self.assertEqual(self.by_case["outside_month"].pickup_month, 2)
        self.assertNotIn("pickup_outside_month", self.quality["rejection_counts"])
        self.assertEqual(self.clean.schema["RatecodeID"].dataType.simpleString(), "int")
        self.assertEqual(self.clean.schema["passenger_count"].dataType.simpleString(), "int")
        self.assertNotIn("trip_duration_minutes", self.clean.columns)
        self.assertTrue({"trip_duration_min", "pickup_date", "pickup_hour", "pickup_month", "is_weekend"}.issubset(self.clean.columns))
        self.assertIn("unknown_pickup_zone", self.by_case["unknown_zone"].warning_flags)
        self.assertIn("invalid_pickup_location", self.reasons["missing_pickup_zone"])
        self.assertIn("invalid_dropoff_location", self.reasons["missing_dropoff_zone"])

    def test_sequential_counts_hand_checked(self):
        removed = [1, 1, 3, 1, 1, 4, 1, 1, 7, 1, 2]
        before = 47
        for index, (code, count) in enumerate(zip(RULE_ORDER, removed)):
            actual = self.quality["sequential_rule_counts"][index]
            self.assertEqual(actual, {"step": index + 1, "rule": code, "input_rows": before,
                                      "removed_rows": count, "output_rows": before - count})
            before -= count
        self.assertEqual(before, 24)
        self.assertEqual(sum(removed), 23)
        self.assertEqual(self.quality["rejection_counts"]["invalid_trip_distance"], 5)
        self.assertEqual(set(self.reasons["multiple"]), {"nonpositive_duration", "invalid_trip_distance", "invalid_fare_amount"})

    def test_early_filter_matches_audit_and_handles_zero_division(self):
        early = early_cleaned_source(self.raw, "2023-01")
        early_marked, _ = mark_rows(early, "2023-01", self.zones)
        early_clean = clean_projection(early_marked, self.columns, already_eligible=True)
        self.assertEqual(Counter(row.case_id for row in early_clean.select("case_id").collect()),
                         Counter(row.case_id for row in self.clean_rows))
        self.assertEqual(early_marked.filter(F.size("rejection_reasons") > 0).count(), 0)

    def test_empty_missing_and_reserved_columns(self):
        with self.assertRaisesRegex(ValueError, "Missing source columns"):
            mark_rows(self.raw.drop("RatecodeID"), "2023-01", self.zones)
        with self.assertRaisesRegex(ValueError, "already contains"):
            mark_rows(self.raw.withColumn("pickup_month", F.lit(1)), "2023-01", self.zones)
        with self.assertRaises(ValueError):
            month_bounds("2023-00")
        empty, codes = mark_rows(self.raw.limit(0), "2023-01", self.zones)
        quality = profile_rows(empty, self.columns, codes)
        self.assertEqual(quality["input_rows"], 0)
        self.assertTrue(all(step["output_rows"] == 0 for step in quality["sequential_rule_counts"]))
        self.assertEqual(clean_projection(empty, self.columns).count(), 0)


def main():
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(GuideV2Test))
    print("SYNTHETIC GUIDE V2: 47 rows; expected clean=24 rejected=23; TLC input not modified.", flush=True)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
