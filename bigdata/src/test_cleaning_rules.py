"""Small synthetic cases with hand-checked expectations; never mixed with TLC results."""

import sys
import unittest
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F

from clean_data import clean_projection, mark_rows, profile_rows


class CleaningRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Phase2-Synthetic-Policy-Test").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.columns = ["case_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                       "trip_distance", "fare_amount", "tip_amount", "total_amount", "PULocationID",
                       "DOLocationID", "payment_type", "extra"]
        base = dict(zip(cls.columns, ["normal", "2023-01-02 08:00:00", "2023-01-02 08:10:00",
                                     "1", "2.5", "10.005", "2.50", "15.005", "161", "237", "1", "0.50"]))
        cases = [base.copy(), base.copy()]
        def add(name, **changes):
            row = dict(base, case_id=name)
            row.update(changes)
            cases.append(row)
        add("zero_fare", fare_amount="0", tip_amount="0", total_amount="0",
            tpep_pickup_datetime="2023-01-07 08:00:00", tpep_dropoff_datetime="2023-01-07 08:10:00")
        add("missing_tip", tip_amount=None, passenger_count="2.0")
        add("unknown_zone", PULocationID="999", DOLocationID="1.5")
        add("month_boundary", tpep_pickup_datetime="2023-01-31 23:55:00", tpep_dropoff_datetime="2023-02-01 00:05:00")
        add("optional_bad_money", extra="not-a-number")
        add("negative_fare", fare_amount="-0.004")
        add("zero_distance", trip_distance="0")
        add("negative_distance", trip_distance="-1")
        add("zero_passengers", passenger_count="0")
        add("fractional_passengers", passenger_count="1.5")
        add("missing_pickup", tpep_pickup_datetime=None)
        add("bad_dropoff", tpep_dropoff_datetime="garbage")
        add("backwards", tpep_dropoff_datetime="2023-01-02 07:59:00")
        add("zero_duration", tpep_dropoff_datetime="2023-01-02 08:00:00")
        add("outside_month", tpep_pickup_datetime="2022-12-31 08:00:00")
        add("bad_fare", fare_amount="bad")
        add("overflow_fare", fare_amount="100000000000000000000")
        add("nan_distance", trip_distance="NaN")
        add("infinite_passengers", passenger_count="Infinity")
        add("bad_total", total_amount="-1")
        add("nan_tip", tip_amount="NaN")
        add("negative_tip", tip_amount="-0.001")
        add("multiple", passenger_count="0", trip_distance="0", fare_amount="-1")
        cls.raw = cls.spark.createDataFrame([tuple(row[c] for c in cls.columns) for row in cases],
                                           ", ".join(c + " string" for c in cls.columns))
        cls.marked, cls.warning_codes = mark_rows(cls.raw, "2023-01", {161, 237, 264, 265})
        cls.marked.cache()
        cls.quality = profile_rows(cls.marked, cls.columns, cls.warning_codes)
        cls.clean = clean_projection(cls.marked, cls.columns)
        cls.clean_rows = cls.clean.collect()
        cls.marked_rows = cls.marked.select("case_id", "rejection_reasons", "warning_flags").collect()

    @classmethod
    def tearDownClass(cls):
        cls.marked.unpersist()
        cls.spark.stop()

    def test_conservation_and_expected_cases(self):
        self.assertEqual(self.quality["input_rows"], 25)
        self.assertEqual(self.quality["cleaned_rows"], 7)
        self.assertEqual(self.quality["rejected_rows"], 18)
        self.assertEqual(self.quality["input_rows"], self.quality["cleaned_rows"] + self.quality["rejected_rows"])
        self.assertEqual(sum(r.case_id == "normal" for r in self.clean_rows), 2)

    def test_nullable_tip_and_warning_only_fields(self):
        by_case = {row.case_id: row for row in self.clean_rows}
        self.assertIsNone(by_case["missing_tip"].tip_amount)
        self.assertIn("missing_tip_amount", by_case["missing_tip"].warning_flags)
        self.assertIn("unknown_pickup_zone", by_case["unknown_zone"].warning_flags)
        self.assertIsNone(by_case["unknown_zone"].DOLocationID)
        self.assertIsNone(by_case["optional_bad_money"].extra)
        self.assertIn("invalid_extra", by_case["optional_bad_money"].warning_flags)

    def test_types_rounding_dates_and_duration(self):
        normal = next(r for r in self.clean_rows if r.case_id == "normal")
        self.assertEqual(normal.fare_amount, Decimal("10.01"))
        self.assertEqual(normal.total_amount, Decimal("15.01"))
        self.assertEqual(normal.passenger_count, 1)
        self.assertEqual(normal.pickup_hour, 8)
        self.assertFalse(normal.is_weekend)
        self.assertAlmostEqual(normal.trip_duration_min, 10.0)
        self.assertTrue(next(r for r in self.clean_rows if r.case_id == "zero_fare").is_weekend)
        self.assertAlmostEqual(next(r for r in self.clean_rows if r.case_id == "month_boundary").trip_duration_min, 10.0)
        self.assertNotIn("trip_duration_minutes", self.clean.columns)
        self.assertTrue({"trip_duration_min", "pickup_hour", "pickup_date", "is_weekend"}.issubset(self.clean.columns))
        self.assertEqual(self.clean.schema["fare_amount"].dataType.simpleString(), "decimal(18,2)")

    def test_nonfinite_invalid_and_overlapping_reasons(self):
        by_case = {row.case_id: row for row in self.marked_rows}
        expected = {"nan_distance": "invalid_trip_distance", "infinite_passengers": "invalid_passenger_count",
                    "fractional_passengers": "invalid_passenger_count", "negative_fare": "invalid_fare_amount",
                    "bad_fare": "invalid_fare_amount", "overflow_fare": "invalid_fare_amount",
                    "negative_tip": "invalid_tip_amount", "bad_dropoff": "invalid_dropoff_time"}
        for case, reason in expected.items():
            self.assertIn(reason, by_case[case].rejection_reasons)
        self.assertEqual(set(by_case["multiple"].rejection_reasons),
                         {"invalid_passenger_count", "invalid_trip_distance", "invalid_fare_amount"})
        self.assertGreater(sum(self.quality["rejection_counts"].values()), self.quality["rejected_rows"])

    def test_sequential_before_after_counts(self):
        # Hand counted: the multi-error row is removed at passengers, never counted twice.
        expected = [
            ("invalid_pickup_time", 25, 1, 24),
            ("invalid_dropoff_time", 24, 1, 23),
            ("nonpositive_duration", 23, 2, 21),
            ("pickup_outside_month", 21, 1, 20),
            ("invalid_passenger_count", 20, 4, 16),
            ("invalid_trip_distance", 16, 3, 13),
            ("invalid_fare_amount", 13, 3, 10),
            ("invalid_total_amount", 10, 1, 9),
            ("invalid_tip_amount", 9, 2, 7),
        ]
        actual = [(row["rule"], row["input_rows"], row["removed_rows"], row["output_rows"])
                  for row in self.quality["sequential_rule_counts"]]
        self.assertEqual(actual, expected)
        self.assertEqual(sum(row[2] for row in actual), 18)
        self.assertEqual(self.quality["rejection_counts"]["invalid_trip_distance"], 4)
        self.assertEqual(self.quality["rejection_counts"]["invalid_fare_amount"], 4)

    def test_missing_columns_and_empty_projection(self):
        with self.assertRaisesRegex(ValueError, "Missing source columns"):
            mark_rows(self.raw.drop("fare_amount"), "2023-01", {161})
        empty, _ = mark_rows(self.raw.limit(0), "2023-01", {161, 237})
        projected = clean_projection(empty, self.columns)
        self.assertEqual(projected.count(), 0)
        self.assertEqual(projected.schema["passenger_count"].dataType.simpleString(), "int")
        empty_quality = profile_rows(empty, self.columns, self.warning_codes)
        self.assertEqual(len(empty_quality["sequential_rule_counts"]), 9)
        self.assertTrue(all(row["input_rows"] == row["removed_rows"] == row["output_rows"] == 0
                            for row in empty_quality["sequential_rule_counts"]))


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CleaningRulesTest))
    print("SYNTHETIC FIXTURE: 25 rows; expected clean=7 rejected=18; TLC input not modified.", flush=True)
    sys.exit(0 if result.wasSuccessful() else 1)
