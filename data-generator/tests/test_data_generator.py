import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


DATA_GENERATOR_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(DATA_GENERATOR_DIR))

from main import build_snapshot
from utils.dirty_data_utils import inject_duplicates, inject_nulls
from utils.snapshot_utils import load_previous_snapshot, parse_odate


class DirtyDataUtilsTest(unittest.TestCase):

    def test_inject_nulls_applies_column_specific_percentages(self):
        df = pd.DataFrame({
            "patient_id": [1, 2, 3],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "phone": ["1", "2", "3"]
        })

        result = inject_nulls(
            df,
            column_percentages={
                "email": 1.0,
                "phone": 0.0
            },
            excluded_columns=["patient_id"]
        )

        self.assertEqual(result["email"].isna().sum(), 3)
        self.assertEqual(result["phone"].isna().sum(), 0)
        self.assertEqual(result["patient_id"].isna().sum(), 0)

    def test_inject_duplicates_keeps_schema_when_percentage_is_zero(self):
        df = pd.DataFrame({
            "id": [1, 2],
            "value": ["a", "b"]
        })

        result = inject_duplicates(df, 0)

        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), ["id", "value"])


class SnapshotUtilsTest(unittest.TestCase):

    def test_load_previous_snapshot_returns_latest_valid_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            old_partition = base_path / "odate=2026-05-20"
            latest_partition = base_path / "odate=2026-05-22"
            ignored_partition = base_path / "invalid=2026-05-23"

            old_partition.mkdir()
            latest_partition.mkdir()
            ignored_partition.mkdir()

            pd.DataFrame({"id": [1]}).to_csv(
                old_partition / "patients.csv",
                index=False
            )
            pd.DataFrame({"id": [2]}).to_csv(
                latest_partition / "patients.csv",
                index=False
            )

            result = load_previous_snapshot(
                base_path,
                "2026-05-23",
                "patients.csv"
            )

            self.assertEqual(result["id"].tolist(), [2])

    def test_parse_odate_rejects_invalid_format(self):
        with self.assertRaises(ValueError):
            parse_odate("20260523")


class BuildSnapshotTest(unittest.TestCase):

    def test_build_snapshot_applies_churn_before_appending_new_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            previous_partition = base_path / "odate=2026-05-20"
            previous_partition.mkdir()

            previous = pd.DataFrame({
                "patient_id": range(1, 101),
                "email": [f"user{i}@test.com" for i in range(1, 101)]
            })
            previous.to_csv(
                previous_partition / "patients.csv",
                index=False
            )

            new_records = pd.DataFrame({
                "patient_id": range(101, 121),
                "email": [f"user{i}@test.com" for i in range(101, 121)]
            })

            result = build_snapshot(
                "patients",
                "patients.csv",
                base_path,
                "2026-05-21",
                new_records
            )

            self.assertEqual(len(result), 119)
            self.assertEqual(result["patient_id"].max(), 120)
            self.assertEqual(
                set(new_records["patient_id"]),
                set(result["patient_id"]).intersection(new_records["patient_id"])
            )


if __name__ == "__main__":
    unittest.main()
