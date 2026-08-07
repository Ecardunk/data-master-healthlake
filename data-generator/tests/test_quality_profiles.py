import sys

import pandas as pd
import pytest

import main as generator_main
from utils.clean_data_utils import DATASET_COLUMNS, sanitize_clean_snapshots


def dirty_snapshots():
    return {
        "patients": pd.DataFrame(
            [
                {
                    "patient_id": 1,
                    "full_name": "Patient One",
                    "cpf": "111.111.111-11",
                    "email": "one@example.com",
                    "phone": "11999999999",
                    "gender": " m ",
                    "blood_type": "O+",
                    "birth_date": "1990-01-01",
                    "city": "Sao Paulo",
                    "state": " sp ",
                    "created_at": "2025-01-01 10:00:00",
                },
                {
                    "patient_id": 1,
                    "full_name": "Patient One Duplicate",
                    "cpf": "111.111.111-11",
                    "email": "duplicate@example.com",
                    "phone": "11999999999",
                    "gender": "M",
                    "blood_type": "O+",
                    "birth_date": "1990-01-01",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "created_at": "2025-01-02 10:00:00",
                },
                {
                    "patient_id": 2,
                    "full_name": "Future Patient",
                    "cpf": "222.222.222-22",
                    "email": "future@example.com",
                    "phone": "11988888888",
                    "gender": "F",
                    "blood_type": "A+",
                    "birth_date": "2999-01-01",
                    "city": "Rio de Janeiro",
                    "state": "RJ",
                    "created_at": "2025-01-01 10:00:00",
                },
            ]
        ),
        "hospitals": pd.DataFrame(
            [
                {
                    "hospital_id": 10,
                    "hospital_name": "Hospital Valid",
                    "hospital_type": "Public",
                    "state": " sp ",
                    "city": "Sao Paulo",
                    "capacity": 100,
                    "created_at": "2025-01-01 10:00:00",
                },
                {
                    "hospital_id": 11,
                    "hospital_name": "Hospital Invalid",
                    "hospital_type": "Public",
                    "state": "SP",
                    "city": "Sao Paulo",
                    "capacity": 0,
                    "created_at": "2025-01-01 10:00:00",
                },
            ]
        ),
        "doctors": pd.DataFrame(
            [
                {
                    "doctor_id": 20,
                    "doctor_name": "Doctor Valid",
                    "crm": 12345,
                    "specialty": "Cardiology",
                    "hospital_id": 10,
                    "created_at": "2025-01-01 10:00:00",
                },
                {
                    "doctor_id": 21,
                    "doctor_name": "Doctor Orphan",
                    "crm": 54321,
                    "specialty": "Neurology",
                    "hospital_id": 11,
                    "created_at": "2025-01-01 10:00:00",
                },
            ]
        ),
        "diseases": pd.DataFrame(
            [
                {
                    "disease_id": 30,
                    "disease_name": "Valid Disease",
                    "category": "Respiratory",
                    "severity_level": 3,
                    "created_at": "2025-01-01 10:00:00",
                },
                {
                    "disease_id": 31,
                    "disease_name": "Invalid Disease",
                    "category": "Respiratory",
                    "severity_level": 9,
                    "created_at": "2025-01-01 10:00:00",
                },
            ]
        ),
        "attendance": pd.DataFrame(
            [
                {
                    "attendance_id": 100,
                    "patient_id": 1,
                    "doctor_id": 20,
                    "hospital_id": 999,
                    "disease_id": 30,
                    "attendance_date": "2025-02-01 10:00:00",
                    "wait_time_minutes": 999,
                    "cost": -1,
                    "severity_score": 3,
                    "discharge_flag": 7,
                    "created_at": "2025-02-01 10:00:00",
                },
                {
                    "attendance_id": 100,
                    "patient_id": 1,
                    "doctor_id": 20,
                    "hospital_id": 999,
                    "disease_id": 30,
                    "attendance_date": "2025-02-01 10:00:00",
                    "wait_time_minutes": 999,
                    "cost": -1,
                    "severity_score": 3,
                    "discharge_flag": 7,
                    "created_at": "2025-02-01 10:00:00",
                },
                {
                    "attendance_id": 101,
                    "patient_id": 2,
                    "doctor_id": 20,
                    "hospital_id": 10,
                    "disease_id": 30,
                    "attendance_date": "2025-02-01 10:00:00",
                    "wait_time_minutes": 20,
                    "cost": 100,
                    "severity_score": 2,
                    "discharge_flag": 1,
                    "created_at": "2025-02-01 10:00:00",
                },
                {
                    "attendance_id": 102,
                    "patient_id": 1,
                    "doctor_id": 21,
                    "hospital_id": 11,
                    "disease_id": 30,
                    "attendance_date": "2025-02-01 10:00:00",
                    "wait_time_minutes": 20,
                    "cost": 100,
                    "severity_score": 2,
                    "discharge_flag": 1,
                    "created_at": "2025-02-01 10:00:00",
                },
                {
                    "attendance_id": 103,
                    "patient_id": 1,
                    "doctor_id": 20,
                    "hospital_id": 10,
                    "disease_id": 31,
                    "attendance_date": "2025-02-01 10:00:00",
                    "wait_time_minutes": 20,
                    "cost": 100,
                    "severity_score": 2,
                    "discharge_flag": 1,
                    "created_at": "2025-02-01 10:00:00",
                },
            ]
        ),
    }


def test_cli_defaults_to_chaos_and_accepts_clean(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--odate", "2026-08-07"])
    assert generator_main.parse_args().profile == "chaos"

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--odate", "2026-08-07", "--profile", "clean"],
    )
    assert generator_main.parse_args().profile == "clean"


def test_clean_profile_disables_new_anomalies():
    clean = generator_main.quality_kwargs("patients", "clean")
    chaos = generator_main.quality_kwargs("patients", "chaos")

    assert clean == {"null_percentages": {}, "duplicate_percentage": 0}
    assert chaos["null_percentages"]
    assert chaos["duplicate_percentage"] > 0


def test_clean_snapshot_satisfies_rules_and_referential_integrity():
    cleaned = sanitize_clean_snapshots(
        dirty_snapshots(),
        reference_time="2026-01-01 00:00:00",
    )

    assert cleaned["patients"]["patient_id"].tolist() == [1]
    assert cleaned["patients"].iloc[0]["gender"] == "M"
    assert cleaned["patients"].iloc[0]["state"] == "SP"
    assert cleaned["hospitals"]["hospital_id"].tolist() == [10]
    assert cleaned["hospitals"].iloc[0]["state"] == "SP"
    assert cleaned["doctors"]["doctor_id"].tolist() == [20]
    assert cleaned["diseases"]["disease_id"].tolist() == [30]

    attendance = cleaned["attendance"]
    assert attendance["attendance_id"].tolist() == [100]
    assert attendance.iloc[0]["hospital_id"] == 10
    assert pd.isna(attendance.iloc[0]["wait_time_minutes"])
    assert pd.isna(attendance.iloc[0]["cost"])
    assert pd.isna(attendance.iloc[0]["discharge_flag"])

    assert set(attendance["patient_id"]).issubset(set(cleaned["patients"]["patient_id"]))
    assert set(attendance["doctor_id"]).issubset(set(cleaned["doctors"]["doctor_id"]))
    assert set(attendance["hospital_id"]).issubset(set(cleaned["hospitals"]["hospital_id"]))
    assert set(attendance["disease_id"]).issubset(set(cleaned["diseases"]["disease_id"]))
    assert all(not table[key].duplicated().any() for table, key in [
        (cleaned["patients"], "patient_id"),
        (cleaned["hospitals"], "hospital_id"),
        (cleaned["doctors"], "doctor_id"),
        (cleaned["diseases"], "disease_id"),
        (cleaned["attendance"], "attendance_id"),
    ])


def test_generate_snapshots_cleans_retained_rows_only_for_clean_profile(
    tmp_path,
    monkeypatch,
):
    previous_partition = tmp_path / "odate=2026-01-01"
    previous_partition.mkdir()
    snapshots = dirty_snapshots()
    for dataset_name, dataframe in snapshots.items():
        dataframe.to_csv(previous_partition / f"{dataset_name}.csv", index=False)

    monkeypatch.setattr(
        generator_main,
        "RECORD_COUNTS",
        {dataset_name: 0 for dataset_name in DATASET_COLUMNS},
    )
    metadata = {
        "patient_id": 2,
        "hospital_id": 11,
        "doctor_id": 21,
        "disease_id": 31,
        "attendance_id": 103,
    }

    clean = generator_main.generate_snapshots(
        tmp_path,
        "2026-02-01",
        metadata,
        "clean",
    )
    chaos = generator_main.generate_snapshots(
        tmp_path,
        "2026-02-01",
        metadata,
        "chaos",
    )

    assert clean["patients"]["patient_id"].is_unique
    assert clean["attendance"]["attendance_id"].is_unique
    assert not chaos["patients"]["patient_id"].is_unique
    assert not chaos["attendance"]["attendance_id"].is_unique


def test_clean_empty_snapshot_keeps_csv_contract_headers():
    cleaned = sanitize_clean_snapshots(
        {},
        reference_time="2026-01-01 00:00:00",
    )

    for dataset_name, columns in DATASET_COLUMNS.items():
        assert cleaned[dataset_name].empty
        assert cleaned[dataset_name].columns.tolist() == columns


def test_existing_partition_is_not_overwritten_by_default(tmp_path):
    partition = tmp_path / "odate=2026-08-07"
    partition.mkdir()
    (partition / "patients.csv").write_text("patient_id\n1\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        generator_main.validate_output_partition(partition, overwrite=False)
