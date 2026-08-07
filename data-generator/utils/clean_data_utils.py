from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pandas as pd


DATASET_COLUMNS = {
    "patients": [
        "patient_id",
        "full_name",
        "cpf",
        "email",
        "phone",
        "gender",
        "blood_type",
        "birth_date",
        "city",
        "state",
        "created_at",
    ],
    "hospitals": [
        "hospital_id",
        "hospital_name",
        "hospital_type",
        "state",
        "city",
        "capacity",
        "created_at",
    ],
    "doctors": [
        "doctor_id",
        "doctor_name",
        "crm",
        "specialty",
        "hospital_id",
        "created_at",
    ],
    "diseases": [
        "disease_id",
        "disease_name",
        "category",
        "severity_level",
        "created_at",
    ],
    "attendance": [
        "attendance_id",
        "patient_id",
        "doctor_id",
        "hospital_id",
        "disease_id",
        "attendance_date",
        "wait_time_minutes",
        "cost",
        "severity_score",
        "discharge_flag",
        "created_at",
    ],
}


def _with_contract(dataframe: pd.DataFrame | None, dataset_name: str) -> pd.DataFrame:
    """Return a copy with the source contract, including for empty snapshots."""
    columns = DATASET_COLUMNS[dataset_name]
    result = pd.DataFrame() if dataframe is None else dataframe.copy()

    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA

    return result.loc[:, columns]


def _numeric(dataframe: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(dataframe[column], errors="coerce")


def _timestamp(dataframe: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(dataframe[column], errors="coerce", utc=True).dt.tz_convert(None)


def _normalize_id(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    result = dataframe.copy()
    result[column] = _numeric(result, column)
    result = result.loc[result[column].notna() & result[column].gt(0)].copy()
    result[column] = result[column].astype("int64")
    return result


def _deduplicate(dataframe: pd.DataFrame, key: str) -> pd.DataFrame:
    return dataframe.drop_duplicates(subset=[key], keep="last").reset_index(drop=True)


def _clean_patients(dataframe: pd.DataFrame, reference_time: pd.Timestamp) -> pd.DataFrame:
    result = _normalize_id(dataframe, "patient_id")
    result["gender"] = result["gender"].astype("string").str.strip().str.upper()
    result["state"] = result["state"].astype("string").str.strip().str.upper()
    result["birth_date"] = _timestamp(result, "birth_date")

    valid = (
        result["gender"].isin(["M", "F"])
        & result["state"].str.fullmatch(r"[A-Z]{2}", na=False)
        & result["birth_date"].notna()
        & result["birth_date"].le(reference_time)
    )
    result = result.loc[valid].copy()
    result["birth_date"] = result["birth_date"].dt.date
    return _deduplicate(result, "patient_id")


def _clean_hospitals(dataframe: pd.DataFrame) -> pd.DataFrame:
    result = _normalize_id(dataframe, "hospital_id")
    result["state"] = result["state"].astype("string").str.strip().str.upper()
    result["capacity"] = _numeric(result, "capacity")

    valid = (
        result["state"].str.fullmatch(r"[A-Z]{2}", na=False)
        & result["capacity"].between(1, 2000, inclusive="both")
    )
    result = result.loc[valid].copy()
    result["capacity"] = result["capacity"].astype("int64")
    return _deduplicate(result, "hospital_id")


def _clean_doctors(dataframe: pd.DataFrame, hospital_ids: set[int]) -> pd.DataFrame:
    result = _normalize_id(dataframe, "doctor_id")
    result["hospital_id"] = _numeric(result, "hospital_id")
    result["crm"] = _numeric(result, "crm")

    valid = (
        result["hospital_id"].isin(hospital_ids)
        & result["crm"].notna()
        & result["crm"].gt(0)
    )
    result = result.loc[valid].copy()
    result["hospital_id"] = result["hospital_id"].astype("int64")
    result["crm"] = result["crm"].astype("int64")
    return _deduplicate(result, "doctor_id")


def _clean_diseases(dataframe: pd.DataFrame) -> pd.DataFrame:
    result = _normalize_id(dataframe, "disease_id")
    result["severity_level"] = _numeric(result, "severity_level")
    result = result.loc[
        result["severity_level"].between(1, 5, inclusive="both")
    ].copy()
    result["severity_level"] = result["severity_level"].astype("int64")
    return _deduplicate(result, "disease_id")


def _nullable_in_range(
    dataframe: pd.DataFrame,
    column: str,
    minimum: float,
    maximum: float | None = None,
) -> None:
    values = _numeric(dataframe, column)
    valid = values.ge(minimum)
    if maximum is not None:
        valid &= values.le(maximum)
    dataframe[column] = values.where(values.isna() | valid)


def _clean_attendance(
    dataframe: pd.DataFrame,
    patients: pd.DataFrame,
    doctors: pd.DataFrame,
    hospitals: pd.DataFrame,
    diseases: pd.DataFrame,
    reference_time: pd.Timestamp,
) -> pd.DataFrame:
    result = _normalize_id(dataframe, "attendance_id")

    for column in ["patient_id", "doctor_id", "hospital_id", "disease_id"]:
        result[column] = _numeric(result, column)

    result["attendance_date"] = _timestamp(result, "attendance_date")
    result["severity_score"] = _numeric(result, "severity_score")
    _nullable_in_range(result, "wait_time_minutes", 0, 300)
    _nullable_in_range(result, "cost", 0)

    discharge_flag = _numeric(result, "discharge_flag")
    result["discharge_flag"] = discharge_flag.where(
        discharge_flag.isna() | discharge_flag.isin([0, 1])
    )

    patient_ids = set(patients["patient_id"].tolist())
    doctor_ids = set(doctors["doctor_id"].tolist())
    disease_ids = set(diseases["disease_id"].tolist())
    hospital_ids = set(hospitals["hospital_id"].tolist())

    valid = (
        result["patient_id"].isin(patient_ids)
        & result["doctor_id"].isin(doctor_ids)
        & result["disease_id"].isin(disease_ids)
        & result["attendance_date"].notna()
        & result["attendance_date"].le(reference_time)
        & result["severity_score"].between(1, 5, inclusive="both")
    )
    result = result.loc[valid].copy()

    # A valid doctor always belongs to a valid hospital. Aligning attendance
    # to that mapping prevents a clean snapshot from carrying an inconsistent
    # doctor/hospital relationship inherited from a previous chaos snapshot.
    doctor_hospitals = doctors.set_index("doctor_id")["hospital_id"]
    result["hospital_id"] = result["doctor_id"].map(doctor_hospitals)
    result = result.loc[result["hospital_id"].isin(hospital_ids)].copy()

    for column in [
        "patient_id",
        "doctor_id",
        "hospital_id",
        "disease_id",
        "severity_score",
    ]:
        result[column] = result[column].astype("int64")

    return _deduplicate(result, "attendance_id")


def sanitize_clean_snapshots(
    snapshots: Mapping[str, pd.DataFrame],
    reference_time: datetime | pd.Timestamp | None = None,
) -> dict[str, pd.DataFrame]:
    """Sanitize a complete snapshot for fail-closed DQ validation.

    Invalid retained rows are removed, optional numeric values that violate
    nullable DQ constraints are converted to null, and attendance references
    are reconciled with the valid dimension snapshots.
    """
    current_time = pd.Timestamp.now(tz="UTC") if reference_time is None else pd.Timestamp(reference_time)
    if current_time.tzinfo is not None:
        current_time = current_time.tz_convert(None)

    contracted = {
        dataset_name: _with_contract(snapshots.get(dataset_name), dataset_name)
        for dataset_name in DATASET_COLUMNS
    }
    patients = _clean_patients(contracted["patients"], current_time)
    hospitals = _clean_hospitals(contracted["hospitals"])
    diseases = _clean_diseases(contracted["diseases"])
    doctors = _clean_doctors(
        contracted["doctors"],
        set(hospitals["hospital_id"].tolist()),
    )
    attendance = _clean_attendance(
        contracted["attendance"],
        patients,
        doctors,
        hospitals,
        diseases,
        current_time,
    )

    return {
        "hospitals": hospitals,
        "patients": patients,
        "doctors": doctors,
        "diseases": diseases,
        "attendance": attendance,
    }
