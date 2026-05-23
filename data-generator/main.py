import argparse
import random

import numpy as np
import pandas as pd
from faker import Faker

from config.settings import (
    DATASET_PROFILES,
    N_ATTENDANCE,
    N_DISEASES,
    N_DOCTORS,
    N_HOSPITALS,
    N_PATIENTS,
    OUTPUT_DIR_RAW
)

from utils.churn_utils import remove_random_rows
from utils.file_utils import ensure_directories
from utils.metadata_utils import load_metadata, save_metadata
from utils.snapshot_utils import load_previous_snapshot, parse_odate

from generators.attendance_generator import AttendanceGenerator
from generators.diseases_generator import DiseaseGenerator
from generators.doctors_generator import DoctorGenerator
from generators.hospitals_generator import HospitalGenerator
from generators.patients_generator import PatientGenerator


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--odate",
        required=True,
        help="Logical processing date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional seed for reproducible synthetic data generation"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing odate partition"
    )

    return parser.parse_args()


def validate_odate(odate):
    parse_odate(odate)


def configure_randomness(seed):
    if seed is None:
        return

    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def profile_for(dataset_name):
    return DATASET_PROFILES.get(dataset_name, {})


def quality_kwargs(dataset_name):
    profile = profile_for(dataset_name)

    return {
        "null_percentages": profile.get("null_percentages", {}),
        "duplicate_percentage": profile.get("duplicate_percentage", 0)
    }


def build_snapshot(
    dataset_name,
    file_name,
    raw_base_dir,
    odate,
    new_records
):
    profile = profile_for(dataset_name)
    previous_snapshot = load_previous_snapshot(
        raw_base_dir,
        odate,
        file_name
    )

    if previous_snapshot is None:
        previous_count = 0
        retained_snapshot = None
    else:
        previous_count = len(previous_snapshot)
        retained_snapshot = remove_random_rows(
            previous_snapshot,
            profile.get("churn_percentage", 0)
        )

    snapshot_parts = [
        part
        for part in [retained_snapshot, new_records]
        if part is not None and not part.empty
    ]

    if not snapshot_parts:
        snapshot = pd.DataFrame()
    else:
        snapshot = pd.concat(
            snapshot_parts,
            ignore_index=True
        )

    retained_count = 0 if retained_snapshot is None else len(retained_snapshot)
    new_count = len(new_records)
    churn_count = previous_count - retained_count

    print(
        f"{dataset_name}: previous={previous_count}, "
        f"churn={churn_count}, new={new_count}, output={len(snapshot)}"
    )

    return snapshot


def save_snapshot(df, output_dir, file_name):
    df.to_csv(
        output_dir / file_name,
        index=False
    )


def validate_output_partition(output_dir, overwrite):
    existing_files = list(output_dir.glob("*.csv"))

    if existing_files and not overwrite:
        raise FileExistsError(
            f"Partition {output_dir} already has generated files. "
            "Use --overwrite to replace it."
        )


def main():
    args = parse_args()
    validate_odate(args.odate)
    configure_randomness(args.seed)

    raw_base_dir = OUTPUT_DIR_RAW
    output_dir = raw_base_dir / f"odate={args.odate}"

    ensure_directories([
        raw_base_dir,
        output_dir
    ])
    validate_output_partition(output_dir, args.overwrite)

    metadata = load_metadata()

    print("Current metadata:")
    print(metadata)
    print("\nGenerating snapshots:")

    hospital_df = build_snapshot(
        "hospitals",
        "hospitals.csv",
        raw_base_dir,
        args.odate,
        HospitalGenerator(**quality_kwargs("hospitals")).generate(
            N_HOSPITALS,
            metadata["hospital_id"]
        )
    )

    patient_df = build_snapshot(
        "patients",
        "patients.csv",
        raw_base_dir,
        args.odate,
        PatientGenerator(**quality_kwargs("patients")).generate(
            N_PATIENTS,
            metadata["patient_id"]
        )
    )

    doctor_df = build_snapshot(
        "doctors",
        "doctors.csv",
        raw_base_dir,
        args.odate,
        DoctorGenerator(**quality_kwargs("doctors")).generate(
            N_DOCTORS,
            metadata["hospital_id"] + N_HOSPITALS,
            metadata["doctor_id"]
        )
    )

    disease_df = build_snapshot(
        "diseases",
        "diseases.csv",
        raw_base_dir,
        args.odate,
        DiseaseGenerator(**quality_kwargs("diseases")).generate(
            N_DISEASES,
            metadata["disease_id"]
        )
    )

    attendance_df = build_snapshot(
        "attendance",
        "attendance.csv",
        raw_base_dir,
        args.odate,
        AttendanceGenerator(**quality_kwargs("attendance")).generate(
            N_ATTENDANCE,
            metadata["patient_id"] + N_PATIENTS,
            metadata["doctor_id"] + N_DOCTORS,
            metadata["hospital_id"] + N_HOSPITALS,
            metadata["disease_id"] + N_DISEASES,
            metadata["attendance_id"]
        )
    )

    metadata["patient_id"] += N_PATIENTS
    metadata["doctor_id"] += N_DOCTORS
    metadata["hospital_id"] += N_HOSPITALS
    metadata["disease_id"] += N_DISEASES
    metadata["attendance_id"] += N_ATTENDANCE

    save_metadata(metadata)

    save_snapshot(hospital_df, output_dir, "hospitals.csv")
    save_snapshot(patient_df, output_dir, "patients.csv")
    save_snapshot(doctor_df, output_dir, "doctors.csv")
    save_snapshot(disease_df, output_dir, "diseases.csv")
    save_snapshot(attendance_df, output_dir, "attendance.csv")

    print("\nMetadata updated successfully")
    print("\n===================================")
    print("All datasets successfully generated")
    print("===================================")


if __name__ == "__main__":
    main()
