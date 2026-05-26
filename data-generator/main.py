import argparse
import os
import random
from pathlib import Path

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
from generators.streaming_generator import StreamingEventGenerator
from producers.eventhub_producer import send_dataframe_to_eventhub


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--odate",
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
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Generate streaming vital sign events instead of raw snapshots"
    )
    parser.add_argument(
        "--stream-count",
        type=int,
        default=100,
        help="Number of streaming events to generate"
    )
    parser.add_argument(
        "--send-eventhub",
        action="store_true",
        help="Send generated streaming events to Azure Event Hubs"
    )
    parser.add_argument(
        "--eventhub-name",
        default=None,
        help="Event Hub name. Defaults to EVENTHUB_NAME from .env"
    )
    parser.add_argument(
        "--eventhub-connection-str",
        default=None,
        help="Event Hub connection string. Defaults to EVENTHUB_CONNECTION_STR from .env"
    )

    return parser.parse_args()


def validate_odate(odate):
    parse_odate(odate)


def validate_args(args):
    if args.streaming:
        if args.stream_count <= 0:
            raise ValueError("--stream-count must be greater than zero")
        return

    if not args.odate:
        raise ValueError("--odate is required unless --streaming is used")

    validate_odate(args.odate)


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


def load_env():
    try:
        from dotenv import load_dotenv

        load_dotenv()
        return
    except ImportError:
        pass

    env_paths = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[1] / ".env"
    ]

    for env_path in env_paths:
        if not env_path.exists():
            continue

        with open(env_path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                os.environ.setdefault(
                    key.strip(),
                    value.strip()
                )


def resolve_eventhub_config(args):
    load_env()

    connection_str = (
        args.eventhub_connection_str
        or os.getenv("EVENTHUB_CONNECTION_STR")
        or ""
    ).strip()
    eventhub_name = (
        args.eventhub_name
        or os.getenv("EVENTHUB_NAME")
        or ""
    ).strip()

    if not connection_str:
        raise ValueError(
            "Event Hub connection string is required. "
            "Set EVENTHUB_CONNECTION_STR in .env or pass --eventhub-connection-str."
        )

    if not eventhub_name and "EntityPath=" not in connection_str:
        raise ValueError(
            "Event Hub name is required. "
            "Set EVENTHUB_NAME in .env, pass --eventhub-name, "
            "or use an event hub-level connection string with EntityPath."
        )

    return connection_str, eventhub_name or None


def run_streaming(args):
    metadata = load_metadata()
    metadata.setdefault("event_id", 0)

    current_patient_id = metadata.get("patient_id", 0)

    if current_patient_id <= 0:
        raise ValueError(
            "patient_id in id_control must be greater than zero "
            "before generating streaming events."
        )

    print("Current metadata:")
    print(metadata)
    print("\nGenerating streaming events:")

    events_df = StreamingEventGenerator().generate(
        args.stream_count,
        current_patient_id,
        metadata["event_id"]
    )

    metadata["event_id"] += args.stream_count
    save_metadata(metadata)

    print(
        f"streaming_events: previous_event_id="
        f"{metadata['event_id'] - args.stream_count}, "
        f"new={args.stream_count}, "
        f"last_event_id={metadata['event_id']}"
    )

    if args.send_eventhub:
        connection_str, eventhub_name = resolve_eventhub_config(args)
        sent_count = send_dataframe_to_eventhub(
            events_df,
            connection_str,
            eventhub_name
        )
        target_name = eventhub_name or "connection string EntityPath"
        print(f"\nSent {sent_count} events to Event Hub '{target_name}'")

    print("\nMetadata updated successfully")
    print("\n===================================")
    print("Streaming events successfully generated")
    print("===================================")


def validate_output_partition(output_dir, overwrite):
    existing_files = list(output_dir.glob("*.csv"))

    if existing_files and not overwrite:
        raise FileExistsError(
            f"Partition {output_dir} already has generated files. "
            "Use --overwrite to replace it."
        )


def main():
    args = parse_args()
    validate_args(args)
    configure_randomness(args.seed)

    if args.streaming:
        run_streaming(args)
        return

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
