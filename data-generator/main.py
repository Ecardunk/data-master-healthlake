import argparse
import os
import random
from dataclasses import dataclass
from uuid import uuid4

import numpy as np
import pandas as pd
from faker import Faker
from dotenv import load_dotenv

from config.settings import (
    BASE_DIR,
    DATASET_PROFILES,
    OUTPUT_DIR_RAW,
    OUTPUT_DIR_STREAMING,
    RECORD_COUNTS
)

from utils.churn_utils import remove_random_rows
from utils.file_utils import ensure_directories
from utils.metadata_utils import load_metadata, save_metadata
from utils.snapshot_utils import load_previous_snapshot, parse_odate

from generators.attendance_generator import AttendanceGenerator
from generators.base_generator import BaseGenerator
from generators.diseases_generator import DiseaseGenerator
from generators.doctors_generator import DoctorGenerator
from generators.hospitals_generator import HospitalGenerator
from generators.patients_generator import PatientGenerator
from generators.streaming_generator import StreamingEventGenerator
from producers.eventhub_producer import (
    record_to_json,
    send_dataframe_to_eventhub
)


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    generator_type: type[BaseGenerator]
    metadata_key: str
    reference_limits: tuple[tuple[str, str], ...] = ()

    @property
    def file_name(self):
        return f"{self.name}.csv"


DATASET_SPECS = (
    DatasetSpec("hospitals", HospitalGenerator, "hospital_id"),
    DatasetSpec("patients", PatientGenerator, "patient_id"),
    DatasetSpec(
        "doctors",
        DoctorGenerator,
        "doctor_id",
        (("n_hospitals", "hospitals"),)
    ),
    DatasetSpec("diseases", DiseaseGenerator, "disease_id"),
    DatasetSpec(
        "attendance",
        AttendanceGenerator,
        "attendance_id",
        (
            ("n_patients", "patients"),
            ("n_doctors", "doctors"),
            ("n_hospitals", "hospitals"),
            ("n_diseases", "diseases")
        )
    )
)


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


def validate_args(args):
    if args.streaming:
        if args.stream_count <= 0:
            raise ValueError("--stream-count must be greater than zero")
        return

    if not args.odate:
        raise ValueError("--odate is required unless --streaming is used")

    parse_odate(args.odate)


def configure_randomness(seed):
    if seed is None:
        return

    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def quality_kwargs(dataset_name):
    profile = DATASET_PROFILES.get(dataset_name, {})

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
    profile = DATASET_PROFILES.get(dataset_name, {})
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


def temporary_path_for(output_path):
    return output_path.with_name(
        f".{output_path.name}.{uuid4().hex}.tmp"
    )


def write_snapshot(df, output_path):
    df.to_csv(output_path, index=False)


def save_snapshot(df, output_path):
    temp_path = temporary_path_for(output_path)

    try:
        write_snapshot(df, temp_path)
        temp_path.replace(output_path)
    finally:
        temp_path.unlink(missing_ok=True)


def save_snapshots(snapshots, output_dir):
    pending_files = []

    try:
        for spec in DATASET_SPECS:
            output_path = output_dir / spec.file_name
            temp_path = temporary_path_for(output_path)
            pending_files.append((temp_path, output_path))
            write_snapshot(snapshots[spec.name], temp_path)

        for temp_path, output_path in pending_files:
            temp_path.replace(output_path)
    finally:
        for temp_path, _ in pending_files:
            temp_path.unlink(missing_ok=True)


def save_streaming_events(df, output_dir):
    if df.empty:
        raise ValueError("Cannot save an empty streaming events dataset")

    start_event_id = int(df["event_id"].min())
    end_event_id = int(df["event_id"].max())
    file_name = f"streaming_events_{start_event_id}_{end_event_id}.jsonl"
    output_path = output_dir / file_name
    temp_path = temporary_path_for(output_path)

    try:
        with open(temp_path, "w", encoding="utf-8") as file:
            for record in df.to_dict(orient="records"):
                file.write(record_to_json(record))
                file.write("\n")

        temp_path.replace(output_path)
    finally:
        temp_path.unlink(missing_ok=True)

    return output_path


def load_env():
    load_dotenv(BASE_DIR.parent / ".env")


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
        n_records=args.stream_count,
        starting_id=metadata["event_id"],
        n_patients=current_patient_id
    )

    ensure_directories([OUTPUT_DIR_STREAMING])
    output_path = save_streaming_events(
        events_df,
        OUTPUT_DIR_STREAMING
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

    previous_event_id = metadata["event_id"]
    metadata["event_id"] += args.stream_count
    save_metadata(metadata)

    print(
        f"streaming_events: previous_event_id={previous_event_id}, "
        f"new={args.stream_count}, "
        f"last_event_id={metadata['event_id']}"
    )
    print(f"saved_file={output_path}")

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


def calculate_max_ids(metadata):
    return {
        spec.name: metadata[spec.metadata_key] + RECORD_COUNTS[spec.name]
        for spec in DATASET_SPECS
    }


def generate_new_records(spec, metadata, max_ids):
    reference_kwargs = {
        argument_name: max_ids[dataset_name]
        for argument_name, dataset_name in spec.reference_limits
    }

    generator = spec.generator_type(**quality_kwargs(spec.name))

    return generator.generate(
        n_records=RECORD_COUNTS[spec.name],
        starting_id=metadata[spec.metadata_key],
        **reference_kwargs
    )


def generate_snapshots(raw_base_dir, odate, metadata):
    max_ids = calculate_max_ids(metadata)

    return {
        spec.name: build_snapshot(
            spec.name,
            spec.file_name,
            raw_base_dir,
            odate,
            generate_new_records(spec, metadata, max_ids)
        )
        for spec in DATASET_SPECS
    }


def advance_metadata(metadata):
    updated_metadata = metadata.copy()

    for spec in DATASET_SPECS:
        updated_metadata[spec.metadata_key] += RECORD_COUNTS[spec.name]

    return updated_metadata


def main():
    args = parse_args()
    validate_args(args)
    configure_randomness(args.seed)

    if args.streaming:
        run_streaming(args)
        return

    raw_base_dir = OUTPUT_DIR_RAW
    output_dir = raw_base_dir / f"odate={args.odate}"

    ensure_directories([raw_base_dir])
    validate_output_partition(output_dir, args.overwrite)
    ensure_directories([output_dir])

    metadata = load_metadata()

    print("Current metadata:")
    print(metadata)
    print("\nGenerating snapshots:")

    snapshots = generate_snapshots(
        raw_base_dir,
        args.odate,
        metadata
    )
    updated_metadata = advance_metadata(metadata)

    save_snapshots(snapshots, output_dir)
    save_metadata(updated_metadata)

    print("\nMetadata updated successfully")
    print("\n===================================")
    print("All datasets successfully generated")
    print("===================================")


if __name__ == "__main__":
    main()
