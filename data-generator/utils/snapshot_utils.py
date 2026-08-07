from pathlib import Path
from datetime import datetime

import pandas as pd


PARTITION_PREFIX = "odate="


def parse_odate(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_partition_date(partition_path: Path):
    if not partition_path.name.startswith(PARTITION_PREFIX):
        return None

    partition_value = partition_path.name.removeprefix(PARTITION_PREFIX)

    try:
        return parse_odate(partition_value)
    except ValueError:
        return None


def load_previous_snapshot(
    base_path: Path,
    current_odate: str,
    file_name: str
):
    current_date = parse_odate(current_odate)

    if not base_path.exists():
        return None

    previous_partitions = [
        (partition_date, path)
        for path in base_path.iterdir()
        if path.is_dir()
        for partition_date in [parse_partition_date(path)]
        if partition_date is not None and partition_date < current_date
    ]

    if not previous_partitions:
        return None

    latest_partition = max(previous_partitions, key=lambda item: item[0])[1]

    snapshot_file = (
        latest_partition /
        file_name
    )

    if not snapshot_file.exists():
        return None

    return pd.read_csv(snapshot_file)
