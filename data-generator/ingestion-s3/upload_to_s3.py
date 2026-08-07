import argparse
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from config.settings import OUTPUT_DIR_RAW
from utils.snapshot_utils import parse_odate


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--odate",
        required=True,
        help="Logical partition date to upload in YYYY-MM-DD format"
    )

    return parser.parse_args()


def build_s3_client(aws_region):
    client_kwargs = {}

    if aws_region:
        client_kwargs["region_name"] = aws_region

    return boto3.client("s3", **client_kwargs)


def upload_file(s3_client, bucket_name, local_file_path, s3_key):
    print(
        f"Uploading {local_file_path.name} -> "
        f"s3://{bucket_name}/{s3_key}"
    )

    s3_client.upload_file(
        str(local_file_path),
        bucket_name,
        s3_key
    )


def main():
    args = parse_args()
    parse_odate(args.odate)

    load_dotenv()
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_region = os.getenv("AWS_REGION")
    s3_client = build_s3_client(aws_region)

    if not s3_bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is required")

    partition_path = OUTPUT_DIR_RAW / f"odate={args.odate}"

    if not partition_path.exists():
        raise FileNotFoundError(
            f"Partition not found: {partition_path}"
        )

    csv_files = sorted(partition_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in partition: {partition_path}"
        )

    for file_path in csv_files:
        dataset_name = file_path.stem
        s3_key = (
            f"raw/{dataset_name}/"
            f"odate={args.odate}/"
            f"{file_path.name}"
        )

        upload_file(
            s3_client,
            s3_bucket_name,
            file_path,
            s3_key
        )

    print("\n===================================")
    print("All files uploaded successfully")
    print("===================================")


if __name__ == "__main__":
    main()
