import json
from datetime import date, datetime, timezone

import pandas as pd


def serialize_value(value):
    if value is None or pd.isna(value):
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


def record_to_json(record):
    clean_record = {
        key: serialize_value(value)
        for key, value in record.items()
    }

    return json.dumps(
        clean_record,
        ensure_ascii=False
    )


def _eventhub_types(producer_client_type, event_data_type):
    if producer_client_type is not None and event_data_type is not None:
        return producer_client_type, event_data_type

    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise ImportError(
            "Missing dependency azure-eventhub. "
            "Install data-generator/requirements.txt before sending events."
        ) from exc

    return (
        producer_client_type or EventHubProducerClient,
        event_data_type or EventData,
    )


def _default_credential_factory():
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        raise ImportError(
            "Missing dependency azure-identity. "
            "Install data-generator/requirements.txt before sending events."
        ) from exc

    return DefaultAzureCredential()


def _send_patient_records(
    producer,
    records,
    patient_partition_key,
    event_data_type,
):
    sent_count = 0
    event_batch = producer.create_batch(
        partition_key=patient_partition_key
    )

    for record in records:
        event_data = event_data_type(record_to_json(record))

        try:
            event_batch.add(event_data)
        except ValueError:
            if len(event_batch) == 0:
                raise ValueError(
                    "A single event exceeds the Event Hubs batch size limit"
                )

            # send_batch retries transient failures in the Azure SDK. The
            # stable event_id makes a possible duplicate safe to identify.
            producer.send_batch(event_batch)
            sent_count += len(event_batch)

            event_batch = producer.create_batch(
                partition_key=patient_partition_key
            )
            try:
                event_batch.add(event_data)
            except ValueError as exc:
                raise ValueError(
                    "A single event exceeds the Event Hubs batch size limit"
                ) from exc

    if len(event_batch) > 0:
        producer.send_batch(event_batch)
        sent_count += len(event_batch)

    return sent_count


def send_dataframe_to_eventhub(
    df,
    fully_qualified_namespace,
    eventhub_name,
    *,
    credential=None,
    producer_client_type=None,
    event_data_type=None,
):
    if df.empty:
        return 0
    if "patient_id" not in df.columns:
        raise ValueError("patient_id is required for Event Hubs partitioning")
    if df["patient_id"].isna().any():
        raise ValueError("patient_id cannot be null for Event Hubs partitioning")

    namespace = (fully_qualified_namespace or "").strip()
    hub_name = (eventhub_name or "").strip()
    if not namespace:
        raise ValueError("fully_qualified_namespace is required")
    if not hub_name:
        raise ValueError("eventhub_name is required")

    producer_client_type, event_data_type = _eventhub_types(
        producer_client_type,
        event_data_type,
    )
    owns_credential = credential is None
    active_credential = (
        _default_credential_factory()
        if owns_credential
        else credential
    )
    sent_count = 0

    try:
        with producer_client_type(
            fully_qualified_namespace=namespace,
            eventhub_name=hub_name,
            credential=active_credential,
        ) as producer:
            for patient_id, patient_events in df.groupby(
                "patient_id",
                sort=False,
            ):
                sent_count += _send_patient_records(
                    producer=producer,
                    records=patient_events.to_dict(orient="records"),
                    patient_partition_key=str(patient_id),
                    event_data_type=event_data_type,
                )
    finally:
        if owns_credential and hasattr(active_credential, "close"):
            active_credential.close()

    return sent_count
