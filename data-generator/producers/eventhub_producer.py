import json
import math
from datetime import date, datetime

import pandas as pd


def serialize_value(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    if pd.isna(value):
        return None

    if isinstance(value, (datetime, date)):
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


def send_dataframe_to_eventhub(
    df,
    connection_str,
    eventhub_name
):
    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise ImportError(
            "Missing dependency azure-eventhub. "
            "Install data-generator/requirements.txt before sending events."
        ) from exc

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_str,
        eventhub_name=eventhub_name
    )

    sent_count = 0
    event_batch = producer.create_batch()

    with producer:
        for record in df.to_dict(orient="records"):
            event_data = EventData(record_to_json(record))

            try:
                event_batch.add(event_data)
            except ValueError:
                producer.send_batch(event_batch)
                sent_count += len(event_batch)

                event_batch = producer.create_batch()
                event_batch.add(event_data)

        if len(event_batch) > 0:
            producer.send_batch(event_batch)
            sent_count += len(event_batch)

    return sent_count
