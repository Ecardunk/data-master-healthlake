import json
from datetime import date, datetime

import pandas as pd


def serialize_value(value):
    if value is None or pd.isna(value):
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

    sent_count = 0

    with EventHubProducerClient.from_connection_string(
        conn_str=connection_str,
        eventhub_name=eventhub_name
    ) as producer:
        event_batch = producer.create_batch()

        for record in df.to_dict(orient="records"):
            event_data = EventData(record_to_json(record))

            try:
                event_batch.add(event_data)
            except ValueError:
                if len(event_batch) == 0:
                    raise ValueError(
                        "A single event exceeds the Event Hubs batch size limit"
                    )

                producer.send_batch(event_batch)
                sent_count += len(event_batch)

                event_batch = producer.create_batch()
                event_batch.add(event_data)

        if len(event_batch) > 0:
            producer.send_batch(event_batch)
            sent_count += len(event_batch)

    return sent_count
