import json
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest

import main as generator_main
import producers.eventhub_producer as eventhub_producer
from generators.streaming_generator import StreamingEventGenerator
from producers.eventhub_producer import (
    record_to_json,
    send_dataframe_to_eventhub,
)


CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts"
    / "vital_event.v1.schema.json"
)


def generate_events(count=8):
    event_ids = iter(
        UUID(f"00000000-0000-4000-8000-{index:012d}")
        for index in range(1, count + 1)
    )
    generator = StreamingEventGenerator(
        clock=lambda: datetime(
            2026,
            8,
            9,
            12,
            30,
            tzinfo=timezone.utc,
        ),
        event_id_factory=lambda: next(event_ids),
    )

    return generator.generate(
        n_records=count,
        n_patients=10,
        producer_run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )


def test_generated_events_follow_versioned_contract():
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    events = generate_events()

    assert events.columns.tolist() == contract["required"]
    assert contract["additionalProperties"] is False
    assert events["schema_version"].eq(1).all()
    assert events["event_type"].eq("patient_vital_signs").all()
    assert events["source"].eq("healthlake-synthetic-generator").all()
    assert events["event_id"].is_unique
    assert events["producer_run_id"].eq(
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    ).all()

    for record in events.to_dict(orient="records"):
        assert str(UUID(record["event_id"])) == record["event_id"]
        assert record["event_time"].endswith("Z")
        assert record["produced_at"].endswith("Z")
        assert datetime.fromisoformat(
            record["event_time"].replace("Z", "+00:00")
        ) <= datetime.fromisoformat(
            record["produced_at"].replace("Z", "+00:00")
        )
        assert (
            datetime.fromisoformat(
                record["produced_at"].replace("Z", "+00:00")
            )
            - datetime.fromisoformat(
                record["event_time"].replace("Z", "+00:00")
            )
        ).total_seconds() <= 5 * 60
        assert (
            record["blood_pressure_systolic_mmhg"]
            > record["blood_pressure_diastolic_mmhg"]
        )


def test_streaming_generator_rejects_invalid_run_id():
    with pytest.raises(ValueError, match="valid UUID"):
        StreamingEventGenerator().generate(
            n_records=1,
            n_patients=1,
            producer_run_id="not-a-uuid",
        )


def test_record_to_json_serializes_utc_with_z():
    payload = json.loads(record_to_json({
        "event_time": datetime(2026, 8, 9, 12, 30),
        "value": 72,
    }))

    assert payload == {
        "event_time": "2026-08-09T12:30:00Z",
        "value": 72,
    }


def test_streaming_file_is_named_by_run_id_and_not_overwritten(tmp_path):
    events = generate_events(count=2)
    run_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    output_path = generator_main.save_streaming_events(
        events,
        tmp_path,
        run_id,
    )

    assert output_path.name == f"streaming_events_{run_id}.jsonl"
    persisted = [
        json.loads(line)
        for line in output_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event_id"] for row in persisted] == events["event_id"].tolist()

    with pytest.raises(FileExistsError, match="already exists"):
        generator_main.save_streaming_events(events, tmp_path, run_id)


class FakeEventData:
    def __init__(self, body):
        self.body = body


class FakeBatch:
    def __init__(self, partition_key, max_events=2):
        self.partition_key = partition_key
        self.max_events = max_events
        self.events = []

    def add(self, event):
        if len(self.events) >= self.max_events:
            raise ValueError("batch full")
        self.events.append(event)

    def __len__(self):
        return len(self.events)


class FakeProducer:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.sent_batches = []
        self.created_partition_keys = []
        self.__class__.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def create_batch(self, partition_key):
        self.created_partition_keys.append(partition_key)
        return FakeBatch(partition_key)

    def send_batch(self, batch):
        self.sent_batches.append((batch.partition_key, list(batch.events)))


class FakeCredential:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_eventhub_uses_oauth_and_patient_partitioned_batches():
    FakeProducer.instances.clear()
    credential = FakeCredential()
    events = pd.DataFrame([
        {"event_id": "event-1", "patient_id": 7},
        {"event_id": "event-2", "patient_id": 8},
        {"event_id": "event-3", "patient_id": 7},
        {"event_id": "event-4", "patient_id": 7},
    ])

    sent = send_dataframe_to_eventhub(
        events,
        "healthlake.servicebus.windows.net",
        "vital-signs",
        credential=credential,
        producer_client_type=FakeProducer,
        event_data_type=FakeEventData,
    )

    producer = FakeProducer.instances[-1]
    assert sent == len(events)
    assert producer.kwargs == {
        "fully_qualified_namespace": "healthlake.servicebus.windows.net",
        "eventhub_name": "vital-signs",
        "credential": credential,
    }
    assert credential.closed is False
    assert producer.created_partition_keys == ["7", "7", "8"]

    sent_event_ids = []
    for partition_key, batch in producer.sent_batches:
        decoded = [json.loads(event.body) for event in batch]
        assert {str(event["patient_id"]) for event in decoded} == {
            partition_key
        }
        sent_event_ids.extend(event["event_id"] for event in decoded)

    assert sorted(sent_event_ids) == [
        "event-1",
        "event-2",
        "event-3",
        "event-4",
    ]


def test_eventhub_owns_and_closes_default_credential(monkeypatch):
    FakeProducer.instances.clear()
    credential = FakeCredential()
    monkeypatch.setattr(
        eventhub_producer,
        "_default_credential_factory",
        lambda: credential,
    )

    sent = send_dataframe_to_eventhub(
        pd.DataFrame([{"event_id": "event-1", "patient_id": 7}]),
        "healthlake.servicebus.windows.net",
        "vital-signs",
        producer_client_type=FakeProducer,
        event_data_type=FakeEventData,
    )

    assert sent == 1
    assert credential.closed is True


def test_eventhub_config_uses_namespace_and_name_environment(monkeypatch):
    monkeypatch.setattr(generator_main, "load_env", lambda: None)
    monkeypatch.setenv(
        "EVENTHUB_FULLY_QUALIFIED_NAMESPACE",
        "healthlake.servicebus.windows.net",
    )
    monkeypatch.setenv("EVENTHUB_NAME", "vital-signs")
    args = Namespace(
        eventhub_fully_qualified_namespace=None,
        eventhub_name=None,
    )

    assert generator_main.resolve_eventhub_config(args) == (
        "healthlake.servicebus.windows.net",
        "vital-signs",
    )
