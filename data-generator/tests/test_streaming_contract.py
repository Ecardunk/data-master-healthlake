import json
from datetime import datetime

import pytest

from generators.streaming_generator import StreamingEventGenerator
from producers.eventhub_producer import record_to_json


def test_streaming_generator_rejects_invalid_run_id():
    with pytest.raises(ValueError, match="valid UUID"):
        StreamingEventGenerator().generate(
            n_records=1,
            n_patients=1,
            producer_run_id="not-a-uuid",
        )


def test_record_to_json_serializes_utc_with_z():
    payload = json.loads(
        record_to_json(
            {
                "event_time": datetime(2026, 8, 9, 12, 30),
                "value": 72,
            }
        )
    )

    assert payload == {
        "event_time": "2026-08-09T12:30:00Z",
        "value": 72,
    }
