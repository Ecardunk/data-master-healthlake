import random
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from generators.base_generator import BaseGenerator


EVENT_TYPE = "patient_vital_signs"
SCHEMA_VERSION = 1
DEFAULT_SOURCE = "healthlake-synthetic-generator"


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return value.isoformat().replace("+00:00", "Z")


class StreamingEventGenerator(BaseGenerator):
    def __init__(self, *args, clock=None, event_id_factory=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.event_id_factory = event_id_factory or uuid4

    def generate(
        self,
        n_records: int,
        n_patients: int,
        producer_run_id,
        source: str = DEFAULT_SOURCE,
    ):
        if n_records < 0:
            raise ValueError("n_records cannot be negative")
        if n_patients <= 0:
            raise ValueError("n_patients must be greater than zero")
        if not source or not source.strip():
            raise ValueError("source cannot be empty")

        try:
            normalized_run_id = str(UUID(str(producer_run_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("producer_run_id must be a valid UUID") from exc

        produced_at = self.clock()
        events = []

        for _ in range(n_records):
            diastolic = random.randint(60, 120)
            systolic = random.randint(max(90, diastolic + 1), 180)
            event_time = produced_at - timedelta(
                # Operational telemetry is recent. A short event-time delay
                # keeps the downstream watermark bounded and inexpensive.
                seconds=random.randint(0, 5 * 60)
            )

            events.append({
                "schema_version": SCHEMA_VERSION,
                "event_id": str(self.event_id_factory()),
                "event_type": EVENT_TYPE,
                "patient_id": random.randint(1, n_patients),
                "heart_rate_bpm": random.randint(40, 180),
                "oxygen_saturation_pct": random.randint(70, 100),
                "temperature_c": round(random.uniform(35, 41), 1),
                "blood_pressure_systolic_mmhg": systolic,
                "blood_pressure_diastolic_mmhg": diastolic,
                "event_time": format_utc(event_time),
                "produced_at": format_utc(produced_at),
                "producer_run_id": normalized_run_id,
                "source": source.strip(),
            })

        return self.build_dataframe(events)
