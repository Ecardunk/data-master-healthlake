"""Versioned contract and data-quality rules for vital-sign events."""

from collections import OrderedDict


EVENT_SCHEMA_DDL = """
    schema_version INT,
    event_id STRING,
    event_type STRING,
    patient_id LONG,
    heart_rate_bpm INT,
    oxygen_saturation_pct DOUBLE,
    temperature_c DOUBLE,
    blood_pressure_systolic_mmhg INT,
    blood_pressure_diastolic_mmhg INT,
    event_time STRING,
    produced_at STRING,
    producer_run_id STRING,
    source STRING
"""

EVENT_FIELDS = (
    "schema_version",
    "event_id",
    "event_type",
    "patient_id",
    "heart_rate_bpm",
    "oxygen_saturation_pct",
    "temperature_c",
    "blood_pressure_systolic_mmhg",
    "blood_pressure_diastolic_mmhg",
    "event_time",
    "produced_at",
    "producer_run_id",
    "source",
)
EVENT_FIELDS_SQL = "array({})".format(
    ", ".join(f"'{field}'" for field in EVENT_FIELDS)
)

UTC_TIMESTAMP_PATTERN = (
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    "[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z$"
)

UUID_PATTERN = (
    "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

# The SQL predicates are evaluated only after JSON parsing, trimming, casting,
# and timestamp normalization in vital_events_classified().
DQ_RULES = OrderedDict(
    [
        (
            "payload_parseable",
            "_parsed_payload IS NOT NULL AND _payload_keys IS NOT NULL",
        ),
        (
            "payload_fields_exact",
            f"size(_payload_keys) = {len(EVENT_FIELDS)} AND "
            f"size(array_except(_payload_keys, {EVENT_FIELDS_SQL})) = 0",
        ),
        ("schema_version_supported", "schema_version = 1"),
        ("event_type_supported", "event_type = 'patient_vital_signs'"),
        ("event_id_valid_uuid", f"event_id RLIKE '{UUID_PATTERN}'"),
        ("producer_run_id_valid_uuid", f"producer_run_id RLIKE '{UUID_PATTERN}'"),
        ("patient_id_positive", "patient_id > 0"),
        (
            "event_time_utc_format",
            f"_event_time_raw RLIKE '{UTC_TIMESTAMP_PATTERN}'",
        ),
        ("event_time_present", "event_time IS NOT NULL"),
        (
            "produced_at_utc_format",
            f"_produced_at_raw RLIKE '{UTC_TIMESTAMP_PATTERN}'",
        ),
        ("produced_at_present", "produced_at IS NOT NULL"),
        (
            "event_time_not_future",
            "event_time <= eventhub_enqueued_at + INTERVAL 5 MINUTES",
        ),
        (
            "event_time_not_stale",
            "event_time >= eventhub_enqueued_at - INTERVAL 24 HOURS",
        ),
        (
            "produced_at_not_future",
            "produced_at <= eventhub_enqueued_at + INTERVAL 5 MINUTES",
        ),
        (
            "produced_at_not_stale",
            "produced_at >= eventhub_enqueued_at - INTERVAL 24 HOURS",
        ),
        (
            "event_precedes_production",
            "event_time <= produced_at + INTERVAL 5 MINUTES",
        ),
        ("heart_rate_in_range", "heart_rate_bpm BETWEEN 30 AND 220"),
        (
            "oxygen_saturation_in_range",
            "oxygen_saturation_pct BETWEEN 50.0 AND 100.0",
        ),
        ("temperature_in_range", "temperature_c BETWEEN 30.0 AND 45.0"),
        (
            "systolic_pressure_in_range",
            "blood_pressure_systolic_mmhg BETWEEN 60 AND 250",
        ),
        (
            "diastolic_pressure_in_range",
            "blood_pressure_diastolic_mmhg BETWEEN 30 AND 150",
        ),
        (
            "systolic_above_diastolic",
            "blood_pressure_systolic_mmhg > blood_pressure_diastolic_mmhg",
        ),
        ("source_present", "source IS NOT NULL AND length(source) > 0"),
    ]
)

SILVER_COLUMNS = (
    "schema_version",
    "event_id",
    "event_type",
    "patient_id",
    "heart_rate_bpm",
    "oxygen_saturation_pct",
    "temperature_c",
    "blood_pressure_systolic_mmhg",
    "blood_pressure_diastolic_mmhg",
    "event_time",
    "produced_at",
    "producer_run_id",
    "source",
    "eventhub_partition",
    "eventhub_offset",
    "eventhub_enqueued_at",
    "ingested_at",
    "payload_sha256",
)
