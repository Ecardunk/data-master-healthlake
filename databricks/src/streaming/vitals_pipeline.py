"""Triggered Event Hubs to Bronze, Silver, quarantine, and Gold pipeline."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from contracts import DQ_RULES, EVENT_SCHEMA_DDL, SILVER_COLUMNS


CATALOG = spark.conf.get("healthlake.catalog")
BOOTSTRAP_SERVERS = spark.conf.get("healthlake.eventhub.bootstrap_servers")
EVENTHUB_NAME = spark.conf.get("healthlake.eventhub.name")
CONSUMER_GROUP = spark.conf.get("healthlake.eventhub.consumer_group")
SERVICE_CREDENTIAL = spark.conf.get("healthlake.eventhub.service_credential")
MAX_OFFSETS_PER_TRIGGER = spark.conf.get(
    "healthlake.eventhub.max_offsets_per_trigger",
    "10000",
)

BRONZE_TABLE = f"{CATALOG}.bronze.vital_events_raw"
SILVER_TABLE = f"{CATALOG}.silver.vital_events"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.vital_events"
GOLD_PATIENT_TABLE = f"{CATALOG}.gold.vital_patient_5m"
GOLD_POPULATION_TABLE = f"{CATALOG}.gold.vital_population_hourly"


def read_eventhub():
    """Use Event Hubs' Kafka endpoint with secretless UC authentication."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", EVENTHUB_NAME)
        .option("kafka.group.id", CONSUMER_GROUP)
        .option("databricks.serviceCredential", SERVICE_CREDENTIAL)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .load()
    )

@dp.table(
    name=BRONZE_TABLE,
    comment=(
        "Immutable Event Hubs payloads with Kafka coordinates for replay, "
        "audit, and idempotency."
    ),
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
    cluster_by=["eventhub_partition", "eventhub_enqueued_at"],
)
def vital_events_raw():
    return read_eventhub().select(
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic").alias("eventhub_name"),
        F.col("partition").cast("int").alias("eventhub_partition"),
        F.col("offset").cast("long").alias("eventhub_offset"),
        F.col("timestamp").alias("eventhub_enqueued_at"),
        F.current_timestamp().alias("ingested_at"),
        F.sha2(F.col("value").cast("string"), 256).alias("payload_sha256"),
    )


def dq_error_array():
    """Return the violated rule names without discarding the raw event."""
    failures = [
        F.when(~F.coalesce(F.expr(predicate), F.lit(False)), F.lit(name))
        for name, predicate in DQ_RULES.items()
    ]
    return F.filter(F.array(*failures), lambda error: error.isNotNull())


@dp.temporary_view(name="vital_events_classified")
@dp.expect_all(DQ_RULES)
def vital_events_classified():
    """Parse and clean first, then evaluate every DQ rule once."""
    parsed = (
        spark.readStream.table(BRONZE_TABLE)
        .withColumn(
            "_parsed_payload",
            F.from_json(F.col("raw_payload"), EVENT_SCHEMA_DDL),
        )
        .withColumn(
            "_payload_keys",
            F.json_object_keys(F.col("raw_payload")),
        )
        .select("*", "_parsed_payload.*")
        .withColumn("event_id", F.lower(F.trim(F.col("event_id"))))
        .withColumn("event_type", F.trim(F.col("event_type")))
        .withColumn("producer_run_id", F.lower(F.trim(F.col("producer_run_id"))))
        .withColumn("source", F.trim(F.col("source")))
        .withColumn("_event_time_raw", F.trim(F.col("event_time")))
        .withColumn("_produced_at_raw", F.trim(F.col("produced_at")))
        # Malformed timestamps become NULL and are quarantined by DQ instead
        # of aborting parsing under an ANSI-enabled runtime.
        .withColumn("event_time", F.try_to_timestamp(F.col("_event_time_raw")))
        .withColumn("produced_at", F.try_to_timestamp(F.col("_produced_at_raw")))
    )
    return parsed.withColumn("_dq_errors", dq_error_array())


@dp.table(
    name=QUARANTINE_TABLE,
    comment="Vital-sign events rejected after parsing, cleaning, and contract checks.",
    table_properties={"quality": "quarantine"},
    cluster_by=["eventhub_enqueued_at"],
)
def vital_events_quarantine():
    return (
        spark.readStream.table("vital_events_classified")
        .where(F.size(F.col("_dq_errors")) > 0)
        .select(
            "raw_payload",
            "eventhub_name",
            "eventhub_partition",
            "eventhub_offset",
            "eventhub_enqueued_at",
            "ingested_at",
            "payload_sha256",
            "event_id",
            "patient_id",
            "producer_run_id",
            "_dq_errors",
        )
    )


@dp.temporary_view(name="vital_events_dq_approved")
@dp.expect_or_fail("dq_clean_output", "size(_dq_errors) = 0")
def vital_events_dq_approved():
    """Fail before watermark/dedup so no invalid event can disappear silently."""
    return spark.readStream.table("vital_events_classified")


@dp.table(
    name=SILVER_TABLE,
    comment=(
        "Typed valid vital-sign events, deduplicated by event_id with bounded state."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["event_time", "patient_id"],
)
def vital_events():
    # The upstream fatal view evaluates every classified row before stateful
    # operators. A single violation leaves the complete Silver micro-batch
    # unchanged; Bronze and the parallel quarantine retain the evidence.
    cleaned = spark.readStream.table("vital_events_dq_approved")
    return (
        # DQ accepts at most 24 hours of event-time delay. Keep one extra hour
        # so a valid boundary event cannot be silently dropped by the watermark.
        cleaned.withWatermark("event_time", "25 hours")
        .dropDuplicatesWithinWatermark(["event_id"])
        .select(*SILVER_COLUMNS, "_dq_errors")
    )


def with_abnormal_flag(events):
    return events.withColumn(
        "is_abnormal",
        (F.col("heart_rate_bpm") < 50)
        | (F.col("heart_rate_bpm") > 120)
        | (F.col("oxygen_saturation_pct") < 92.0)
        | (F.col("temperature_c") < 35.5)
        | (F.col("temperature_c") > 38.0)
        | (F.col("blood_pressure_systolic_mmhg") > 180)
        | (F.col("blood_pressure_diastolic_mmhg") > 120),
    )


@dp.materialized_view(
    name=GOLD_PATIENT_TABLE,
    comment="Five-minute patient vital-sign aggregates for operational monitoring.",
)
def vital_patient_5m():
    events = with_abnormal_flag(spark.read.table(SILVER_TABLE))
    return (
        events.groupBy(
            "patient_id",
            F.window("event_time", "5 minutes").alias("event_window"),
        )
        .agg(
            F.count("event_id").alias("measurement_count"),
            F.round(F.avg("heart_rate_bpm"), 2).alias("avg_heart_rate_bpm"),
            F.min("oxygen_saturation_pct").alias("min_oxygen_saturation_pct"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.round(F.avg("blood_pressure_systolic_mmhg"), 2).alias(
                "avg_systolic_mmhg"
            ),
            F.round(F.avg("blood_pressure_diastolic_mmhg"), 2).alias(
                "avg_diastolic_mmhg"
            ),
            F.sum(F.col("is_abnormal").cast("long")).alias("abnormal_count"),
            F.max("event_time").alias("last_event_time"),
        )
        .select(
            "patient_id",
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "measurement_count",
            "avg_heart_rate_bpm",
            "min_oxygen_saturation_pct",
            "avg_temperature_c",
            "avg_systolic_mmhg",
            "avg_diastolic_mmhg",
            "abnormal_count",
            F.round(F.col("abnormal_count") / F.col("measurement_count"), 4).alias(
                "abnormal_rate"
            ),
            "last_event_time",
        )
    )


@dp.materialized_view(
    name=GOLD_POPULATION_TABLE,
    comment="Hourly population-level vital-sign KPIs without patient identifiers.",
)
def vital_population_hourly():
    events = with_abnormal_flag(spark.read.table(SILVER_TABLE))
    return (
        events.groupBy(F.window("event_time", "1 hour").alias("event_window"))
        .agg(
            F.count("event_id").alias("event_count"),
            F.count_distinct("patient_id").alias("patient_count"),
            F.round(F.avg("heart_rate_bpm"), 2).alias("avg_heart_rate_bpm"),
            F.round(F.avg("oxygen_saturation_pct"), 2).alias(
                "avg_oxygen_saturation_pct"
            ),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.sum(F.col("is_abnormal").cast("long")).alias("abnormal_count"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "event_count",
            "patient_count",
            "avg_heart_rate_bpm",
            "avg_oxygen_saturation_pct",
            "avg_temperature_c",
            "abnormal_count",
            F.round(F.col("abnormal_count") / F.col("event_count"), 4).alias(
                "abnormal_rate"
            ),
        )
    )
