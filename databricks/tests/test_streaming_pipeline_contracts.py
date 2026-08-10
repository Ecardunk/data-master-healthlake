import ast
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = REPO_ROOT / "databricks" / "src" / "streaming" / "contracts.py"
PIPELINE = REPO_ROOT / "databricks" / "src" / "streaming" / "vitals_pipeline.py"
RESOURCE = REPO_ROOT / "databricks" / "resources" / "vitals_streaming.prod.yml"
BUNDLE = REPO_ROOT / "databricks" / "databricks.yml"
DEPLOY_PROD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy-prod.yml"
RUN_STREAMING_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "run-streaming-prod.yml"
)
JSON_CONTRACT = (
    REPO_ROOT / "data-generator" / "contracts" / "vital_event.v1.schema.json"
)


def test_streaming_contract_has_versioned_fields_and_post_cleaning_dq_rules():
    source = CONTRACTS.read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments = {
        target.id: node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    schema = ast.literal_eval(assignments["EVENT_SCHEMA_DDL"])
    assert "schema_version INT" in schema
    assert "event_id STRING" in schema
    assert "event_time STRING" in schema
    assert "produced_at STRING" in schema

    assert "payload_parseable" in source
    assert "payload_fields_exact" in source
    assert "event_time_utc_format" in source
    assert "produced_at_utc_format" in source
    assert "schema_version_supported" in source
    assert "systolic_above_diastolic" in source
    assert "heart_rate_in_range" in source

    pipeline = PIPELINE.read_text(encoding="utf-8")
    classified = pipeline.split("def vital_events_classified():", 1)[1]
    assert classified.index("F.from_json") < classified.index("dq_error_array()")
    assert classified.index("F.json_object_keys") < classified.index(
        "dq_error_array()"
    )
    assert classified.index("F.try_to_timestamp") < classified.index(
        "dq_error_array()"
    )
    assert "F.to_timestamp" not in classified


def test_producer_and_consumer_v1_contract_fields_are_identical():
    contract = json.loads(JSON_CONTRACT.read_text(encoding="utf-8"))
    namespace = {}
    exec(CONTRACTS.read_text(encoding="utf-8"), namespace)

    ddl_fields = {
        line.strip().rstrip(",").split()[0]
        for line in namespace["EVENT_SCHEMA_DDL"].splitlines()
        if line.strip()
    }

    assert contract["additionalProperties"] is False
    assert tuple(contract["required"]) == namespace["EVENT_FIELDS"]
    assert ddl_fields == set(contract["required"])


def test_streaming_uses_secretless_oauth_and_preserves_bronze_coordinates():
    source = PIPELINE.read_text(encoding="utf-8")

    assert 'option("databricks.serviceCredential", SERVICE_CREDENTIAL)' in source
    assert "sasl.jaas.config" not in source
    assert "ConnectionString" not in source
    assert "dbutils.secrets" not in source
    assert "allowNonConsecutiveOffsets" not in source
    assert 'alias("eventhub_partition")' in source
    assert 'alias("eventhub_offset")' in source
    assert 'alias("payload_sha256")' in source
    assert '.option("startingOffsets", "earliest")' in source
    assert '"pipelines.reset.allowed": "false"' in source


def test_streaming_is_one_triggered_prod_pipeline_with_paused_schedule():
    resource = RESOURCE.read_text(encoding="utf-8")

    assert resource.startswith("targets:\n  prod:")
    assert resource.count("healthlake_vitals_streaming:") == 1
    assert "continuous: false" in resource
    assert "serverless: true" in resource
    assert "max_concurrent_runs: 1" in resource
    assert "enabled: false" in resource
    assert "pause_status: ${var.streaming_schedule_pause_status}" in resource
    assert "pipeline_task:" in resource

    bundle = BUNDLE.read_text(encoding="utf-8")
    assert "streaming_schedule_pause_status:" in bundle
    assert "default: PAUSED" in bundle
    assert "evhns-healthlake-prod-brs-01.servicebus.windows.net:9093" in bundle
    assert "svc_healthlake_prod_eventhubs_receiver" in bundle


def test_streaming_dq_quarantines_and_blocks_the_whole_silver_update():
    source = PIPELINE.read_text(encoding="utf-8")

    assert "vital_events_classified" in source
    assert "vital_events_quarantine" in source
    assert '@dp.temporary_view(name="vital_events_dq_approved")' in source
    assert 'F.size(F.col("_dq_errors")) > 0' in source
    assert '@dp.expect_or_fail("dq_clean_output", "size(_dq_errors) = 0")' in source
    approved_body = source.split("def vital_events_dq_approved():", 1)[1].split(
        "def vital_events():", 1
    )[0]
    silver_body = source.split("def vital_events():", 1)[1].split(
        "def with_abnormal_flag", 1
    )[0]
    assert 'spark.readStream.table("vital_events_classified")' in approved_body
    assert '.where(F.size(F.col("_dq_errors")) == 0)' not in approved_body
    assert 'spark.readStream.table("vital_events_dq_approved")' in silver_body
    assert "dropDuplicatesWithinWatermark" in source
    assert 'withWatermark("event_time", "25 hours")' in source
    assert source.index("def vital_events_quarantine") < source.index(
        "def vital_events():"
    )
    assert source.index("def vital_events():") < source.index(
        "def vital_patient_5m"
    )


def test_paid_prod_runs_are_explicit_and_separate_from_deploy():
    deploy = DEPLOY_PROD_WORKFLOW.read_text(encoding="utf-8")
    streaming = RUN_STREAMING_WORKFLOW.read_text(encoding="utf-8")
    paid_run_step = streaming.split(
        "- name: Consume the backlog once through Bronze, Silver, and Gold", 1
    )[1]

    assert "run_batch_refresh:" in deploy
    assert "default: false" in deploy
    assert "if: inputs.run_batch_refresh" in deploy
    assert "healthlake_vitals_streaming_refresh" not in deploy

    assert "confirm_run:" in streaming
    assert "confirm_run must be enabled" in streaming
    assert "STREAMING_JOB_NAME: healthlake-vitals-streaming-refresh-prod" in streaming
    assert 'databricks jobs list --name "$STREAMING_JOB_NAME"' in streaming
    assert "jobs run-now" in streaming
    assert '--idempotency-token "gh-${GITHUB_RUN_ID}"' in streaming
    assert "databricks bundle summary" not in streaming
    assert "--force-pull" not in streaming
    assert "bundle deploy" not in streaming
    assert "working-directory:" not in paid_run_step
