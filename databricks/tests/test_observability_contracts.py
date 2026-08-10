import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY_RESOURCE = (
    REPO_ROOT / "databricks" / "resources" / "observability.resources.yml"
)
ALERT_RESOURCE = (
    REPO_ROOT / "databricks" / "resources" / "alerts.prod.yml"
)
DQ_RESOURCE = (
    REPO_ROOT / "databricks" / "resources" / "data_quality.jobs.yml"
)
STREAMING_RESOURCE = (
    REPO_ROOT / "databricks" / "resources" / "vitals_streaming.prod.yml"
)
BUNDLE = REPO_ROOT / "databricks" / "databricks.yml"
DASHBOARD = (
    REPO_ROOT
    / "databricks"
    / "src"
    / "dashboard"
    / "healthlake_observability.prod.lvdash.json"
)
PRODUCTION_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy-prod.yml"
PUBLISHER_GRANTS = (
    REPO_ROOT
    / "databricks"
    / "src"
    / "governance"
    / "observability_service_principal_access.prod.sql"
)


def test_observability_resources_exist_only_in_production():
    resource = OBSERVABILITY_RESOURCE.read_text(encoding="utf-8")

    assert resource.startswith("targets:\n  prod:")
    assert "  dev:" not in resource
    assert "healthlake-observability-prod" in resource
    assert "healthlake_observability.prod.lvdash.json" in resource
    assert "embed_credentials: true" in resource
    assert resource.count("level: CAN_RUN") == 2
    assert "group_name: data-engineering-admin" in resource
    assert "group_name: data-engineering" in resource
    for consumer_group in ("data-analysts", "data-scientists", "power-bi"):
        assert consumer_group not in resource


def test_observability_is_on_demand_and_has_cost_guardrails():
    resource = OBSERVABILITY_RESOURCE.read_text(encoding="utf-8")
    lower_resource = resource.lower()

    assert "cluster_size: 2X-Small" in resource
    assert "min_num_clusters: 1" in resource
    assert "max_num_clusters: 1" in resource
    assert "auto_stop_mins: 10" in resource
    assert "schedule:" not in lower_resource
    assert "subscription" not in lower_resource
    assert "alert" not in lower_resource
    assert "monitor" not in lower_resource


def test_dashboard_is_valid_curated_and_does_not_expose_sensitive_rows():
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    source = DASHBOARD.read_text(encoding="utf-8")

    assert len(dashboard["datasets"]) == 8
    assert len(dashboard["pages"]) == 2
    assert "system.lakeflow.pipeline_update_timeline" in source
    assert "system.lakeflow.job_run_timeline" in source
    assert "vital_streaming_pipeline_events" in source
    assert "dq_run_metrics" in source
    assert "dq_promotion_control" in source
    assert "silver.vital_events" in source
    assert "quarantine.vital_events" in source
    assert "observation_age_minutes" in source
    assert "Backlog no último refresh" in source
    assert "raw_payload" not in source
    assert "patient_id" not in source
    assert "e.message" not in source
    assert "system.billing" not in source
    assert "event_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS" in source
    assert (
        "MAX_BY(backlog_bytes, observed_at) FILTER "
        "(WHERE backlog_bytes IS NOT NULL)"
    ) in source

    component_ids = [dataset["name"] for dataset in dashboard["datasets"]]
    dataset_ids = set(component_ids)
    references = []
    for page in dashboard["pages"]:
        component_ids.append(page["name"])
        for layout_item in page["layout"]:
            widget = layout_item["widget"]
            component_ids.append(widget["name"])
            references.extend(
                query["query"]["datasetName"]
                for query in widget.get("queries", [])
            )

    assert len(component_ids) == len(set(component_ids))
    assert set(references) <= dataset_ids


def test_dashboard_publisher_gets_only_the_required_system_table_access():
    grants = PUBLISHER_GRANTS.read_text(encoding="utf-8")

    assert "GRANT USE CATALOG ON CATALOG system" in grants
    assert "GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow" in grants
    assert grants.count("bfeb3006-1824-4361-bacb-3697f6e33262") == 2
    assert "data-engineering" not in grants


def test_alerts_are_event_driven_and_only_overlaid_on_production_jobs():
    alerts = ALERT_RESOURCE.read_text(encoding="utf-8")
    dq = DQ_RESOURCE.read_text(encoding="utf-8")
    streaming = STREAMING_RESOURCE.read_text(encoding="utf-8")
    bundle = BUNDLE.read_text(encoding="utf-8")

    assert alerts.startswith("targets:\n  prod:")
    assert "  dev:" not in alerts
    assert alerts.count("on_failure:") == 4
    assert alerts.count("on_duration_warning_threshold_exceeded:") == 4
    assert alerts.count("RUN_DURATION_SECONDS") == 2
    assert "schedule:" not in alerts
    assert "webhook_notifications:" not in dq
    assert "email_notifications:" not in dq
    assert "notifications:" not in streaming
    assert "email_notifications:" not in streaming
    variable_block = bundle.split(
        "  logic_app_notification_destination_id:", 1
    )[1].split("  raw_root:", 1)[0]
    assert "default: disabled" in variable_block
    prod_target = bundle.split("  prod:", 1)[1]
    dev_target = bundle.split("  dev:", 1)[1].split("  prod:", 1)[0]
    assert "7d5d65e4-ec0d-4658-b40e-1924cef9f521" in prod_target
    assert "7d5d65e4-ec0d-4658-b40e-1924cef9f521" not in dev_target


def test_production_deploy_always_leaves_the_observability_warehouse_stopped():
    workflow = PRODUCTION_WORKFLOW.read_text(encoding="utf-8")
    cleanup_step = workflow.split(
        "- name: Leave observability warehouse stopped", 1
    )[1].split("- name: Run production refresh", 1)[0]

    assert "Leave observability warehouse stopped" in workflow
    assert "if: always()" in workflow
    assert "OBSERVABILITY_WAREHOUSE_NAME: healthlake-observability-prod" in workflow
    assert "databricks warehouses list --output json" in workflow
    assert "databricks bundle summary" not in workflow
    assert "--force-pull" not in workflow
    assert 'databricks warehouses stop "$warehouse_id"' in workflow
    assert 'databricks warehouses get "$warehouse_id"' in workflow
    assert 'if [[ "$state" == "STOPPED" ]]' in workflow
    assert 'if [[ "$state" != "STOPPING" ]]' in workflow
    assert "Observability warehouse is absent; nothing to stop." in workflow
    assert "working-directory:" not in cleanup_step
    assert workflow.index("Deploy production target") < workflow.index(
        "Leave observability warehouse stopped"
    )
