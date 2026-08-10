import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_INGESTION = REPO_ROOT / "databricks" / "src" / "bronze" / "ingestion.py"
PRODUCTION_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy-prod.yml"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
DEVELOPMENT_SERVICE_PRINCIPAL = "03b5799c-110f-484f-8b1b-e3fd88809c64"
PRODUCTION_SERVICE_PRINCIPAL = "bfeb3006-1824-4361-bacb-3697f6e33262"


def assigned_literal(path: Path, variable_name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == variable_name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{variable_name} was not assigned in {path}")


def assigned_string(path: Path, variable_name: str) -> str:
    value = assigned_literal(path, variable_name)
    assert isinstance(value, str)
    return value


def test_bronze_odate_regex_matches_the_adf_path_contract():
    pattern = assigned_string(BRONZE_INGESTION, "ODATE_PATH_PATTERN")

    match = re.search(
        pattern,
        "abfss://raw@storage.dfs.core.windows.net/patients/"
        "odate=2026-08-06/patients.csv",
    )

    assert match is not None
    assert match.group(1) == "2026-08-06"
    assert re.search(pattern, "/patients/not-odate=2026-08-06/patients.csv") is None


def test_medallion_layers_do_not_silently_drop_failed_rows():
    pipeline_sources = [
        BRONZE_INGESTION,
        REPO_ROOT / "databricks" / "src" / "silver" / "transforms.py",
        REPO_ROOT / "databricks" / "src" / "gold" / "marts.py",
    ]

    for source in pipeline_sources:
        assert "expect_or_drop" not in source.read_text(encoding="utf-8")


def test_gate_requires_odate_and_applies_cleaning_before_checks():
    source = (
        REPO_ROOT / "databricks" / "src" / "dq" / "quality_gate.py"
    ).read_text(encoding="utf-8")

    assert 'parser.add_argument("--odate", required=True' in source
    assert "source_df = with_effective_odate(source_df)" in source
    assert "source_for_odate = source_df.where(" in source
    assert "checked_df = prepare_for_checks(source_for_odate" in source
    assert source.index("checked_df = prepare_for_checks") < source.index(
        "dq_engine.apply_checks_and_split"
    )
    assert ".cache()" not in source
    assert ".persist()" not in source
    assert ".saveAsTable(source_table)" not in source


def test_silver_and_gold_read_only_gate_approved_snapshots():
    silver_source = (
        REPO_ROOT / "databricks" / "src" / "silver" / "transforms.py"
    ).read_text(encoding="utf-8")
    gold_source = (
        REPO_ROOT / "databricks" / "src" / "gold" / "marts.py"
    ).read_text(encoding="utf-8")

    assert 'F.col("dq_stage") == "bronze_to_silver"' in silver_source
    assert 'F.col("dq_stage") == "silver_to_gold"' in gold_source
    assert "approved_silver(" in gold_source


def test_cleaning_removes_only_incomplete_non_key_records_before_dq():
    cleaning_source = (
        REPO_ROOT / "databricks" / "src" / "silver" / "cleaning.py"
    )
    required_columns = assigned_literal(cleaning_source, "CLEANUP_DROP_NULLS")

    assert required_columns == {
        "patients": ["birth_date", "gender", "state"],
        "hospitals": ["capacity", "state"],
        "doctors": ["crm", "hospital_id"],
        "diseases": ["severity_level"],
        "attendance": [
            "patient_id",
            "doctor_id",
            "hospital_id",
            "disease_id",
            "attendance_timestamp",
            "severity_score",
        ],
    }


def test_each_environment_reads_its_own_raw_storage():
    bundle_config = (
        REPO_ROOT / "databricks" / "databricks.yml"
    ).read_text(encoding="utf-8")
    workflow = PRODUCTION_WORKFLOW.read_text(encoding="utf-8")

    assert "abfss://raw@sthealthdatalake001.dfs.core.windows.net" in bundle_config
    assert "abfss://raw@sthlkprodbrs01.dfs.core.windows.net" in bundle_config
    assert bundle_config.count("raw_root:") == 3

    dev_target = bundle_config.split("  dev:", 1)[1].split("  prod:", 1)[0]
    prod_target = bundle_config.split("  prod:", 1)[1]
    assert "sthealthdatalake001" in dev_target
    assert "sthlkprodbrs01" not in dev_target
    assert "sthlkprodbrs01" in prod_target
    assert "sthealthdatalake001" not in prod_target
    assert "DATABRICKS_RAW_ROOT" not in workflow


def test_successful_main_ci_automatically_deploys_without_running_data():
    ci_workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    workflow = PRODUCTION_WORKFLOW.read_text(encoding="utf-8")

    assert "deploy-production:" in ci_workflow
    assert "needs: [test, validate-bundle]" in ci_workflow
    assert (
        "if: github.event_name == 'push' && "
        "github.ref == 'refs/heads/main'"
    ) in ci_workflow
    assert "uses: ./.github/workflows/deploy-prod.yml" in ci_workflow
    assert "secrets: inherit" in ci_workflow
    assert "run_batch_refresh: false" in ci_workflow
    assert "cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}" in ci_workflow
    assert "workflow_call:" in workflow
    assert "if: github.ref == 'refs/heads/main'" in workflow
    assert "ref: ${{ github.sha }}" in workflow
    assert "ODATE: ${{ inputs.odate }}" in workflow
    assert "RUN_BATCH_REFRESH: ${{ inputs.run_batch_refresh }}" in workflow
    assert "inputs.ref" not in workflow
    assert "bundle run healthlake_medallion_refresh" in workflow
    assert '--params "odate=${ODATE}"' in workflow
    assert (
        "if: inputs.run_batch_refresh && "
        "github.event_name == 'workflow_dispatch'"
    ) in workflow
    assert '"$GITHUB_EVENT_NAME" != "workflow_dispatch"' in workflow
    assert '[[ -z "$DATABRICKS_CLIENT_SECRET" ]]' in workflow
    assert "databricks current-user me --output json >/dev/null" in workflow
    assert workflow.count("default: false") == 2


def test_production_deploy_leaves_observability_compute_stopped():
    workflow = PRODUCTION_WORKFLOW.read_text(encoding="utf-8")

    assert "Leave observability warehouse stopped" in workflow
    assert "continue-on-error: ${{ job.status != 'success' }}" in workflow
    assert "databricks warehouses list --output json" in workflow
    assert "databricks bundle summary" not in workflow
    assert 'databricks warehouses stop "$warehouse_id"' in workflow
    assert 'databricks warehouses get "$warehouse_id"' in workflow
    assert 'if [[ "$state" == "STOPPED" ]]' in workflow


def test_production_deploy_blocks_destructive_bundle_plans():
    workflow = PRODUCTION_WORKFLOW.read_text(encoding="utf-8")

    assert "bundle plan" in workflow
    assert "--output json" in workflow
    assert '.value.action == "delete"' in workflow
    assert '.value.action == "recreate"' in workflow
    assert ".value.gone != true" in workflow
    assert "Destructive production plan blocked" in workflow
    assert "--auto-approve" not in workflow


def test_all_jobs_and_pipelines_run_as_the_environment_service_principal():
    bundle_config = (
        REPO_ROOT / "databricks" / "databricks.yml"
    ).read_text(encoding="utf-8")
    resource_sources = [
        REPO_ROOT / "databricks" / "resources" / "bronze.pipeline.yml",
        REPO_ROOT / "databricks" / "resources" / "silver.pipeline.yml",
        REPO_ROOT / "databricks" / "resources" / "gold.pipeline.yml",
        REPO_ROOT / "databricks" / "resources" / "data_quality.jobs.yml",
        REPO_ROOT / "databricks" / "resources" / "medallion.job.yml",
    ]
    resources = "\n".join(
        source.read_text(encoding="utf-8") for source in resource_sources
    )

    assert DEVELOPMENT_SERVICE_PRINCIPAL in bundle_config
    assert PRODUCTION_SERVICE_PRINCIPAL in bundle_config
    assert resources.count(
        "service_principal_name: ${var.run_as_service_principal_name}"
    ) == 6
    assert "run_as:\n        user_name:" not in resources


def test_observability_dashboard_is_production_only():
    dashboard = (
        REPO_ROOT
        / "databricks"
        / "src"
        / "dashboard"
        / "healthlake_observability.prod.lvdash.json"
    ).read_text(encoding="utf-8")

    assert "7405616424934600" in dashboard
    assert "system.lakeflow.pipeline_update_timeline" in dashboard
    assert "system.lakeflow.job_run_timeline" in dashboard
    assert "vital_streaming_pipeline_events" in dashboard
    assert "result_state = 'SUCCEEDED'" not in dashboard
    assert "QUALIFY ROW_NUMBER()" in dashboard
