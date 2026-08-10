import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ADF_ROOT = REPO_ROOT / "adf"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_all_adf_json_files_are_valid():
    for path in ADF_ROOT.rglob("*.json"):
        read_json(path)


def test_environment_endpoints_are_explicit_and_isolated():
    dev = read_json(ADF_ROOT / "environments" / "dev.json")
    prod = read_json(ADF_ROOT / "environments" / "prod.json")

    assert dev["factoryName"] == "adf-data-master-dev"
    assert prod["factoryName"] == "adf-healthlake-prod-brs-01"
    assert "sthealthdatalake001" in dev["adlsUrl"]
    assert "sthlkprodbrs01" in prod["adlsUrl"]
    assert "kv-data-master-case" in dev["keyVaultUrl"]
    assert "kv-hlk-prod-brs-01" in prod["keyVaultUrl"]
    assert dev["adlsUrl"] != prod["adlsUrl"]
    assert dev["keyVaultUrl"] != prod["keyVaultUrl"]
    assert dev["s3BucketName"] == prod["s3BucketName"] == "s3-data-master-bucket"
    assert dev["desiredTriggerState"] == "Stopped"
    assert prod["desiredTriggerState"] == "Stopped"

    bundle = (REPO_ROOT / "databricks" / "databricks.yml").read_text(
        encoding="utf-8"
    )
    assert dev["rawRoot"] in bundle
    assert prod["rawRoot"] in bundle


def test_adf_templates_are_safe_to_promote():
    trigger = read_json(ADF_ROOT / "trigger" / "trigger_case.json")
    pipeline_text = (
        ADF_ROOT / "pipeline" / "pl_copy_s3_to_adls_raw.json"
    ).read_text(encoding="utf-8")

    assert trigger["properties"]["runtimeState"] == "Stopped"
    assert trigger["properties"]["typeProperties"]["recurrence"]["schedule"][
        "monthDays"
    ] == [5]
    assert "sthealthdatalake001" not in pipeline_text
    assert "sthlkprodbrs01" not in pipeline_text

    for factory_file in (ADF_ROOT / "factory").glob("*.json"):
        factory = read_json(factory_file)
        assert factory["identity"] == {"type": "SystemAssigned"}
        assert "principalId" not in factory_file.read_text(encoding="utf-8")
        assert "tenantId" not in factory_file.read_text(encoding="utf-8")


def test_s3_credentials_are_key_vault_references_only():
    linked_service = read_json(
        ADF_ROOT / "linkedService" / "ls_s3_healthlake.json"
    )
    properties = linked_service["properties"]["typeProperties"]

    assert properties["accessKeyId"]["type"] == "AzureKeyVaultSecret"
    assert properties["secretAccessKey"]["type"] == "AzureKeyVaultSecret"
    assert "value" not in properties["accessKeyId"]
    assert "value" not in properties["secretAccessKey"]


def test_deployment_script_never_starts_a_trigger_or_pipeline():
    script = (ADF_ROOT / "scripts" / "deploy.ps1").read_text(encoding="utf-8")

    assert 'desiredTriggerState -ne "Stopped"' in script
    assert '"datafactory", "trigger", "stop"' in script
    assert '"datafactory", "trigger", "start"' not in script
    assert "pipeline create-run" not in script
