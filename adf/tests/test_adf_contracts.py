import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ADF_ROOT = REPO_ROOT / "adf"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_all_adf_json_files_are_valid():
    for path in ADF_ROOT.rglob("*.json"):
        read_json(path)


def test_s3_credentials_are_key_vault_references_only():
    linked_service = read_json(
        ADF_ROOT / "linkedService" / "ls_s3_healthlake.json"
    )
    properties = linked_service["properties"]["typeProperties"]

    assert properties["accessKeyId"]["type"] == "AzureKeyVaultSecret"
    assert properties["secretAccessKey"]["type"] == "AzureKeyVaultSecret"
    assert "value" not in properties["accessKeyId"]
    assert "value" not in properties["secretAccessKey"]
