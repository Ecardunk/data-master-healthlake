from pathlib import Path


EVENTHUB_ROOT = Path(__file__).resolve().parents[1]


def test_no_sas_or_key_vault_resources_are_declared():
    template = "\n".join(
        path.read_text(encoding="utf-8")
        for path in EVENTHUB_ROOT.rglob("*.bicep")
    ).lower()

    forbidden_tokens = (
        "authorizationrules",
        "listkeys(",
        "sharedaccesskey",
        "microsoft.keyvault",
        "vaults/secrets",
    )
    for token in forbidden_tokens:
        assert token not in template
