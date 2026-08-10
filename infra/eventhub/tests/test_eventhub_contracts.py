import json
from pathlib import Path


EVENTHUB_ROOT = Path(__file__).resolve().parents[1]
BICEP_PATH = EVENTHUB_ROOT / "main.bicep"
PARAMETERS_PATH = EVENTHUB_ROOT / "parameters" / "prod.parameters.json"
SCRIPT_PATH = EVENTHUB_ROOT / "scripts" / "deploy.ps1"
UC_CREDENTIAL_PATH = (
    EVENTHUB_ROOT / "parameters" / "uc-service-credential.prod.json"
)
UC_BINDINGS_PATH = (
    EVENTHUB_ROOT
    / "parameters"
    / "uc-service-credential.bindings.prod.json"
)
UC_GRANTS_PATH = (
    EVENTHUB_ROOT / "parameters" / "uc-service-credential.grants.prod.json"
)
ROLE_MODULE_PATH = (
    EVENTHUB_ROOT / "modules" / "eventhub-role-assignment.bicep"
)


def read_parameters():
    return json.loads(PARAMETERS_PATH.read_text(encoding="utf-8"))["parameters"]


def test_production_parameters_are_exact_and_sender_is_fail_closed():
    parameters = read_parameters()

    assert parameters["location"]["value"] == "brazilsouth"
    assert parameters["namespaceName"]["value"] == "evhns-healthlake-prod-brs-01"
    assert parameters["eventHubName"]["value"] == "evh-vitals-prod"
    assert (
        parameters["consumerGroupName"]["value"]
        == "cg-healthlake-databricks-prod"
    )
    assert (
        parameters["accessConnectorName"]["value"]
        == "databricks-connector-healthlake-prod-eventhubs"
    )
    assert parameters["throughputUnits"]["value"] == 1
    assert parameters["partitionCount"]["value"] == 2
    assert parameters["messageRetentionInDays"]["value"] == 3
    assert parameters["producerPrincipalObjectId"]["value"] == ""


def test_bicep_enforces_oauth_capacity_and_stream_contracts():
    template = BICEP_PATH.read_text(encoding="utf-8")

    assert template.count("'Microsoft.EventHub/namespaces@2024-01-01'") == 1
    assert (
        template.count(
            "'Microsoft.EventHub/namespaces/eventhubs@2024-01-01'"
        )
        == 1
    )
    assert "name: 'Standard'" in template
    assert "tier: 'Standard'" in template
    assert "capacity: throughputUnits" in template
    assert "disableLocalAuth: true" in template
    assert "isAutoInflateEnabled: false" in template
    assert "kafkaEnabled: true" in template
    assert "minimumTlsVersion: '1.2'" in template
    assert "captureDescription:" not in template
    assert "Capture is disabled by omission" in template
    assert "messageRetentionInDays: messageRetentionInDays" in template
    assert "partitionCount: partitionCount" in template
    assert "Microsoft.Databricks/accessConnectors@2024-05-01" in template
    assert "type: 'SystemAssigned'" in template


def test_unity_catalog_contract_is_prod_only_and_secretless():
    credential = json.loads(UC_CREDENTIAL_PATH.read_text(encoding="utf-8"))
    bindings = json.loads(UC_BINDINGS_PATH.read_text(encoding="utf-8"))
    grants = json.loads(UC_GRANTS_PATH.read_text(encoding="utf-8"))

    assert credential["name"] == "svc_healthlake_prod_eventhubs_receiver"
    assert credential["purpose"] == "SERVICE"
    assert credential["skip_validation"] is False
    assert credential["azure_managed_identity"]["access_connector_id"] == (
        "/subscriptions/6b409a82-932c-4136-b8d5-1cb02345e23e/"
        "resourceGroups/rg-healthlake-prod-brs-01/providers/"
        "Microsoft.Databricks/accessConnectors/"
        "databricks-connector-healthlake-prod-eventhubs"
    )
    assert bindings == {
        "add": [
            {
                "workspace_id": 7405616424934600,
                "binding_type": "BINDING_TYPE_READ_WRITE",
            }
        ],
        "remove": [],
    }
    assert grants == {
        "changes": [
            {
                "principal": "bfeb3006-1824-4361-bacb-3697f6e33262",
                "add": ["ACCESS"],
            }
        ]
    }
    assert "client_secret" not in json.dumps(credential).lower()


def test_rbac_is_least_privilege_and_scoped_to_the_event_hub():
    template = BICEP_PATH.read_text(encoding="utf-8")
    role_module = ROLE_MODULE_PATH.read_text(encoding="utf-8")

    assert "a638d3c7-ab3a-418d-83e6-5f17a39d4fde" in template
    assert "2b629674-e913-4c01-ae53-ef4638d8f975" in template
    assert template.count("scope: eventHub") == 1
    assert role_module.count("scope: eventHub") == 1
    assert "if (!empty(producerPrincipalObjectId))" in template
    assert "principalId: databricksAccessConnector.identity.principalId" in template
    assert "guid(eventHub.id, roleDefinitionGuid, principalId)" in role_module


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


def test_deploy_script_is_preview_first_and_uc_is_explicit():
    script = SCRIPT_PATH.read_text(encoding="utf-8")
    lower_script = script.lower()

    assert "[switch]$Apply" in script
    assert "if (-not $Apply)" in script
    assert '"deployment", "group", "what-if"' in script
    assert '"--result-format", "ResourceIdOnly"' in script
    assert "$ConfigureUnityCatalog -and -not $Apply" in script
    assert '"credentials", "create-credential"' in script
    assert '"credentials", "get-credential"' in script
    assert "--page-token" not in script
    assert "producerPrincipalObjectId=$ProducerPrincipalObjectId" in script
    assert "$ReconcileProducerRole -and -not $Apply" in script
    assert '"role", "assignment", "delete"' in script
    assert '"--include-inherited"' in script
    assert script.index("$preDeploymentUnexpectedSenders") < script.index(
        '"deployment", "group", "create"'
    )
    assert "Start-Sleep -Seconds 5" in script
    assert '"--isolation-mode", "ISOLATION_MODE_ISOLATED"' in script
    assert '"workspace-bindings", "update-bindings", "credential"' in script
    assert '"grants", "update", "credential"' in script
    assert '"--owner", $RuntimePrincipal' in script
    assert "uc-service-credential.prod.json" in script
    assert "uc-service-credential.bindings.prod.json" in script
    assert "uc-service-credential.grants.prod.json" in script
    assert "authorization-rule" not in lower_script
    assert "keyvault" not in lower_script
    assert "sharedaccesskey" not in lower_script
