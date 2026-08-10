import json
from pathlib import Path


OBSERVABILITY_ROOT = Path(__file__).resolve().parents[1]
BICEP_PATH = OBSERVABILITY_ROOT / "main.bicep"
PARAMETERS_PATH = (
    OBSERVABILITY_ROOT / "parameters" / "prod.parameters.json"
)
SCRIPT_PATH = OBSERVABILITY_ROOT / "scripts" / "deploy.ps1"


def test_logic_app_is_production_only_and_event_driven():
    template = BICEP_PATH.read_text(encoding="utf-8")
    parameters = json.loads(PARAMETERS_PATH.read_text(encoding="utf-8"))[
        "parameters"
    ]

    assert parameters["location"]["value"] == "brazilsouth"
    assert (
        parameters["logicAppName"]["value"]
        == "logic-healthlake-alerts-prod-brs-01"
    )
    assert parameters["databricksWorkspaceId"]["value"] == (
        "7405616424934600"
    )
    assert parameters["tags"]["value"]["environment"] == "prod"
    assert "Microsoft.Logic/workflows@2019-05-01" in template
    assert "type: 'Request'" in template
    assert "kind: 'Http'" in template
    assert "Recurrence" not in template
    assert "recurrence" not in template
    assert "jobs.on_failure" in template
    assert "jobs.on_duration_warning_threshold_exceeded" in template
    assert "740561" not in template


def test_logic_app_does_not_expose_its_signed_callback_url():
    template = BICEP_PATH.read_text(encoding="utf-8")
    lower_template = template.lower()

    assert "listcallbackurl" not in lower_template
    assert "callbackurl" not in lower_template
    assert "sig=" not in lower_template
    assert "output logicappresourceid" in lower_template


def test_deploy_is_preview_first_and_never_sends_a_test_alert():
    script = SCRIPT_PATH.read_text(encoding="utf-8")
    lower_script = script.lower()

    assert "[switch]$Apply" in script
    assert "$ConfigureDatabricks -and -not $Apply" in script
    assert '"deployment", "group", "validate"' in script
    assert '"deployment", "group", "what-if"' in script
    assert script.index('"deployment", "group", "what-if"') < script.index(
        '"deployment", "group", "create"'
    )
    assert '"notification-destinations", "create"' in script
    assert '"notification-destinations", "update"' in script
    assert "listcallbackurl" in lower_script
    assert "test-webhook" not in lower_script
    assert "create-notifications" not in lower_script
    assert "recurrence" not in lower_script
    assert "Production Logic App is deployed and idle" in script


def test_databricks_notification_destination_is_locked_to_production():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert (
        '$expectedDatabricksHost = '
        '"https://adb-7405616424934600.0.azuredatabricks.net"'
    ) in script
    assert "$DatabricksHost.TrimEnd(\"/\").ToLowerInvariant()" in script
    assert (
        "-ConfigureDatabricks is restricted to the production Databricks "
        "workspace."
    ) in script
