[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "prod")]
    [string]$Environment
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$adfRoot = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $adfRoot "environments/$Environment.json"
$config = Get-Content -Raw -LiteralPath $configPath | ConvertFrom-Json

if ($config.desiredTriggerState -ne "Stopped") {
    throw "ADF deployments are fail-closed: desiredTriggerState must be Stopped."
}

if ($Environment -eq "dev") {
    if ($config.adlsUrl -notlike "*sthealthdatalake001*" -or $config.keyVaultUrl -notlike "*kv-data-master-case*") {
        throw "Development ADF configuration does not point exclusively to development resources."
    }
    if ($config.adlsUrl -like "*sthlkprodbrs01*") {
        throw "Development ADF configuration references production storage."
    }
}
else {
    if ($config.adlsUrl -notlike "*sthlkprodbrs01*" -or $config.keyVaultUrl -notlike "*kv-hlk-prod-brs-01*") {
        throw "Production ADF configuration does not point exclusively to production resources."
    }
    if ($config.adlsUrl -like "*sthealthdatalake001*" -or $config.keyVaultUrl -like "*kv-data-master-case*") {
        throw "Production ADF configuration references development resources."
    }
}

function Invoke-AzChecked {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $result = & az @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI failed: az $($Arguments -join ' ')"
    }
    return $result
}

function Invoke-AzWithJsonFile {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$JsonFlag,
        [Parameter(Mandatory = $true)][object]$Payload
    )

    $temporaryPath = Join-Path ([System.IO.Path]::GetTempPath()) (
        "healthlake-adf-{0}.json" -f [guid]::NewGuid().ToString("N")
    )
    try {
        $json = $Payload | ConvertTo-Json -Depth 100
        [System.IO.File]::WriteAllText(
            $temporaryPath,
            $json,
            [System.Text.UTF8Encoding]::new($false)
        )
        Invoke-AzChecked -Arguments ($Arguments + @($JsonFlag, "@$temporaryPath")) | Out-Null
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
}

$factoryName = Invoke-AzChecked -Arguments @(
    "datafactory", "list",
    "--resource-group", $config.resourceGroup,
    "--query", "[?name=='$($config.factoryName)'].name | [0]",
    "--output", "tsv",
    "--only-show-errors"
)
$factoryExists = -not [string]::IsNullOrWhiteSpace([string]$factoryName)

if (-not $factoryExists) {
    Invoke-AzChecked -Arguments @(
        "datafactory", "create",
        "--resource-group", $config.resourceGroup,
        "--factory-name", $config.factoryName,
        "--location", $config.location,
        "--public-network-access", "Enabled",
        "--tags", "environment=$Environment", "project=data-master-healthlake", "managed-by=adf-deploy-script",
        "--output", "none",
        "--only-show-errors"
    ) | Out-Null
}

$factoryId = (Invoke-AzChecked -Arguments @(
    "datafactory", "show",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--query", "id",
    "--output", "tsv",
    "--only-show-errors"
)).Trim()

$principalId = (Invoke-AzChecked -Arguments @(
    "datafactory", "show",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--query", "identity.principalId",
    "--output", "tsv",
    "--only-show-errors"
)).Trim()

if ([string]::IsNullOrWhiteSpace($principalId)) {
    Invoke-AzChecked -Arguments @(
        "resource", "update",
        "--ids", $factoryId,
        "--set", "identity.type=SystemAssigned",
        "--output", "none",
        "--only-show-errors"
    ) | Out-Null
}

$adlsProperties = (Get-Content -Raw -LiteralPath (
    Join-Path $adfRoot "linkedService/ls_adls_healthlake.json"
) | ConvertFrom-Json).properties
$adlsProperties.typeProperties.url = $config.adlsUrl
Invoke-AzWithJsonFile -Arguments @(
    "datafactory", "linked-service", "create",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", "ls_adls_healthlake",
    "--output", "none",
    "--only-show-errors"
) -JsonFlag "--properties" -Payload $adlsProperties

$keyVaultProperties = (Get-Content -Raw -LiteralPath (
    Join-Path $adfRoot "linkedService/ls_keyvault_healthlake.json"
) | ConvertFrom-Json).properties
$keyVaultProperties.typeProperties.baseUrl = $config.keyVaultUrl
Invoke-AzWithJsonFile -Arguments @(
    "datafactory", "linked-service", "create",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", "ls_keyvault_healthlake",
    "--output", "none",
    "--only-show-errors"
) -JsonFlag "--properties" -Payload $keyVaultProperties

$s3Properties = (Get-Content -Raw -LiteralPath (
    Join-Path $adfRoot "linkedService/ls_s3_healthlake.json"
) | ConvertFrom-Json).properties
Invoke-AzWithJsonFile -Arguments @(
    "datafactory", "linked-service", "create",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", "ls_s3_healthlake",
    "--output", "none",
    "--only-show-errors"
) -JsonFlag "--properties" -Payload $s3Properties

foreach ($datasetFile in @(
    "ds_s3_raw_file_binary.json",
    "ds_adls_raw_file_binary.json"
)) {
    $datasetPath = Join-Path $adfRoot "dataset/$datasetFile"
    $dataset = Get-Content -Raw -LiteralPath $datasetPath | ConvertFrom-Json
    Invoke-AzWithJsonFile -Arguments @(
        "datafactory", "dataset", "create",
        "--resource-group", $config.resourceGroup,
        "--factory-name", $config.factoryName,
        "--name", $dataset.name,
        "--output", "none",
        "--only-show-errors"
    ) -JsonFlag "--properties" -Payload $dataset.properties
}

$pipeline = Get-Content -Raw -LiteralPath (
    Join-Path $adfRoot "pipeline/pl_copy_s3_to_adls_raw.json"
) | ConvertFrom-Json
$pipeline.properties.parameters.s3_bucket_name.defaultValue = $config.s3BucketName
Invoke-AzWithJsonFile -Arguments @(
    "datafactory", "pipeline", "create",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", $pipeline.name,
    "--output", "none",
    "--only-show-errors"
) -JsonFlag "--pipeline" -Payload $pipeline.properties

$triggerName = Invoke-AzChecked -Arguments @(
    "datafactory", "trigger", "list",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--query", "[?name=='trigger_case'].name | [0]",
    "--output", "tsv",
    "--only-show-errors"
)
$triggerExists = -not [string]::IsNullOrWhiteSpace([string]$triggerName)
if ($triggerExists) {
    Invoke-AzChecked -Arguments @(
        "datafactory", "trigger", "stop",
        "--resource-group", $config.resourceGroup,
        "--factory-name", $config.factoryName,
        "--name", "trigger_case",
        "--output", "none",
        "--only-show-errors"
    ) | Out-Null
}

$trigger = Get-Content -Raw -LiteralPath (
    Join-Path $adfRoot "trigger/trigger_case.json"
) | ConvertFrom-Json
$trigger.properties.PSObject.Properties.Remove("runtimeState")
$trigger.properties.pipelines[0].parameters.s3_bucket_name = $config.s3BucketName
Invoke-AzWithJsonFile -Arguments @(
    "datafactory", "trigger", "create",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", $trigger.name,
    "--output", "none",
    "--only-show-errors"
) -JsonFlag "--properties" -Payload $trigger.properties

Invoke-AzChecked -Arguments @(
    "datafactory", "trigger", "stop",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", $trigger.name,
    "--output", "none",
    "--only-show-errors"
) | Out-Null

$triggerState = (Invoke-AzChecked -Arguments @(
    "datafactory", "trigger", "show",
    "--resource-group", $config.resourceGroup,
    "--factory-name", $config.factoryName,
    "--name", $trigger.name,
    "--query", "properties.runtimeState",
    "--output", "tsv",
    "--only-show-errors"
)).Trim()

if ($triggerState -ne "Stopped") {
    throw "Trigger deployment was not fail-closed. Current state: $triggerState"
}

Write-Output "ADF $($config.factoryName) deployed for $Environment; trigger=$triggerState; no pipeline run was started."
