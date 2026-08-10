[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")]
    [string]$SubscriptionId,

    [switch]$Apply,

    [switch]$ConfigureDatabricks,

    [ValidateNotNullOrEmpty()]
    [string]$DatabricksHost = "https://adb-7405616424934600.0.azuredatabricks.net",

    [string]$DatabricksProfile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$observabilityRoot = Split-Path -Parent $PSScriptRoot
$templatePath = Join-Path $observabilityRoot "main.bicep"
$parametersPath = Join-Path $observabilityRoot "parameters/prod.parameters.json"
$resourceGroupName = "rg-healthlake-prod-brs-01"
$deploymentName = "healthlake-observability-alerts-prod"
$destinationDisplayName = "healthlake-prod-logicapp-alerts"
$expectedDatabricksHost = "https://adb-7405616424934600.0.azuredatabricks.net"

function Invoke-AzChecked {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $result = & az @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI failed: az $($Arguments -join ' ')`n$(($result | Out-String).Trim())"
    }
    return ($result | Out-String).Trim()
}

function Invoke-DatabricksChecked {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $profileArguments = @()
    if (-not [string]::IsNullOrWhiteSpace($DatabricksProfile)) {
        $profileArguments = @("--profile", $DatabricksProfile)
    }
    $result = & databricks @Arguments @profileArguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Databricks CLI failed: databricks $($Arguments -join ' ')`n$(($result | Out-String).Trim())"
    }
    return ($result | Out-String).Trim()
}

if ($ConfigureDatabricks -and -not $Apply) {
    throw "-ConfigureDatabricks changes the production workspace and requires -Apply."
}
if (
    $ConfigureDatabricks -and
    $DatabricksHost.TrimEnd("/").ToLowerInvariant() -ne $expectedDatabricksHost
) {
    throw "-ConfigureDatabricks is restricted to the production Databricks workspace."
}
if ($null -eq (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI is required."
}
if ($ConfigureDatabricks -and $null -eq (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI is required when -ConfigureDatabricks is used."
}

$configuration = Get-Content -Raw -LiteralPath $parametersPath | ConvertFrom-Json
if (
    $configuration.parameters.location.value -ne "brazilsouth" -or
    $configuration.parameters.logicAppName.value -ne "logic-healthlake-alerts-prod-brs-01" -or
    $configuration.parameters.databricksWorkspaceId.value -ne "7405616424934600" -or
    $configuration.parameters.tags.value.environment -ne "prod"
) {
    throw "The committed Logic App parameters are not production-only."
}

$SubscriptionId = $SubscriptionId.ToLowerInvariant()
$account = Invoke-AzChecked -Arguments @(
    "account", "show",
    "--subscription", $SubscriptionId,
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
if ($account.id -ne $SubscriptionId) {
    throw "Azure CLI resolved a subscription different from -SubscriptionId."
}

$deploymentArguments = @(
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--name", $deploymentName,
    "--template-file", $templatePath,
    "--parameters", "@$parametersPath",
    "--only-show-errors"
)

Invoke-AzChecked -Arguments (@(
    "deployment", "group", "validate"
) + $deploymentArguments + @("--output", "none")) | Out-Null

$whatIf = Invoke-AzChecked -Arguments (@(
    "deployment", "group", "what-if"
) + $deploymentArguments + @("--result-format", "ResourceIdOnly"))
Write-Output $whatIf

if (-not $Apply) {
    Write-Output "Validation and what-if completed. No Azure or Databricks resource was changed."
    return
}

if (-not $PSCmdlet.ShouldProcess(
    "$resourceGroupName/$deploymentName",
    "Apply the production-only Logic App deployment"
)) {
    Write-Output "Apply cancelled. No resource was changed."
    return
}

Invoke-AzChecked -Arguments (@(
    "deployment", "group", "create"
) + $deploymentArguments + @("--output", "none")) | Out-Null

$logicAppName = [string]$configuration.parameters.logicAppName.value
$logicApp = Invoke-AzChecked -Arguments @(
    "resource", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--resource-type", "Microsoft.Logic/workflows",
    "--name", $logicAppName,
    "--api-version", "2019-05-01",
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
if (
    $logicApp.location -ne "brazilsouth" -or
    $logicApp.properties.state -ne "Enabled" -or
    $logicApp.tags.environment -ne "prod"
) {
    throw "The deployed Logic App does not match the production contract."
}

if ($ConfigureDatabricks) {
    $env:DATABRICKS_HOST = $DatabricksHost
    if ([string]::IsNullOrWhiteSpace($DatabricksProfile) -and [string]::IsNullOrWhiteSpace($env:DATABRICKS_AUTH_TYPE)) {
        $env:DATABRICKS_AUTH_TYPE = "azure-cli"
    }

    $triggerResourceId = "$($logicApp.id)/triggers/when_databricks_job_alert_arrives"
    $callbackResponse = Invoke-AzChecked -Arguments @(
        "rest",
        "--method", "post",
        "--url", "https://management.azure.com$triggerResourceId/listCallbackUrl?api-version=2016-06-01",
        "--output", "json",
        "--only-show-errors"
    ) | ConvertFrom-Json
    $callbackUrl = [string]$callbackResponse.value
    if ([string]::IsNullOrWhiteSpace($callbackUrl) -or -not $callbackUrl.StartsWith("https://")) {
        throw "Azure did not return a valid HTTPS callback URL."
    }

    $destinationResponse = Invoke-DatabricksChecked -Arguments @(
        "notification-destinations", "list", "--output", "json"
    ) | ConvertFrom-Json
    $destinations = @()
    if (
        $null -ne $destinationResponse -and
        $destinationResponse.PSObject.Properties.Name -contains "results"
    ) {
        $destinations = @($destinationResponse.results)
    }
    elseif ($null -ne $destinationResponse) {
        $destinations = @($destinationResponse)
    }
    $matchingDestinations = @($destinations | Where-Object {
        $_.display_name -eq $destinationDisplayName
    })
    if ($matchingDestinations.Count -gt 1) {
        throw "More than one Databricks notification destination has the production display name."
    }

    $payload = [ordered]@{
        display_name = $destinationDisplayName
        config = [ordered]@{
            generic_webhook = [ordered]@{
                url = $callbackUrl
            }
        }
    }
    $temporaryPayloadPath = Join-Path ([System.IO.Path]::GetTempPath()) (
        "healthlake-logicapp-destination-{0}.json" -f [guid]::NewGuid().ToString("N")
    )
    try {
        [System.IO.File]::WriteAllText(
            $temporaryPayloadPath,
            ($payload | ConvertTo-Json -Depth 10),
            [System.Text.UTF8Encoding]::new($false)
        )
        if ($matchingDestinations.Count -eq 0) {
            $destination = Invoke-DatabricksChecked -Arguments @(
                "notification-destinations", "create",
                "--json", "@$temporaryPayloadPath",
                "--output", "json"
            ) | ConvertFrom-Json
        }
        else {
            $destinationId = [string]$matchingDestinations[0].id
            $destination = Invoke-DatabricksChecked -Arguments @(
                "notification-destinations", "update", $destinationId,
                "--json", "@$temporaryPayloadPath",
                "--output", "json"
            ) | ConvertFrom-Json
        }
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPayloadPath) {
            Remove-Item -LiteralPath $temporaryPayloadPath -Force
        }
        $callbackUrl = $null
    }

    if (
        $destination.display_name -ne $destinationDisplayName -or
        $destination.destination_type -ne "WEBHOOK" -or
        [string]::IsNullOrWhiteSpace([string]$destination.id)
    ) {
        throw "The Databricks webhook destination failed its post-deployment contract."
    }
    Write-Output "Databricks production notification destination ID: $($destination.id)"
}

Write-Output "Production Logic App is deployed and idle; no webhook test was sent."
