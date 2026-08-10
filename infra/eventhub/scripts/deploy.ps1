[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")]
    [string]$SubscriptionId,

    [ValidatePattern("^$|^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")]
    [string]$ProducerPrincipalObjectId = "",

    [ValidateSet("ServicePrincipal", "Group", "User")]
    [string]$ProducerPrincipalType = "ServicePrincipal",

    [switch]$ReconcileProducerRole,

    [switch]$Apply,

    [switch]$ConfigureUnityCatalog,

    [ValidateNotNullOrEmpty()]
    [string]$DatabricksProfile = "DEFAULT"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$eventHubRoot = Split-Path -Parent $PSScriptRoot
$templatePath = Join-Path $eventHubRoot "main.bicep"
$parametersPath = Join-Path $eventHubRoot "parameters/prod.parameters.json"
$ucCredentialPath = Join-Path $eventHubRoot "parameters/uc-service-credential.prod.json"
$ucBindingsPath = Join-Path $eventHubRoot "parameters/uc-service-credential.bindings.prod.json"
$ucGrantsPath = Join-Path $eventHubRoot "parameters/uc-service-credential.grants.prod.json"
$deploymentName = "healthlake-eventhub-prod"
$resourceGroupName = "rg-healthlake-prod-brs-01"
$senderRoleDefinitionGuid = "2b629674-e913-4c01-ae53-ef4638d8f975"

function Invoke-AzChecked {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $result = & az @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $message = ($result | Out-String).Trim()
        throw "Azure CLI failed: az $($Arguments -join ' ')`n$message"
    }
    return ($result | Out-String).Trim()
}

function Invoke-DatabricksChecked {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $result = & databricks @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $message = ($result | Out-String).Trim()
        throw "Databricks CLI failed: databricks $($Arguments -join ' ')`n$message"
    }
    return ($result | Out-String).Trim()
}

function Assert-ExpectedProductionParameters {
    param([Parameter(Mandatory = $true)][object]$Configuration)

    $expectedValues = [ordered]@{
        location = "brazilsouth"
        namespaceName = "evhns-healthlake-prod-brs-01"
        eventHubName = "evh-vitals-prod"
        consumerGroupName = "cg-healthlake-databricks-prod"
        accessConnectorName = "databricks-connector-healthlake-prod-eventhubs"
        throughputUnits = 1
        partitionCount = 2
        messageRetentionInDays = 3
    }

    foreach ($entry in $expectedValues.GetEnumerator()) {
        $actualValue = $Configuration.parameters.($entry.Key).value
        if ($actualValue -ne $entry.Value) {
            throw "Unexpected production parameter '$($entry.Key)': '$actualValue'."
        }
    }

    if (-not [string]::IsNullOrWhiteSpace(
        [string]$Configuration.parameters.producerPrincipalObjectId.value
    )) {
        throw "The committed production parameters must not grant a producer identity. Use the explicit script parameter."
    }
}

function Assert-UnityCatalogProductionContract {
    param(
        [Parameter(Mandatory = $true)][object]$Credential,
        [Parameter(Mandatory = $true)][object]$Bindings,
        [Parameter(Mandatory = $true)][object]$Grants,
        [Parameter(Mandatory = $true)][string]$Subscription
    )

    $expectedCredentialName = "svc_healthlake_prod_eventhubs_receiver"
    $expectedWorkspaceId = 7405616424934600
    $expectedRuntimePrincipal = "bfeb3006-1824-4361-bacb-3697f6e33262"
    $expectedConnectorId = (
        "/subscriptions/{0}/resourceGroups/rg-healthlake-prod-brs-01/providers/" +
        "Microsoft.Databricks/accessConnectors/" +
        "databricks-connector-healthlake-prod-eventhubs"
    ) -f $Subscription

    if (
        $Credential.name -ne $expectedCredentialName -or
        $Credential.purpose -ne "SERVICE" -or
        $Credential.skip_validation -ne $false -or
        $Credential.azure_managed_identity.access_connector_id -ne $expectedConnectorId
    ) {
        throw "The versioned Unity Catalog service credential contract is invalid."
    }

    $bindingAdds = @($Bindings.add)
    if (
        $bindingAdds.Count -ne 1 -or
        [long]$bindingAdds[0].workspace_id -ne $expectedWorkspaceId -or
        $bindingAdds[0].binding_type -ne "BINDING_TYPE_READ_WRITE" -or
        @($Bindings.remove).Count -ne 0
    ) {
        throw "The versioned Unity Catalog workspace binding contract is invalid."
    }

    $grantChanges = @($Grants.changes)
    if (
        $grantChanges.Count -ne 1 -or
        $grantChanges[0].principal -ne $expectedRuntimePrincipal -or
        @($grantChanges[0].add).Count -ne 1 -or
        $grantChanges[0].add[0] -ne "ACCESS"
    ) {
        throw "The versioned Unity Catalog grant contract is invalid."
    }
}

function Get-UnityCatalogCredential {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Profile
    )

    $arguments = @(
        "credentials", "get-credential", $Name,
        "--profile", $Profile,
        "--output", "json"
    )
    $result = & databricks @arguments 2>&1
    $exitCode = $LASTEXITCODE
    $message = ($result | Out-String).Trim()

    if ($exitCode -eq 0) {
        return ($message | ConvertFrom-Json)
    }
    if ($message -match "(?i)RESOURCE_DOES_NOT_EXIST|credential.+(not found|does not exist)") {
        return $null
    }
    throw "Databricks CLI failed while reading credential '$Name':`n$message"
}

function Ensure-UnityCatalogCredential {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$AccessConnectorId,
        [Parameter(Mandatory = $true)][string]$Profile,
        [Parameter(Mandatory = $true)][string]$PayloadPath
    )

    if ($null -eq (Get-Command databricks -ErrorAction SilentlyContinue)) {
        throw "Databricks CLI is required when -ConfigureUnityCatalog is used."
    }

    $existingCredential = Get-UnityCatalogCredential -Name $Name -Profile $Profile
    if ($null -ne $existingCredential) {
        if ($existingCredential.purpose -ne "SERVICE") {
            throw "Unity Catalog credential '$Name' exists but is not a service credential."
        }
        if (
            $existingCredential.PSObject.Properties.Name -notcontains "azure_managed_identity" -or
            $existingCredential.azure_managed_identity.access_connector_id -ne $AccessConnectorId
        ) {
            throw "Unity Catalog credential '$Name' points to another Access Connector."
        }
        Write-Output "Unity Catalog service credential '$Name' already matches the Access Connector."
        return
    }

    $credentialCreated = $false
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        try {
            Invoke-DatabricksChecked -Arguments @(
                "credentials", "create-credential", $Name,
                "--json", "@$PayloadPath",
                "--profile", $Profile,
                "--output", "json"
            ) | Out-Null
            $credentialCreated = $true
            break
        }
        catch {
            $credentialAfterFailure = Get-UnityCatalogCredential `
                -Name $Name `
                -Profile $Profile
            if ($null -ne $credentialAfterFailure) {
                if (
                    $credentialAfterFailure.purpose -ne "SERVICE" -or
                    $credentialAfterFailure.PSObject.Properties.Name -notcontains "azure_managed_identity" -or
                    $credentialAfterFailure.azure_managed_identity.access_connector_id -ne $AccessConnectorId
                ) {
                    throw "Unity Catalog credential '$Name' appeared during retry with an unexpected definition."
                }
                $credentialCreated = $true
                break
            }
            if ($attempt -eq 6) {
                throw
            }
            Write-Warning "Unity Catalog credential creation has not propagated; retrying in 10 seconds."
            Start-Sleep -Seconds 10
        }
    }
    if (-not $credentialCreated) {
        throw "Unity Catalog service credential '$Name' was not created."
    }

    $createdCredential = Get-UnityCatalogCredential -Name $Name -Profile $Profile
    if (
        $null -eq $createdCredential -or
        $createdCredential.purpose -ne "SERVICE" -or
        $createdCredential.PSObject.Properties.Name -notcontains "azure_managed_identity" -or
        $createdCredential.azure_managed_identity.access_connector_id -ne $AccessConnectorId
    ) {
        throw "Unity Catalog service credential '$Name' failed its post-deployment contract."
    }

    Write-Output "Unity Catalog service credential '$Name' created without a client secret."
}

function Set-UnityCatalogCredentialGovernance {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Profile,
        [Parameter(Mandatory = $true)][long]$WorkspaceId,
        [Parameter(Mandatory = $true)][string]$RuntimePrincipal,
        [Parameter(Mandatory = $true)][string]$BindingsPath,
        [Parameter(Mandatory = $true)][string]$GrantsPath
    )

    $desiredBindings = Get-Content -Raw -LiteralPath $BindingsPath | ConvertFrom-Json
    $currentBindingsResponse = Invoke-DatabricksChecked -Arguments @(
        "workspace-bindings", "get-bindings", "credential", $Name,
        "--profile", $Profile,
        "--output", "json"
    ) | ConvertFrom-Json
    $currentBindings = @()
    if ($currentBindingsResponse.PSObject.Properties.Name -contains "bindings") {
        $currentBindings = @($currentBindingsResponse.bindings)
    }

    $bindingsToRemove = @($currentBindings | Where-Object {
        [long]$_.workspace_id -ne $WorkspaceId
    } | ForEach-Object {
        [ordered]@{
            workspace_id = [long]$_.workspace_id
            binding_type = [string]$_.binding_type
        }
    })
    $bindingPayload = [ordered]@{
        add = @($desiredBindings.add)
        remove = $bindingsToRemove
    }

    $temporaryBindingsPath = Join-Path ([System.IO.Path]::GetTempPath()) (
        "healthlake-uc-bindings-{0}.json" -f [guid]::NewGuid().ToString("N")
    )
    try {
        [System.IO.File]::WriteAllText(
            $temporaryBindingsPath,
            ($bindingPayload | ConvertTo-Json -Depth 10),
            [System.Text.UTF8Encoding]::new($false)
        )
        Invoke-DatabricksChecked -Arguments @(
            "workspace-bindings", "update-bindings", "credential", $Name,
            "--json", "@$temporaryBindingsPath",
            "--profile", $Profile,
            "--output", "json"
        ) | Out-Null
    }
    finally {
        if (Test-Path -LiteralPath $temporaryBindingsPath) {
            Remove-Item -LiteralPath $temporaryBindingsPath -Force
        }
    }

    Invoke-DatabricksChecked -Arguments @(
        "credentials", "update-credential", $Name,
        "--isolation-mode", "ISOLATION_MODE_ISOLATED",
        "--profile", $Profile,
        "--output", "json"
    ) | Out-Null

    Invoke-DatabricksChecked -Arguments @(
        "grants", "update", "credential", $Name,
        "--json", "@$GrantsPath",
        "--profile", $Profile,
        "--output", "json"
    ) | Out-Null

    Invoke-DatabricksChecked -Arguments @(
        "credentials", "update-credential", $Name,
        "--owner", $RuntimePrincipal,
        "--profile", $Profile,
        "--output", "json"
    ) | Out-Null

    $finalCredential = Get-UnityCatalogCredential -Name $Name -Profile $Profile
    if (
        $null -eq $finalCredential -or
        $finalCredential.PSObject.Properties.Name -notcontains "isolation_mode" -or
        $finalCredential.PSObject.Properties.Name -notcontains "owner" -or
        $finalCredential.isolation_mode -ne "ISOLATION_MODE_ISOLATED" -or
        $finalCredential.owner -ne $RuntimePrincipal
    ) {
        throw "Unity Catalog credential isolation/ownership does not match production."
    }

    $finalBindingsResponse = Invoke-DatabricksChecked -Arguments @(
        "workspace-bindings", "get-bindings", "credential", $Name,
        "--profile", $Profile,
        "--output", "json"
    ) | ConvertFrom-Json
    $finalBindings = @()
    if ($finalBindingsResponse.PSObject.Properties.Name -contains "bindings") {
        $finalBindings = @($finalBindingsResponse.bindings)
    }
    if (
        $finalBindings.Count -ne 1 -or
        [long]$finalBindings[0].workspace_id -ne $WorkspaceId -or
        $finalBindings[0].binding_type -ne "BINDING_TYPE_READ_WRITE"
    ) {
        throw "Unity Catalog credential is not bound exclusively to the production workspace."
    }

    $runtimeGrantsResponse = Invoke-DatabricksChecked -Arguments @(
        "grants", "get", "credential", $Name,
        "--principal", $RuntimePrincipal,
        "--profile", $Profile,
        "--output", "json"
    ) | ConvertFrom-Json
    $privilegeAssignments = @()
    if ($runtimeGrantsResponse.PSObject.Properties.Name -contains "privilege_assignments") {
        $privilegeAssignments = @($runtimeGrantsResponse.privilege_assignments)
    }
    $runtimeHasAccess = @($privilegeAssignments | Where-Object {
        $principalValue = ""
        if ($_.PSObject.Properties.Name -contains "principal") {
            $principalValue = [string]$_.principal
        }
        elseif ($_.PSObject.Properties.Name -contains "principal_id") {
            $principalValue = [string]$_.principal_id
        }
        $principalValue -eq $RuntimePrincipal -and @($_.privileges) -contains "ACCESS"
    }).Count -eq 1
    if (-not $runtimeHasAccess) {
        throw "The production runtime does not have ACCESS on the service credential."
    }

    Write-Output "Unity Catalog credential '$Name' is isolated, bound to prod, granted and runtime-owned."
}

function Get-EventHubRoleAssignments {
    param(
        [Parameter(Mandatory = $true)][string]$Scope,
        [Parameter(Mandatory = $true)][string]$RoleDefinitionGuid,
        [Parameter(Mandatory = $true)][string]$Subscription
    )

    $response = Invoke-AzChecked -Arguments @(
        "role", "assignment", "list",
        "--subscription", $Subscription,
        "--scope", $Scope,
        "--include-inherited",
        "--fill-principal-name", "false",
        "--output", "json",
        "--only-show-errors"
    ) | ConvertFrom-Json

    return @($response | Where-Object {
        ([string]$_.roleDefinitionId).EndsWith(
            "/$RoleDefinitionGuid",
            [System.StringComparison]::OrdinalIgnoreCase
        )
    })
}

function Test-AzureResourceExists {
    param(
        [Parameter(Mandatory = $true)][string]$ResourceId,
        [Parameter(Mandatory = $true)][string]$Subscription
    )

    $result = & az resource show `
        --ids $ResourceId `
        --subscription $Subscription `
        --output none `
        --only-show-errors 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) {
        return $true
    }

    $message = ($result | Out-String).Trim()
    if ($message -match "(?i)ResourceNotFound|could not be found|was not found") {
        return $false
    }
    throw "Azure CLI failed while checking '$ResourceId':`n$message"
}

function Wait-ForDirectRoleAssignment {
    param(
        [Parameter(Mandatory = $true)][string]$Scope,
        [Parameter(Mandatory = $true)][string]$RoleDefinitionGuid,
        [Parameter(Mandatory = $true)][string]$PrincipalId,
        [Parameter(Mandatory = $true)][string]$Subscription
    )

    for ($attempt = 1; $attempt -le 12; $attempt++) {
        $matches = @(Get-EventHubRoleAssignments `
            -Scope $Scope `
            -RoleDefinitionGuid $RoleDefinitionGuid `
            -Subscription $Subscription | Where-Object {
                $_.scope -eq $Scope -and $_.principalId -eq $PrincipalId
            })
        if ($matches.Count -eq 1) {
            return $matches[0]
        }
        if ($matches.Count -gt 1) {
            throw "More than one identical role assignment exists for principal '$PrincipalId'."
        }
        if ($attempt -lt 12) {
            Start-Sleep -Seconds 5
        }
    }

    throw "Role assignment propagation timed out for principal '$PrincipalId'."
}

if ($ConfigureUnityCatalog -and -not $Apply) {
    throw "-ConfigureUnityCatalog changes Unity Catalog and therefore requires -Apply."
}
if ($ReconcileProducerRole -and -not $Apply) {
    throw "-ReconcileProducerRole removes stale role assignments and therefore requires -Apply."
}

$SubscriptionId = $SubscriptionId.ToLowerInvariant()
if (-not [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId)) {
    $ProducerPrincipalObjectId = $ProducerPrincipalObjectId.ToLowerInvariant()
}

if ($null -eq (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI is required."
}

$configuration = Get-Content -Raw -LiteralPath $parametersPath | ConvertFrom-Json
Assert-ExpectedProductionParameters -Configuration $configuration
$ucCredentialConfiguration = Get-Content -Raw -LiteralPath $ucCredentialPath | ConvertFrom-Json
$ucBindingsConfiguration = Get-Content -Raw -LiteralPath $ucBindingsPath | ConvertFrom-Json
$ucGrantsConfiguration = Get-Content -Raw -LiteralPath $ucGrantsPath | ConvertFrom-Json
Assert-UnityCatalogProductionContract `
    -Credential $ucCredentialConfiguration `
    -Bindings $ucBindingsConfiguration `
    -Grants $ucGrantsConfiguration `
    -Subscription $SubscriptionId

$namespaceName = [string]$configuration.parameters.namespaceName.value
$eventHubName = [string]$configuration.parameters.eventHubName.value
$consumerGroupName = [string]$configuration.parameters.consumerGroupName.value
$accessConnectorName = [string]$configuration.parameters.accessConnectorName.value
$unityCatalogCredentialName = [string]$ucCredentialConfiguration.name
$unityCatalogWorkspaceId = [long]$ucBindingsConfiguration.add[0].workspace_id
$unityCatalogRuntimePrincipal = [string]$ucGrantsConfiguration.changes[0].principal

$account = Invoke-AzChecked -Arguments @(
    "account", "show",
    "--subscription", $SubscriptionId,
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
if ($account.id -ne $SubscriptionId) {
    throw "Azure CLI resolved a subscription different from -SubscriptionId."
}

$resourceGroupExists = Invoke-AzChecked -Arguments @(
    "group", "exists",
    "--name", $resourceGroupName,
    "--subscription", $SubscriptionId,
    "--output", "tsv",
    "--only-show-errors"
)
if ($resourceGroupExists -ne "true") {
    throw "Required resource group '$resourceGroupName' does not exist in the selected subscription."
}

foreach ($providerNamespace in @("Microsoft.EventHub", "Microsoft.Databricks")) {
    $registrationState = Invoke-AzChecked -Arguments @(
        "provider", "show",
        "--namespace", $providerNamespace,
        "--subscription", $SubscriptionId,
        "--query", "registrationState",
        "--output", "tsv",
        "--only-show-errors"
    )
    if ($registrationState -ne "Registered") {
        throw "Azure provider '$providerNamespace' must be Registered before deployment."
    }
}

$parameterFileArgument = "@$parametersPath"
$parameterOverrides = @()
if (-not [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId)) {
    $parameterOverrides += @(
        "producerPrincipalObjectId=$ProducerPrincipalObjectId",
        "producerPrincipalType=$ProducerPrincipalType"
    )
}

$deploymentArguments = @(
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--name", $deploymentName,
    "--template-file", $templatePath,
    "--parameters", $parameterFileArgument
) + $parameterOverrides + @("--only-show-errors")

Invoke-AzChecked -Arguments (@(
    "deployment", "group", "validate"
) + $deploymentArguments + @("--output", "none")) | Out-Null

$whatIfResult = Invoke-AzChecked -Arguments (@(
    "deployment", "group", "what-if"
) + $deploymentArguments + @("--result-format", "ResourceIdOnly"))
Write-Output $whatIfResult

$plannedEventHubResourceId = (
    "/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.EventHub/" +
    "namespaces/{2}/eventhubs/{3}"
) -f $SubscriptionId, $resourceGroupName, $namespaceName, $eventHubName
$preDeploymentUnexpectedSenders = @()
if (Test-AzureResourceExists `
    -ResourceId $plannedEventHubResourceId `
    -Subscription $SubscriptionId
) {
    $preDeploymentSenders = @(Get-EventHubRoleAssignments `
        -Scope $plannedEventHubResourceId `
        -RoleDefinitionGuid $senderRoleDefinitionGuid `
        -Subscription $SubscriptionId)
    $preDeploymentInheritedSenders = @($preDeploymentSenders | Where-Object {
        $_.scope -ne $plannedEventHubResourceId
    })
    if ($preDeploymentInheritedSenders.Count -gt 0) {
        $inheritedScopes = ($preDeploymentInheritedSenders.scope | Sort-Object -Unique) -join ", "
        throw "Inherited Data Sender assignments must be removed before deployment. Scopes: $inheritedScopes"
    }

    $preDeploymentUnexpectedSenders = @($preDeploymentSenders | Where-Object {
        $_.scope -eq $plannedEventHubResourceId -and (
            [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId) -or
            $_.principalId -ne $ProducerPrincipalObjectId
        )
    })
    if (
        $preDeploymentUnexpectedSenders.Count -gt 0 -and
        -not $ReconcileProducerRole
    ) {
        $unexpectedPrincipals = (
            $preDeploymentUnexpectedSenders.principalId | Sort-Object -Unique
        ) -join ", "
        throw "Existing Data Sender assignments differ from the requested producer: $unexpectedPrincipals. No deployment was applied."
    }
}

if (-not $Apply) {
    Write-Output "Validation and what-if completed. No Azure or Unity Catalog resource was changed."
    return
}

if (-not $PSCmdlet.ShouldProcess(
    "$resourceGroupName/$deploymentName",
    "Apply the production Event Hubs deployment"
)) {
    Write-Output "Apply cancelled. No resource was changed."
    return
}

foreach ($assignment in $preDeploymentUnexpectedSenders) {
    if ($PSCmdlet.ShouldProcess(
        [string]$assignment.id,
        "Remove stale Data Sender before granting the requested producer"
    )) {
        Invoke-AzChecked -Arguments @(
            "role", "assignment", "delete",
            "--subscription", $SubscriptionId,
            "--ids", [string]$assignment.id,
            "--output", "none",
            "--only-show-errors"
        ) | Out-Null
    }
}

Invoke-AzChecked -Arguments (@(
    "deployment", "group", "create"
) + $deploymentArguments + @("--output", "none")) | Out-Null

$namespace = Invoke-AzChecked -Arguments @(
    "eventhubs", "namespace", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--name", $namespaceName,
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
if (
    $namespace.sku.name -ne "Standard" -or
    $namespace.sku.capacity -ne 1 -or
    $namespace.isAutoInflateEnabled -ne $false -or
    $namespace.kafkaEnabled -ne $true -or
    $namespace.minimumTlsVersion -ne "1.2" -or
    $namespace.disableLocalAuth -ne $true
) {
    throw "The deployed Event Hubs namespace does not match the production security/capacity contract."
}

$eventHub = Invoke-AzChecked -Arguments @(
    "eventhubs", "eventhub", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--namespace-name", $namespaceName,
    "--name", $eventHubName,
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
$captureEnabled = $false
if (
    $eventHub.PSObject.Properties.Name -contains "captureDescription" -and
    $null -ne $eventHub.captureDescription
) {
    $captureEnabled = [bool]$eventHub.captureDescription.enabled
}
if (
    $eventHub.partitionCount -ne 2 -or
    $eventHub.messageRetentionInDays -ne 3 -or
    $captureEnabled -ne $false
) {
    throw "The deployed event hub does not match the production partition/retention/capture contract."
}

Invoke-AzChecked -Arguments @(
    "eventhubs", "eventhub", "consumer-group", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--namespace-name", $namespaceName,
    "--eventhub-name", $eventHubName,
    "--name", $consumerGroupName,
    "--output", "none",
    "--only-show-errors"
) | Out-Null

$accessConnector = Invoke-AzChecked -Arguments @(
    "databricks", "access-connector", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $resourceGroupName,
    "--name", $accessConnectorName,
    "--output", "json",
    "--only-show-errors"
) | ConvertFrom-Json
$accessConnectorId = [string]$accessConnector.id
$accessConnectorPrincipalId = [string]$accessConnector.identity.principalId
if ([string]::IsNullOrWhiteSpace($accessConnectorPrincipalId)) {
    throw "The Databricks Access Connector has no system-assigned managed identity."
}

$eventHubResourceId = [string]$eventHub.id
$receiverRoleDefinitionGuid = "a638d3c7-ab3a-418d-83e6-5f17a39d4fde"
Wait-ForDirectRoleAssignment `
    -Scope $eventHubResourceId `
    -RoleDefinitionGuid $receiverRoleDefinitionGuid `
    -PrincipalId $accessConnectorPrincipalId `
    -Subscription $SubscriptionId | Out-Null

$senderAssignments = @(Get-EventHubRoleAssignments `
    -Scope $eventHubResourceId `
    -RoleDefinitionGuid $senderRoleDefinitionGuid `
    -Subscription $SubscriptionId)
$inheritedSenderAssignments = @($senderAssignments | Where-Object {
    $_.scope -ne $eventHubResourceId
})
if ($inheritedSenderAssignments.Count -gt 0) {
    $inheritedScopes = ($inheritedSenderAssignments.scope | Sort-Object -Unique) -join ", "
    throw "Inherited Azure Event Hubs Data Sender assignments violate hub-level least privilege. Scopes: $inheritedScopes"
}

$unexpectedSenderAssignments = @($senderAssignments | Where-Object {
    [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId) -or
    $_.principalId -ne $ProducerPrincipalObjectId
})
if ($unexpectedSenderAssignments.Count -gt 0 -and -not $ReconcileProducerRole) {
    $unexpectedPrincipals = ($unexpectedSenderAssignments.principalId | Sort-Object -Unique) -join ", "
    throw "Unexpected direct Data Sender assignments exist for: $unexpectedPrincipals. Re-run with -ReconcileProducerRole only after approving their removal."
}

foreach ($assignment in $unexpectedSenderAssignments) {
    if ($PSCmdlet.ShouldProcess(
        [string]$assignment.id,
        "Remove stale direct Azure Event Hubs Data Sender assignment"
    )) {
        Invoke-AzChecked -Arguments @(
            "role", "assignment", "delete",
            "--subscription", $SubscriptionId,
            "--ids", [string]$assignment.id,
            "--output", "none",
            "--only-show-errors"
        ) | Out-Null
    }
}

if (-not [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId)) {
    Wait-ForDirectRoleAssignment `
        -Scope $eventHubResourceId `
        -RoleDefinitionGuid $senderRoleDefinitionGuid `
        -PrincipalId $ProducerPrincipalObjectId `
        -Subscription $SubscriptionId | Out-Null
}

$expectedSenderCount = if (
    [string]::IsNullOrWhiteSpace($ProducerPrincipalObjectId)
) { 0 } else { 1 }
$senderStateMatches = $false
for ($attempt = 1; $attempt -le 12; $attempt++) {
    $remainingDirectSenders = @(Get-EventHubRoleAssignments `
        -Scope $eventHubResourceId `
        -RoleDefinitionGuid $senderRoleDefinitionGuid `
        -Subscription $SubscriptionId | Where-Object {
            $_.scope -eq $eventHubResourceId
        })
    $senderStateMatches = (
        $remainingDirectSenders.Count -eq $expectedSenderCount -and
        (
            $expectedSenderCount -eq 0 -or
            $remainingDirectSenders[0].principalId -eq $ProducerPrincipalObjectId
        )
    )
    if ($senderStateMatches) {
        break
    }
    if ($attempt -lt 12) {
        Start-Sleep -Seconds 5
    }
}
if (-not $senderStateMatches) {
    throw "Direct Data Sender assignments do not match the explicitly approved producer identity."
}

if ($ConfigureUnityCatalog) {
    Ensure-UnityCatalogCredential `
        -Name $unityCatalogCredentialName `
        -AccessConnectorId $accessConnectorId `
        -Profile $DatabricksProfile `
        -PayloadPath $ucCredentialPath
    Set-UnityCatalogCredentialGovernance `
        -Name $unityCatalogCredentialName `
        -Profile $DatabricksProfile `
        -WorkspaceId $unityCatalogWorkspaceId `
        -RuntimePrincipal $unityCatalogRuntimePrincipal `
        -BindingsPath $ucBindingsPath `
        -GrantsPath $ucGrantsPath
}

Write-Output (
    "Production Event Hubs infrastructure is compliant: namespace={0}; hub={1}; consumerGroup={2}; localAuth=disabled." -f
    $namespaceName, $eventHubName, $consumerGroupName
)
