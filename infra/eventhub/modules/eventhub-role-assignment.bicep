targetScope = 'resourceGroup'

@description('Existing Event Hubs namespace that owns the target hub.')
param namespaceName string

@description('Existing event hub used as the exact RBAC scope.')
param eventHubName string

@description('Microsoft Entra object ID that receives the role.')
param principalId string

@description('Built-in Azure role definition GUID.')
param roleDefinitionGuid string

@description('Azure RBAC principal type.')
@allowed([
  'ServicePrincipal'
  'Group'
  'User'
])
param principalType string = 'ServicePrincipal'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2024-01-01' existing = {
  name: namespaceName
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' existing = {
  parent: eventHubsNamespace
  name: eventHubName
}

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventHub.id, roleDefinitionGuid, principalId)
  scope: eventHub
  properties: {
    principalId: principalId
    principalType: principalType
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roleDefinitionGuid
    )
  }
}

output roleAssignmentId string = roleAssignment.id
