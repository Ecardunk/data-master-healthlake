targetScope = 'resourceGroup'

@description('Azure region used by all resources in this deployment.')
param location string = resourceGroup().location

@description('Globally unique Azure Event Hubs namespace name.')
@minLength(6)
@maxLength(50)
param namespaceName string

@description('Event hub that receives the vital-sign events.')
@minLength(1)
@maxLength(256)
param eventHubName string

@description('Dedicated consumer group used only by the production Databricks consumer.')
@minLength(1)
@maxLength(50)
param consumerGroupName string

@description('Azure Databricks Access Connector backed by a system-assigned managed identity.')
@minLength(3)
@maxLength(64)
param accessConnectorName string

@description('Number of Event Hubs throughput units. Auto-inflate remains disabled.')
@minValue(1)
@maxValue(20)
param throughputUnits int = 1

@description('Number of partitions in the event hub.')
@minValue(1)
@maxValue(32)
param partitionCount int = 2

@description('Event retention in days. Standard supports up to seven days with this API.')
@minValue(1)
@maxValue(7)
param messageRetentionInDays int = 3

@description('Optional Microsoft Entra object ID for the production event producer. Empty means no sender role assignment.')
param producerPrincipalObjectId string = ''

@description('Principal type associated with producerPrincipalObjectId.')
@allowed([
  'ServicePrincipal'
  'Group'
  'User'
])
param producerPrincipalType string = 'ServicePrincipal'

@description('Common Azure resource tags.')
param tags object = {}

var eventHubsDataReceiverRoleDefinitionId = 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde'
var eventHubsDataSenderRoleDefinitionId = '2b629674-e913-4c01-ae53-ef4638d8f975'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: throughputUnits
  }
  properties: {
    disableLocalAuth: true
    isAutoInflateEnabled: false
    kafkaEnabled: true
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubsNamespace
  name: eventHubName
  properties: {
    // Capture is disabled by omission. Azure requires destination/encoding
    // fields whenever captureDescription is present, even when enabled=false.
    messageRetentionInDays: messageRetentionInDays
    partitionCount: partitionCount
    status: 'Active'
  }
}

resource databricksConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: consumerGroupName
  properties: {
    userMetadata: 'Dedicated to the production HealthLake Databricks streaming consumer.'
  }
}

resource databricksAccessConnector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: accessConnectorName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

module databricksReceiverRoleAssignment './modules/eventhub-role-assignment.bicep' = {
  name: 'databricks-eventhub-receiver-role'
  params: {
    namespaceName: eventHubsNamespace.name
    eventHubName: eventHub.name
    principalId: databricksAccessConnector.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionGuid: eventHubsDataReceiverRoleDefinitionId
  }
}

resource producerSenderRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(producerPrincipalObjectId)) {
  name: guid(eventHub.id, eventHubsDataSenderRoleDefinitionId, producerPrincipalObjectId)
  scope: eventHub
  properties: {
    principalId: producerPrincipalObjectId
    principalType: producerPrincipalType
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      eventHubsDataSenderRoleDefinitionId
    )
  }
}

// Only non-secret identifiers and connection metadata are returned.
output namespaceResourceId string = eventHubsNamespace.id
output fullyQualifiedNamespace string = '${eventHubsNamespace.name}.servicebus.windows.net'
output eventHubResourceId string = eventHub.id
output eventHubName string = eventHub.name
output consumerGroupName string = databricksConsumerGroup.name
output accessConnectorResourceId string = databricksAccessConnector.id
output accessConnectorPrincipalId string = databricksAccessConnector.identity.principalId
