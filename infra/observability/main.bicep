targetScope = 'resourceGroup'

@description('Azure region for the production Consumption Logic App.')
param location string = resourceGroup().location

@description('Production-only Logic App that receives Databricks alerts and checks the monthly ADF ingestion.')
@minLength(1)
@maxLength(80)
param logicAppName string

@description('Production Azure Databricks workspace ID accepted by the webhook.')
param databricksWorkspaceId string

@description('Production Data Factory monitored on day 05.')
param dataFactoryName string

@description('Production Data Factory pipeline that copies the five S3 datasets to ADLS.')
param dataFactoryPipelineName string = 'pl_copy_s3_to_adls_raw'

@description('Existing authorized Outlook API connection, owned by the production resource group.')
param outlookConnectionResourceId string

@description('Mailbox that receives the production ADF missing-ingestion alert.')
param adfAlertRecipient string

@description('Mailbox used as sender and reply-to by the authorized Outlook connection.')
param adfAlertSender string

@description('Common Azure resource tags.')
param tags object = {}

var expectedAdfDatasets = [
  'patients'
  'hospitals'
  'doctors'
  'diseases'
  'attendance'
]
var adfMonitorRoleName = 'HealthLake Production ADF Pipeline Run Reader'

resource productionDataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: dataFactoryName
}

resource adfMonitorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: guid(subscription().id, resourceGroup().id, adfMonitorRoleName)
  properties: {
    roleName: adfMonitorRoleName
    description: 'Least-privilege access for the production Logic App to query ADF pipeline-run status.'
    type: 'CustomRole'
    permissions: [
      {
        actions: [
          'Microsoft.DataFactory/factories/read'
          'Microsoft.DataFactory/factories/queryPipelineRuns/action'
          'Microsoft.DataFactory/factories/queryPipelineRuns/read'
          'Microsoft.DataFactory/factories/pipelineruns/read'
        ]
        notActions: []
        dataActions: []
        notDataActions: []
      }
    ]
    assignableScopes: [
      resourceGroup().id
    ]
  }
}

resource databricksAlertRouter 'Microsoft.Logic/workflows@2019-05-01' = {
  name: logicAppName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        '$connections': {
          type: 'Object'
          defaultValue: {}
        }
      }
      triggers: {
        when_databricks_job_alert_arrives: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              required: [
                'event_type'
                'workspace_id'
                'run'
                'job'
              ]
              properties: {
                event_type: {
                  type: 'string'
                }
                workspace_id: {
                  type: 'string'
                }
                run: {
                  type: 'object'
                  required: [
                    'run_id'
                  ]
                  properties: {
                    run_id: {
                      type: 'string'
                    }
                  }
                }
                job: {
                  type: 'object'
                  required: [
                    'job_id'
                    'name'
                  ]
                  properties: {
                    job_id: {
                      type: 'string'
                    }
                    name: {
                      type: 'string'
                    }
                  }
                }
              }
            }
          }
        }
        check_adf_ingestion_day_05: {
          type: 'Recurrence'
          recurrence: {
            frequency: 'Month'
            interval: 1
            timeZone: 'E. South America Standard Time'
            schedule: {
              monthDays: [
                5
              ]
              hours: [
                23
              ]
              minutes: [
                55
              ]
            }
          }
        }
      }
      actions: {
        accept_only_production_failures_and_duration_warnings: {
          type: 'If'
          expression: {
            and: [
              {
                equals: [
                  '@triggerBody()?[\'workspace_id\']'
                  databricksWorkspaceId
                ]
              }
              {
                or: [
                  {
                    equals: [
                      '@triggerBody()?[\'event_type\']'
                      'jobs.on_failure'
                    ]
                  }
                  {
                    equals: [
                      '@triggerBody()?[\'event_type\']'
                      'jobs.on_duration_warning_threshold_exceeded'
                    ]
                  }
                ]
              }
            ]
          }
          actions: {
            normalize_alert_context: {
              type: 'Compose'
              inputs: {
                environment: 'prod'
                event_type: '@triggerBody()?[\'event_type\']'
                workspace_id: '@triggerBody()?[\'workspace_id\']'
                job_id: '@triggerBody()?[\'job\']?[\'job_id\']'
                job_name: '@triggerBody()?[\'job\']?[\'name\']'
                run_id: '@triggerBody()?[\'run\']?[\'run_id\']'
                received_at_utc: '@utcNow()'
              }
            }
          }
          else: {
            actions: {}
          }
        }
        monitor_production_adf_ingestion: {
          type: 'If'
          expression: '@empty(triggerBody())'
          actions: {
            target_odate: {
              type: 'Compose'
              inputs: '@convertTimeZone(utcNow(), \'UTC\', \'E. South America Standard Time\', \'yyyy-MM-dd\')'
            }
            query_production_adf_pipeline_runs: {
              type: 'Http'
              runAfter: {
                target_odate: [
                  'Succeeded'
                ]
              }
              inputs: {
                method: 'POST'
                uri: '${environment().resourceManager}${substring(productionDataFactory.id, 1)}/queryPipelineRuns?api-version=2018-06-01'
                authentication: {
                  type: 'ManagedServiceIdentity'
                  audience: environment().resourceManager
                }
                body: {
                  lastUpdatedAfter: '@convertTimeZone(concat(outputs(\'target_odate\'), \'T00:00:00\'), \'E. South America Standard Time\', \'UTC\', \'yyyy-MM-ddTHH:mm:ssZ\')'
                  lastUpdatedBefore: '@utcNow()'
                  filters: [
                    {
                      operand: 'PipelineName'
                      operator: 'Equals'
                      values: [
                        dataFactoryPipelineName
                      ]
                    }
                  ]
                  orderBy: [
                    {
                      orderBy: 'RunStart'
                      order: 'DESC'
                    }
                  ]
                }
              }
            }
            successful_target_odate_runs: {
              type: 'Query'
              runAfter: {
                query_production_adf_pipeline_runs: [
                  'Succeeded'
                ]
              }
              inputs: {
                from: '@body(\'query_production_adf_pipeline_runs\')?[\'value\']'
                where: '@and(equals(item()?[\'pipelineName\'], \'${dataFactoryPipelineName}\'), equals(item()?[\'status\'], \'Succeeded\'), equals(item()?[\'parameters\']?[\'odate\'], outputs(\'target_odate\')))'
              }
            }
            alert_when_adf_ingestion_is_missing: {
              type: 'If'
              runAfter: {
                successful_target_odate_runs: [
                  'Succeeded'
                ]
              }
              expression: '@equals(length(body(\'successful_target_odate_runs\')), 0)'
              actions: {
                send_adf_missing_ingestion_email: {
                  type: 'ApiConnection'
                  inputs: {
                    host: {
                      connection: {
                        name: '@parameters(\'$connections\')[\'outlook\'][\'connectionId\']'
                      }
                    }
                    method: 'post'
                    path: '/v2/Mail'
                    body: {
                      From: adfAlertSender
                      To: adfAlertRecipient
                      ReplyTo: adfAlertSender
                      Importance: 'High'
                      Subject: 'PROD ADF - ingestão S3 para ADLS não concluída no dia 05'
                      Body: '<p>Não foi encontrada uma execução <strong>Succeeded</strong> do pipeline <code>${dataFactoryPipelineName}</code> no ADF PROD <code>${dataFactoryName}</code> para a partição <strong>@{outputs(\'target_odate\')}</strong>.</p><p>Datasets esperados: ${join(expectedAdfDatasets, ', ')}.</p><p>Verifique o trigger, os arquivos no S3 e a execução do pipeline. O monitor não iniciou nenhuma execução automaticamente.</p>'
                    }
                  }
                }
              }
              else: {
                actions: {}
              }
            }
          }
          else: {
            actions: {}
          }
        }
      }
      outputs: {}
    }
    parameters: {
      '$connections': {
        value: {
          outlook: {
            connectionId: outlookConnectionResourceId
            connectionName: last(split(outlookConnectionResourceId, '/'))
            id: subscriptionResourceId('Microsoft.Web/locations/managedApis', location, 'outlook')
          }
        }
      }
    }
  }
}

resource adfMonitorRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(productionDataFactory.id, databricksAlertRouter.id, adfMonitorRole.id)
  scope: productionDataFactory
  properties: {
    principalId: databricksAlertRouter.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: adfMonitorRole.id
  }
}

// The signed callback URL is intentionally not emitted as a deployment output.
output logicAppResourceId string = databricksAlertRouter.id
output triggerName string = 'when_databricks_job_alert_arrives'
output adfMonitorTriggerName string = 'check_adf_ingestion_day_05'
