targetScope = 'resourceGroup'

@description('Azure region for the production Consumption Logic App.')
param location string = resourceGroup().location

@description('Production-only Logic App that receives Databricks job alerts.')
@minLength(1)
@maxLength(80)
param logicAppName string

@description('Production Azure Databricks workspace ID accepted by the webhook.')
param databricksWorkspaceId string

@description('Common Azure resource tags.')
param tags object = {}

resource databricksAlertRouter 'Microsoft.Logic/workflows@2019-05-01' = {
  name: logicAppName
  location: location
  tags: tags
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {}
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
            acknowledge_alert: {
              type: 'Response'
              runAfter: {
                normalize_alert_context: [
                  'Succeeded'
                ]
              }
              inputs: {
                statusCode: 202
                body: '@outputs(\'normalize_alert_context\')'
              }
            }
          }
          else: {
            actions: {
              reject_non_production_or_unsupported_event: {
                type: 'Response'
                inputs: {
                  statusCode: 403
                  body: {
                    accepted: false
                  }
                }
              }
            }
          }
        }
      }
      outputs: {}
    }
    parameters: {}
  }
}

// The signed callback URL is intentionally not emitted as a deployment output.
output logicAppResourceId string = databricksAlertRouter.id
output triggerName string = 'when_databricks_job_alert_arrives'
