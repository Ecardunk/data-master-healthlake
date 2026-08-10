# Event Hubs de produção do HealthLake

Este diretório provisiona o único Azure Event Hubs usado pelo streaming de
produção. A implantação é OAuth-only: não cria políticas SAS, não obtém chaves
e não grava connection strings ou outros secrets no Key Vault.

## Contrato implantado

| Item | Valor |
| --- | --- |
| Resource group existente | `rg-healthlake-prod-brs-01` |
| Subscription prod | `6b409a82-932c-4136-b8d5-1cb02345e23e` |
| Região | `brazilsouth` |
| Namespace | `evhns-healthlake-prod-brs-01` |
| SKU/capacidade | Standard, 1 TU |
| Kafka | habilitado |
| TLS mínimo | 1.2 |
| Autenticação local/SAS | desabilitada (`disableLocalAuth: true`) |
| Auto-inflate | desabilitado |
| Hub | `evh-vitals-prod` |
| Partições | 2 |
| Retenção | 3 dias |
| Capture | desabilitado |
| Consumer group | `cg-healthlake-databricks-prod` |
| Access Connector | `databricks-connector-healthlake-prod-eventhubs` |

Três dias cobrem a execução diária e uma janela curta de recuperação, limitando
o volume retido. Uma interrupção maior que essa janela pode causar perda de
eventos e deve bloquear a operação até avaliação explícita.
Essa retenção pressupõe consumo streaming ao menos diário; ela não serve para um
consumer executado somente no dia 05 de cada mês. A agenda mensal continua
válida para o fluxo batch, não para este hub.

O Access Connector usa managed identity atribuída pelo sistema e recebe somente
`Azure Event Hubs Data Receiver`, no escopo de `evh-vitals-prod`. O produtor não
recebe permissão por padrão. Quando informado explicitamente ao script, seu
**object ID** do Microsoft Entra (não application/client ID) recebe somente
`Azure Event Hubs Data Sender`, também no escopo do hub.

O endpoint público permanece habilitado para Kafka em
`evhns-healthlake-prod-brs-01.servicebus.windows.net:9093`, mas aceita somente
OAuth e TLS 1.2. Private Endpoint e regras de rede são uma evolução separada;
não foram inferidos porque o repositório não declara a rede do workspace. Essa
exposição é aceitável somente para o case com dados sintéticos. Para dados
clínicos reais, Private Link/NCC e `publicNetworkAccess: 'Disabled'` são um gate
obrigatório antes do go-live.

## Arquivos

- `main.bicep`: namespace, hub, consumer group, Access Connector e RBAC.
- `modules/eventhub-role-assignment.bicep`: role assignment do receiver depois
  que o `principalId` da managed identity foi resolvido.
- `parameters/prod.parameters.json`: configuração produtiva fail-closed.
- `parameters/uc-service-credential*.prod.json`: definição da credencial,
  binding exclusivo ao workspace prod e grant do runtime prod.
- `scripts/deploy.ps1`: validação, what-if, aplicação opcional e criação
  idempotente da service credential do Unity Catalog.
- `tests/test_eventhub_contracts.py`: contratos estáticos de custo e segurança.

## Pré-requisitos

- Azure CLI com Bicep CLI instalado e uma sessão autenticada.
- Providers `Microsoft.EventHub` e `Microsoft.Databricks` já registrados.
- O resource group `rg-healthlake-prod-brs-01` já existente.
- A identidade de deploy com `Contributor` e `User Access Administrator` (ou
  `Owner`) no resource group, pois o template cria recursos e role assignments.
- Para `-ConfigureUnityCatalog`, Databricks CLI autenticado no workspace de
  produção. Um service principal precisa ser account admin para criar uma
  service credential baseada em managed identity e metastore admin para
  reconciliar binding/grants depois que o ownership já pertence ao runtime.

## Validar sem implantar

Sem `-Apply`, o script executa `validate` e `what-if` com
`ResourceIdOnly`. Nenhum recurso Azure ou Unity Catalog é alterado:

```powershell
./infra/eventhub/scripts/deploy.ps1 `
  -SubscriptionId '<subscription-guid>'
```

O script falha se o arquivo produtivo divergir dos nomes, capacidade, partições
ou retenção aprovados. Ele também falha se os providers ou o resource group não
existirem; não registra providers implicitamente.

## Aplicar deliberadamente

Implantação OAuth-only, sem produtor:

```powershell
./infra/eventhub/scripts/deploy.ps1 `
  -SubscriptionId '<subscription-guid>' `
  -Apply
```

Implantação com uma identidade produtora existente:

```powershell
./infra/eventhub/scripts/deploy.ps1 `
  -SubscriptionId '<subscription-guid>' `
  -ProducerPrincipalObjectId '<entra-object-guid>' `
  -ProducerPrincipalType ServicePrincipal `
  -Apply
```

O parâmetro versionado do produtor continua vazio. O pós-check exige exatamente
zero ou uma identidade `Data Sender` direta, conforme o argumento. Se encontrar
uma identidade antiga, o script falha sem removê-la. A revogação é deliberada:

```powershell
# Sem ProducerPrincipalObjectId, remove todos os Data Sender diretos do hub.
./infra/eventhub/scripts/deploy.ps1 `
  -SubscriptionId '<subscription-guid>' `
  -ReconcileProducerRole `
  -Apply
```

Para trocar o produtor, use `-ReconcileProducerRole` junto com o novo
`-ProducerPrincipalObjectId`. Permissões herdadas do namespace, resource group
ou assinatura nunca são removidas automaticamente: elas bloqueiam o pós-check
e exigem correção administrativa no escopo de origem.

## Service credential no Unity Catalog

Opcionalmente, a mesma execução cria a service credential
`svc_healthlake_prod_eventhubs_receiver` referenciando o Access Connector:

```powershell
./infra/eventhub/scripts/deploy.ps1 `
  -SubscriptionId '<subscription-guid>' `
  -Apply `
  -ConfigureUnityCatalog `
  -DatabricksProfile PROD
```

A operação é idempotente e usa os três contratos JSON versionados. Ela cria ou
verifica a credencial, impõe `ISOLATION_MODE_ISOLATED`, remove bindings de outros
workspaces, mantém apenas o workspace prod `7405616424934600`, concede `ACCESS`
e transfere ownership ao runtime prod
`bfeb3006-1824-4361-bacb-3697f6e33262`. Se uma credencial existente apontar para
outro Access Connector, o script falha. Nenhum desses arquivos contém client
secret.

O pós-check confirma isolamento, binding exclusivo, grant e owner. Ainda é
obrigatório validar a obtenção de credenciais temporárias e a autenticação Kafka
OAuth no runtime suportado antes de ativar a agenda de streaming.

## Testes locais

```powershell
az bicep build --file ./infra/eventhub/main.bicep --stdout | Out-Null
python -m pytest ./infra/eventhub/tests -q
```

O build é local. Não execute `deployment group create` durante revisão.

## Referências oficiais

- [Event Hubs com Microsoft Entra ID](https://learn.microsoft.com/azure/event-hubs/authorize-access-azure-active-directory)
- [Referência Bicep do namespace Event Hubs](https://learn.microsoft.com/azure/templates/microsoft.eventhub/2024-01-01/namespaces)
- [Access Connector do Azure Databricks](https://learn.microsoft.com/azure/templates/microsoft.databricks/accessconnectors)
- [Criar service credentials no Unity Catalog](https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-services/service-credentials)
