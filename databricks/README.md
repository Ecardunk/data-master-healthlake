# HealthLake Medallion Bundle

## Governança de acesso

Os grupos são criados no nível da conta Databricks e atribuídos ao workspace
com a entitlement `USER`; a matriz de privilégios do Unity Catalog está em
[`src/governance/unity_catalog_access.sql`](src/governance/unity_catalog_access.sql).

| Grupo | Dev | Prod |
| --- | --- | --- |
| `data-engineering-admin` | Leitura em Bronze, Silver e Gold | Leitura em Bronze, Silver e Gold |
| `data-engineering` | Leitura em Silver e Gold | Leitura em Silver e Gold |
| `data-analysts` | Sem acesso | Leitura em Silver e Gold |
| `data-scientists` | Sem acesso | Leitura em Silver e Gold |
| `power-bi` | Sem acesso | Leitura em Silver e Gold |

Os cinco grupos são estritamente leitores. Apesar do nome,
`data-engineering-admin` não recebe escrita, ownership, `MANAGE`, acesso a
external locations nem administração do workspace. Apenas os dois grupos de
engenharia são atribuídos ao workspace dev; todos os cinco são atribuídos ao
workspace prod.

Workspace access, `CAN USE` em SQL warehouses, acesso a dashboards e
permissões sobre jobs/pipelines são controles separados dos grants do Unity
Catalog e devem seguir o mesmo princípio de menor privilégio. Escrita, deploy e
execução pertencem a service principals dedicados, nunca aos grupos humanos.

O `run_as` dos três Jobs batch, mais o Job e a Pipeline streaming exclusivos
de produção, é declarado no Bundle e não depende de quem
executou o deploy: dev usa `sp-healthlake-dev-pipeline`
(`03b5799c-110f-484f-8b1b-e3fd88809c64`) e prod usa
`sp-healthlake-prod-pipeline` (`bfeb3006-1824-4361-bacb-3697f6e33262`). O CI/CD
autentica com OAuth M2M. Um usuário humano pode validar o Bundle, mas os deploys
regulares devem ser feitos pelo workflow do ambiente para evitar uma segunda
instância de desenvolvimento sob o diretório pessoal do usuário.

Os grupos começam vazios de propósito: a associação de pessoas deve ser feita
no IdP/SCIM de acordo com a função de cada colaborador, sem conceder privilégios
diretamente a usuários.

Este diretório é o deployável do Databricks para o case. O batch usa
**Lakeflow Jobs** com tabelas Delta gerenciadas; a trilha de sinais vitais usa
**Lakeflow Pipelines**. Todos os recursos são versionados com **Declarative
Automation Bundles**, o nome atual do Databricks Asset Bundles (DAB).

## O que cada recurso faz

- A tarefa Bronze lê exclusivamente
  `raw/<dataset>/odate=<parâmetro>/`, com schema raw explícito, e grava a mesma
  `odate` em tabelas Delta históricas. `_source_file` e `_ingested_at` mantêm a
  linhagem.
- A tarefa Silver lê exclusivamente a partição Bronze aprovada pelo gate
  Bronze→Silver. As transformações compartilhadas com o gate deduplicam o
  snapshot, convertem tipos, normalizam textos e mascaram irreversivelmente os
  identificadores diretos do paciente.
- A tarefa Gold lê exclusivamente a partição Silver aprovada pelo segundo gate,
  remove os identificadores diretos e publica também o KPI diário por hospital.
  O join do KPI usa `odate` e `hospital_id`, evitando cruzar snapshots.
- As três camadas usam os mesmos nomes `patients`, `hospitals`, `doctors`,
  `diseases` e `attendance`, e todas são particionadas fisicamente por `odate`.
  Gold possui ainda `kpi_hospital_daily`, também particionada por `odate`.
- `healthlake_bronze_to_silver_dq` e `healthlake_silver_to_gold_dq`: quality
  gates intermediários com DQX. Cada gate recebe uma `odate`, filtra somente
  essa partição, registra `odate`, `input_rows`, `checked_rows` e
  `removed_by_cleaning` no `violation_summary` das métricas, envia linhas
  reprovadas para tabelas `<catalog>.quarantine.<stage>_<table>` e falha o Job para
  bloquear a promoção. Em produção, a falha do orquestrador gera e-mail e
  webhook para a Logic App; dev não possui alertas. Cada estágio usa uma tabela
  de quarentena própria para isolar os contratos raw e tipado. No gate Bronze→Silver, limpeza,
  tipagem e deduplicação acontecem antes das regras. A dependência DQX está
  fixada em `0.15.0`.
- `healthlake_medallion_refresh`: job que respeita a dependência Bronze →
  DQ Bronze→Silver → Silver → DQ Silver→Gold → Gold e propaga a
  mesma `odate` às cinco tarefas.
- `healthlake_vitals_streaming` (somente prod): consome o único Event Hub pelo
  endpoint Kafka com OAuth e a service credential
  `svc_healthlake_prod_eventhubs_receiver`. Preserva payload e coordenadas na
  Bronze, limpa e tipa antes do DQ, registra inválidos na quarentena, bloqueia
  a atualização completa da Silver se qualquer evento falhar e atualiza dois
  produtos Gold temporais quando o lote é válido.
- `healthlake_vitals_streaming_refresh` (somente prod): executa a Pipeline em
  modo triggered, drena apenas offsets novos a partir do checkpoint e encerra
  o compute. A agenda fica `PAUSED`; fila e retries estão desabilitados e a
  concorrência máxima é uma execução.
- `HealthLake Observability - prod` (somente prod): dashboard AI/BI sob demanda
  com estado real de Jobs/Pipeline, métricas por flow, DQ streaming, frescor,
  latência, quarentena e gates batch por `odate`. Não há dashboard nem SQL
  Warehouse de observabilidade em dev.

O catálogo e os schemas já são criados pela configuração de Unity Catalog:
`healthlake_dev.bronze`, `healthlake_dev.silver` e `healthlake_dev.gold`.
Os arquivos Delta gerenciados ficam em `managed/healthlake_dev`; o container
`raw` continua como landing zone somente leitura.

Cada target lê a Raw do próprio ambiente: dev usa
`abfss://raw@sthealthdatalake001.dfs.core.windows.net/` e prod usa
`abfss://raw@sthlkprodbrs01.dfs.core.windows.net/`. O catálogo
`healthlake_prod` também grava suas tabelas gerenciadas no storage produtivo.

## Comandos de operação

Execute a partir deste diretório, usando o profile já autenticado:

```powershell
$odate = "2026-07-05"

databricks bundle validate --target dev --profile HEALTHLAKE_DEV
databricks bundle deploy --target dev --profile HEALTHLAKE_DEV
databricks bundle run healthlake_medallion_refresh `
  --target dev `
  --profile HEALTHLAKE_DEV `
  --params "odate=$odate"
```

O último comando deve ser executado apenas após o ADF concluir com sucesso a
cópia dos cinco arquivos de uma mesma `odate`. Nenhuma tarefa procura outras
datas no landing ou nas tabelas de origem. A escrita Delta usa `replaceWhere`
com a condição exata da partição; portanto, reexecutar a mesma data corrige
somente essa data e não duplica nem reprocessa o histórico.
O default do parâmetro de Job é vazio e não usa o relógio como fallback;
`--params "odate=YYYY-MM-DD"` é obrigatório para uma execução válida.

Cada tarefa Python serverless publica progresso amigável no painel **Output** do
Databricks, com horário UTC, emojis e envio imediato via `flush=True`. Linhas
como `🚀 [BRONZE] Execução iniciada`, `🧪 [DQX] Regras de qualidade avaliadas` e
`✅ [DELTA] Partição Delta substituída` tornam o andamento fácil de acompanhar.
Os detalhes mostram `odate`, tabela, estratégia de escrita e, nos gates DQX,
contagens de entrada, registros verificados, válidos e enviados à quarentena.
Essas linhas são texto simples, sem JSON. Em caso de falha, a linha visual mostra
somente o tipo do erro; o traceback original permanece disponível no Output para
diagnóstico. Os detalhes são limitados a 500 caracteres e contêm somente
metadados operacionais, nunca registros nem identificadores de pacientes.

## Semântica fail-closed dos gates

O gate falha se qualquer uma das cinco tabelas não tiver linhas para a `odate`
solicitada ou se qualquer linha, depois da limpeza, violar uma regra crítica.
A limpeza remove registros incompletos em campos não-chave antes do DQ; qualquer
violação que permaneça bloqueia o salvamento da tabela inteira. O split válido
não é gravado diretamente. Somente depois que as cinco tabelas passam, o Job
registra a combinação (`dq_stage`, `odate`) em
`observability.dq_promotion_control`; a tarefa seguinte exige exatamente essa
aprovação. A mesma estratégia bloqueia a Gold no segundo gate. Cada escrita
de tabela substitui atomicamente uma única partição; se houver uma falha de
infraestrutura entre tabelas, o downstream não executa e a reexecução
idempotente conclui a mesma `odate`.

As métricas ficam em `observability.dq_run_metrics`: `input_rows` representa as
linhas Bronze/Silver filtradas pela data e `checked_rows` representa o conjunto
depois da limpeza compartilhada. A diferença
`removed_by_cleaning = input_rows - checked_rows`, registrada no JSON de
`violation_summary`, contabiliza o que a limpeza removeu antes das regras.
`odate`, estágio, tabela, válidos, quarentena, status e run ID permitem
reconciliar cada execução.

## Streaming de sinais vitais em produção

O fluxo é separado do batch mensal e não usa `odate`:

```text
Event Hubs -> bronze.vital_events_raw -> limpeza/tipagem -> DQ
                                                |          |
                                                | inválido | falha Silver/Gold
                                                v          v
                                  quarantine.vital_events  silver.vital_events
                                                                  |
                                                                  v
                                      gold.vital_patient_5m
                                      gold.vital_population_hourly
```

A Bronze é append-only e mantém `partition`, `offset`, horário de enfileiramento
e hash do payload. A Silver usa watermark de 25 horas — uma hora de margem
sobre a regra DQ de atraso máximo de 24 horas — e deduplica por `event_id`.
O limite `maxOffsetsPerTrigger=10000` restringe o volume de cada
micro-batch. O Event Hubs retém três dias; portanto, com o produtor ativo, o
backlog precisa ser consumido pelo menos diariamente. A agenda mensal do dia 05
é adequada ao batch, não a esta trilha.

As regras em [`src/streaming/contracts.py`](src/streaming/contracts.py) são
avaliadas somente depois de `from_json`, trim/lowercase, casts e normalização
UTC. Um erro deixa a Bronze disponível para auditoria, alimenta a quarentena e
faz `expect_or_fail` abortar a atualização inteira da Silver; as duas Gold não
avançam. Isso é deliberadamente fail-closed. O evento continuará sendo tentado
a partir do checkpoint até haver um forward-fix aprovado; não use full refresh,
não apague o pipeline e não recrie o consumer group para “pular” o erro.

Contrato DQ streaming versionado:

| Regra | Condição |
| --- | --- |
| `payload_parseable` | JSON parseável como struct e map |
| `payload_fields_exact` | Exatamente os 13 campos do contrato v1, sem extras |
| `schema_version_supported` | `schema_version = 1` |
| `event_type_supported` | `event_type = patient_vital_signs` |
| `event_id_valid_uuid` | UUID válido |
| `producer_run_id_valid_uuid` | UUID válido |
| `patient_id_positive` | `patient_id > 0` |
| `event_time_utc_format` | ISO-8601 UTC terminado em `Z` |
| `event_time_present` | Timestamp convertível |
| `produced_at_utc_format` | ISO-8601 UTC terminado em `Z` |
| `produced_at_present` | Timestamp convertível |
| `event_time_not_future` | Até 5 minutos após o enqueue |
| `event_time_not_stale` | Até 24 horas antes do enqueue |
| `produced_at_not_future` | Até 5 minutos após o enqueue |
| `produced_at_not_stale` | Até 24 horas antes do enqueue |
| `event_precedes_production` | Evento não pode superar produção em mais de 5 minutos |
| `heart_rate_in_range` | 30–220 bpm |
| `oxygen_saturation_in_range` | 50–100% |
| `temperature_in_range` | 30–45 °C |
| `systolic_pressure_in_range` | 60–250 mmHg |
| `diastolic_pressure_in_range` | 30–150 mmHg |
| `systolic_above_diastolic` | Sistólica maior que diastólica |
| `source_present` | Origem não vazia |
| `dq_clean_output` | Invariante fatal: nenhuma violação pode entrar na Silver |

O conjunto de campos, versão e formato UTC são contratuais. As faixas numéricas
do consumidor são intencionalmente mais amplas que as faixas usadas para gerar
o dado sintético: representam um envelope fisiológico plausível, permitindo que
valores anormais cheguem à Gold com `is_abnormal` sem serem confundidos com
payload corrompido.

Deploy sem executar compute:

```powershell
databricks bundle validate --target prod --profile HEALTHLAKE_PROD
databricks bundle plan --target prod --profile HEALTHLAKE_PROD
databricks bundle deploy --target prod --profile HEALTHLAKE_PROD `
  --fail-on-active-runs
databricks warehouses stop b1e5fa5733d587b8 --profile HEALTHLAKE_PROD
```

Uma única execução manual paga:

```powershell
databricks bundle run healthlake_vitals_streaming_refresh `
  --target prod `
  --profile HEALTHLAKE_PROD
```

No GitHub, use `Deploy production` com `run_batch_refresh=false` para somente
promover o Bundle. Para consumir o backlog, use o workflow separado
`Run production streaming backlog`, marque a confirmação explícita e acompanhe
as métricas do event log. O workflow usa o `github.run_id` como token de
idempotência, evitando uma segunda run paga se a mesma requisição for repetida.
O schedule permanece pausado depois dessas ações.

## Observabilidade e alertas somente em produção

O dashboard lê diretamente `system.lakeflow.pipeline_update_timeline`,
`system.lakeflow.job_run_timeline`,
`observability.vital_streaming_pipeline_events`, `dq_run_metrics`,
`dq_promotion_control`, Silver e quarentena. Ele não materializa cópias, não
contém payload, mensagem bruta nem `patient_id` e não possui schedule,
subscription, SQL Alert ou Lakehouse Monitor. O Warehouse é serverless
2X-Small, máximo de um cluster e auto-stop de 10 minutos. Abrir/atualizar o
dashboard inicia esse Warehouse; o deploy de produção também o para
explicitamente ao terminar.

O dashboard publicado usa a credencial do service principal de produção.
`data-engineering-admin` e `data-engineering` recebem apenas `CAN_RUN`; os três
grupos consumidores não têm acesso. O event log e as system tables não são
concedidos diretamente aos grupos humanos. O acesso mínimo do publicador a
`system.lakeflow` está versionado em
[`src/governance/observability_service_principal_access.prod.sql`](src/governance/observability_service_principal_access.prod.sql)
e deve ser aplicado por um account/metastore admin.

Os Jobs produtivos `healthlake_medallion_refresh` e
`healthlake_vitals_streaming_refresh` notificam falha e duração excessiva por
e-mail e por uma notification destination do Databricks. Essa destination
aponta para a Logic App Consumption
`logic-healthlake-alerts-prod-brs-01`, versionada em
[`../infra/observability`](../infra/observability). A Logic App valida o
`workspace_id` no trigger HTTP e também possui uma recorrência mensal para
consultar, por identidade gerenciada read-only, a conclusão do ADF PROD no dia
05. A consulta não inicia pipelines; quando não encontra a run esperada, envia
e-mail. Nenhum webhook de teste é disparado no deploy e DEV não possui recursos
de observabilidade ou alerta.

Como a Pipeline é triggered e fica `IDLE`, o backlog exibido é o observado no
último refresh e inclui o horário/idade da observação. Não representa eventos
que chegaram ao Event Hubs depois que o consumidor parou.

Rollback deve ser feito por revert/forward-fix do código mantendo as mesmas
resource keys, IDs do pipeline, tabelas e checkpoints. Primeiro pause produtor
e agenda, depois implante o último código bom. Nunca exclua Bronze, Event Hub,
consumer group ou service credential durante um rollback normal.
