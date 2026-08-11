# Observabilidade e alertas de produção

Este módulo mantém somente em `rg-healthlake-prod-brs-01` a Logic App
Consumption `logic-healthlake-alerts-prod-brs-01`. Ela possui dois gatilhos:

- HTTP para falhas e avisos de duração dos Jobs Databricks de produção;
- recorrência mensal no dia 05 às 23:55, no horário de São Paulo, para confirmar
  que o pipeline `pl_copy_s3_to_adls_raw` terminou com sucesso para a `odate` do
  próprio dia 05.

## Arquitetura

```text
Job Databricks PROD ── falha/duração ──┬── e-mail nativo Databricks
                                      └── notification destination
                                                │ webhook HTTPS assinado
                                                ▼
                                      Logic App: valida workspace/evento
                                                └── normaliza Job, run e horário

Recorrência mensal ── dia 05, 23:55 BRT ── Logic App (identidade gerenciada)
                                                │ consulta runs do ADF PROD
                                                ├── encontrou Succeeded: encerra
                                                └── não encontrou: e-mail Outlook
```

| Recurso | Responsabilidade |
| --- | --- |
| Logic App Consumption | Hospedar os triggers HTTP e mensal e executar as ramificações de validação |
| Identidade gerenciada da Logic App | Consultar o histórico de runs do ADF sem credencial estática |
| Role customizada `HealthLake Production ADF Pipeline Run Reader` | Limitar a identidade a leitura do Data Factory e `queryPipelineRuns` |
| Conexão Outlook existente | Enviar somente o alerta de ausência da ingestão ADF |
| Notification destination `healthlake-prod-logicapp-alerts` | Manter a URL assinada do webhook criptografada no Databricks |

## Eventos e decisões

O Databricks envia ao webhook os eventos dos dois Jobs produtivos configurados
em [`../../databricks/resources/alerts.prod.yml`](../../databricks/resources/alerts.prod.yml):

| Job | Evento | Limiar |
| --- | --- | --- |
| `healthlake_medallion_refresh` | `jobs.on_failure` | Falha terminal |
| `healthlake_medallion_refresh` | `jobs.on_duration_warning_threshold_exceeded` | Mais de 7.200 segundos |
| `healthlake_vitals_streaming_refresh` | `jobs.on_failure` | Falha terminal |
| `healthlake_vitals_streaming_refresh` | `jobs.on_duration_warning_threshold_exceeded` | Mais de 900 segundos |

O trigger rejeita logicamente eventos de outro workspace e tipos diferentes dos
dois acima. Para um evento aceito, a ação `normalize_alert_context` registra no
histórico da própria run o ambiente, `event_type`, `workspace_id`, `job_id`,
`job_name`, `run_id` e `received_at_utc`. O e-mail desse evento já é enviado
pelo canal nativo do Databricks; a Logic App não envia uma segunda cópia.

Na recorrência mensal, a Logic App calcula a data local no formato `yyyy-MM-dd`,
consulta as runs do ADF desde o início daquele dia e filtra pelo pipeline,
status `Succeeded` e parâmetro `odate`. A ausência de correspondência envia um
e-mail de alta importância contendo o pipeline, Data Factory, partição e os
cinco datasets esperados.

O monitor do ADF consulta a API de runs diretamente com a identidade gerenciada
da Logic App. Uma role customizada concede somente leitura e
`queryPipelineRuns`; ela não permite iniciar, cancelar, alterar ou excluir
pipelines. Como uma execução bem-sucedida do pipeline exige a cópia de
`patients`, `hospitals`, `doctors`, `diseases` e `attendance`, a ausência dessa
run gera o e-mail produtivo. O monitor nunca inicia o ADF automaticamente.

O Databricks envia para o trigger HTTP somente falhas e avisos de duração dos
Jobs produtivos. A URL assinada nunca é gravada no repositório nem exibida como
output do Bicep; o script a copia diretamente para uma notification destination
criptografada no workspace de produção. Sem uma ação `Response` explícita, o
gatilho HTTP usa a resposta assíncrona padrão aceita pelo webhook; essa forma é
necessária para coexistir com o gatilho recorrente no mesmo workflow.

## Segurança e limites

- O módulo existe somente em produção; dev não recebe Logic App nem notification
  destination.
- O schema HTTP exige `event_type`, `workspace_id`, `run.run_id`, `job.job_id` e
  `job.name`, e a condição interna restringe workspace e tipos de evento.
- A URL assinada não é output do Bicep, não é commitada e não aparece nos logs
  normais do deploy. Ela passa por arquivo temporário, removido ao final, até a
  configuração criptografada do Databricks.
- A identidade gerenciada não possui ações para iniciar, cancelar, editar ou
  excluir pipelines ADF.
- Não há autorremediação: nenhum dos triggers executa ADF ou Databricks.
- Runs canceladas ou puladas permanecem consultáveis no dashboard, mas não
  geram webhook para evitar ruído operacional.

## Deploy e reconciliação

O script é preview-first. Sem `-Apply`, executa apenas validate e what-if:

```powershell
./infra/observability/scripts/deploy.ps1 `
  -SubscriptionId 6b409a82-932c-4136-b8d5-1cb02345e23e
```

Para aplicar a Logic App e reconciliar a notification destination do Databricks
de produção:

```powershell
./infra/observability/scripts/deploy.ps1 `
  -SubscriptionId 6b409a82-932c-4136-b8d5-1cb02345e23e `
  -Apply `
  -ConfigureDatabricks `
  -Confirm:$false
```

O deploy não envia alerta de teste, não inicia ADF e não executa Databricks.
O trigger ADF produtivo continua com seu estado operacional administrado fora
deste módulo.

O modo `-Apply -ConfigureDatabricks` também:

1. confirma que a Logic App está habilitada em `brazilsouth`, possui identidade
   gerenciada, tag `environment=prod` e o trigger mensal;
2. obtém a callback URL HTTPS sem imprimi-la;
3. cria ou atualiza, pelo nome estável, a notification destination no workspace
   produtivo;
4. valida que a destination resultante é do tipo `WEBHOOK` e informa somente seu
   UUID não secreto, que deve coincidir com
   `logic_app_notification_destination_id` no Bundle de produção.

## Runbook de atendimento

### Falha ou duração excessiva no Databricks

1. Abra `HealthLake Observability - prod` e selecione **Operação batch** ou
   **Operação streaming**.
2. Confirme Job, run, estado e duração. No batch, identifique a `odate` e o gate
   DQX; no streaming, compare o Job com o update e os flows da Pipeline.
3. Consulte quarentena e métricas DQ antes de qualquer reexecução.
4. Reexecute manualmente apenas a carga/partição afetada após corrigir a causa.

### Ausência da ingestão ADF no dia 05

1. Confirme a presença dos arquivos `patients`, `hospitals`, `doctors`,
   `diseases` e `attendance` na origem S3.
2. Consulte a run de `pl_copy_s3_to_adls_raw` e valide que o parâmetro `odate`
   corresponde ao dia alertado.
3. Corrija arquivo, credencial ou conectividade e só então inicie uma nova run
   manual. A Logic App continuará apenas como detector.
