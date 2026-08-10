# Observabilidade e alertas de produção

Este módulo mantém somente em `rg-healthlake-prod-brs-01` a Logic App
Consumption `logic-healthlake-alerts-prod-brs-01`. Ela possui dois gatilhos:

- HTTP para falhas e avisos de duração dos Jobs Databricks de produção;
- recorrência mensal no dia 05 às 23:55, no horário de São Paulo, para confirmar
  que o pipeline `pl_copy_s3_to_adls_raw` terminou com sucesso para a `odate` do
  próprio dia 05.

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
