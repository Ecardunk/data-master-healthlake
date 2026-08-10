# Observabilidade e alertas de produção

Este módulo cria somente em `rg-healthlake-prod-brs-01` a Logic App Consumption
`logic-healthlake-alerts-prod-brs-01`. Ela possui apenas um trigger HTTP por
evento: não há recurrence, polling, plano Standard nem execução em repouso.

O Databricks envia para esse endpoint somente falhas e avisos de duração dos
Jobs produtivos. A URL assinada do trigger nunca é gravada no repositório nem
exibida como output do Bicep; o script a copia diretamente para uma notification
destination criptografada no workspace de produção. A Logic App rejeita outro
`workspace_id`, normaliza o contexto do alerta e preserva o payload no histórico
da execução. O e-mail nativo do Job continua sendo o canal humano, sem exigir
uma conexão Outlook/SMTP adicional na Logic App.

O script é preview-first. Sem `-Apply`, executa apenas validate e what-if:

```powershell
./infra/observability/scripts/deploy.ps1 `
  -SubscriptionId 6b409a82-932c-4136-b8d5-1cb02345e23e
```

Para provisionar a Logic App e reconciliar a notification destination do
Databricks de produção, use uma identidade administradora do workspace:

```powershell
./infra/observability/scripts/deploy.ps1 `
  -SubscriptionId 6b409a82-932c-4136-b8d5-1cb02345e23e `
  -Apply `
  -ConfigureDatabricks `
  -Confirm:$false
```

O comando informa somente o UUID não secreto da destination. Configure esse
valor em `logic_app_notification_destination_id` no target `prod` do Bundle.
Não execute um teste do webhook como parte do deploy: isso criaria uma run
real da Logic App.
