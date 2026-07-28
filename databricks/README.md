# HealthLake Medallion Bundle

## Governança de acesso

Os grupos são criados no nível da conta Databricks e atribuídos ao workspace
com a entitlement `USER`; a matriz de privilégios do Unity Catalog está em
[`src/governance/unity_catalog_access.sql`](src/governance/unity_catalog_access.sql).

| Grupo | Acesso a dados | Não recebe |
| --- | --- | --- |
| `healthlake-dev-bi-readers` | `SELECT` na camada Gold | Bronze, Silver, escrita e administração |
| `healthlake-dev-data-analysts` | `SELECT` na camada Gold | Bronze, Silver, escrita e administração |
| `healthlake-dev-data-engineers-readers` | `SELECT` nas camadas Bronze e Silver | Gold, escrita, acesso direto ao ADLS e administração |

Os grupos começam vazios de propósito: a associação de pessoas deve ser feita
no IdP/SCIM de acordo com a função de cada colaborador, sem conceder privilégios
diretamente a usuários.

Este diretório é o deployável do Databricks para o case. Ele usa **Lakeflow
Pipelines**, o nome atual do Delta Live Tables (DLT), e **Declarative
Automation Bundles**, o nome atual do Databricks Asset Bundles (DAB).

## O que cada recurso faz

- `healthlake_bronze`: usa Auto Loader para ler incrementalmente os CSVs
  entregues pelo ADF em `raw/<dataset>/odate=YYYY-MM-DD/`. Mantém a fonte e
  acrescenta metadados de linhagem.
- `healthlake_silver`: considera cada `odate` como um *snapshot completo*,
  deduplica a última fotografia, converte os tipos de dados e mascara
  irreversivelmente os identificadores diretos do paciente. Assim, a remoção
  de uma entidade na origem também se reflete na camada atual.
- `healthlake_gold`: publica dimensões, o fato de atendimentos e o KPI diário
  por hospital. CPF, e-mail, telefone e nome do paciente não chegam à Gold.
- `healthlake_bronze_to_silver_dq` e `healthlake_silver_to_gold_dq`: quality
  gates intermediários com DQX. Eles registram métricas, enviam a linha
  reprovada para `healthlake_dev.quarantine` e falham o Job para bloquear a
  promoção e notificar por e-mail.
- `healthlake_medallion_refresh`: job que respeita a dependência Bronze →
  DQ Bronze→Silver → Silver → DQ Silver→Gold → Gold.
- `HealthLake Observability - dev`: dashboard AI/BI com últimas execuções
  bem-sucedidas, quarentenas e falhas de qualidade.

O catálogo e os schemas já são criados pela configuração de Unity Catalog:
`healthlake_dev.bronze`, `healthlake_dev.silver` e `healthlake_dev.gold`.
Os arquivos Delta gerenciados ficam em `managed/healthlake_dev`; o container
`raw` continua como landing zone somente leitura.

## Comandos de operação

Execute a partir deste diretório, usando o profile já autenticado:

```powershell
databricks bundle validate --target dev --profile HEALTHLAKE_DEV
databricks bundle deploy --target dev --profile HEALTHLAKE_DEV
databricks bundle run healthlake_medallion_refresh --target dev --profile HEALTHLAKE_DEV
```

O último comando deve ser executado apenas após o ADF concluir com sucesso a
cópia dos cinco arquivos de uma mesma `odate`. Na primeira execução, o Auto
Loader cria seu estado de processamento e carrega todos os arquivos existentes
no landing path; nas execuções seguintes, carrega apenas arquivos novos.
