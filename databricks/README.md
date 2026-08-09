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

O `run_as` dos três Jobs e das três Pipelines é declarado no Bundle e não
depende de quem executou o deploy: dev usa `sp-healthlake-dev-pipeline`
(`03b5799c-110f-484f-8b1b-e3fd88809c64`) e prod usa
`sp-healthlake-prod-pipeline` (`bfeb3006-1824-4361-bacb-3697f6e33262`). O CI/CD
autentica com OAuth M2M. Um usuário humano pode validar o Bundle, mas os deploys
regulares devem ser feitos pelo workflow do ambiente para evitar uma segunda
instância de desenvolvimento sob o diretório pessoal do usuário.

Os grupos começam vazios de propósito: a associação de pessoas deve ser feita
no IdP/SCIM de acordo com a função de cada colaborador, sem conceder privilégios
diretamente a usuários.

Este diretório é o deployável do Databricks para o case. Ele usa **Lakeflow
Pipelines**, o nome atual do Delta Live Tables (DLT), e **Declarative
Automation Bundles**, o nome atual do Databricks Asset Bundles (DAB).

## O que cada recurso faz

- `healthlake_bronze`: usa Auto Loader para ler incrementalmente os CSVs
  entregues pelo ADF em `raw/<dataset>/odate=YYYY-MM-DD/`. Mantém a fonte e
  acrescenta metadados de linhagem. O regex de partição extrai a data do
  segmento completo do path, e `expect_or_fail` interrompe a atualização se
  `odate` não puder ser determinada.
- `healthlake_silver`: publica somente a `odate` aprovada pelo gate
  Bronze→Silver. As transformações compartilhadas com o gate deduplicam o
  snapshot, convertem tipos, normalizam textos e mascaram irreversivelmente os
  identificadores diretos do paciente. Expectations `expect_or_fail` impedem
  que uma tabela inteira seja atualizada se o contrato final for violado.
- `healthlake_gold`: publica dimensões, o fato de atendimentos e o KPI diário
  por hospital. CPF, e-mail, telefone e nome do paciente não chegam à Gold.
- `healthlake_bronze_to_silver_dq` e `healthlake_silver_to_gold_dq`: quality
  gates intermediários com DQX. Cada gate recebe uma `odate`, filtra somente
  essa partição, registra `odate`, `input_rows`, `checked_rows` e
  `removed_by_cleaning` no `violation_summary` das métricas, envia linhas
  reprovadas para tabelas `healthlake_dev.quarantine.*_v2` e falha o Job para
  bloquear a promoção e notificar por e-mail. O sufixo separa o schema tipado
  pós-limpeza das quarentenas raw legadas. No gate Bronze→Silver, limpeza,
  tipagem e deduplicação acontecem antes das regras. A dependência DQX está
  fixada em `0.15.0`.
- `healthlake_medallion_refresh`: job que respeita a dependência Bronze →
  DQ Bronze→Silver → Silver → DQ Silver→Gold → Gold e propaga a
  mesma `odate` aos dois gates.
- `HealthLake Observability - dev`: dashboard AI/BI com últimas execuções
  bem-sucedidas, quarentenas e falhas de qualidade.

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
cópia dos cinco arquivos de uma mesma `odate`. Na primeira execução, o Auto
Loader cria seu estado de processamento e carrega todos os arquivos existentes
no landing path; nas execuções seguintes, carrega apenas arquivos novos.
O default do parâmetro de Job é vazio e não usa o relógio como fallback;
`--params "odate=YYYY-MM-DD"` é obrigatório para uma execução válida.

## Semântica fail-closed dos gates

O gate falha se qualquer uma das cinco tabelas não tiver linhas para a `odate`
solicitada ou se qualquer linha, depois da limpeza, violar uma regra crítica.
A limpeza remove registros incompletos em campos não-chave antes do DQ; qualquer
violação que permaneça bloqueia o salvamento da tabela inteira. O split válido
não é gravado diretamente. Somente depois que as cinco tabelas passam, o Job
atualiza `observability.dq_promotion_control`; a Silver lê exclusivamente essa
partição aprovada. Assim, uma falha não promove parcialmente uma tabela ou um
subconjunto de entidades. A mesma estratégia bloqueia a Gold no segundo gate,
e expectations `expect_or_fail` protegem as materializações Silver/Gold.

As métricas ficam em `observability.dq_run_metrics`: `input_rows` representa as
linhas Bronze/Silver filtradas pela data e `checked_rows` representa o conjunto
depois da limpeza compartilhada. A diferença
`removed_by_cleaning = input_rows - checked_rows`, registrada no JSON de
`violation_summary`, contabiliza o que a limpeza removeu antes das regras.
`odate`, estágio, tabela, válidos, quarentena, status e run ID permitem
reconciliar cada execução.
