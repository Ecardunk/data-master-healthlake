# HealthLake Medallion Bundle

## Governança de acesso

Os grupos são criados no nível da conta Databricks e atribuídos ao workspace
com a entitlement `USER`; a matriz de privilégios do Unity Catalog está em
[`src/governance/unity_catalog_access.sql`](src/governance/unity_catalog_access.sql).

| Grupo | Acesso a dados | Não recebe |
| --- | --- | --- |
| `healthlake-dev-data-engineers-contributors` | Criar tabelas e escrever em Bronze/Silver; criar jobs e pipelines próprios | Gold, quarentena, external locations e administração global |

Os contribuidores usam o diretório de autoria
`/Workspace/Users/cardosoestevo@yahoo.com.br/healthlake-engineering`, onde o
grupo tem `CAN_MANAGE`. Cada pessoa passa a ter `CAN_MANAGE` somente nos jobs
e pipelines que ela criar; o grupo não recebeu gestão dos pipelines publicados.
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
