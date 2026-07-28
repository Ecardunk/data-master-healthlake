# CI/CD do HealthLake

Esta esteira usa GitHub Actions e Declarative Automation Bundles (DAB) em dois
ambientes:

| Ambiente | Gatilho | Ação |
| --- | --- | --- |
| `development` | Pull request, push em `develop` | CI; o push em `develop` também faz deploy no target `dev` |
| `production` | Execução manual do workflow | Valida e faz deploy no target `prod`, após aprovação do GitHub Environment |

## Configuração inicial

1. Crie um Service Principal Databricks por ambiente e associe-o ao respectivo
   workspace. O deployer precisa ser proprietário, ou ter `CAN_MANAGE`, nos
   jobs, pipelines, dashboard e warehouse gerenciados pelo bundle; também deve
   ter os privilégios Unity Catalog necessários no catálogo do ambiente.
2. Gere um OAuth secret para cada Service Principal.
3. No repositório GitHub, crie os Environments `development` e `production`.
   Em `production`, habilite *required reviewers*.
4. Em cada Environment, cadastre:

   | Tipo | Nome | Valor |
   | --- | --- | --- |
   | Variable | `DATABRICKS_HOST` | URL do workspace daquele ambiente |
   | Variable | `DATABRICKS_CLIENT_ID` | Application ID do Service Principal |
   | Secret | `DATABRICKS_CLIENT_SECRET` | OAuth secret do Service Principal |
   | Variable (somente produção) | `DATABRICKS_RAW_ROOT` | Caminho `abfss://` do landing zone produtivo |

5. Antes do primeiro deploy produtivo, provisione o catálogo
   `healthlake_prod`, os schemas Bronze/Silver/Gold/Quarantine/Observability,
   as external locations e o storage credential correspondentes. O workflow
   não reutiliza o catálogo ou o ADLS de desenvolvimento.

## O que cada workflow faz

- `ci.yml`: executa testes do gerador, verifica a sintaxe Python do Databricks
  e valida o bundle `dev`.
- `deploy-dev.yml`: em todo push na branch `develop`, valida e implanta o
  target `dev`. Não executa a carga de dados automaticamente.
- `deploy-prod.yml`: só roda manualmente. Faz checkout da ref informada,
  aguarda a aprovação do Environment `production`, valida e implanta o target
  `prod`.

Os workflows usam OAuth M2M. Não adicione tokens ou client secrets aos arquivos
YAML, ao `databricks.yml` ou ao repositório.
