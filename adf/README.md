# HealthLake Azure Data Factory

Os artefatos ADF são compartilhados, mas cada ambiente possui configuração
explícita em `environments/dev.json` e `environments/prod.json`.

| Ambiente | Factory | ADLS Raw | Key Vault |
| --- | --- | --- | --- |
| dev | `adf-data-master-dev` | `sthealthdatalake001` | `kv-data-master-case` |
| prod | `adf-healthlake-prod-brs-01` | `sthlkprodbrs01` | `kv-hlk-prod-brs-01` |

O bucket S3 continua sendo a origem upstream compartilhada do case. Credenciais
ficam em Key Vaults distintos; nenhum valor secreto é versionado.

## Deploy seguro

O script cria/atualiza apenas recursos de control plane e sempre deixa
`trigger_case` parado. Ele nunca inicia uma pipeline:

```powershell
./adf/scripts/deploy.ps1 -Environment dev
./adf/scripts/deploy.ps1 -Environment prod
```

Pré-requisitos de cada factory:

- managed identity com `Storage Blob Data Contributor` somente no container
  `raw` do próprio ambiente;
- `Key Vault Secrets User` somente no Key Vault do próprio ambiente;
- secrets `aws-s3-access-key-id` e `aws-s3-secret-access-key` presentes no
  vault correspondente.

Depois do deploy, a pós-condição obrigatória é:

```text
properties.runtimeState = Stopped
```

Habilitar o trigger é uma mudança operacional separada e não faz parte do
deploy de aplicação.
