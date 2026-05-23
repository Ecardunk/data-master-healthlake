# ADF - ingestao S3 para ADLS raw

Este pipeline copia snapshots CSV sinteticos do S3 para o container `raw` no
ADLS Gen2.

## Layout de origem e destino

Origem esperada no S3:

```text
s3://<bucket>/raw/<dataset>/odate=<YYYY-MM-DD>/<dataset>.csv
```

Exemplos:

```text
s3://healthlake-landing/raw/patients/odate=2026-05-21/patients.csv
s3://healthlake-landing/raw/attendance/odate=2026-05-21/attendance.csv
```

Destino esperado no ADLS:

```text
abfss://raw@<storage-account>.dfs.core.windows.net/<dataset>/odate=<YYYY-MM-DD>/<dataset>.csv
```

## Artefatos ADF

O repositorio contem os seguintes artefatos do Data Factory:

```text
adf/
  linkedService/
    ls_keyvault_healthlake.json
    ls_s3_healthlake.json
    ls_adls_healthlake.json
  dataset/
    ds_s3_raw_file_binary.json
    ds_adls_raw_file_binary.json
  pipeline/
    pl_copy_s3_to_adls_raw.json
```

O pipeline nao possui trigger. A execucao deve ser manual durante os testes da
ingestao batch.

## Parametros

Pipeline: `pl_copy_s3_to_adls_raw`

| Parametro | Tipo | Exemplo | Descricao |
| --- | --- | --- | --- |
| `odate` | string | `2026-05-21` | Data logica da particao de origem. |
| `s3_bucket_name` | string | `healthlake-landing` | Bucket S3 que recebe os arquivos gerados. |
| `dataset_names` | array | `["patients","hospitals","doctors","diseases","attendance"]` | Datasets copiados pelo pipeline. |

## Substituicoes obrigatorias

Antes de publicar os artefatos, substitua estes placeholders:

| Arquivo | Placeholder | Valor |
| --- | --- | --- |
| `adf/linkedService/ls_keyvault_healthlake.json` | `<KEY_VAULT_NAME>` | Nome do Azure Key Vault. |
| `adf/linkedService/ls_s3_healthlake.json` | `<AWS_ACCESS_KEY_ID>` | AWS access key id. Use uma credencial IAM com menor privilegio possivel. |
| `adf/linkedService/ls_s3_healthlake.json` | `<KEY_VAULT_SECRET_NAME_FOR_AWS_SECRET_ACCESS_KEY>` | Nome do segredo no Key Vault que contem a AWS secret access key. |
| `adf/linkedService/ls_adls_healthlake.json` | `<STORAGE_ACCOUNT_NAME>` | Nome da storage account ADLS Gen2. |
| `adf/pipeline/pl_copy_s3_to_adls_raw.json` | `<storage-account>` | Nome da storage account usado apenas em user properties da atividade. |

## Permissoes

Conceda para a managed identity do Data Factory:

- `Storage Blob Data Contributor` na storage account ADLS Gen2 ou no container
  `raw`.
- `Key Vault Secrets User` no Key Vault, caso a secret do S3 esteja armazenada
  nele.

Conceda para a identidade IAM usada pelo ADF:

- `s3:ListBucket` no bucket de landing.
- `s3:GetObject` em `raw/*`.

## Contingencia para particao ausente

Para cada dataset, o pipeline primeiro executa `GetMetadata` no arquivo
esperado:

```text
s3://<bucket>/raw/<dataset>/odate=<odate>/<dataset>.csv
```

Se o arquivo nao existir, o pipeline executa `FailMissingS3File` com o codigo:

```text
S3_RAW_FILE_NOT_FOUND
```

A mensagem inclui o dataset, a `odate` e o caminho S3 esperado.

Monitoramento recomendado:

1. Habilitar diagnostic settings do Data Factory para Log Analytics.
2. Criar um alerta no Azure Monitor para falhas do pipeline
   `pl_copy_s3_to_adls_raw`.
3. Enviar o alerta para um Action Group, como e-mail, Teams webhook, Slack
   webhook ou Logic App.

Em uma versao futura, e possivel adicionar uma Web activity no ramo de falha
para postar a mesma mensagem diretamente em Teams/Slack, mantendo a atividade
`Fail` no final para que a execucao continue marcada como falha.

## Teste manual

Execute manualmente no ADF Studio usando parametros como:

```json
{
  "odate": "2026-05-21",
  "s3_bucket_name": "healthlake-landing",
  "dataset_names": [
    "patients",
    "hospitals",
    "doctors",
    "diseases",
    "attendance"
  ]
}
```

Resultado esperado:

```text
raw/
  patients/odate=2026-05-21/patients.csv
  hospitals/odate=2026-05-21/hospitals.csv
  doctors/odate=2026-05-21/doctors.csv
  diseases/odate=2026-05-21/diseases.csv
  attendance/odate=2026-05-21/attendance.csv
```
