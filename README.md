# data-master-healthlake

End-to-end healthcare data engineering platform built with Azure Data Factory, Azure Databricks and Delta Lake following Medallion Architecture principles.

## Azure Data Factory

O primeiro pipeline batch copia particoes raw do S3 para o container `raw` no
ADLS Gen2:

- Pipeline: `adf/pipeline/pl_copy_s3_to_adls_raw.json`
- Documentacao: `docs/adf_s3_to_adls_raw.md`
