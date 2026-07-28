# data-master-healthlake

End-to-end healthcare data engineering platform built with Azure Data Factory, Azure Databricks and Delta Lake following Medallion Architecture principles.

## CI/CD

O projeto possui CI e CD com GitHub Actions para desenvolvimento e produção.
Consulte [docs/cicd_databricks.md](docs/cicd_databricks.md) para configurar os
GitHub Environments, Service Principals e os secrets necessários.

## Azure Data Factory

O primeiro pipeline batch copia particoes raw do S3 para o container `raw` no
ADLS Gen2:

- Pipeline: `adf/pipeline/pl_copy_s3_to_adls_raw.json`
- Documentacao: `docs/adf_s3_to_adls_raw.md`
