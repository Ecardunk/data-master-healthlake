-- HealthLake: matriz mínima de acesso no Unity Catalog.
--
-- Os grupos devem ser grupos de CONTA (SCIM/IdP), nunca grupos locais do
-- workspace. Isso permite que a mesma política seja concedida a objetos UC
-- e que a inclusão/remoção de pessoas seja auditável no provedor de identidade.
--
-- Não conceda MODIFY, CREATE, MANAGE, READ_FILES ou WRITE_FILES a estes
-- grupos. A permissão SELECT em schema se aplica a tabelas e views atuais e
-- futuras daquele schema.

-- Consumidores: somente produtos analíticos curados.
GRANT USE CATALOG ON CATALOG healthlake_dev TO `healthlake-dev-bi-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.gold TO `healthlake-dev-bi-readers`;

GRANT USE CATALOG ON CATALOG healthlake_dev TO `healthlake-dev-data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.gold TO `healthlake-dev-data-analysts`;

-- Engenharia: inspeção e depuração das camadas operacionais, sem escrita.
GRANT USE CATALOG ON CATALOG healthlake_dev TO `healthlake-dev-data-engineers-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.bronze TO `healthlake-dev-data-engineers-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.silver TO `healthlake-dev-data-engineers-readers`;

-- Contributors can author jobs/pipelines and write operational layers.
-- They have no Gold, quarantine, external-location, or global-admin access.
GRANT USE CATALOG ON CATALOG healthlake_dev TO `healthlake-dev-data-engineers-contributors`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA healthlake_dev.bronze TO `healthlake-dev-data-engineers-contributors`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA healthlake_dev.silver TO `healthlake-dev-data-engineers-contributors`;

-- -------------------------------------------------------------------------
-- Production: acesso humano estritamente de leitura.
--
-- Estes grupos devem receber somente a atribuição USER no workspace de
-- produção. Não crie um grupo "contributors" no workspace produtivo: a
-- escrita e a criação de jobs/pipelines ficam restritas à identidade de
-- serviço usada pelo CI/CD, com privilégios mínimos e auditáveis.
--
-- Não conceda aos grupos abaixo MODIFY, CREATE TABLE, CREATE SCHEMA,
-- MANAGE, READ_FILES, WRITE_FILES, acesso a external locations ou acesso
-- ao schema quarantine. Também não conceda permissões diretas a usuários.

-- Consumidores de dados: somente produtos curados da Gold.
GRANT USE CATALOG ON CATALOG healthlake_prod TO `healthlake-prod-bi-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `healthlake-prod-bi-readers`;

GRANT USE CATALOG ON CATALOG healthlake_prod TO `healthlake-prod-data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `healthlake-prod-data-analysts`;

-- Engenharia: diagnóstico das camadas operacionais, sempre sem escrita.
-- Gold permanece reservada aos consumidores analíticos acima.
GRANT USE CATALOG ON CATALOG healthlake_prod TO `healthlake-prod-data-engineers-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.bronze TO `healthlake-prod-data-engineers-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `healthlake-prod-data-engineers-readers`;
