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
