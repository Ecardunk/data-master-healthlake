-- HealthLake: matriz humana de leitura no Unity Catalog.
--
-- Estes cinco principals devem ser grupos de CONTA/External sincronizados pelo
-- IdP. Não crie grupos locais do workspace e não conceda privilégios direto a
-- pessoas. A associação aos workspaces deve seguir:
--   dev  : data-engineering-admin, data-engineering
--   prod : os cinco grupos declarados neste arquivo
--
-- Mesmo data-engineering-admin é somente leitor de dados. Escrita, deploy,
-- ownership e execução das pipelines pertencem a service principals separados.
-- Nenhum grupo abaixo recebe MODIFY, CREATE, MANAGE, READ_FILES, WRITE_FILES,
-- external locations, quarantine ou observability.

-- -------------------------------------------------------------------------
-- Development

-- Administrador funcional de engenharia: lê Bronze, Silver e Gold.
GRANT USE CATALOG ON CATALOG healthlake_dev TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.bronze TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.silver TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.gold TO `data-engineering-admin`;

-- Engenharia: lê somente Silver e Gold.
GRANT USE CATALOG ON CATALOG healthlake_dev TO `data-engineering`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.silver TO `data-engineering`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_dev.gold TO `data-engineering`;

-- -------------------------------------------------------------------------
-- Production

-- Administrador funcional de engenharia: lê Bronze, Silver e Gold.
GRANT USE CATALOG ON CATALOG healthlake_prod TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.bronze TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `data-engineering-admin`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `data-engineering-admin`;

-- Engenharia: lê somente Silver e Gold.
GRANT USE CATALOG ON CATALOG healthlake_prod TO `data-engineering`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `data-engineering`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `data-engineering`;

-- Consumidores produtivos: leem somente Silver e Gold de produção.
GRANT USE CATALOG ON CATALOG healthlake_prod TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `data-analysts`;

GRANT USE CATALOG ON CATALOG healthlake_prod TO `data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `data-scientists`;

GRANT USE CATALOG ON CATALOG healthlake_prod TO `power-bi`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.silver TO `power-bi`;
GRANT USE SCHEMA, SELECT ON SCHEMA healthlake_prod.gold TO `power-bi`;
