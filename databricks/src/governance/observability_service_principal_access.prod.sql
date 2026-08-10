-- Minimum production-only access required by the embedded dashboard identity.
-- Run once as an account or metastore administrator. Human groups do not
-- receive access to system.lakeflow through this script.

GRANT USE CATALOG ON CATALOG system
TO `bfeb3006-1824-4361-bacb-3697f6e33262`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow
TO `bfeb3006-1824-4361-bacb-3697f6e33262`;
