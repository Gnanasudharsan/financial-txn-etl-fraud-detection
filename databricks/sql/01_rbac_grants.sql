-- Unity Catalog RBAC for the fintxn lakehouse.
-- Run as a metastore/catalog admin after `databricks bundle deploy` has created the schemas.
-- Groups are account-level groups: data_engineers, analysts, auditors.
-- Replace `workspace` if you deploy to a different catalog.

-- ---------------------------------------------------------------------------------------
-- Catalog access
-- ---------------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG workspace TO `data_engineers`;
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE CATALOG ON CATALOG workspace TO `auditors`;

-- ---------------------------------------------------------------------------------------
-- data_engineers: own the pipeline (read/write every layer + raw landing volume)
-- ---------------------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA workspace.fintxn_raw    TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA workspace.fintxn_bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA workspace.fintxn_silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA workspace.fintxn_gold   TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA workspace.fintxn_audit  TO `data_engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.fintxn_raw.landing TO `data_engineers`;

-- ---------------------------------------------------------------------------------------
-- analysts: gold marts only (aggregated, tokenized, no PII) + the masked customer view
-- ---------------------------------------------------------------------------------------
GRANT USE SCHEMA ON SCHEMA workspace.fintxn_gold TO `analysts`;
GRANT SELECT     ON SCHEMA workspace.fintxn_gold TO `analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.fintxn_silver TO `analysts`;
GRANT SELECT ON VIEW workspace.fintxn_silver.customers_secure TO `analysts`;
-- No grant on silver/bronze tables, raw volume, or audit schema.

-- ---------------------------------------------------------------------------------------
-- auditors: read-only on every curated layer and the audit trail (not the raw landing zone)
-- ---------------------------------------------------------------------------------------
GRANT USE SCHEMA, SELECT ON SCHEMA workspace.fintxn_bronze TO `auditors`;
GRANT USE SCHEMA, SELECT ON SCHEMA workspace.fintxn_silver TO `auditors`;
GRANT USE SCHEMA, SELECT ON SCHEMA workspace.fintxn_gold   TO `auditors`;
GRANT USE SCHEMA, SELECT ON SCHEMA workspace.fintxn_audit  TO `auditors`;

-- Verify
SHOW GRANTS ON SCHEMA workspace.fintxn_gold;
SHOW GRANTS ON SCHEMA workspace.fintxn_silver;
