-- Own schema, so the scheduler can share a database with other services'
-- tables (onikisu's pgq `job` lives in public).
CREATE SCHEMA scheduler;
ALTER TABLE job SET SCHEMA scheduler;
ALTER TABLE queue SET SCHEMA scheduler;
ALTER TABLE target SET SCHEMA scheduler;
