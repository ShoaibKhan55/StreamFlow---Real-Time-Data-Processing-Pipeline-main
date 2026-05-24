-- Creates the separate airflow_db database inside the same PostgreSQL instance.
-- This script is run automatically by postgres on first startup via
-- docker-entrypoint-initdb.d.
CREATE DATABASE airflow_db
    WITH OWNER = admin
    ENCODING = 'UTF8';
