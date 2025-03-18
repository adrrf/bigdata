CREATE DATABASE metastore_db;

CREATE USER hive
WITH
    PASSWORD 'password';

GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
