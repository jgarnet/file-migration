-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- DDL
-- Stores job ranges for parallel batch processing
CREATE TABLE migration_ranges (
    range_id SERIAL PRIMARY KEY,
    min_id BIGINT NOT NULL,
    max_id BIGINT NOT NULL,
    status TEXT DEFAULT 'PENDING', -- PENDING, PROCESSING, COMPLETE
    last_updated TIMESTAMP DEFAULT now()
);

-- Stores individual file migration statuses
CREATE TABLE migration_files (
    file_id BIGINT PRIMARY KEY,
    old_uri VARCHAR(150) NOT NULL,
    new_uri VARCHAR(150),
    file_name VARCHAR(200),
    create_date TIMESTAMP,
    status VARCHAR(8) NOT NULL,
    migration_date TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    last_attempt_date TIMESTAMP
);

-- Sample source file table

CREATE TABLE source_files (
    file_id SERIAL PRIMARY KEY,
    file_uri VARCHAR(150) NOT NULL,
    file_name VARCHAR(200) NOT NULL,
    create_date TIMESTAMP DEFAULT now()
);

-- DML
-- Seed source files table

CREATE OR REPLACE PROCEDURE seed_files(records BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
       max_id BIGINT;
BEGIN
    SELECT coalesce(max(file_id), 0) INTO max_id FROM source_files;
    RAISE NOTICE 'The current max ID is %', max_id;
    RAISE NOTICE 'The number of records being added is %', records;
    INSERT INTO source_files (file_uri, file_name)
    SELECT
        '/files/' || uuid_generate_v4(),
        'Test File' || gs || '.pdf'
    FROM generate_series(max_id + 1, max_id + records) AS gs;
END;
$$;

-- Views
CREATE VIEW processing_report AS
select count(status), status from migration_ranges group by status;

CREATE VIEW retry_report AS
select count(retry_count), retry_count from migration_files group by retry_count;

CREATE VIEW migration_status_report AS
select count(status), status from migration_files group by status;