CREATE OR REPLACE PROCEDURE program_outreach.process_outreach_changes()
LANGUAGE plpgsql
AS $procedure$
BEGIN

    -- Step 1: Insert records with NULL source_timestamp_ms into the main table
    INSERT INTO program_outreach.inc_outreach (
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms
    )
    SELECT
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms
    FROM program_outreach.stg_outreach
    WHERE source_timestamp_ms IS NULL;

    -- Step 2: Archive NULL-source_timestamp_ms records
    INSERT INTO program_outreach.archived_outreach (
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    )
    SELECT
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    FROM program_outreach.stg_outreach
    WHERE source_timestamp_ms IS NULL;

    -- Step 3: Delete NULL-source_timestamp_ms records from staging
    DELETE FROM program_outreach.stg_outreach
    WHERE source_timestamp_ms IS NULL;

    -- Step 4: Merge latest records by id based on operation type
    WITH latest_ops AS (
        SELECT
            id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, rn
        FROM (
            SELECT
            id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_timestamp_ms DESC) AS rn
            FROM program_outreach.stg_outreach
            WHERE op IN ('+I', '+U', '-D')
        ) sub
        WHERE rn = 1
    )
    MERGE INTO program_outreach.inc_outreach AS tgt
    USING latest_ops AS src
    ON tgt.id = src.id
    WHEN MATCHED AND src.op IN ('+I', '+U') THEN
        UPDATE SET
            client_id = src.client_id,
            program_id = src.program_id,
            start_datetime = src.start_datetime,
            end_datetime = src.end_datetime,
            status = src.status,
            deleted_date = src.deleted_date,
            created_date = src.created_date,
            created_by = src.created_by,
            last_modified_by = src.last_modified_by,
            last_modified_date = src.last_modified_date,
            from_request = src.from_request,
            op = src.op,
            kafka_timestamp_ms = src.kafka_timestamp_ms,
            source_timestamp_ms = src.source_timestamp_ms,
            dw_last_modified_timestamp_ms = CURRENT_TIMESTAMP(3) AT TIME ZONE 'UTC'
    WHEN MATCHED AND src.op = '-D' THEN
        DELETE
    WHEN NOT MATCHED AND src.op IN ('+I', '+U') THEN
        INSERT (
            id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms
        )
        VALUES (
            src.id, src.client_id, src.program_id, src.start_datetime, src.end_datetime, src.status, src.deleted_date, src.created_date, src.created_by, src.last_modified_by, src.last_modified_date, src.from_request, src.op, src.kafka_timestamp_ms, src.source_timestamp_ms
        );

    -- Step 5: Archive all remaining staging records
    INSERT INTO program_outreach.archived_outreach (
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    )
    SELECT
        id, client_id, program_id, start_datetime, end_datetime, status, deleted_date, created_date, created_by, last_modified_by, last_modified_date, from_request, op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    FROM program_outreach.stg_outreach;

    -- Step 6: Truncate staging table
    DELETE FROM program_outreach.stg_outreach;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in process_outreach_changes: %', SQLERRM;

END;
$procedure$
;