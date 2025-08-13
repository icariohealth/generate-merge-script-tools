CREATE OR REPLACE PROCEDURE program_outreach.process_outreach_action_event_changes()
LANGUAGE plpgsql
AS $procedure$
BEGIN

    -- Step 1: Insert records with NULL source_timestamp_ms into the main table
    INSERT INTO program_outreach.outreach_action_event (
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms
    )
    SELECT
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms
    FROM program_outreach.stg_outreach_action_event
    WHERE source_timestamp_ms IS NULL;

    -- Step 2: Archive NULL-source_timestamp_ms records
    INSERT INTO program_outreach.archived_outreach_action_event (
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    )
    SELECT
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    FROM program_outreach.stg_outreach_action_event
    WHERE source_timestamp_ms IS NULL;

    -- Step 3: Delete NULL-source_timestamp_ms records from staging
    DELETE FROM program_outreach.stg_outreach_action_event
    WHERE source_timestamp_ms IS NULL;

    -- Step 4: Merge latest records by id based on operation type
    WITH latest_ops AS (
        SELECT
            id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, rn
        FROM (
            SELECT
            id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_timestamp_ms DESC) AS rn
            FROM program_outreach.stg_outreach_action_event
            WHERE op IN ('+I', '+U', '-D')
        ) sub
        WHERE rn = 1
    )
    MERGE INTO program_outreach.outreach_action_event AS tgt
    USING latest_ops AS src
    ON tgt.id = src.id AND tgt.client_id = src.client_id
    WHEN MATCHED AND src.op IN ('+I', '+U') THEN
        UPDATE SET
            program_id = src.program_id,
            message_id = src.message_id,
            outreach_attempt_id = src.outreach_attempt_id,
            outreach_detail_id = src.outreach_detail_id,
            channel_id = src.channel_id,
            vendor = src.vendor,
            "result" = src."result",
            reason = src.reason,
            metadata = src.metadata,
            "timestamp" = src."timestamp",
            op = src.op,
            kafka_timestamp_ms = src.kafka_timestamp_ms,
            source_timestamp_ms = src.source_timestamp_ms,
            dw_last_modified_timestamp_ms = CURRENT_TIMESTAMP(3) AT TIME ZONE 'UTC'
    WHEN MATCHED AND src.op = '-D' THEN
        DELETE
    WHEN NOT MATCHED AND src.op IN ('+I', '+U') THEN
        INSERT (
            id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms
        )
        VALUES (
            src.id, src.client_id, src.program_id, src.message_id, src.outreach_attempt_id, src.outreach_detail_id, src.channel_id, src.vendor, src."result", src.reason, src.metadata, src."timestamp", src.op, src.kafka_timestamp_ms, src.source_timestamp_ms
        );

    -- Step 5: Archive all remaining staging records
    INSERT INTO program_outreach.archived_outreach_action_event (
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    )
    SELECT
        id, client_id, program_id, message_id, outreach_attempt_id, outreach_detail_id, channel_id, vendor, "result", reason, metadata, "timestamp", op, kafka_timestamp_ms, source_timestamp_ms, dw_created_timestamp_ms
    FROM program_outreach.stg_outreach_action_event;

    -- Step 6: Truncate staging table
    DELETE FROM program_outreach.stg_outreach_action_event;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in process_outreach_action_event_changes: %', SQLERRM;

END;
$procedure$
;