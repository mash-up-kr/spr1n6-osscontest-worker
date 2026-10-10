-- Schema-owner migration proposal. Worker does not execute this file automatically.
-- Stop legacy unfenced Workers before deploying this contract and the DB queue Worker.
BEGIN;

ALTER TABLE indexing_job
    ADD COLUMN execution_epoch BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN job_type VARCHAR(20) NOT NULL DEFAULT 'INDEX',
    ALTER COLUMN document_version_id DROP NOT NULL,
    DROP COLUMN kafka_topic,
    DROP COLUMN kafka_partition,
    DROP COLUMN kafka_offset;

-- Old PROCESSING rows have no issued lease. Invalidate them at cutover.
UPDATE indexing_job SET status = CASE WHEN status = 'PROCESSING' THEN 'PENDING' ELSE status END,
    worker_id = NULL, execution_epoch = execution_epoch + 1
WHERE status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT');

ALTER TABLE indexing_job
    ADD CONSTRAINT ck_indexing_job_type CHECK (
        (job_type = 'INDEX' AND document_version_id IS NOT NULL) OR
        (job_type = 'DELETE' AND document_version_id IS NULL)
    ),
    ADD CONSTRAINT ck_indexing_job_epoch CHECK (execution_epoch >= 0),
    ADD CONSTRAINT ck_indexing_job_lease CHECK (
        (lease_until IS NULL AND (worker_id IS NULL OR status IN ('COMPLETED', 'FAILED'))) OR
        (worker_id IS NOT NULL AND lease_until IS NOT NULL)
    ),
    ADD CONSTRAINT fk_indexing_job_document FOREIGN KEY (document_id) REFERENCES document(id);

CREATE INDEX idx_indexing_job_document_order ON indexing_job (document_id, id)
    WHERE status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT');
CREATE INDEX idx_indexing_job_expired_lease ON indexing_job (lease_until)
    WHERE worker_id IS NOT NULL AND status IN ('PROCESSING', 'RETRY_WAIT');
CREATE INDEX idx_indexing_job_retry_admission ON indexing_job (next_retry_at)
    WHERE next_retry_at IS NOT NULL AND status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT');

-- May be called by the Worker or an existing DB scheduler. Its WHERE is rechecked
-- after a competing heartbeat UPDATE, with no recovery grace period.
CREATE FUNCTION recover_expired_indexing_jobs() RETURNS BIGINT LANGUAGE SQL AS $$
    WITH recovered AS (
        UPDATE indexing_job SET
            status = CASE WHEN status = 'RETRY_WAIT' THEN 'RETRY_WAIT' ELSE 'PENDING' END,
            worker_id = NULL, lease_until = NULL,
            execution_epoch = execution_epoch + 1, updated_at = clock_timestamp()
        WHERE worker_id IS NOT NULL AND status IN ('PROCESSING', 'RETRY_WAIT')
          AND lease_until <= clock_timestamp()
        RETURNING id
    ) SELECT COUNT(*) FROM recovered;
$$;

COMMIT;

-- Producer contract (Server/Relay scope): enqueue INDEX/DELETE Jobs durably in the
-- document mutation transaction, ordered by indexing_job.id for each document.
-- Lock the document row before enqueue, reject historical retries, and do not
-- enqueue INDEX for a deleted document. DELETE has document_version_id = NULL.
-- next_retry_at remains populated during retry PROCESSING; clear only on terminal
-- status. Reclaim and renewal preserve it. All timestamps are TIMESTAMPTZ.
