ALTER TABLE document_version
    ADD COLUMN IF NOT EXISTS embedding_version_no BIGINT;

UPDATE document_version
SET embedding_version_no = version_no
WHERE embedding_version_no IS NULL;

ALTER TABLE document_version
    ALTER COLUMN embedding_version_no SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'document_version'::regclass
          AND conname = 'ck_document_version_embedding_version_no'
    ) THEN
        ALTER TABLE document_version
            ADD CONSTRAINT ck_document_version_embedding_version_no
                CHECK (embedding_version_no > 0);
    END IF;
END
$$;

ALTER TABLE indexing_job
    ADD COLUMN IF NOT EXISTS phase VARCHAR(30),
    ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(255),
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

ALTER TABLE indexing_job
    ALTER COLUMN kafka_topic SET NOT NULL,
    ALTER COLUMN kafka_partition SET NOT NULL,
    ALTER COLUMN kafka_offset SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'indexing_job'::regclass
          AND conname = 'ck_indexing_job_kafka_topic'
    ) THEN
        ALTER TABLE indexing_job
            ADD CONSTRAINT ck_indexing_job_kafka_topic
                CHECK (BTRIM(kafka_topic) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'indexing_job'::regclass
          AND conname = 'ck_indexing_job_kafka_partition'
    ) THEN
        ALTER TABLE indexing_job
            ADD CONSTRAINT ck_indexing_job_kafka_partition
                CHECK (kafka_partition >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'indexing_job'::regclass
          AND conname = 'ck_indexing_job_kafka_offset'
    ) THEN
        ALTER TABLE indexing_job
            ADD CONSTRAINT ck_indexing_job_kafka_offset
                CHECK (kafka_offset >= 0);
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_indexing_job_active_version
    ON indexing_job (document_version_id)
    WHERE status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT');

COMMENT ON COLUMN document_version.embedding_version_no IS
    '검색 버전 승격 순서를 결정하는 단조 증가 버전 번호.';
COMMENT ON COLUMN indexing_job.phase IS
    '현재 인덱싱 처리 단계.';
COMMENT ON COLUMN indexing_job.kafka_topic IS
    '수신한 Kafka record의 topic.';
COMMENT ON COLUMN indexing_job.kafka_partition IS
    '수신한 Kafka record의 partition.';
COMMENT ON COLUMN indexing_job.kafka_offset IS
    '수신한 Kafka record의 offset.';
