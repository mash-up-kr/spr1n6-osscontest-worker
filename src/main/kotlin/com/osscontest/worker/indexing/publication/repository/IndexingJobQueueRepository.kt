package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.pipeline.domain.ClaimedJob
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobType
import com.osscontest.worker.indexing.pipeline.domain.JobContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Types
import java.time.LocalDateTime

/**
 * DB Job 큐의 획득과 실행권 보호 SQL을 모은다.
 *
 * 스키마(document, indexing_job)는 Core 서버가 소유하며, `execution_epoch` 등 신규 컬럼이 적용돼 있어야 동작한다.
 * `RETURNING`으로 갱신된 행을 돌려받아야 해서 Spring Data 대신 JDBC 템플릿을 쓴다.
 * 각 메서드는 SQL 한 문장이라 호출자의 트랜잭션 여부와 관계없이 원자적이다.
 */
@Repository
class IndexingJobQueueRepository(
    jdbcTemplate: JdbcTemplate,
) {
    private val jdbc = NamedParameterJdbcTemplate(jdbcTemplate)

    /**
     * 실행 가능한 Job 하나를 잡고 문서의 execution_epoch를 올려 새 실행권을 발급한다.
     *
     * 대기 중(`PENDING`, 또는 재시도 시각이 지난 `RETRY_WAIT`)이면서 같은 문서에 더 앞선 미종결
     * Job이 없는 것 중 `enqueue_seq`가 가장 작은 Job을 고른다. 다른 트랜잭션이 잠근 Job이나
     * 문서는 기다리지 않고 건너뛰므로 후보가 없으면 null을 돌려준다.
     * 시도 횟수 상한은 검사하지 않는다. 한도 소진은 실패 기록과 만료 회수가 `FAILED`로 처리한다.
     * [leaseSeconds]는 양수여야 한다.
     */
    fun claim(
        workerId: String,
        leaseSeconds: Long,
    ): ClaimedJob? =
        jdbc
            .query(
                CLAIM_SQL,
                mapOf("workerId" to workerId, "leaseSeconds" to leaseSeconds),
            ) { rs, _ ->
                ClaimedJob(
                    context =
                        JobContext(
                            jobId = rs.getLong("id"),
                            documentId = rs.getLong("document_id"),
                            workerId = workerId,
                            epoch = rs.getLong("execution_epoch"),
                        ),
                    documentVersionId = rs.getLong("document_version_id"),
                    jobType = IndexingJobType.valueOf(rs.getString("job_type")),
                    attemptCount = rs.getInt("attempt_count"),
                )
            }.firstOrNull()

    /**
     * 실행권이 유효한 Job을 `COMPLETED`로 만들고 [outcome]과 완료 시각을 기록한다.
     *
     * 상태, 소유자, epoch만 확인하고 lease 만료는 보지 않는다. 이 셋이 일치하면 아직 아무도 이
     * 실행을 대체하지 않았다는 뜻이라, 임베딩을 끝낸 정상 결과를 늦었다는 이유로 버리지 않는다.
     * [outcome]은 `indexing_job.outcome`(VARCHAR(40))에 들어가는 값이어야 한다.
     * 0을 돌려주면 실행권을 잃은 것이므로 호출자는 결과를 반영하지 말아야 한다.
     */
    fun complete(
        ctx: JobContext,
        outcome: String,
    ): Int =
        jdbc.update(
            """
            UPDATE indexing_job
            SET status = 'COMPLETED',
                outcome = :outcome,
                completed_at = CURRENT_TIMESTAMP,
                next_retry_at = NULL,
                lease_until = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE $OWNED_GUARD
            """,
            ownership(ctx).addValue("outcome", outcome),
        )

    /**
     * 실행권이 유효한 Job의 실패를 기록하고 lease를 해제한다.
     *
     * [status]는 `RETRY_WAIT`(이때 [nextRetryAt] 필수) 또는 `FAILED`(이때 null)만 허용한다.
     * 재시도 여부와 시각은 호출자가 정하고, 여기서는 기록만 한다.
     * [nextRetryAt]은 DB 시계 기준이어야 한다. JVM 시계를 넘기면 시간대가 다를 때 재시도 시각이 어긋난다.
     * `complete`와 달리 lease 만료는 보지 않는다. 오류 코드와 메시지는 길이 상한에서 잘린다.
     * 0을 돌려주면 실행권을 잃은 것이다.
     */
    fun fail(
        ctx: JobContext,
        status: IndexingJobStatus,
        nextRetryAt: LocalDateTime?,
        errorCode: String,
        errorMessage: String,
    ): Int {
        require(status == IndexingJobStatus.RETRY_WAIT || status == IndexingJobStatus.FAILED) {
            "fail은 RETRY_WAIT 또는 FAILED만 기록한다: $status"
        }
        require((status == IndexingJobStatus.RETRY_WAIT) == (nextRetryAt != null)) {
            "RETRY_WAIT이면 nextRetryAt이 필수이고 FAILED면 null이어야 한다: status=$status, nextRetryAt=$nextRetryAt"
        }
        return jdbc.update(
            """
            UPDATE indexing_job
            SET status = :status,
                last_error_code = :errorCode,
                last_error_message = :errorMessage,
                next_retry_at = :nextRetryAt,
                completed_at = CASE WHEN :status = 'FAILED' THEN CURRENT_TIMESTAMP ELSE NULL END,
                lease_until = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE $OWNED_GUARD
            """,
            ownership(ctx)
                .addValue("status", status.name)
                .addValue("errorCode", errorCode.take(MAX_ERROR_CODE_LENGTH))
                .addValue("errorMessage", errorMessage.take(MAX_ERROR_MESSAGE_LENGTH))
                .addValue("nextRetryAt", nextRetryAt, Types.TIMESTAMP),
        )
    }

    /**
     * 실행권이 유효한 Job의 lease를 [leaseSeconds]초 뒤로 연장한다.
     *
     * 이미 만료된 lease는 되살리지 않는다. 만료 회수가 lease를 판단 기준으로 쓰기 때문이다.
     * 0을 돌려주면 실행권을 잃었거나 lease가 이미 만료된 것이다. [leaseSeconds]는 양수여야 한다.
     */
    fun renewLease(
        ctx: JobContext,
        leaseSeconds: Long,
    ): Int =
        jdbc.update(
            """
            UPDATE indexing_job
            SET lease_until = CURRENT_TIMESTAMP + make_interval(secs => CAST(:leaseSeconds AS double precision)),
                updated_at = CURRENT_TIMESTAMP
            WHERE $OWNED_GUARD
              AND lease_until > CURRENT_TIMESTAMP
            """,
            ownership(ctx).addValue("leaseSeconds", leaseSeconds),
        )

    /** 실행권이 유효한 Job의 진행 단계를 기록한다. 0을 돌려주면 실행권을 잃은 것이다. */
    fun updatePhase(
        ctx: JobContext,
        phase: String,
    ): Int =
        jdbc.update(
            """
            UPDATE indexing_job
            SET phase = :phase,
                updated_at = CURRENT_TIMESTAMP
            WHERE $OWNED_GUARD
            """,
            ownership(ctx).addValue("phase", phase),
        )

    private fun ownership(ctx: JobContext) =
        MapSqlParameterSource()
            .addValue("jobId", ctx.jobId)
            .addValue("workerId", ctx.workerId)
            .addValue("epoch", ctx.epoch)

    private companion object {
        const val OWNED_GUARD = """
            id = :jobId
              AND status = 'PROCESSING'
              AND worker_id = :workerId
              AND execution_epoch = :epoch
        """

        // `indexing_job.last_error_code` 컬럼 길이(VARCHAR(100))와 맞춘 값이다.
        const val MAX_ERROR_CODE_LENGTH = 100

        // `last_error_message`는 TEXT라 컬럼 제한이 없다. 과도하게 긴 예외 메시지가 행을 키우지 않도록 상한을 둔다.
        const val MAX_ERROR_MESSAGE_LENGTH = 1_000

        // 후보의 Job 행과 문서 행을 한 번에 SKIP LOCKED로 잠가 기다림 없이 다음 후보로 넘어간다.
        // 기다리지 않으므로 `complete`·`fail` 등 이후 단계가 따르는 "문서 → Job" 잠금 순서와 교착하지 않는다.
        // 문서의 updated_at은 건드리지 않는다. Core 쪽 트리거·CDC에 영향을 주지 않기 위해서다.
        const val CLAIM_SQL = """
            WITH candidate AS (
                SELECT j.id AS job_id, j.document_id
                FROM indexing_job j
                JOIN document d ON d.id = j.document_id
                WHERE (j.status = 'PENDING'
                       OR (j.status = 'RETRY_WAIT'
                           AND (j.next_retry_at IS NULL OR j.next_retry_at <= CURRENT_TIMESTAMP)))
                  AND NOT EXISTS (
                      SELECT 1
                      FROM indexing_job p
                      WHERE p.document_id = j.document_id
                        AND p.enqueue_seq < j.enqueue_seq
                        AND p.status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT'))
                ORDER BY j.enqueue_seq
                LIMIT 1
                FOR UPDATE OF j, d SKIP LOCKED
            ),
            bumped AS (
                UPDATE document d
                SET execution_epoch = d.execution_epoch + 1
                FROM candidate c
                WHERE d.id = c.document_id
                RETURNING d.id AS document_id, d.execution_epoch
            )
            UPDATE indexing_job j
            SET status = 'PROCESSING',
                worker_id = :workerId,
                execution_epoch = b.execution_epoch,
                lease_until = CURRENT_TIMESTAMP + make_interval(secs => CAST(:leaseSeconds AS double precision)),
                attempt_count = j.attempt_count + 1,
                next_retry_at = NULL,
                started_at = COALESCE(j.started_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            FROM candidate c
            JOIN bumped b ON b.document_id = c.document_id
            WHERE j.id = c.job_id
            RETURNING j.id, j.document_id, j.document_version_id, j.job_type,
                      j.attempt_count, j.execution_epoch
        """
    }
}
