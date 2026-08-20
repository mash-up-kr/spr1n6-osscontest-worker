package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.util.UUID

interface IndexingJobRepository : JpaRepository<IndexingJobEntity, Long> {
    fun findBySourceEventId(sourceEventId: UUID): IndexingJobEntity?

    // uk_indexing_job_source_event 또는 uq_indexing_job_active_version 위반 시
    // 조용히 0행 처리된다(ON CONFLICT DO NOTHING, 대상 미지정 — 두 제약 모두 커버).
    // 그 뒤 findBySourceEventId로 실제 소유 행을 다시 조회한다.
    // source_event_id 컬럼이 UUID 타입이므로 파라미터도 UUID로 바인딩한다(문자열 캐스팅 불필요).
    @Modifying
    @Transactional
    @Query(
        value = """
            INSERT INTO indexing_job
                (
                    source_event_id,
                    document_id,
                    document_version_id,
                    status,
                    attempt_count,
                    trace_id,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    updated_at
                )
            VALUES
                (
                    :sourceEventId,
                    :documentId,
                    :documentVersionId,
                    'PENDING',
                    0,
                    :traceId,
                    :kafkaTopic,
                    :kafkaPartition,
                    :kafkaOffset,
                    CURRENT_TIMESTAMP
                )
            ON CONFLICT DO NOTHING
        """,
        nativeQuery = true,
    )
    fun insertIfAbsent(
        @Param("sourceEventId") sourceEventId: UUID,
        @Param("documentId") documentId: Long,
        @Param("documentVersionId") documentVersionId: Long,
        @Param("traceId") traceId: String?,
        @Param("kafkaTopic") kafkaTopic: String,
        @Param("kafkaPartition") kafkaPartition: Int,
        @Param("kafkaOffset") kafkaOffset: Long,
    ): Int

    // §1.4-(1): 크래시 재획득(PENDING/PROCESSING) + §3.8: RETRY_WAIT 인프로세스 재시도 재획득을
    // 하나로 처리. attempt_count 상한을 넘으면 재획득 자체가 안 된다(크래시 루프 방어).
    @Modifying
    @Transactional
    @Query(
        value = """
            UPDATE indexing_job
            SET status = 'PROCESSING',
                worker_id = :workerId,
                attempt_count = attempt_count + 1,
                next_retry_at = NULL,
                started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :jobId
              AND attempt_count < :maxAttempts
              AND (
                  status IN ('PENDING', 'PROCESSING')
                  OR (
                      status = 'RETRY_WAIT'
                      AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                  )
              )
        """,
        nativeQuery = true,
    )
    fun start(
        @Param("jobId") jobId: Long,
        @Param("workerId") workerId: String,
        @Param("maxAttempts") maxAttempts: Int,
    ): Int

    // 상한 초과로 재획득 자체가 막힌 PENDING/PROCESSING 행을 FAILED로 종결한다.
    // start()가 0을 반환했을 때, "이미 완료됐다"와 "상한 초과다"를 구분하기 위해 호출자가 사용한다.
    @Modifying
    @Transactional
    @Query(
        value = """
            UPDATE indexing_job
            SET status = 'FAILED',
                last_error_code = 'MAX_ATTEMPTS_EXCEEDED',
                last_error_message = 'Exceeded max attempts while reacquiring after worker crash',
                next_retry_at = NULL,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :jobId
              AND status IN ('PENDING', 'PROCESSING')
              AND attempt_count >= :maxAttempts
        """,
        nativeQuery = true,
    )
    fun failIfAttemptsExceeded(
        @Param("jobId") jobId: Long,
        @Param("maxAttempts") maxAttempts: Int,
    ): Int

    // 완료 쓰기는 가드가 필요 없다 — 청킹이 결정적이라 중복 성공은 무해하고,
    // 실패로 잘못 표시된 걸 진짜 성공이 덮어쓰는 게 맞는 동작이다.
    @Modifying
    @Transactional
    @Query(
        value = """
            UPDATE indexing_job
            SET status = 'COMPLETED',
                completed_at = CURRENT_TIMESTAMP,
                next_retry_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :jobId
        """,
        nativeQuery = true,
    )
    fun complete(
        @Param("jobId") jobId: Long,
    ): Int

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select job from IndexingJobEntity job where job.id = :jobId")
    fun findByIdForUpdate(
        @Param("jobId") jobId: Long,
    ): IndexingJobEntity?

    // §3.9-(1): 재시도 폴러가 삭제된 문서의 Job을 다시 집지 못하게, 활성 Job을 먼저 종결시킨다.
    // PROCESSING 중인 Job은 이 시점에 즉시 인터럽트되진 않지만(§3.9), 그 뒤 완료돼도
    // Step 1의 chunk 삭제가 스윕으로 재실행되며 수렴한다.
    @Modifying
    @Transactional
    @Query(
        value = """
            UPDATE indexing_job
            SET status = 'FAILED',
                last_error_code = 'DOCUMENT_DELETED',
                last_error_message = 'Document was deleted while job was active',
                next_retry_at = NULL,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE document_id = :documentId
              AND status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT')
        """,
        nativeQuery = true,
    )
    fun failActiveJobsForDocument(
        @Param("documentId") documentId: Long,
    ): Int

    // P1-1: 진행률 노출. 완료 후가 아니라 각 단계 "진입 직전"에 호출해야 사용자가 가장 오래
    // 걸리는 구간(특히 EMBEDDING)에서도 멈춘 것처럼 보이지 않는다.
    @Modifying
    @Transactional
    @Query(
        value = "UPDATE indexing_job SET phase = :phase, updated_at = CURRENT_TIMESTAMP WHERE id = :jobId",
        nativeQuery = true,
    )
    fun updatePhase(
        @Param("jobId") jobId: Long,
        @Param("phase") phase: String,
    ): Int

    // P2-2: recordFailure()가 next_retry_at을 계산할 때 애플리케이션 시각이 아니라 DB 시각을
    // 쓰도록 한다 — start()의 재획득 비교(next_retry_at <= CURRENT_TIMESTAMP)도 DB 시각이라
    // 두 시각의 출처를 맞춰야 NTP 오차로 인한 크래시 재획득 타이밍 어긋남이 없다(B-Gap-5).
    @Query(value = "SELECT CURRENT_TIMESTAMP", nativeQuery = true)
    fun currentDbTimestamp(): LocalDateTime
}
