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

/** Core 서버가 마이그레이션하는 공용 스키마에서 Worker의 인덱싱 Job 상태만 관리한다. */
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

    // 삭제된 문서의 활성 Job을 먼저 종결한다. 실행 중인 Job은 즉시 중단되지 않지만
    // 뒤이어 완료되더라도 삭제 스윕이 청크 정리를 반복해 같은 상태로 수렴한다.
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

    // 완료 후가 아니라 각 단계 진입 직전에 기록해야 사용자가 가장 오래
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

    // 실패 시각과 재획득 조건을 모두 DB 시각으로 계산해 호스트 간 시계 오차를 제거한다.
    // CURRENT_TIMESTAMP(timestamptz)는 쓰지 않는다. 드라이버가 Instant로 돌려줘 이 반환
    // 타입 LocalDateTime으로의 캐스팅이 깨진다. LOCALTIMESTAMP는 세션 timezone 기준
    // timestamp(시간대 없음)라 LocalDateTime과 안전하게 매핑된다.
    @Query(value = "SELECT LOCALTIMESTAMP", nativeQuery = true)
    fun currentDbTimestamp(): LocalDateTime
}
