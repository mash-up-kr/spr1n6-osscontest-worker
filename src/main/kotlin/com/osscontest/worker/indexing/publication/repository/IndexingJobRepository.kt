package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
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
                (source_event_id, document_id, document_version_id, status, attempt_count, trace_id, updated_at)
            VALUES
                (:sourceEventId, :documentId, :documentVersionId, 'PENDING', 0, :traceId, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING
        """,
        nativeQuery = true,
    )
    fun insertIfAbsent(
        @Param("sourceEventId") sourceEventId: UUID,
        @Param("documentId") documentId: Long,
        @Param("documentVersionId") documentVersionId: Long,
        @Param("traceId") traceId: String?,
    ): Int

    // §1.4-(1): 크래시 재획득(PENDING/PROCESSING) + §3.8: RETRY_WAIT 재시도 재획득을 하나로 처리.
    // attempt_count 상한을 넘으면 재획득 자체가 안 된다(크래시 루프 방어).
    //
    // 트레이드오프 (accepted, not a bug): 이 메서드는 크래시 재획득(Kafka 재배달 —
    // 컨슈머 그룹의 파티션 소유권이 상호 배제를 보장한다)과 재시도 폴러 재획득(Task 17,
    // IndexingRetryScheduler) 양쪽에서 공유된다. 크래시 재획득 쪽은 Kafka 컨슈머 그룹이
    // 동일 파티션을 두 인스턴스가 동시에 못 읽게 막아주지만, 재시도 폴러 쪽은 그런 인스턴스 간
    // 상호 배제 장치가 없다 — 여러 워커가 동시에 폴링하면 같은 RETRY_WAIT Job을 둘 다 start()로
    // 획득해 중복 처리(불필요한 S3 다운로드/처리, attempt_count 이중 증가)가 발생할 수 있다.
    // 청킹이 결정적이고 저장이 UPSERT 기반(멱등)이라 데이터가 깨지지는 않는다는 전제로 이 상태를
    // 감수한다 — 최초 처리와 재시도 처리를 같은 코드 경로에 태워 결정성을 실질적으로 보장한다는
    // §3.6 목표를 위한 트레이드오프다. 나중에 중복 S3 egress 비용 등 운영 비용이 실제 문제가
    // 되면, 재시도 전용으로 RETRY_WAIT 상태만 대상으로 하는 별도 조건부 UPDATE 쿼리를 분리해야 한다.
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

    @Query(
        value = """
            SELECT id FROM indexing_job
            WHERE status = 'RETRY_WAIT' AND next_retry_at <= :now
            ORDER BY next_retry_at
            LIMIT :limit
        """,
        nativeQuery = true,
    )
    fun findRetryWaitDue(
        @Param("now") now: java.time.LocalDateTime,
        @Param("limit") limit: Int,
    ): List<Long>

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
}
