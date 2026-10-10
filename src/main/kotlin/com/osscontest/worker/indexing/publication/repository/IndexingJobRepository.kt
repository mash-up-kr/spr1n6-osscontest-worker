package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

interface IndexingJobRepository : JpaRepository<IndexingJobEntity, Long> {
    fun findBySourceEventId(sourceEventId: UUID): IndexingJobEntity?

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select job from IndexingJobEntity job where job.id = :jobId")
    fun findByIdForUpdate(@Param("jobId") jobId: Long): IndexingJobEntity?

    /** 실행권과 lease가 유효할 때만 완료하고 갱신 행 수를 돌려준다. [outcome]에는 `JobOutcome` 상수를 넘긴다. */
    @Modifying
    @Transactional
    @Query(value = """
        UPDATE indexing_job SET status = 'COMPLETED', outcome = :outcome, completed_at = clock_timestamp(),
            next_retry_at = NULL, updated_at = clock_timestamp()
        WHERE id = :jobId AND worker_id = :workerId AND execution_epoch = :epoch
          AND status = 'PROCESSING' AND lease_until > clock_timestamp()
    """, nativeQuery = true)
    fun complete(@Param("jobId") jobId: Long, @Param("workerId") workerId: String, @Param("epoch") epoch: Long,
                 @Param("outcome") outcome: String): Int

    @Modifying
    @Transactional
    @Query(value = """
        UPDATE indexing_job SET phase = :phase, updated_at = clock_timestamp()
        WHERE id = :jobId AND worker_id = :workerId AND execution_epoch = :epoch
          AND status = 'PROCESSING' AND lease_until > clock_timestamp()
    """, nativeQuery = true)
    fun updatePhase(@Param("jobId") jobId: Long, @Param("phase") phase: String,
                    @Param("workerId") workerId: String, @Param("epoch") epoch: Long): Int

    @Query(value = "SELECT clock_timestamp()", nativeQuery = true)
    fun currentDbTimestamp(): Instant
}
