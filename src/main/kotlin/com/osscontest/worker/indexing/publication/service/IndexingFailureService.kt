package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.LocalDateTime

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
    private val meterRegistry: MeterRegistry,
) {
    /**
     * §3.8: next_retry_at = now() + base_delay * attempt_count (선형 백오프).
     *
     * permanent=true면(호출자가 이미 "재시도해도 항상 같은 결과"라고 판단한 예외) attempt_count와
     * 무관하게 즉시 FAILED로 종결한다. permanent=false면 attempt_count가 maxAttempts 미만인 동안은
     * RETRY_WAIT, 도달하면 FAILED다.
     *
     * 곱해야 할 attempt_count를 실제로 아는 곳은 잠금과 함께 Job을 읽는 이 메서드다.
     * attempt_count는 IndexingJobRepository.start()가 이미 증가시킨 이번 시도 회차다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun recordFailure(
        jobId: Long,
        errorCode: String,
        errorMessage: String,
        permanent: Boolean,
        maxAttempts: Int,
        baseDelay: Duration,
        failedAt: LocalDateTime,
    ): IndexingJobStatus {
        val job =
            checkNotNull(indexingJobRepository.findByIdForUpdate(jobId)) {
                "Indexing job $jobId does not exist"
            }
        if (job.status != IndexingJobStatus.PROCESSING) {
            // 다른(더 빠른) 워커가 이미 COMPLETED로 끝냈다 — 이 실패 기록은 조용히 무시한다.
            return job.status
        }

        job.lastErrorCode = errorCode.take(MAX_ERROR_CODE_LENGTH)
        job.lastErrorMessage = errorMessage.take(MAX_ERROR_MESSAGE_LENGTH)
        job.updatedAt = failedAt

        return if (permanent || job.attemptCount >= maxAttempts) {
            job.status = IndexingJobStatus.FAILED
            job.nextRetryAt = null
            job.completedAt = failedAt
            // P1-2: DLQ가 없는 이 설계에서 실패를 감지하는 핵심 지표(FAULT_TOLERANCE.md §3 P1-2).
            meterRegistry.counter("indexing_job_failed_total", "errorCode", job.lastErrorCode!!).increment()
            IndexingJobStatus.FAILED
        } else {
            job.status = IndexingJobStatus.RETRY_WAIT
            // attemptCount는 start()를 거쳤다면 항상 1 이상이지만, 0이면 지연이 0이 되어 즉시
            // 재시도하는 핫 루프가 되므로 최소 1회차로 보정한다.
            val multiplier = job.attemptCount.coerceAtLeast(1).toLong()
            job.nextRetryAt = failedAt.plus(baseDelay.multipliedBy(multiplier))
            job.completedAt = null
            IndexingJobStatus.RETRY_WAIT
        }
    }

    private companion object {
        const val MAX_ERROR_CODE_LENGTH = 100
        const val MAX_ERROR_MESSAGE_LENGTH = 1_000
    }
}
