package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.LocalDateTime

@Service
class IndexingFailureService(
    private val indexingJobRepository: IndexingJobRepository,
) {
    /**
     * §3.8: next_retry_at = now() + base_delay * attempt_count (선형 백오프).
     *
     * 호출자가 nextRetryAt을 미리 계산해 넘기지 않고 baseDelay만 넘기는 이유는, 곱해야 할
     * attempt_count를 실제로 알고 있는 곳이 여기이기 때문이다 — 이 메서드가 잠금과 함께 읽는
     * job.attemptCount는 IndexingJobRepository.start()가 이미 증가시킨 "이번 시도 회차"다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun recordFailure(
        jobId: Long,
        errorCode: String,
        errorMessage: String,
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

        return if (job.attemptCount >= maxAttempts) {
            job.status = IndexingJobStatus.FAILED
            job.nextRetryAt = null
            job.completedAt = failedAt
            IndexingJobStatus.FAILED
        } else {
            job.status = IndexingJobStatus.RETRY_WAIT
            // attemptCount는 start()를 거쳤다면 항상 1 이상이지만, 0이면 지연이 0이 되어 폴러가
            // 즉시 다시 집는 핫 루프가 되므로 최소 1회차로 보정한다.
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
