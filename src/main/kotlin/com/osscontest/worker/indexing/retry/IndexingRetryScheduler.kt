package com.osscontest.worker.indexing.retry

import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class IndexingRetryScheduler(
    private val indexingJobRepository: IndexingJobRepository,
    private val retryEventSource: RetryEventSource,
    private val pipelineRunner: IndexingPipelineRunner,
    @Value("\${indexing.retry.batch-size:50}")
    private val batchSize: Int = 50,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // 동시 폴링 인스턴스 간 중복 획득 가능성에 대한 트레이드오프 설명은
    // IndexingJobRepository.start()의 KDoc 참고 — 여기서 호출하는 pipelineRunner.run()이
    // 내부적으로 그 start()를 탄다.
    @Scheduled(
        initialDelayString = "\${indexing.retry.initial-delay-ms:10000}",
        fixedDelayString = "\${indexing.retry.poll-interval-ms:10000}",
    )
    fun retryDueJobs() {
        val dueJobIds = indexingJobRepository.findRetryWaitDue(LocalDateTime.now(), batchSize)
        for (jobId in dueJobIds) {
            var event: IndexingRequestedEvent? = null
            try {
                event = retryEventSource.toEvent(jobId) ?: continue
                pipelineRunner.run(event)
            } catch (e: Exception) {
                // pipelineRunner 내부에서 이미 RETRY_WAIT/FAILED로 기록됐다 — 폴러는 로그만 남기고 다음 Job으로 진행한다.
                log.warn(
                    "retry failed for job {} documentId={} eventId={}",
                    jobId, event?.documentId, event?.eventId, e,
                )
            }
        }
    }
}
