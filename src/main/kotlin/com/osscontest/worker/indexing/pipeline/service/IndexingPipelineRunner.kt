package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.consumer.IndexingEvent
import com.osscontest.worker.indexing.consumer.IndexingEventValidator
import com.osscontest.worker.indexing.consumer.InvalidEventException
import com.osscontest.worker.indexing.consumer.KafkaRecordIdentity
import com.osscontest.worker.indexing.fault.FaultInjectionContext
import com.osscontest.worker.indexing.fault.IndexingFaultInjector
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.net.InetAddress
import java.time.Duration
import java.time.LocalDateTime

@Component
class IndexingPipelineRunner(
    private val indexingJobRepository: IndexingJobRepository,
    private val documentRepository: DocumentRepository,
    private val eventValidator: IndexingEventValidator,
    private val downloadClient: DocumentDownloadClient,
    private val parserRegistry: DocumentParserRegistry,
    private val parsingTimeoutGuard: ParsingTimeoutGuard,
    private val chunkingService: ChunkingService,
    private val chunkGuard: ChunkGuard,
    private val indexingProcessor: IndexingProcessor,
    private val indexingFailureService: IndexingFailureService,
    private val faultInjector: IndexingFaultInjector,
    @Value("\${indexing.worker-id:#{T(java.util.UUID).randomUUID().toString()}}")
    private val workerId: String,
    @Value("\${indexing.retry.max-attempts}")
    private val maxAttempts: Int,
    @Value("\${indexing.retry.base-delay}")
    private val baseDelay: Duration,
    @Value("\${indexing.limits.max-file-size-bytes}")
    private val maxFileSizeBytes: Long,
    // 청킹 전략은 설정으로 바꿀 수 있다(기본값은 기존 동작과 같은 FIXED_TOKEN).
    // @Value는 String → enum 변환을 기본 ConversionService로 처리한다.
    @Value("\${indexing.chunking.strategy:FIXED_TOKEN}")
    private val chunkingStrategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN,
    private val retryWaiter: RetryWaiter,
    private val meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val attemptProcessor =
        IndexingAttemptProcessor(
            indexingJobRepository = indexingJobRepository,
            downloadClient = downloadClient,
            parserRegistry = parserRegistry,
            parsingTimeoutGuard = parsingTimeoutGuard,
            chunkingService = chunkingService,
            chunkGuard = chunkGuard,
            indexingProcessor = indexingProcessor,
            faultInjector = faultInjector,
            maxFileSizeBytes = maxFileSizeBytes,
            chunkingStrategy = chunkingStrategy,
        )

    @PostConstruct
    fun logWorkerInfo() {
        log.info("WORKER_STARTED workerId={} hostname={}", workerId, hostname())
    }

    fun currentWorkerId(): String = workerId

    /**
     * Kafka 배치 리스너가 documentId 그룹마다 부르는 단일 진입점이다.
     *
     * 획득과 상태 변경은 리포지토리의 개별 트랜잭션으로 처리한다. 실패를 영구·일시 오류로
     * 분류하고 `RETRY_WAIT`이면 DB 시각의 `next_retry_at`까지 기다린 뒤 같은 이벤트를 재시도한다.
     *
     * 순서가 중요하다: Job 획득(acquireJobId + start)이 **검증보다 먼저** 일어난다.
     * 검증을 먼저 하면, 재시도 경로에서 검증 실패가 발생했을 때 start()에 도달하지 못해
     * attempt_count가 영원히 늘지 않는다. 검증 실패는 재시도해도 절대 성공할 수 없는
     * 부류이므로, start()로 attempt_count를 올린 뒤 catch에서 permanent=true로 FAILED
     * 종결시킨다.
     */
    fun run(
        event: IndexingEvent,
        recordIdentity: KafkaRecordIdentity,
    ) {
        // documentVersionId가 없으면 NOT NULL FK인 Job을 만들 수 없다. 운영 정책에 따라
        // 오류 로그만 남기고 소비하며, Kafka 리스너가 배치를 ack한다.
        val documentVersionId = event.documentVersionId
        if (documentVersionId == null) {
            log.error(
                "INDEXING_REQUESTED event {} (documentId={}) has no documentVersionId, dropping",
                event.eventId, event.documentId,
            )
            return
        }

        val jobId = acquireJobId(event, documentVersionId, recordIdentity) ?: return
        val jobStartedAt = System.nanoTime()
        log.info(
            "indexing job registered: jobId={} eventId={} documentId={} documentVersionId={} " +
                "topic={} partition={} offset={}",
            jobId,
            event.eventId,
            event.documentId,
            documentVersionId,
            recordIdentity.topic,
            recordIdentity.partition,
            recordIdentity.offset,
        )
        val sample = Timer.start(meterRegistry)
        while (true) {
            var currentStage = "ACQUIRING"
            val acquired =
                indexingJobRepository.start(
                    jobId = jobId,
                    workerId = workerId,
                    maxAttempts = maxAttempts,
                )
            if (acquired != 1) {
                // 상한 초과인지, 이미 완료됐는지, 아직 due가 아닌 RETRY_WAIT인지 구분한다.
                if (indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts) == 1) {
                    log.error(
                        "indexing job failed before acquisition: jobId={} eventId={} errorCode={} durationMs={}",
                        jobId,
                        event.eventId,
                        "MAX_ATTEMPTS_EXCEEDED",
                        elapsedMillis(jobStartedAt),
                    )
                    sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                    return
                }
                val job = indexingJobRepository.findById(jobId).orElse(null)
                if (
                    job != null &&
                    job.status == IndexingJobStatus.RETRY_WAIT &&
                    job.nextRetryAt != null
                ) {
                    waitUntilRetryDue(jobId, job.nextRetryAt!!)
                    continue
                }
                log.info(
                    "indexing job skipped: jobId={} eventId={} status={} reason=NOT_ACQUIRED durationMs={}",
                    jobId,
                    event.eventId,
                    job?.status,
                    elapsedMillis(jobStartedAt),
                )
                sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                return
            }
            val attemptStartedAt = System.nanoTime()
            log.info(
                "INDEXING_JOB_STARTED jobId={} sourceEventId={} status={} workerId={} partition={} offset={}",
                jobId,
                event.eventId,
                IndexingJobStatus.PROCESSING,
                workerId,
                recordIdentity.partition,
                recordIdentity.offset,
            )
            try {
                currentStage = "VALIDATING"
                val validationStartedAt = System.nanoTime()
                log.info(
                    "indexing stage started: stage={} jobId={} documentId={} documentVersionId={}",
                    currentStage,
                    jobId,
                    event.documentId,
                    documentVersionId,
                )
                val documentVersion = eventValidator.validate(event)
                val document =
                    documentRepository.findById(event.documentId).orElseThrow {
                        InvalidEventException("DOCUMENT_NOT_FOUND", "document ${event.documentId} does not exist")
                    }
                if (document.tenantId != event.tenantId) {
                    throw InvalidEventException(
                        "TENANT_MISMATCH",
                        "event tenantId=${event.tenantId} but document belongs to tenant ${document.tenantId}",
                    )
                }
                log.info(
                    "indexing stage completed: stage={} jobId={} mimeType={} fileSizeBytes={} durationMs={}",
                    currentStage,
                    jobId,
                    documentVersion.mimeType,
                    documentVersion.fileSize,
                    elapsedMillis(validationStartedAt),
                )

                attemptProcessor.process(
                    jobId,
                    event,
                    documentVersion,
                    FaultInjectionContext(
                        sourceEventId = event.eventId,
                        workerId = workerId,
                        recordIdentity = recordIdentity,
                    ),
                ) { stage -> currentStage = stage }
                log.info(
                    "INDEXING_JOB_COMPLETED jobId={} sourceEventId={} workerId={} partition={} offset={} durationMs={}",
                    jobId,
                    event.eventId,
                    workerId,
                    recordIdentity.partition,
                    recordIdentity.offset,
                    elapsedMillis(jobStartedAt),
                )
                sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                return
            } catch (e: Exception) {
                // 검증부터 publication까지 발생한 실패를 같은 Job 실패 정책으로 기록한다.
                // IndexingProcessor가 이미 자체적으로 기록한 예외가
                // 다시 올라온 경우는 recordFailure 내부의 "status != PROCESSING이면 조용히 반환"
                // 가드 덕분에 중복 기록되지 않는다.
                val error = IndexingErrorClassifier.classify(e)
                val failedAt = indexingJobRepository.currentDbTimestamp()
                val status =
                    indexingFailureService.recordFailure(
                        jobId = jobId,
                        errorCode = error.code,
                        errorMessage = e.message ?: "indexing failed",
                        permanent = !error.retryable,
                        maxAttempts = maxAttempts,
                        // 선형 백오프 계산은 attempt_count를 가진 recordFailure가 수행한다.
                        baseDelay = baseDelay,
                        failedAt = failedAt,
                    )
                val logMessage =
                    "indexing attempt failed: stage={} jobId={} eventId={} documentId={} " +
                        "errorCode={} errorType={} retryable={} resultingStatus={} attemptDurationMs={}"
                if (status == IndexingJobStatus.RETRY_WAIT) {
                    log.warn(
                        logMessage,
                        currentStage,
                        jobId,
                        event.eventId,
                        event.documentId,
                        error.code,
                        e::class.simpleName,
                        error.retryable,
                        status,
                        elapsedMillis(attemptStartedAt),
                        e,
                    )
                } else {
                    log.error(
                        logMessage,
                        currentStage,
                        jobId,
                        event.eventId,
                        event.documentId,
                        error.code,
                        e::class.simpleName,
                        error.retryable,
                        status,
                        elapsedMillis(attemptStartedAt),
                        e,
                    )
                }
                if (status != IndexingJobStatus.RETRY_WAIT) {
                    // FAILED(영구 실패 또는 상한 도달)이거나, 다른 워커가 이미 COMPLETED로 끝냈다.
                    sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
                    return
                }
                val job = indexingJobRepository.findById(jobId).orElseThrow()
                // 실제로 인라인 재시도에 진입한 실패만 센다. 영구 실패나 상한 도달은
                // indexing_job_failed_total의 몫이며 inline retry로 집계하지 않는다.
                meterRegistry.counter("indexing_inline_retry_total", "attempt", job.attemptCount.toString()).increment()
                waitUntilRetryDue(jobId, job.nextRetryAt!!, e)
                // 루프 재진입 시 start가 RETRY_WAIT Job을 다시 PROCESSING으로
                // 되돌리고 attempt_count를 증가시킨다. next_retry_at은 이미 지났으므로 즉시 재획득된다.
            }
        }
    }

    // documentVersionId를 별도 파라미터로 받는 이유: event.documentVersionId는 DTO 차원에서
    // nullable(DOCUMENT_DELETED 표현용)이지만 indexing_job.document_version_id는 NOT NULL이라,
    // 호출자가 null 여부를 먼저 판정한 뒤 non-null 값을 명시적으로 전달하게 한다.
    fun acquireJobId(
        event: IndexingEvent,
        documentVersionId: Long,
        recordIdentity: KafkaRecordIdentity,
    ): Long? {
        val inserted =
            indexingJobRepository.insertIfAbsent(
                event.eventId,
                event.documentId,
                documentVersionId,
                event.traceId,
                recordIdentity.topic,
                recordIdentity.partition,
                recordIdentity.offset,
            )
        val job = indexingJobRepository.findBySourceEventId(event.eventId)
        if (job == null) {
            // uq_indexing_job_active_version 위반 — 다른 이벤트가 이미 이 버전을 활성 처리 중이다.
            log.info(
                "no job row for eventId={} — another active job already targets documentVersionId={}",
                event.eventId, documentVersionId,
            )
            return null
        }
        if (inserted == 0 && !job.matches(recordIdentity)) {
            log.warn(
                "INDEXING_EVENT_REPUBLISHED sourceEventId={} " +
                    "originalTopic={} originalPartition={} originalOffset={} " +
                    "republishedTopic={} republishedPartition={} republishedOffset={} action=IGNORED",
                job.sourceEventId,
                job.kafkaTopic,
                job.kafkaPartition,
                job.kafkaOffset,
                recordIdentity.topic,
                recordIdentity.partition,
                recordIdentity.offset,
            )
            return null
        }
        if (inserted == 0) {
            log.info(
                "INDEXING_EVENT_REDELIVERED sourceEventId={} topic={} partition={} offset={} " +
                    "previousStatus={} previousWorkerId={} currentWorkerId={}",
                job.sourceEventId,
                recordIdentity.topic,
                recordIdentity.partition,
                recordIdentity.offset,
                job.status,
                job.workerId,
                workerId,
            )
            if (job.status in RECOVERABLE_STATUSES) {
                log.warn(
                    "INDEXING_JOB_RECOVERY recoveryType={} jobId={} sourceEventId={} previousStatus={} " +
                        "previousWorkerId={} currentWorkerId={} partition={} offset={}",
                    recoveryType(job.workerId),
                    job.id,
                    job.sourceEventId,
                    job.status,
                    job.workerId,
                    workerId,
                    recordIdentity.partition,
                    recordIdentity.offset,
                )
            }
        }
        return job.id
    }

    private fun waitUntilRetryDue(
        jobId: Long,
        nextRetryAt: LocalDateTime,
        cause: Exception? = null,
    ) {
        // next_retry_at과 남은 대기 시간의 기준 시각을 모두 DB 시계로 통일한다.
        val retryClock = indexingJobRepository.currentDbTimestamp()
        val waitDuration =
            Duration.ofMillis(
                Duration.between(retryClock, nextRetryAt).toMillis().coerceAtLeast(0),
            )
        if (cause == null) {
            log.info("job {} redelivered before retry due, waiting {}ms in-process", jobId, waitDuration.toMillis())
        } else {
            log.info(
                "indexing retry scheduled: jobId={} retryDelayMs={} previousErrorType={}",
                jobId,
                waitDuration.toMillis(),
                cause::class.simpleName,
            )
        }
        retryWaiter.waitFor(waitDuration)
    }

    private fun IndexingJobEntity.matches(recordIdentity: KafkaRecordIdentity): Boolean =
        kafkaTopic == recordIdentity.topic &&
            kafkaPartition == recordIdentity.partition &&
            kafkaOffset == recordIdentity.offset

    private fun elapsedMillis(startedAt: Long): Long =
        (System.nanoTime() - startedAt) / 1_000_000

    private fun recoveryType(previousWorkerId: String?): String =
        when (previousWorkerId) {
            null -> "UNOWNED_JOB"
            workerId -> "SAME_WORKER_REDELIVERY"
            else -> "WORKER_HANDOFF"
        }

    private fun hostname(): String =
        System.getenv("HOSTNAME")
            ?.takeIf { it.isNotBlank() }
            ?: runCatching { InetAddress.getLocalHost().hostName }.getOrDefault("unknown")

    private companion object {
        val RECOVERABLE_STATUSES =
            setOf(
                IndexingJobStatus.PENDING,
                IndexingJobStatus.PROCESSING,
                IndexingJobStatus.RETRY_WAIT,
            )
    }
}
