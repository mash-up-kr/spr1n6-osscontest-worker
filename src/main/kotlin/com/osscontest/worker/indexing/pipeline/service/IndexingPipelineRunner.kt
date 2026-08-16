package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkLimitExceededException
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.chunking.service.EmptyExtractionException
import com.osscontest.worker.indexing.chunking.service.TotalTokenLimitExceededException
import com.osscontest.worker.indexing.consumer.IndexingEventValidator
import com.osscontest.worker.indexing.consumer.IndexingRequestedEvent
import com.osscontest.worker.indexing.consumer.InvalidEventException
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.UnsupportedMimeTypeException
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.retrieval.ContentIntegrityException
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.security.MessageDigest
import java.time.Duration
import java.time.LocalDateTime

@Component
class IndexingPipelineRunner(
    private val indexingJobRepository: IndexingJobRepository,
    private val documentRepository: DocumentRepository,
    private val eventValidator: IndexingEventValidator,
    private val downloadClient: DocumentDownloadClient,
    private val parserRegistry: DocumentParserRegistry,
    private val chunkingService: ChunkingService,
    private val chunkGuard: ChunkGuard,
    private val indexingProcessor: IndexingProcessor,
    private val indexingFailureService: IndexingFailureService,
    @Value("\${indexing.worker-id:#{T(java.util.UUID).randomUUID().toString()}}")
    private val workerId: String,
    @Value("\${indexing.retry.max-attempts}")
    private val maxAttempts: Int,
    @Value("\${indexing.retry.base-delay}")
    private val baseDelay: Duration,
    // 청킹 전략은 설정으로 바꿀 수 있다(기본값은 기존 동작과 같은 FIXED_TOKEN).
    // @Value는 String → enum 변환을 기본 ConversionService로 처리한다.
    @Value("\${indexing.chunking.strategy:FIXED_TOKEN}")
    private val chunkingStrategy: ChunkingStrategy = ChunkingStrategy.FIXED_TOKEN,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // 각 repository 호출이 자체 트랜잭션으로 원자적이라 이 메서드 전체에는 별도 @Transactional이
    // 필요 없다 (같은 클래스 안에서 자기 자신을 호출하는 메서드에 @Transactional을 붙여도 Spring AOP
    // 프록시를 안 타서 어차피 무시된다 — self-invocation 문제). acquireJobId()에도 동일하게 적용된다.
    /**
     * Kafka 리스너와 재시도 폴러가 공유하는 단일 진입점.
     *
     * 순서가 중요하다: Job 획득(acquireJobId + start)이 **검증보다 먼저** 일어난다.
     * 검증을 먼저 하면, 재시도 경로에서 검증 실패가 발생했을 때 start()에 도달하지 못해
     * attempt_count가 영원히 늘지 않고 — 이미 RETRY_WAIT이고 next_retry_at도 지난 Job이라 —
     * 폴러가 매 폴링마다 같은 Job을 다시 집는 무한 핫 루프가 된다. 검증 실패는 재시도해도
     * 절대 성공할 수 없는 부류이므로, start()로 attempt_count를 올린 뒤 catch에서
     * recordFailure()가 상한 도달 시 FAILED로 종결하게 해 루프를 끊는다.
     */
    fun run(event: IndexingRequestedEvent) {
        // 유일하게 Job 자체를 만들 수 없는 경우다 — indexing_job.document_version_id가 NOT NULL FK라
        // 어떤 Job 행도 존재할 수 없다. 재시도 경로에서는 발생할 수 없다(RetryEventSource는 항상
        // DB의 Job 행에서 non-null document_version_id를 읽어 이벤트를 재구성한다). 따라서
        // 이 조기 반환은 무한 루프 위험이 없는, 최초 수신 1회성 케이스다.
        val documentVersionId = event.documentVersionId
        if (documentVersionId == null) {
            log.error(
                "INDEXING_REQUESTED event {} (documentId={}) has no documentVersionId, dropping",
                event.eventId, event.documentId,
            )
            return
        }

        val jobId = acquireJobId(event, documentVersionId) ?: return

        val acquired = indexingJobRepository.start(jobId, workerId, maxAttempts)
        if (acquired != 1) {
            // 상한 초과인지, 이미 완료됐는지 구분해서 상한 초과라면 종결한다.
            indexingJobRepository.failIfAttemptsExceeded(jobId, maxAttempts)
            log.info("job {} not acquired (already handled or attempts exceeded)", jobId)
            return
        }

        try {
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

            processAcquiredJob(jobId, event, documentVersion)
        } catch (e: Exception) {
            // Track A 자체 실패(검증/다운로드/파싱/청킹)와 IndexingProcessor 내부 실패를 여기서 동일하게
            // 기록한다(§1.4-(4)). IndexingProcessor가 이미 자체적으로 기록한 예외가 다시 올라온 경우는
            // recordFailure 내부의 "status != PROCESSING이면 조용히 반환" 가드 덕분에 중복 기록되지 않는다.
            val resultStatus =
                indexingFailureService.recordFailure(
                    jobId = jobId,
                    errorCode = errorCodeOf(e),
                    errorMessage = e.message ?: "indexing failed",
                    maxAttempts = maxAttempts,
                    // §3.8 선형 백오프 — 실제 곱셈은 attempt_count를 알고 있는 recordFailure가 한다.
                    baseDelay = baseDelay,
                    failedAt = LocalDateTime.now(),
                )
            when (resultStatus) {
                IndexingJobStatus.RETRY_WAIT -> log.warn("job {} failed, will retry", jobId, e)
                else -> log.error("job {} failed terminally", jobId, e)
            }
        }
    }

    // 의도적으로 .code를 갖고 있는 예외는 그 코드를 그대로 쓰고(SCREAMING_SNAKE 규약 —
    // last_error_code에 SQL 리터럴로 쓰이는 MAX_ATTEMPTS_EXCEEDED/DOCUMENT_DELETED와 같은 형식),
    // 그 외에는 클래스 simpleName으로 폴백한다.
    private fun errorCodeOf(e: Exception): String =
        when (e) {
            is InvalidEventException -> e.code
            is ContentIntegrityException -> e.code
            is EmptyExtractionException -> e.code
            is ChunkLimitExceededException -> e.code
            is TotalTokenLimitExceededException -> e.code
            is UnsupportedMimeTypeException -> e.code
            else -> e::class.simpleName ?: "INDEXING_ERROR"
        }

    private fun processAcquiredJob(
        jobId: Long,
        event: IndexingRequestedEvent,
        documentVersion: DocumentVersionEntity,
    ) {
        // 조기 fencing 판정 (§3.1) — 임베딩은커녕 다운로드도 하지 않는다.
        val searchableVersionNo = documentRepository.findSearchableEmbeddingVersionNo(event.documentId)
        if (searchableVersionNo != null && documentVersion.embeddingVersionNo < searchableVersionNo) {
            log.info(
                "job {} is stale (embeddingVersionNo={} < searchable={}), completing without processing",
                jobId, documentVersion.embeddingVersionNo, searchableVersionNo,
            )
            indexingJobRepository.complete(jobId)
            return
        }

        val bytes = downloadClient.download(documentVersion.sourceObjectKey)
        val actualHash = "sha256:" + sha256Hex(bytes)
        if (actualHash != documentVersion.contentHash) {
            throw ContentIntegrityException(documentVersion.contentHash, actualHash)
        }

        val parser = parserRegistry.findParser(documentVersion.mimeType)
        val blocks = parser.parse(bytes.inputStream()).toList()

        val chunks = chunkingService.chunk(blocks, chunkingStrategy)
        chunkGuard.assertValid(chunks)

        val context =
            IndexingContext(
                jobId = jobId,
                documentId = event.documentId,
                documentVersionId = documentVersion.id,
                versionNo = documentVersion.versionNo,
                extractedMetadata = null,
            )

        indexingProcessor.process(context, chunks)
    }

    // @Transactional 관련 근거는 run() 위 주석 참고 — 이 메서드도 동일하게 적용된다.
    // documentVersionId를 별도 파라미터로 받는 이유: event.documentVersionId는 DTO 차원에서
    // nullable(DOCUMENT_DELETED 표현용)이지만 indexing_job.document_version_id는 NOT NULL이라,
    // 호출자가 null 여부를 먼저 판정한 뒤 non-null 값을 명시적으로 전달하게 한다.
    fun acquireJobId(
        event: IndexingRequestedEvent,
        documentVersionId: Long,
    ): Long? {
        indexingJobRepository.insertIfAbsent(
            event.eventId,
            event.documentId,
            documentVersionId,
            event.traceId,
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
        return job.id
    }

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
