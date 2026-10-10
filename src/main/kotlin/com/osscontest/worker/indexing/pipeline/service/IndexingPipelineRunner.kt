package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.fault.FaultInjectionContext
import com.osscontest.worker.indexing.fault.IndexingFaultInjector
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.publication.service.IndexingFailureService
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class IndexingPipelineRunner(
    indexingJobRepository: IndexingJobRepository,
    private val documentRepository: DocumentRepository,
    private val documentVersionRepository: DocumentVersionRepository,
    downloadClient: DocumentDownloadClient,
    parserRegistry: DocumentParserRegistry,
    parsingTimeoutGuard: ParsingTimeoutGuard,
    chunkingService: ChunkingService,
    chunkGuard: ChunkGuard,
    indexingProcessor: IndexingProcessor,
    private val indexingFailureService: IndexingFailureService,
    faultInjector: IndexingFaultInjector,
    private val execution: ExecutionOperations,
    private val store: JobQueueStore,
    private val deletion: DocumentDeletionService,
    @Value("\${indexing.retry.max-attempts:5}") private val maxAttempts: Int,
    @Value("\${indexing.retry.base-delay:PT30S}") private val baseDelay: Duration,
    @Value("\${indexing.limits.max-file-size-bytes}") maxFileSizeBytes: Long,
    @Value("\${indexing.chunking.strategy:FIXED_TOKEN}") chunkingStrategy: ChunkingStrategy,
    private val meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val attempt = IndexingAttemptProcessor(indexingJobRepository, downloadClient, parserRegistry,
        parsingTimeoutGuard, chunkingService, chunkGuard, indexingProcessor, faultInjector,
        maxFileSizeBytes, chunkingStrategy, execution)

    fun run(job: ClaimedJob) {
        val sample = Timer.start(meterRegistry)
        try {
            while (true) {
                execution.checkpoint(job.token)
                val snapshot = execution.database(job.token) { checkNotNull(store.snapshot(job.token)) }
                if (snapshot.status == IndexingJobStatus.RETRY_WAIT) execution.awaitRetry(job.token)
                try {
                    if (job.type == JobType.DELETE) {
                        execution.database(job.token, allowCompleted = true) { deletion.delete(job) }
                    } else {
                        val document = execution.database(job.token) {
                            documentRepository.findById(job.documentId).orElseThrow {
                                InvalidJobException("DOCUMENT_NOT_FOUND", "Document not found")
                            }
                        }
                        if (document.deletedAt != null) throw InvalidJobException("DOCUMENT_DELETED", "Document deleted")
                        val documentVersionId = job.documentVersionId
                            ?: throw InvalidJobException("MISSING_DOCUMENT_VERSION_ID", "Missing version")
                        val version = execution.database(job.token) {
                            documentVersionRepository.findById(documentVersionId).orElseThrow {
                                InvalidJobException("DOCUMENT_VERSION_NOT_FOUND", "Document version not found")
                            }
                        }
                        if (version.documentId != job.documentId) throw InvalidJobException("DOCUMENT_MISMATCH", "Version mismatch")
                        attempt.process(job.token.jobId, job.token, job.documentId, version,
                            FaultInjectionContext(job.sourceEventId, job.token)) {}
                    }
                    log.info("INDEXING_JOB_COMPLETED jobId={} workerId={} epoch={}", job.token.jobId, job.token.workerId, job.token.epoch)
                    return
                } catch (error: Exception) {
                    if (error is ExecutionLostException || error is ExecutionCompletedException || error is InterruptedException) throw error
                    val classified = IndexingErrorClassifier.classify(error)
                    val status = execution.database(job.token, allowCompleted = true) {
                        indexingFailureService.recordFailure(job.token, classified.code, error.message.orEmpty(),
                            !classified.retryable, maxAttempts, baseDelay)
                    }
                    log.warn("Job attempt ended: token={} status={} error={}", job.token, status, classified.code)
                    if (status != IndexingJobStatus.RETRY_WAIT) return
                    meterRegistry.counter("indexing_inline_retry_total").increment()
                    execution.awaitRetry(job.token)
                }
            }
        } catch (_: ExecutionCompletedException) {
            log.info("Job already completed: {}", job.token)
        } catch (_: ExecutionLostException) {
            log.info("Job execution lost: {}", job.token)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            log.info("Job execution stopped: {}", job.token)
        } finally {
            sample.stop(meterRegistry.timer("indexing_job_duration_seconds", "phase", "total"))
        }
    }
}
