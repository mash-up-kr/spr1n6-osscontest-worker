package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.service.ChunkGuard
import com.osscontest.worker.indexing.chunking.service.ChunkingService
import com.osscontest.worker.indexing.chunking.service.ChunkingStrategy
import com.osscontest.worker.indexing.fault.FaultInjectionContext
import com.osscontest.worker.indexing.fault.IndexingFaultInjector
import com.osscontest.worker.indexing.parsing.DocumentParserRegistry
import com.osscontest.worker.indexing.parsing.ParsingTimeoutGuard
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.queue.ExecutionToken
import com.osscontest.worker.indexing.queue.ExecutionOperations
import com.osscontest.worker.indexing.queue.LeaseExpiredException
import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.retrieval.ContentIntegrityException
import com.osscontest.worker.indexing.retrieval.DocumentDownloadClient
import com.osscontest.worker.indexing.retrieval.FileTooLargeException
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.DigestInputStream
import java.security.MessageDigest

/** 획득한 인덱싱 Job의 다운로드부터 임베딩 발행까지 한 번의 시도를 수행한다. */
internal class IndexingAttemptProcessor(
    private val indexingJobRepository: IndexingJobRepository,
    private val downloadClient: DocumentDownloadClient,
    private val parserRegistry: DocumentParserRegistry,
    private val parsingTimeoutGuard: ParsingTimeoutGuard,
    private val chunkingService: ChunkingService,
    private val chunkGuard: ChunkGuard,
    private val indexingProcessor: IndexingProcessor,
    private val faultInjector: IndexingFaultInjector,
    private val maxFileSizeBytes: Long,
    private val chunkingStrategy: ChunkingStrategy,
    private val execution: ExecutionOperations,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun process(
        jobId: Long,
        token: ExecutionToken,
        documentId: Long,
        documentVersion: DocumentVersionEntity,
        faultInjectionContext: FaultInjectionContext,
        onStageStarted: (String) -> Unit,
    ) {
        if (documentVersion.fileSize > maxFileSizeBytes) {
            throw FileTooLargeException(documentVersion.fileSize, maxFileSizeBytes)
        }

        onStageStarted("DOWNLOADING")
        val downloadStartedAt = System.nanoTime()
        log.info(
            "indexing stage started: stage=DOWNLOADING jobId={} documentId={} documentVersionId={}",
            jobId,
            documentId,
            documentVersion.id,
        )
        updatePhase(token, "DOWNLOADING")
        execution.checkpoint(token)
        val tempFile = downloadClient.download(documentVersion.sourceObjectKey)
        log.info(
            "indexing stage completed: stage=DOWNLOADING jobId={} downloadedBytes={} durationMs={}",
            jobId,
            Files.size(tempFile),
            elapsedMillis(downloadStartedAt),
        )
        try {
            onStageStarted("VERIFYING_CONTENT")
            val verificationStartedAt = System.nanoTime()
            log.info("indexing stage started: stage=VERIFYING_CONTENT jobId={}", jobId)
            val actualHash = "sha256:" + sha256HexOf(tempFile)
            if (actualHash != documentVersion.contentHash) {
                throw ContentIntegrityException(documentVersion.contentHash, actualHash)
            }
            log.info(
                "indexing stage completed: stage=VERIFYING_CONTENT jobId={} durationMs={}",
                jobId,
                elapsedMillis(verificationStartedAt),
            )

            onStageStarted("PARSING")
            val parsingStartedAt = System.nanoTime()
            log.info(
                "indexing stage started: stage=PARSING jobId={} mimeType={}",
                jobId,
                documentVersion.mimeType,
            )
            updatePhase(token, "PARSING")
            val parser = parserRegistry.findParser(documentVersion.mimeType)
            val blocks = parsingTimeoutGuard.parse(parser, tempFile, documentVersion.mimeType)
            log.info(
                "indexing stage completed: stage=PARSING jobId={} blockCount={} durationMs={}",
                jobId,
                blocks.size,
                elapsedMillis(parsingStartedAt),
            )

            onStageStarted("CHUNKING")
            val chunkingStartedAt = System.nanoTime()
            log.info(
                "indexing stage started: stage=CHUNKING jobId={} strategy={} blockCount={}",
                jobId,
                chunkingStrategy,
                blocks.size,
            )
            updatePhase(token, "CHUNKING")
            val chunks = chunkingService.chunk(blocks, chunkingStrategy)
            chunkGuard.assertValid(chunks)
            log.info(
                "indexing stage completed: stage=CHUNKING jobId={} chunkCount={} totalTokens={} durationMs={}",
                jobId,
                chunks.size,
                chunks.sumOf { it.tokenCount },
                elapsedMillis(chunkingStartedAt),
            )

            val context =
                IndexingContext(
                    jobId = jobId,
                    documentId = documentId,
                    documentVersionId = documentVersion.id,
                    versionNo = documentVersion.versionNo,
                    extractedMetadata = null,
                    executionToken = token,
                )

            onStageStarted("EMBEDDING")
            log.info(
                "indexing stage started: stage=EMBEDDING jobId={} chunkCount={}",
                jobId,
                chunks.size,
            )
            updatePhase(token, "EMBEDDING")
            faultInjector.blockIfNeeded(
                documentVersionId = documentVersion.id,
                phase = "EMBEDDING",
                context = faultInjectionContext,
            )
            indexingProcessor.process(context, chunks)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    private fun updatePhase(token: ExecutionToken, phase: String) {
        execution.database(token) {
            if (indexingJobRepository.updatePhase(token.jobId, phase, token.workerId, token.epoch) != 1) {
                execution.checkpoint(token)
                throw LeaseExpiredException()
            }
        }
    }

    private fun sha256HexOf(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        DigestInputStream(Files.newInputStream(path), digest).use { stream ->
            val buffer = ByteArray(8192)
            while (stream.read(buffer) != -1) {
                // DigestInputStream이 읽은 바이트로 digest를 갱신한다.
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun elapsedMillis(startedAt: Long): Long =
        (System.nanoTime() - startedAt) / 1_000_000
}
