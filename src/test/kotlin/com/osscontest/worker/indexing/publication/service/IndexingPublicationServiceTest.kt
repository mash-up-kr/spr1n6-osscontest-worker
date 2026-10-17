/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.publication.service

import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.publication.repository.DocumentChunkRepository
import com.osscontest.worker.indexing.publication.repository.DocumentChunkWriter
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.*
import java.time.LocalDateTime
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever

class IndexingPublicationServiceTest {
    private val documentChunkWriter: DocumentChunkWriter = mock()
    private val documentChunkRepository: DocumentChunkRepository = mock()
    private val documentVersionRepository: DocumentVersionRepository = mock()
    private val documentRepository: DocumentRepository = mock()
    private val indexingJobRepository: IndexingJobRepository = mock()
    private val locks: JobExecutionLocks = mock()
    private val state: com.osscontest.worker.indexing.publication.repository.PublicationStateReader = mock()
    private val service =
        IndexingPublicationService(
            documentChunkWriter,
            documentChunkRepository,
            documentVersionRepository,
            documentRepository,
            indexingJobRepository,
            JobOwnershipGuard(locks),
            state,
        )

    @Test
    fun `삭제 preflight도 만료 lease면 문서 조회와 완료 쓰기를 하지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob().apply { leaseUntil = testNow.minusSeconds(1) }))
        assertThatThrownBy {
            service.preflight(ClaimedJob(testToken, JobType.INDEX, java.util.UUID.randomUUID(), 2, 3, null))
        }.isInstanceOf(LeaseExpiredException::class.java)
        org.mockito.kotlin.verifyNoInteractions(state)
        verify(indexingJobRepository, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `삭제 preflight는 결과 저장 없이 SKIPPED 완료한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(true, null))
        whenever(indexingJobRepository.complete(1, "worker", 1, JobOutcome.SKIPPED_DOCUMENT_DELETED)).thenReturn(1)
        assertThat(service.preflight(ClaimedJob(testToken, JobType.INDEX, java.util.UUID.randomUUID(), 2, 3, null))).isTrue()
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(indexingJobRepository).complete(1, "worker", 1, JobOutcome.SKIPPED_DOCUMENT_DELETED)
    }

    @Test
    fun `활성 문서 preflight는 완료하지 않고 정상 실행을 허용한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, null))
        assertThat(service.preflight(ClaimedJob(testToken, JobType.INDEX, java.util.UUID.randomUUID(), 2, 3, null))).isFalse()
        verifyNoInteractions(indexingJobRepository, documentChunkWriter, documentChunkRepository, documentVersionRepository, documentRepository)
    }

    @Test
    fun `완료된 삭제 INDEX preflight는 소유권 해제와 문서 epoch 증가 뒤에도 다시 쓰지 않는다`() {
        val job = processingJob(status = IndexingJobStatus.COMPLETED).apply {
            workerId = null; leaseUntil = null; outcome = JobOutcome.SKIPPED_DOCUMENT_DELETED.name
        }
        whenever(locks.lock(1)).thenReturn(lockedJob(job, documentEpoch = 2))
        assertThat(service.preflight(ClaimedJob(testToken, JobType.INDEX, java.util.UUID.randomUUID(), 2, 3, null))).isTrue()
        verifyNoInteractions(state, indexingJobRepository, documentChunkWriter, documentChunkRepository, documentVersionRepository, documentRepository)
    }

    @Test
    fun `삭제 preflight 완료 UPDATE가 거절되면 성공으로 반환하지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(true, null))
        whenever(indexingJobRepository.complete(1, "worker", 1, JobOutcome.SKIPPED_DOCUMENT_DELETED)).thenReturn(0)
        assertThatThrownBy {
            service.preflight(ClaimedJob(testToken, JobType.INDEX, java.util.UUID.randomUUID(), 2, 3, null))
        }.isInstanceOf(LeaseExpiredException::class.java)
        verifyNoInteractions(documentChunkWriter, documentChunkRepository, documentVersionRepository, documentRepository)
    }

    @Test
    fun `계산 중 삭제된 문서는 finalize에서 결과를 버리고 SKIPPED 완료한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(true, null))
        whenever(indexingJobRepository.complete(1, "worker", 1, JobOutcome.SKIPPED_DOCUMENT_DELETED)).thenReturn(1)
        assertThat(service.publish(context(), listOf(chunk(0)))).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(documentVersionRepository, never()).complete(any(), any(), any(), org.mockito.kotlin.anyOrNull())
        verify(documentRepository, never()).promoteSearchableVersion(any(), any())
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(longs = [1, 2])
    fun `동일 버전과 구버전 결과는 저장하되 검색 포인터를 변경하지 않는다`(currentNo: Long) {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, 4))
        whenever(state.version(3)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 1))
        whenever(state.version(4)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, currentNo))
        whenever(documentChunkRepository.countByDocumentVersionId(3)).thenReturn(1)
        whenever(documentVersionRepository.complete(3, 2, 1, context().extractedMetadata)).thenReturn(1)
        whenever(indexingJobRepository.complete(1, "worker", 1, JobOutcome.INDEXED)).thenReturn(1)
        val chunks = listOf(chunk(0))
        assertThat(service.publish(context(), chunks)).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter).upsertAll(chunks)
        verify(documentChunkRepository).deleteTrailingChunks(3, 0)
        verify(documentVersionRepository).complete(3, 2, 1, context().extractedMetadata)
        verify(documentRepository, never()).promoteSearchableVersion(any(), any())
        verify(indexingJobRepository).complete(1, "worker", 1, JobOutcome.INDEXED)
    }

    @Test
    fun `잘못된 검색 버전 관계는 결과를 쓰기 전에 거절한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, 4))
        whenever(state.version(3)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 1))
        whenever(state.version(4)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(99, 2))
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(IllegalStateException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["worker", "job-epoch", "document-epoch", "no-lease", "RETRY_WAIT", "FAILED", "expired"])
    fun `구버전이라도 실행권이나 유효 lease가 없으면 결과 버전 검색 포인터 Job을 변경하지 않는다`(condition: String) {
        val job = processingJob()
        var documentEpoch = testToken.epoch
        when (condition) {
            "worker" -> job.workerId = "other"
            "job-epoch" -> job.executionEpoch++
            "document-epoch" -> documentEpoch++
            "no-lease" -> job.leaseUntil = null
            "expired" -> job.leaseUntil = testNow.minusSeconds(1)
            else -> {
                job.status = IndexingJobStatus.valueOf(condition)
                job.workerId = null
                job.leaseUntil = null
                job.nextRetryAt = if (job.status == IndexingJobStatus.RETRY_WAIT) testNow.plusSeconds(30) else null
            }
        }
        whenever(locks.lock(1)).thenReturn(lockedJob(job, documentEpoch))
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, 4))
        whenever(state.version(3)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 1))
        whenever(state.version(4)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 2))

        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }
            .isInstanceOf(if (condition == "expired") LeaseExpiredException::class.java else ExecutionLostException::class.java)
        verifyNoInteractions(state, documentChunkWriter, documentChunkRepository, documentVersionRepository,
            documentRepository, indexingJobRepository)
    }

    @Test
    fun `DB 내용 버전과 context 버전 불일치는 epoch와 무관하게 거절한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob())
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, null))
        whenever(state.version(3)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 9))
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(IllegalStateException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    @Test
    fun `문서 epoch가 다르면 기존 Job 토큰이 맞아도 결과를 쓰지 않는다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(documentEpoch = 2))
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(ExecutionLostException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(indexingJobRepository, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `청크와 검색 버전 저장 후 같은 실행만 완료한다`() {
        val context = context()
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob()))
        whenever(state.document(2)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationDocument(false, null))
        whenever(state.version(3)).thenReturn(com.osscontest.worker.indexing.publication.repository.PublicationVersion(2, 1))
        val chunks = listOf(chunk(0), chunk(1))
        whenever(documentChunkRepository.countByDocumentVersionId(context.documentVersionId)).thenReturn(2)
        whenever(
            documentVersionRepository.complete(
                context.documentVersionId,
                context.documentId,
                chunks.size,
                context.extractedMetadata,
            ),
        ).thenReturn(1)
        whenever(documentRepository.promoteSearchableVersion(context.documentId, context.documentVersionId))
            .thenReturn(1)
        whenever(indexingJobRepository.complete(context.jobId, testToken.workerId, testToken.epoch, JobOutcome.INDEXED)).thenReturn(1)

        val status = service.publish(context, chunks)

        assertThat(status).isEqualTo(IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter).upsertAll(chunks)
        verify(documentChunkRepository).deleteTrailingChunks(context.documentVersionId, 1)
        verify(indexingJobRepository).complete(context.jobId, testToken.workerId, testToken.epoch, JobOutcome.INDEXED)
    }

    @Test
    fun `커밋 응답 유실 후 같은 완료 실행은 결과를 다시 쓰지 않는다`() {
        val job = processingJob(status = IndexingJobStatus.COMPLETED).apply { workerId = null; leaseUntil = null; outcome = JobOutcome.INDEXED.name }
        whenever(locks.lock(1)).thenReturn(lockedJob(job, documentEpoch = 2))
        assertThat(service.publish(context(), listOf(chunk(0))))
            .isEqualTo(com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus.COMPLETED)
        verify(documentChunkWriter, never()).upsertAll(any())
        verify(indexingJobRepository, never()).complete(any(), any(), any(), any())
    }

    @Test
    fun `회수된 이전 epoch의 결과는 쓰지 않는다`() {
        val job = processingJob().apply { executionEpoch++ }
        whenever(locks.lock(1)).thenReturn(lockedJob(job))
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(ExecutionLostException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    @Test
    fun `만료된 lease이면 청크를 쓰기 전에 트랜잭션을 종료한다`() {
        val job = processingJob().apply { leaseUntil = testNow.minusSeconds(1) }
        whenever(locks.lock(1)).thenReturn(lockedJob(job))
        assertThatThrownBy { service.publish(context(), listOf(chunk(0))) }.isInstanceOf(LeaseExpiredException::class.java)
        verify(documentChunkWriter, never()).upsertAll(any())
    }

    private fun context() =
        IndexingContext(
            jobId = 1L,
            documentId = 2L,
            documentVersionId = 3L,
            versionNo = 1L,
            extractedMetadata = mapOf("language" to "ko"),
            executionToken = testToken,
        )

    private fun chunk(no: Int) =
        DocumentChunk(
            documentVersionId = 3L,
            documentId = 2L,
            chunkNo = no,
            content = "content-$no",
            contentTokens = "content $no",
            contentHash = "hash-$no",
            tokenCount = 1,
            pageFrom = null,
            pageTo = null,
            sectionPath = null,
            metadata = null,
            embedding = FloatArray(1536) { 0.1f },
            embeddedAt = LocalDateTime.of(2026, 8, 19, 0, 0),
        )
}
