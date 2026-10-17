/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.deletion.service

import com.osscontest.worker.indexing.publication.repository.*
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.queue.*
import com.osscontest.worker.support.*
import org.junit.jupiter.api.Test
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.mockito.kotlin.*
import java.time.LocalDateTime

class DocumentDeletionServiceTest {
    private val jobs: IndexingJobRepository = mock()
    private val chunks: DocumentChunkRepository = mock()
    private val documents: DocumentRepository = mock()
    private val locks: JobExecutionLocks = mock()
    private val service = DocumentDeletionService(jobs, chunks, documents, JobOwnershipGuard(locks))
    private val job = ClaimedJob(testToken, JobType.DELETE, java.util.UUID.randomUUID(), 2, null, null)

    @Test
    fun `실행권과 삭제 문서를 확인하고 청크 삭제 뒤 Job을 완료한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob().apply { jobType = JobType.DELETE; documentVersionId = null }))
        whenever(documents.findByIdForUpdate(2)).thenReturn(DocumentEntity(2, 1, null, LocalDateTime.now()))
        whenever(jobs.complete(1, testToken.workerId, testToken.epoch, JobOutcome.DELETED)).thenReturn(1)
        service.delete(job)
        inOrder(chunks, jobs).apply {
            verify(chunks).deleteByDocumentId(2)
            verify(jobs).complete(1, testToken.workerId, testToken.epoch, JobOutcome.DELETED)
        }
    }

    @Test
    fun `소유권이 해제된 완료 DELETE를 처리기에 직접 재호출하면 변경 없이 거절한다`() {
        whenever(locks.lock(1)).thenReturn(lockedJob(processingJob(status = IndexingJobStatus.COMPLETED).apply {
            jobType = JobType.DELETE; documentVersionId = null; outcome = JobOutcome.DELETED.name
            workerId = null; leaseUntil = null; completedAt = testNow
        }))
        assertThatThrownBy { service.delete(job) }.isInstanceOf(ExecutionLostException::class.java)
        verifyNoInteractions(chunks)
    }

}
