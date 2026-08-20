package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.Optional
import java.util.UUID

class DocumentDeletionHandlerTest {
    private val documentDeletionService: DocumentDeletionService = mock()
    private val documentRepository: DocumentRepository = mock()
    private val handler = DocumentDeletionHandler(documentDeletionService, documentRepository, setOf(1))

    @Test
    fun `스키마 버전과 테넌트가 모두 맞으면 삭제 서비스에 위임한다`() {
        whenever(documentRepository.findById(42L))
            .thenReturn(Optional.of(DocumentEntity(id = 42L, tenantId = 7L, searchableVersionId = null, deletedAt = null)))

        handler.handle(deletedEvent())

        verify(documentDeletionService).handleDocumentDeleted(42L)
    }

    @Test
    fun `지원하지 않는 스키마 버전이면 삭제하지 않고 예외를 던진다`() {
        val thrown =
            catchInvalidEvent { handler.handle(deletedEvent(schemaVersion = 99)) }

        assertThat(thrown.code).isEqualTo("UNSUPPORTED_SCHEMA_VERSION")
        verify(documentDeletionService, never()).handleDocumentDeleted(any())
        // 스키마 검증이 먼저다 — 문서 조회까지 가지 않는다.
        verify(documentRepository, never()).findById(any())
    }

    @Test
    fun `이벤트의 테넌트가 문서의 테넌트와 다르면 삭제하지 않고 예외를 던진다`() {
        whenever(documentRepository.findById(42L))
            .thenReturn(Optional.of(DocumentEntity(id = 42L, tenantId = 999L, searchableVersionId = null, deletedAt = null)))

        val thrown = catchInvalidEvent { handler.handle(deletedEvent()) }

        assertThat(thrown.code).isEqualTo("TENANT_MISMATCH")
        verify(documentDeletionService, never()).handleDocumentDeleted(any())
    }

    @Test
    fun `문서가 존재하지 않으면 삭제하지 않고 예외를 던진다`() {
        whenever(documentRepository.findById(42L)).thenReturn(Optional.empty())

        val thrown = catchInvalidEvent { handler.handle(deletedEvent()) }

        assertThat(thrown.code).isEqualTo("DOCUMENT_NOT_FOUND")
        verify(documentDeletionService, never()).handleDocumentDeleted(any())
    }

    private fun catchInvalidEvent(block: () -> Unit): InvalidEventException {
        val thrown =
            try {
                block()
                null
            } catch (e: InvalidEventException) {
                e
            }
        assertThat(thrown).describedAs("expected InvalidEventException").isNotNull
        return thrown!!
    }

    private fun deletedEvent(schemaVersion: Int = 1) =
        IndexingRequestedEvent(
            eventId = UUID.randomUUID(),
            eventType = "DOCUMENT_DELETED",
            schemaVersion = schemaVersion,
            tenantId = 7L,
            documentId = 42L,
            documentVersionId = null,
            occurredAt = Instant.now(),
            traceId = null,
        )
}
