package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import com.osscontest.worker.indexing.publication.repository.DocumentVersionRepository
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID

class IndexingEventValidatorTest {
    private val documentVersionRepository: DocumentVersionRepository = mock()
    private val validator = IndexingEventValidator(documentVersionRepository, supportedSchemaVersions = setOf(1))

    @Test
    fun `지원하지 않는 스키마 버전은 거부한다`() {
        val event = sampleEvent(schemaVersion = 99)

        assertThatThrownBy { validator.validate(event) }
            .isInstanceOf(InvalidEventException::class.java)
            .hasFieldOrPropertyWithValue("code", "UNSUPPORTED_SCHEMA_VERSION")
    }

    @Test
    fun `documentVersionId에 해당하는 document_version이 없으면 거부한다`() {
        whenever(documentVersionRepository.findById(1001L)).thenReturn(java.util.Optional.empty())

        assertThatThrownBy { validator.validate(sampleEvent(documentVersionId = 1001L)) }
            .isInstanceOf(InvalidEventException::class.java)
            .hasFieldOrPropertyWithValue("code", "DOCUMENT_VERSION_NOT_FOUND")
    }

    @Test
    fun `documentVersionId가 없으면 거부한다`() {
        assertThatThrownBy { validator.validate(sampleEvent(documentVersionId = null)) }
            .isInstanceOf(InvalidEventException::class.java)
            .hasFieldOrPropertyWithValue("code", "MISSING_DOCUMENT_VERSION_ID")
    }

    @Test
    fun `document_version이 다른 document에 속하면 거부한다`() {
        whenever(documentVersionRepository.findById(1001L))
            .thenReturn(
                java.util.Optional.of(
                    DocumentVersionEntity(
                        id = 1001L,
                        documentId = 99L,
                        versionNo = 1L,
                        sourceObjectKey = "docs/99/v1.pdf",
                        mimeType = "application/pdf",
                        contentHash = "sha256:abc",
                        embeddingVersionNo = 3L,
                    ),
                ),
            )

        assertThatThrownBy { validator.validate(sampleEvent(documentVersionId = 1001L)) }
            .isInstanceOf(InvalidEventException::class.java)
            .hasFieldOrPropertyWithValue("code", "DOCUMENT_MISMATCH")
    }

    @Test
    fun `유효한 이벤트는 예외 없이 통과한다`() {
        whenever(documentVersionRepository.findById(1001L))
            .thenReturn(
                java.util.Optional.of(
                    DocumentVersionEntity(
                        id = 1001L,
                        documentId = 42L,
                        versionNo = 1L,
                        sourceObjectKey = "docs/42/v1.pdf",
                        mimeType = "application/pdf",
                        contentHash = "sha256:abc",
                        embeddingVersionNo = 3L,
                    ),
                ),
            )

        val result = validator.validate(sampleEvent(documentVersionId = 1001L))

        assertThat(result.id).isEqualTo(1001L)
    }

    private fun sampleEvent(
        schemaVersion: Int = 1,
        documentVersionId: Long? = 1001L,
    ) = IndexingRequestedEvent(
        eventId = UUID.randomUUID(),
        eventType = "INDEXING_REQUESTED",
        schemaVersion = schemaVersion,
        tenantId = 7L,
        documentId = 42L,
        documentVersionId = documentVersionId,
        occurredAt = Instant.now(),
        traceId = null,
    )
}
