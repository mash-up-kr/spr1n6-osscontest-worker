package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.support.assertDedicatedIntegrationDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles

@Tag("integration")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("integration")
class DocumentChunkRepositoryIntegrationTest(
    private val documentChunkRepository: DocumentChunkRepository,
    private val jdbcTemplate: JdbcTemplate,
) {
    // document_chunk.embedding의 vector(1536) NOT NULL 제약을 만족시키는 시드 값이다.
    private val zeroVector = "[" + (1..1536).joinToString(",") { "0" } + "]"

    @BeforeEach
    fun seedTenant() {
        assertDedicatedIntegrationDatabase(jdbcTemplate)
        jdbcTemplate.update(
            "INSERT INTO tenant (id, name, created_at) VALUES (1, 'test-tenant', CURRENT_TIMESTAMP)",
        )
    }

    private fun seedDocument(
        id: Long,
        deleted: Boolean,
    ) {
        val deletedAtClause = if (deleted) "CURRENT_TIMESTAMP" else "NULL"
        jdbcTemplate.update(
            """
            INSERT INTO document (id, tenant_id, owner_principal_id, title, deleted_at)
            VALUES ($id, 1, 'owner', 'test title $id', $deletedAtClause)
            """.trimIndent(),
        )
    }

    private fun seedDocumentVersion(
        id: Long,
        documentId: Long,
    ) {
        jdbcTemplate.update(
            """
            INSERT INTO document_version
                (id, document_id, version_no, embedding_version_no, source_object_key, original_filename, mime_type, file_size, content_hash, created_by_principal_id)
            VALUES
                (?, ?, 1, 1, ?, ?, 'application/pdf', 100, ?, 'owner')
            """.trimIndent(),
            id, documentId, "test-object-key-$id", "test.pdf".toByteArray(), "test-content-hash-$id",
        )
    }

    private fun seedChunk(
        documentVersionId: Long,
        documentId: Long,
        chunkNo: Int,
    ) {
        jdbcTemplate.update(
            """
            INSERT INTO document_chunk
                (tenant_id, document_version_id, document_id, chunk_no, content, content_hash, embedding, embedded_at)
            VALUES
                (1, ?, ?, ?, 'test content', ?, ?::vector, CURRENT_TIMESTAMP)
            """.trimIndent(),
            documentVersionId, documentId, chunkNo, "chunk-hash-$documentVersionId-$chunkNo", zeroVector,
        )
    }

    private fun chunkCountForDocument(documentId: Long): Int =
        jdbcTemplate.queryForObject(
            "SELECT count(*) FROM document_chunk WHERE document_id = ?",
            Int::class.java,
            documentId,
        ) ?: 0

    @Test
    fun `deleteByDocumentId는 대상 문서의 chunk만 지우고 다른 문서 chunk는 남긴다`() {
        seedDocument(1, deleted = false)
        seedDocument(2, deleted = false)
        seedDocumentVersion(1, 1)
        seedDocumentVersion(2, 2)
        seedChunk(1, 1, 0)
        seedChunk(1, 1, 1)
        seedChunk(1, 1, 2)
        seedChunk(2, 2, 0)

        val deleted = documentChunkRepository.deleteByDocumentId(1L)

        assertThat(deleted).isEqualTo(3)
        assertThat(chunkCountForDocument(1L)).isZero()
        assertThat(chunkCountForDocument(2L)).isEqualTo(1)
    }

    @Test
    fun `deleteByDocumentId는 두 번째 호출부터 멱등하게 0건을 반환한다`() {
        seedDocument(1, deleted = false)
        seedDocumentVersion(1, 1)
        seedChunk(1, 1, 0)
        seedChunk(1, 1, 1)

        val firstCall = documentChunkRepository.deleteByDocumentId(1L)
        val secondCall = documentChunkRepository.deleteByDocumentId(1L)

        assertThat(firstCall).isEqualTo(2)
        assertThat(secondCall).isZero()
    }

    @Test
    fun `findDeletedDocumentsWithRemainingChunks는 삭제됐고 chunk가 남은 문서만 찾는다`() {
        // 1: 삭제됐고 chunk가 남음 -> 찾혀야 함
        seedDocument(1, deleted = true)
        seedDocumentVersion(1, 1)
        seedChunk(1, 1, 0)

        // 2: 삭제되지 않음, chunk 있음 -> 찾히면 안 됨
        seedDocument(2, deleted = false)
        seedDocumentVersion(2, 2)
        seedChunk(2, 2, 0)

        // 3: 삭제됐지만 chunk 없음 (이미 스윕됨) -> 찾히면 안 됨
        seedDocument(3, deleted = true)
        seedDocumentVersion(3, 3)

        val result = documentChunkRepository.findDeletedDocumentsWithRemainingChunks(50)

        assertThat(result).containsExactly(1L)
    }

    @Test
    fun `findDeletedDocumentsWithRemainingChunks는 limit을 존중한다`() {
        seedDocument(1, deleted = true)
        seedDocumentVersion(1, 1)
        seedChunk(1, 1, 0)

        seedDocument(2, deleted = true)
        seedDocumentVersion(2, 2)
        seedChunk(2, 2, 0)

        seedDocument(3, deleted = true)
        seedDocumentVersion(3, 3)
        seedChunk(3, 3, 0)

        val limited = documentChunkRepository.findDeletedDocumentsWithRemainingChunks(1)
        val unlimited = documentChunkRepository.findDeletedDocumentsWithRemainingChunks(50)

        assertThat(limited).hasSize(1)
        assertThat(unlimited).hasSize(3)
    }
}
