package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.domain.DocumentChunk
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import tools.jackson.databind.ObjectMapper

/** 물리 스키마와 마이그레이션은 Core 서버가 소유하며, 이 Writer는 합의된 청크 저장 계약만 사용한다. */
@Repository
class DocumentChunkWriter(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val objectMapper: ObjectMapper,
) {
    /**
     * (document_version_id, chunk_no) 유니크 키를 기준으로 멱등하게 저장한다.
     * tenant_id는 문서의 소유 정보에서 가져와 FK 정합성을 보장한다.
     */
    fun upsertAll(chunks: List<DocumentChunk>) {
        chunks.forEach { chunk ->
            val updated = jdbcTemplate.update(UPSERT_SQL, chunk.parameters())
            check(updated == 1) {
                "Document ${chunk.documentId} does not exist or chunk ${chunk.chunkNo} could not be stored"
            }
        }
    }

    private fun DocumentChunk.parameters(): MapSqlParameterSource =
        MapSqlParameterSource()
            .addValue("documentVersionId", documentVersionId)
            .addValue("documentId", documentId)
            .addValue("chunkNo", chunkNo)
            .addValue("content", content)
            .addValue("contentTokens", contentTokens)
            .addValue("contentHash", contentHash)
            .addValue("tokenCount", tokenCount)
            .addValue("pageFrom", pageFrom)
            .addValue("pageTo", pageTo)
            .addValue("sectionPath", sectionPath)
            .addValue("metadata", metadata?.let(objectMapper::writeValueAsString))
            .addValue("embedding", embedding.joinToString(prefix = "[", postfix = "]"))
            .addValue("embeddedAt", embeddedAt)

    private companion object {
        val UPSERT_SQL =
            """
            INSERT INTO document_chunk (
                tenant_id,
                document_version_id,
                document_id,
                chunk_no,
                content,
                content_tokens,
                content_hash,
                token_count,
                page_from,
                page_to,
                section_path,
                metadata,
                embedding,
                embedded_at
            )
            SELECT
                d.tenant_id,
                :documentVersionId,
                :documentId,
                :chunkNo,
                :content,
                :contentTokens,
                :contentHash,
                :tokenCount,
                :pageFrom,
                :pageTo,
                :sectionPath,
                CAST(:metadata AS jsonb),
                CAST(:embedding AS vector),
                :embeddedAt
            FROM document d
            WHERE d.id = :documentId
            ON CONFLICT (document_version_id, chunk_no)
            DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                document_id = EXCLUDED.document_id,
                content = EXCLUDED.content,
                content_tokens = EXCLUDED.content_tokens,
                content_hash = EXCLUDED.content_hash,
                token_count = EXCLUDED.token_count,
                page_from = EXCLUDED.page_from,
                page_to = EXCLUDED.page_to,
                section_path = EXCLUDED.section_path,
                metadata = EXCLUDED.metadata,
                embedding = EXCLUDED.embedding,
                embedded_at = EXCLUDED.embedded_at,
                updated_at = CURRENT_TIMESTAMP
            """.trimIndent()
    }
}
