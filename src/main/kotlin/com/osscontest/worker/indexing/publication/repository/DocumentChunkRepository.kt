package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentChunkEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional

/** Core 서버가 소유·마이그레이션하는 청크 스키마에서 Worker가 생성한 인덱싱 데이터만 관리한다. */
interface DocumentChunkRepository : JpaRepository<DocumentChunkEntity, Long> {
    // 몇 번을 다시 실행해도 두 번째부터는 0건 — 멱등(§3.9).
    @Modifying
    @Transactional
    @Query(
        value = "DELETE FROM document_chunk WHERE document_id = :documentId",
        nativeQuery = true,
    )
    fun deleteByDocumentId(
        @Param("documentId") documentId: Long,
    ): Int

    @Modifying
    @Query(
        value = """
            DELETE FROM document_chunk
            WHERE document_version_id = :documentVersionId
              AND chunk_no > :lastChunkNo
        """,
        nativeQuery = true,
    )
    fun deleteTrailingChunks(
        @Param("documentVersionId") documentVersionId: Long,
        @Param("lastChunkNo") lastChunkNo: Int,
    ): Int

    fun countByDocumentVersionId(documentVersionId: Long): Long

}
