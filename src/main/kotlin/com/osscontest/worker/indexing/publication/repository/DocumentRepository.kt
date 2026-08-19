package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface DocumentRepository : JpaRepository<DocumentEntity, Long> {
    // searchable_version_id가 가리키는 버전의 embedding_version_no. 아직 검색 버전이 없으면 null.
    @Query(
        value = """
            SELECT dv.embedding_version_no
            FROM document d
            JOIN document_version dv ON dv.id = d.searchable_version_id
            WHERE d.id = :documentId
        """,
        nativeQuery = true,
    )
    fun findSearchableEmbeddingVersionNo(
        @Param("documentId") documentId: Long,
    ): Long?
}
