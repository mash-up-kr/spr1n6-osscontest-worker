package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface DocumentVersionRepository : JpaRepository<DocumentVersionEntity, Long> {
    @Modifying
    @Query(
        """
        update DocumentVersionEntity version
        set version.chunkCount = :chunkCount,
            version.extractedMetadata = :extractedMetadata,
            version.indexedAt = current_timestamp
        where version.id = :documentVersionId
          and version.documentId = :documentId
        """,
    )
    fun complete(
        @Param("documentVersionId") documentVersionId: Long,
        @Param("documentId") documentId: Long,
        @Param("chunkCount") chunkCount: Int,
        @Param("extractedMetadata") extractedMetadata: Map<String, Any>?,
    ): Int
}
