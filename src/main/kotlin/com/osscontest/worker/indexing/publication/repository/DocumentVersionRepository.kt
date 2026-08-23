package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentVersionEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

/** Core 서버가 소유·마이그레이션하는 문서 버전 스키마에 인덱싱 완료 결과만 기록한다. */
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
