package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface DocumentRepository : JpaRepository<DocumentEntity, Long> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select document from DocumentEntity document where document.id = :id")
    fun findByIdForUpdate(@Param("id") id: Long): DocumentEntity?

    // Job order and execution epochs fence writes. version_no rejects a late historical retry.
    // The shared latest_embedding_version_no column remains Server response metadata, not a fence.
    @Modifying
    @Query(value = """
        UPDATE document d SET searchable_version_id = :documentVersionId,
            latest_embedding_version_no = candidate.version_no, updated_at = clock_timestamp()
        FROM document_version candidate
        WHERE d.id = :documentId AND d.deleted_at IS NULL
          AND candidate.id = :documentVersionId AND candidate.document_id = d.id
          AND NOT EXISTS (
              SELECT 1 FROM document_version searchable
              WHERE searchable.id = d.searchable_version_id AND searchable.version_no > candidate.version_no
          )
    """, nativeQuery = true)
    fun promoteSearchableVersion(@Param("documentId") documentId: Long,
                                @Param("documentVersionId") documentVersionId: Long): Int
}
