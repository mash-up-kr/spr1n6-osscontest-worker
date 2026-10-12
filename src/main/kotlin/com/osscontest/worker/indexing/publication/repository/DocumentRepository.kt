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

    // 쓰기 차단은 Job 순서와 execution_epoch가 맡고, version_no 비교는 늦게 끝난 과거 버전 결과를 거절한다.
    // latest_embedding_version_no는 Server 응답용 공용 메타데이터이며 fencing에 쓰지 않는다.
    @Modifying
    @Query(value = """
        UPDATE document d SET searchable_version_id = :documentVersionId,
            latest_embedding_version_no = candidate.version_no, updated_at = clock_timestamp()
        FROM document_version candidate
        WHERE d.id = :documentId AND d.deleted_at IS NULL
          AND candidate.id = :documentVersionId AND candidate.document_id = d.id
          AND (d.searchable_version_id IS NULL OR EXISTS (
              SELECT 1 FROM document_version searchable
              WHERE searchable.id = d.searchable_version_id AND searchable.document_id = d.id
                AND searchable.version_no < candidate.version_no
          ))
    """, nativeQuery = true)
    fun promoteSearchableVersion(@Param("documentId") documentId: Long,
                                @Param("documentVersionId") documentVersionId: Long): Int
}
