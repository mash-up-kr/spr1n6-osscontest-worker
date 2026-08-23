package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

/** Core 서버가 소유·마이그레이션하는 문서 스키마를 합의된 인덱싱 계약 범위에서만 갱신한다. */
interface DocumentRepository : JpaRepository<DocumentEntity, Long> {
    /**
     * 현재 검색 버전보다 embedding_version_no가 큰 버전만 승격한다.
     * 더 느린 이전 Job과 삭제된 문서는 searchable_version_id를 덮어쓰지 못한다.
     */
    @Modifying
    @Query(
        value = """
            UPDATE document d
            SET searchable_version_id = :documentVersionId,
                latest_embedding_version_no = GREATEST(
                    d.latest_embedding_version_no,
                    (
                        SELECT candidate.embedding_version_no
                        FROM document_version candidate
                        WHERE candidate.id = :documentVersionId
                          AND candidate.document_id = :documentId
                    )
                ),
                updated_at = CURRENT_TIMESTAMP
            WHERE d.id = :documentId
              AND d.deleted_at IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM document_version candidate
                  WHERE candidate.id = :documentVersionId
                    AND candidate.document_id = d.id
              )
              AND (
                  d.searchable_version_id IS NULL
                  OR (
                      SELECT searchable.embedding_version_no
                      FROM document_version searchable
                      WHERE searchable.id = d.searchable_version_id
                  ) < (
                      SELECT candidate.embedding_version_no
                      FROM document_version candidate
                      WHERE candidate.id = :documentVersionId
                        AND candidate.document_id = d.id
                  )
              )
        """,
        nativeQuery = true,
    )
    fun promoteSearchableVersion(
        @Param("documentId") documentId: Long,
        @Param("documentVersionId") documentVersionId: Long,
    ): Int
}
