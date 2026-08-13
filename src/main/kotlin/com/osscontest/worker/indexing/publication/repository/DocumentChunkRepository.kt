package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentChunkEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional

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

    // 스윕 대상: 삭제됐는데 워커 쪽 chunk가 아직 남은 문서. chunk가 없어지면 다음 스윕에서
    // 자연히 더 이상 안 잡힌다 — 별도 완료 마커(purged_at 등)가 없어도 스스로 종료된다.
    // ORDER BY d.deleted_at: 배치가 다 못 치우는 백로그가 생겨도 오래 삭제된 문서부터
    // 결정적으로 처리한다 — 정렬이 없으면 매 스윕마다 다른 부분집합이 뽑혀 특정 문서가
    // 계속 밀릴 이론적 위험(starvation)이 있다.
    // GROUP BY d.id, d.deleted_at (DISTINCT 대신) — Postgres는 SELECT DISTINCT일 때
    // ORDER BY 대상 컬럼이 SELECT 목록에 있어야 한다. d.deleted_at은 d.id에 함수 종속이므로
    // 함께 GROUP BY해도 결과 집합(문서 id 개수)은 DISTINCT와 동일하다.
    @Query(
        value = """
            SELECT d.id
            FROM document d
            JOIN document_chunk c ON c.document_id = d.id
            WHERE d.deleted_at IS NOT NULL
            GROUP BY d.id, d.deleted_at
            ORDER BY d.deleted_at
            LIMIT :limit
        """,
        nativeQuery = true,
    )
    fun findDeletedDocumentsWithRemainingChunks(
        @Param("limit") limit: Int,
    ): List<Long>
}
