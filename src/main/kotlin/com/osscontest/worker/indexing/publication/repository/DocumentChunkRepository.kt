/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.DocumentChunkEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional

/** Core 서버가 소유·마이그레이션하는 청크 스키마에서 Worker가 생성한 인덱싱 데이터만 관리한다. */
interface DocumentChunkRepository : JpaRepository<DocumentChunkEntity, Long> {
    // 같은 문서를 다시 지우면 0건이 되므로 반복 실행해도 결과가 같다.
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
