/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.Instant
import java.util.UUID

interface IndexingJobRepository : JpaRepository<IndexingJobEntity, Long>, IndexingJobWrites {
    /** 통합 테스트가 등록한 Job을 찾는 조회다. 운영 경로는 claim한 Job ID로 조회한다. */
    fun findBySourceEventId(sourceEventId: UUID): IndexingJobEntity?

    @Query(value = "SELECT clock_timestamp()", nativeQuery = true)
    fun currentDbTimestamp(): Instant
}
