package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.Instant
import java.util.UUID

interface IndexingJobRepository : JpaRepository<IndexingJobEntity, Long>, IndexingJobWrites {
    fun findBySourceEventId(sourceEventId: UUID): IndexingJobEntity?

    @Query(value = "SELECT clock_timestamp()", nativeQuery = true)
    fun currentDbTimestamp(): Instant
}
