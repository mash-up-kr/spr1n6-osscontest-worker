package com.osscontest.worker.indexing.publication.repository

import com.osscontest.worker.indexing.publication.entity.IndexingJobEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.Instant
import java.util.UUID

interface IndexingJobRepository : JpaRepository<IndexingJobEntity, Long>, IndexingJobWrites {
    fun findBySourceEventId(sourceEventId: UUID): IndexingJobEntity?

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select job from IndexingJobEntity job where job.id = :jobId")
    fun findByIdForUpdate(@Param("jobId") jobId: Long): IndexingJobEntity?

    @Query(value = "SELECT clock_timestamp()", nativeQuery = true)
    fun currentDbTimestamp(): Instant
}
