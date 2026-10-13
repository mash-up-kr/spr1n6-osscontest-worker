package com.osscontest.worker.indexing.publication.repository

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class PublicationDocument(val deleted: Boolean, val searchableVersionId: Long?)
data class PublicationVersion(val documentId: Long, val versionNo: Long)

/** 문서·Job 잠금 아래에서 새로 조회한다. 잠금 전에 읽은 관리 엔티티를 재사용하지 않는다. */
@Repository
class PublicationStateReader(private val jdbc: NamedParameterJdbcTemplate) {
    fun document(id: Long): PublicationDocument = jdbc.query(
        "SELECT deleted_at, searchable_version_id FROM document WHERE id = :id", mapOf("id" to id),
    ) { rs, _ -> PublicationDocument(rs.getTimestamp("deleted_at") != null,
        (rs.getObject("searchable_version_id") as Number?)?.toLong()) }.single()

    fun version(id: Long): PublicationVersion = jdbc.query(
        "SELECT document_id, version_no FROM document_version WHERE id = :id", mapOf("id" to id),
    ) { rs, _ -> PublicationVersion(rs.getLong("document_id"), rs.getLong("version_no")) }.single()
}
