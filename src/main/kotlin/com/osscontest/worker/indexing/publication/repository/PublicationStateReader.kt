package com.osscontest.worker.indexing.publication.repository

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class PublicationDocument(val deleted: Boolean, val searchableVersionId: Long?)
data class PublicationVersion(val documentId: Long, val versionNo: Long)

/** Fresh projections under the document/Job locks; never reuse a managed entity read before locking. */
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
