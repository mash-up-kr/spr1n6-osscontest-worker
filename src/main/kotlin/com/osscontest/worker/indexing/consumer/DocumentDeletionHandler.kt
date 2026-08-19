package com.osscontest.worker.indexing.consumer

import com.osscontest.worker.indexing.deletion.service.DocumentDeletionService
import com.osscontest.worker.indexing.publication.repository.DocumentRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * DOCUMENT_DELETED 전용 핸들러. 삭제는 되돌릴 수 없는 파괴적 연산이라, 인덱싱 경로와 동일한
 * 수준의 스키마 버전/테넌트 검증을 거친 뒤에만 위임한다 — 검증 없이 통과시키면 잘못된 스키마의
 * 이벤트나 테넌트를 위조한 이벤트가 남의 청크를 지울 수 있다.
 *
 * IndexingEventValidator를 재사용하지 않는 이유: 그 검증기는 KDoc에 명시된 대로
 * INDEXING_REQUESTED 전용이고, documentVersionId가 non-null임을 요구한다 —
 * DOCUMENT_DELETED 이벤트에는 documentVersionId가 항상 null이다. 대신 지원 스키마 버전
 * 설정만 동일한 프로퍼티 키로 공유한다.
 */
@Component
class DocumentDeletionHandler
    @Autowired
    constructor(
        private val documentDeletionService: DocumentDeletionService,
        private val documentRepository: DocumentRepository,
        @Value("\${indexing.consumer.supported-schema-versions:1}")
        supportedSchemaVersions: List<String> = listOf("1"),
    ) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val supportedVersions = supportedSchemaVersions.map { it.trim().toInt() }.toSet()

    // 테스트 편의용 보조 생성자 (IndexingEventValidator와 동일한 패턴)
    constructor(
        documentDeletionService: DocumentDeletionService,
        documentRepository: DocumentRepository,
        supportedSchemaVersions: Set<Int>,
    ) : this(documentDeletionService, documentRepository, supportedSchemaVersions.map(Int::toString))

    fun handle(event: IndexingRequestedEvent) {
        if (event.eventSchemaVersion !in supportedVersions) {
            throw InvalidEventException(
                "UNSUPPORTED_SCHEMA_VERSION",
                "eventSchemaVersion=${event.eventSchemaVersion} is not supported",
            )
        }

        val document =
            documentRepository.findById(event.documentId).orElseThrow {
                InvalidEventException("DOCUMENT_NOT_FOUND", "document ${event.documentId} does not exist")
            }
        if (document.tenantId != event.tenantId) {
            throw InvalidEventException(
                "TENANT_MISMATCH",
                "event tenantId=${event.tenantId} but document belongs to tenant ${document.tenantId}",
            )
        }

        log.info("DOCUMENT_DELETED received for documentId={} (eventId={})", event.documentId, event.eventId)
        documentDeletionService.handleDocumentDeleted(event.documentId)
    }
}
