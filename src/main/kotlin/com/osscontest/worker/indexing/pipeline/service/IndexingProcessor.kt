package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext

/**
 * 임베딩 호출, chunk 저장(UPSERT), embedding_version_no fencing 비교, 검색 버전 전환을 담당한다.
 * 이 계획 밖(별도 브랜치)에서 구현된다 — 여기서는 계약만 정의한다.
 *
 * 계약: 실패 시 예외를 던지면 된다 — 자체적으로 indexing_job을 RETRY_WAIT/FAILED로 기록해도 되고
 * 안 해도 된다. 호출자(IndexingPipelineRunner.run(), Task 10)가 어차피 모든 예외에 대해
 * IndexingFailureService.recordFailure()를 다시 호출하며, 그 안의 "status != PROCESSING이면
 * 조용히 반환" 가드(Task 3) 덕분에 구현체가 이미 기록해둔 경우에도 중복 기록 없이 안전하게
 * 무시된다.
 */
interface IndexingProcessor {
    fun process(
        context: IndexingContext,
        chunks: List<Chunk>,
    )
}
