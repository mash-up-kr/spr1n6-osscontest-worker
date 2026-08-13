package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext

/**
 * 임베딩 호출, chunk 저장(UPSERT), fencing 비교와 검색 버전 승격을 담당한다.
 *
 * Job 획득과 attempt_count 증가, 실패 기록, 재시도는 [IndexingPipelineRunner]가 소유한다.
 * 구현체는 실패 시 Job 상태를 변경하지 말고 예외를 그대로 던져야 한다.
 */
interface IndexingProcessor {
    fun process(
        context: IndexingContext,
        chunks: List<Chunk>,
    )
}
