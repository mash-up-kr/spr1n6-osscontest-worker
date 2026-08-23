package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository

// 단위·통합 테스트에서 전달값, 실패 횟수, Job 완료를 제어하는 IndexingProcessor 대역이다.
// @Component가 아니므로 테스트 설정에서 명시적으로 등록할 때만 사용된다.
class FakeIndexingProcessor(
    // 통합 테스트에서만 리포지토리를 주입해 실제 구현의 완료 처리를 흉내낸다.
    private val indexingJobRepository: IndexingJobRepository? = null,
) : IndexingProcessor {
    val calls = mutableListOf<Pair<IndexingContext, List<Chunk>>>()
    var throwOnNextCall: Exception? = null

    // 호출 횟수가 이 값 이하이면 실패한다. 기본값 0은 즉시 성공, Int.MAX_VALUE는 계속 실패다.
    var failuresBeforeSuccess: Int = 0
    private var callCount = 0

    fun reset() {
        calls.clear()
        throwOnNextCall = null
        failuresBeforeSuccess = 0
        callCount = 0
    }

    override fun process(
        context: IndexingContext,
        chunks: List<Chunk>,
    ) {
        calls.add(context to chunks)
        callCount++
        throwOnNextCall?.let {
            throwOnNextCall = null
            throw it
        }
        if (callCount <= failuresBeforeSuccess) {
            throw RuntimeException("simulated processing failure (call $callCount)")
        }
        indexingJobRepository?.complete(context.jobId)
    }
}
