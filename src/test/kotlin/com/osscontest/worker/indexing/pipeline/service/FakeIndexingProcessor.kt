package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext

// IndexingProcessor의 실제 구현(임베딩 호출 → 청크 저장 → searchable 버전 전환)은
// 이 브랜치 밖(형제 브랜치 feat/indexing-pipeline)에 있고, 이 계획은 그 구현을 존재하지
// 않는 것으로 취급한다(인터페이스+stub만 둠). 하지만 IndexingPipelineRunner가 생성자에서
// IndexingProcessor를 실제 의존성으로 요구하기 때문에, 이게 없으면 Spring이 빈을 못 찾아
// @SpringBootTest로 전체 컨텍스트를 띄우는 테스트가 깨진다 — 이럴 때 @TestConfiguration으로
// 이 fake를 IndexingProcessor 빈으로 등록해 컨텍스트가 뜨게 한다.
// 단위 테스트에서는 직접 생성해 주입하고, calls로 어떤 IndexingContext/청크가 전달됐는지
// 검증하거나 throwOnNextCall로 실패 기록 경로(recordFailure)를 테스트하는 데 쓴다.
// @Component가 아니므로 실제 프로덕션 컨텍스트에는 절대 자동으로 끼어들지 않는다.
class FakeIndexingProcessor : IndexingProcessor {
    val calls = mutableListOf<Pair<IndexingContext, List<Chunk>>>()
    var throwOnNextCall: Exception? = null

    override fun process(
        context: IndexingContext,
        chunks: List<Chunk>,
    ) {
        calls.add(context to chunks)
        throwOnNextCall?.let {
            throwOnNextCall = null
            throw it
        }
    }
}
