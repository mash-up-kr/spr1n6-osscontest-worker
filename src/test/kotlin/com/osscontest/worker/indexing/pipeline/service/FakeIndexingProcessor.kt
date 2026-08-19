package com.osscontest.worker.indexing.pipeline.service

import com.osscontest.worker.indexing.chunking.domain.Chunk
import com.osscontest.worker.indexing.pipeline.domain.IndexingContext
import com.osscontest.worker.indexing.publication.repository.IndexingJobRepository

// IndexingProcessor의 실제 구현(임베딩 호출 → 청크 저장 → searchable 버전 전환)은
// 이 브랜치 밖(형제 브랜치 feat/indexing-pipeline)에 있고, 이 계획은 그 구현을 존재하지
// 않는 것으로 취급한다(인터페이스+stub만 둠). 하지만 IndexingPipelineRunner가 생성자에서
// IndexingProcessor를 실제 의존성으로 요구하기 때문에, 이게 없으면 Spring이 빈을 못 찾아
// @SpringBootTest로 전체 컨텍스트를 띄우는 테스트가 깨진다 — 이럴 때 @TestConfiguration으로
// 이 fake를 IndexingProcessor 빈으로 등록해 컨텍스트가 뜨게 한다.
// 단위 테스트에서는 직접 생성해 주입하고, calls로 어떤 IndexingContext/청크가 전달됐는지
// 검증하거나 throwOnNextCall로 실패 기록 경로(recordFailure)를 테스트하는 데 쓴다.
// @Component가 아니므로 실제 프로덕션 컨텍스트에는 절대 자동으로 끼어들지 않는다.
class FakeIndexingProcessor(
    // IndexingPipelineRunner는 성공 경로에서 스스로 indexingJobRepository.complete()를 부르지
    // 않는다 — 그건 실제 IndexingProcessor 구현의 책임이다(청크 저장 UPSERT/검색 버전 전환과
    // 같은 트랜잭션으로 묶일 예정). 순수 Mockito 단위 테스트(IndexingPipelineRunnerTest)는
    // 이 책임 분리 자체를 검증 대상으로 삼아 항상 null로 두고 complete()가 절대 안 불리는 걸
    // 확인한다. 통합 테스트(IndexingPipelineRunnerIntegrationTest)에서만 실제 리포지토리를 넘겨
    // 성공 시 completion을 흉내낸다.
    private val indexingJobRepository: IndexingJobRepository? = null,
) : IndexingProcessor {
    val calls = mutableListOf<Pair<IndexingContext, List<Chunk>>>()
    var throwOnNextCall: Exception? = null

    // process() 호출 횟수가 이 값 이하인 동안은 (throwOnNextCall과 별개로) 실패하고,
    // 넘어서면 성공한다. 기본 0이면 항상 성공(throwOnNextCall이 없는 한),
    // Int.MAX_VALUE면 사실상 항상 실패. 인프로세스 재시도 통합 테스트에서 "N번 실패 후 성공"을
    // 시뮬레이션하는 데 쓴다 — throwOnNextCall은 1회성이라 여러 번 재시도되는 시나리오를
    // 표현할 수 없어서 별도로 둔다.
    var failuresBeforeSuccess: Int = 0
    private var callCount = 0

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
