package com.osscontest.worker.indexing.queue

import com.osscontest.worker.support.claimFixture
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.UUID

class JobQueueStoreTest {
    private val claims: JobClaimTransactions = mock()
    private val store = JobQueueStore(mock(), QueueProperties(workerId = "configured-worker",
        leaseDuration = Duration.ofSeconds(90)), claims, mock(), mock())

    @Test
    fun `fixture도 운영 claim API의 설정 lease와 서로 다른 owner 준비 토큰을 사용한다`() {
        val owners = mutableListOf<String>()
        whenever(claims.claim(any(), eq(90_000L), eq(5), any(), any())).thenAnswer {
            val owner = it.getArgument<String>(0)
            owners.add(owner)
            val token = ExecutionToken(1, owner, 1)
            it.getArgument<(ExecutionToken) -> Unit>(4)(token)
            ClaimAttempt.Acquired(ClaimedJob(token, JobType.INDEX, UUID.randomUUID(), 2, 3, null))
        }
        val prepared = mutableListOf<ExecutionToken>()
        val first = checkNotNull(store.claimFixture { prepared.add(it) })
        val second = checkNotNull(store.claimFixture { prepared.add(it) })
        assertThat(owners).hasSize(2).doesNotHaveDuplicates()
        assertThat(owners.all { it.matches(Regex("fixture-[0-9]+")) }).isTrue()
        assertThat(prepared).containsExactly(first.token, second.token)
    }

    @Test
    fun `운영 claim은 owner 없는 호출과 잘못된 시도 한도를 거절한다`() {
        assertThatThrownBy { store.claim(5, " ") }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { store.claim(0, "instance-1") }.isInstanceOf(IllegalArgumentException::class.java)
        verify(claims, never()).claim(any(), any(), any(), any(), any())
    }
}
