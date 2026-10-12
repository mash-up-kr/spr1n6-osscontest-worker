package com.osscontest.worker.indexing.queue

import org.springframework.stereotype.Component
import java.time.Duration

/** 예상치 못한 claim 오류 뒤 루프가 다시 claim하기 전까지 기다린다. 트랜잭션 밖에서만 호출한다. */
fun interface QueueLoopWaiter {
    fun waitFor(duration: Duration)
}

@Component
class ThreadSleepQueueLoopWaiter : QueueLoopWaiter {
    override fun waitFor(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}
