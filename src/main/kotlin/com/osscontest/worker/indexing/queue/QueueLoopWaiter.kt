package com.osscontest.worker.indexing.queue

import org.springframework.stereotype.Component
import java.time.Duration

/** Unexpected claim errors back off outside transactions; empty claims return to the pool. */
fun interface QueueLoopWaiter {
    fun waitFor(duration: Duration)
}

@Component
class ThreadSleepQueueLoopWaiter : QueueLoopWaiter {
    override fun waitFor(duration: Duration) {
        Thread.sleep(duration.toMillis())
    }
}
