package com.osscontest.worker.indexing.pipeline.service

import org.springframework.stereotype.Component
import java.time.Duration

fun interface RetryWaiter {
    fun waitFor(duration: Duration)
}

@Component
class ThreadSleepRetryWaiter : RetryWaiter {
    override fun waitFor(duration: Duration) {
        Thread.sleep(duration.toMillis().coerceAtLeast(0))
    }
}
