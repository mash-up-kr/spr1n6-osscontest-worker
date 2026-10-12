package com.osscontest.worker.indexing.concurrent

import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import java.util.concurrent.atomic.AtomicInteger

/** Releases resources after actual run exit, or immediately if cancelled before run starts. */
internal class ManagedTask<T>(action: () -> T, private val onFinished: () -> Unit) : FutureTask<T>(Callable(action)) {
    private val state = AtomicInteger(READY)

    override fun run() {
        if (!state.compareAndSet(READY, RUNNING)) return
        try { super.run() } finally {
            state.set(FINISHED)
            onFinished()
        }
    }

    override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
        val cancelled = super.cancel(mayInterruptIfRunning)
        if (cancelled && state.compareAndSet(READY, FINISHED)) onFinished()
        return cancelled
    }

    private companion object {
        const val READY = 0
        const val RUNNING = 1
        const val FINISHED = 2
    }
}
