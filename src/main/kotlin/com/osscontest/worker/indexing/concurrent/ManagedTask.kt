package com.osscontest.worker.indexing.concurrent

import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import java.util.concurrent.atomic.AtomicInteger

/** 실행이 실제로 끝난 뒤, 또는 시작 전에 취소되면 즉시 [onFinished]로 자원을 반납한다. */
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
