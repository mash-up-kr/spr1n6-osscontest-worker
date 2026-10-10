package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.domain.IndexingJobStatus
import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@Component
@ConditionalOnProperty(prefix = "indexing.queue", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class DbJobWorker(
    private val properties: QueueProperties,
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
    private val external: ExternalApiOperations,
    private val runner: IndexingPipelineRunner,
    @Value("\${indexing.retry.max-attempts:5}") private val maxAttempts: Int,
) : SmartLifecycle {
    private val running = AtomicBoolean(false)
    private val slots = Semaphore(properties.concurrency)
    private class RunningExecution(private val thread: Thread) {
        private var closed = false
        @Synchronized fun close() { closed = true }
        @Synchronized fun revoke() { if (!closed) thread.interrupt() }
    }
    private val active = ConcurrentHashMap<ExecutionToken, RunningExecution>()
    private val workers = Executors.newFixedThreadPool(properties.concurrency)
    private val claims = Executors.newSingleThreadScheduledExecutor { Thread(it, "job-claim") }
    private val heartbeats = Executors.newSingleThreadScheduledExecutor { Thread(it, "job-heartbeat") }
    private val log = LoggerFactory.getLogger(javaClass)

    override fun isRunning() = running.get()

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        claims.scheduleWithFixedDelay({ guarded { poll() } }, 0, properties.pollInterval.toMillis(), TimeUnit.MILLISECONDS)
        heartbeats.scheduleWithFixedDelay({ guarded { heartbeat() } }, properties.heartbeatInterval.toMillis(),
            properties.heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS)
        log.info("WORKER_STARTED workerId={} concurrency={}", properties.workerId, properties.concurrency)
    }

    override fun stop() {
        running.set(false)
        claims.shutdownNow()
        heartbeats.shutdownNow()
        workers.shutdownNow()
    }

    internal fun poll() {
        database.execute { store.recover(maxAttempts) }
        external.refreshTargets()
        while (running.get() && slots.tryAcquire()) {
            val prepared = AtomicReference<ExecutionToken?>()
            val job = try {
                database.execute {
                    val previous = prepared.get()
                    if (previous != null) store.resolveClaim(previous)
                    else store.claim(external.retryOnly(), maxAttempts) { prepared.set(it) }
                }
            } catch (error: Exception) {
                slots.release()
                throw error
            }
            if (job == null) {
                slots.release()
                break
            }
            try {
                workers.execute {
                    val handle = RunningExecution(Thread.currentThread())
                    active[job.token] = handle
                    MDC.put("traceId", job.traceId ?: "-")
                    try {
                        runner.run(job)
                    } catch (error: Exception) {
                        log.error("Job execution exited unexpectedly: {}", job.token, error)
                    } finally {
                        MDC.remove("traceId")
                        handle.close()
                        active.remove(job.token, handle)
                        slots.release()
                    }
                }
            } catch (error: Exception) {
                slots.release()
                throw error
            }
        }
    }

    internal fun heartbeat() {
        active.forEach { (token, handle) ->
            val renewed = database.execute { store.renew(token) }
            if (renewed == 0) {
                val snapshot = database.execute { store.snapshot(token) }
                if (active.remove(token, handle) &&
                    (snapshot == null || !snapshot.matches(token) || snapshot.status != IndexingJobStatus.COMPLETED)) {
                    handle.revoke()
                }
            }
        }
    }

    private fun guarded(action: () -> Unit) {
        try {
            action()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (error: Exception) {
            log.error("DB queue loop failed", error)
        }
    }
}
