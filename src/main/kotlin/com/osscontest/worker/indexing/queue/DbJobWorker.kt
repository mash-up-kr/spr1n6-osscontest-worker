package com.osscontest.worker.indexing.queue

import com.osscontest.worker.indexing.pipeline.service.IndexingPipelineRunner
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "indexing.queue", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class DbJobWorker(
    private val properties: QueueProperties,
    private val database: DatabaseOperations,
    private val store: JobQueueStore,
    private val runner: IndexingPipelineRunner,
    private val waiter: QueueLoopWaiter,
    @Value("\${indexing.retry.max-attempts:5}") private val maxAttempts: Int,
    private val results: ExecutionResults,
) : SmartLifecycle {
    init { require(maxAttempts > 0) }

    private val lifecycle = Any()
    @Volatile private var current: Generation? = null
    // Reservations include claim/DB waits and survive stop/start until old loops actually exit.
    private val slots = Semaphore(properties.concurrency)
    // A configured instance label may survive a restart; the boot ID must not.
    private val instanceId = "${properties.workerId}.${UUID.randomUUID()}"
    private val claimIds = AtomicLong()
    private class RunningExecution(private val thread: Thread, val job: ClaimedJob) {
        private var closed = false
        @Synchronized fun close() { closed = true }
        @Synchronized fun revoke() { if (!closed) thread.interrupt() }
    }
    private class Generation(properties: QueueProperties) {
        val running = AtomicBoolean(true)
        val active = ConcurrentHashMap<ExecutionToken, RunningExecution>()
        private val threadIds = AtomicInteger()
        val workers = ThreadPoolExecutor(0, properties.concurrency, properties.workerIdleTimeout.toMillis(),
            TimeUnit.MILLISECONDS, SynchronousQueue<Runnable>(),
            { task -> Thread(task, "job-worker-${threadIds.incrementAndGet()}") })
        val probes = Executors.newSingleThreadScheduledExecutor { Thread(it, "job-probe") }
        val heartbeats = Executors.newSingleThreadScheduledExecutor { Thread(it, "job-heartbeat") }
    }
    private val log = LoggerFactory.getLogger(javaClass)

    override fun isRunning() = current?.running?.get() == true

    override fun start() = synchronized(lifecycle) {
        if (isRunning()) return@synchronized
        val generation = Generation(properties)
        current = generation
        generation.probes.scheduleWithFixedDelay({ guarded { activate(generation) } }, 0,
            properties.pollInterval.toMillis(), TimeUnit.MILLISECONDS)
        generation.heartbeats.scheduleWithFixedDelay({ guarded { heartbeat(generation) } },
            properties.heartbeatInterval.toMillis(), properties.heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS)
        log.info("WORKER_STARTED workerId={} maxConcurrency={}", instanceId, properties.concurrency)
    }

    override fun stop(): Unit = synchronized(lifecycle) {
        val generation = current ?: return@synchronized
        current = null
        generation.running.set(false)
        generation.probes.shutdownNow()
        generation.heartbeats.shutdownNow()
        generation.workers.shutdownNow()
    }

    /** Wake at most one loop. Claiming happens only after the pool accepts and starts it. */
    internal fun probe(): Boolean = current?.let { activate(it) } ?: false

    private fun activate(generation: Generation): Boolean = synchronized(lifecycle) {
        if (current !== generation || !generation.running.get() || !slots.tryAcquire()) return@synchronized false
        try {
            generation.workers.execute { runLoop(generation) }
            true
        } catch (_: RejectedExecutionException) {
            // A retiring pool thread can still be returning after its reservation was released.
            slots.release()
            false
        } catch (error: Exception) {
            slots.release()
            throw error
        }
    }

    private fun runLoop(generation: Generation) {
        try {
            while (generation.running.get()) {
                DatabaseOperations.checkInterrupted()
                val job = try {
                    claim(generation)
                } catch (error: Exception) {
                    if (error is InterruptedException) throw error
                    if (!generation.running.get()) return
                    log.error("Job claim failed", error)
                    waiter.waitFor(properties.pollInterval)
                    continue
                } ?: return // Return this thread to the pool; the probe timer discovers new work.
                runJob(generation, job)
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            slots.release()
        }
    }

    private fun claim(generation: Generation): ClaimedJob? {
        var prepared: ExecutionToken? = null
        val ownerId = "$instanceId-${claimIds.updateAndGet { Math.incrementExact(it) }}"
        return database.execute {
            if (!generation.running.get()) throw InterruptedException("Worker stopping")
            val previous = prepared
            if (previous != null) store.resolveClaim(previous)
            else store.claim(maxAttempts, ownerId) { prepared = it }
        }
    }

    private fun runJob(generation: Generation, job: ClaimedJob) {
        val handle = RunningExecution(Thread.currentThread(), job)
        synchronized(lifecycle) {
            if (current !== generation || !generation.running.get()) return
            generation.active[job.token] = handle
        }
        MDC.put("traceId", job.traceId ?: "-")
        try {
            if (!generation.running.get()) return
            DatabaseOperations.checkInterrupted()
            // More work may exist. Expand up to N while this loop processes its committed claim.
            try {
                activate(generation)
            } catch (error: Exception) {
                log.error("Could not activate another Job loop", error)
            }
            if (generation.running.get()) {
                DatabaseOperations.checkInterrupted()
                runner.run(job)
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (_: ExecutionLostException) {
            log.info("Job execution lost: {}", job.token)
        } catch (_: ExecutionCompletedException) {
            log.info("Job already completed: {}", job.token)
        } catch (error: Exception) {
            log.error("Job execution exited unexpectedly: {}", job.token, error)
        } finally {
            handle.close()
            generation.active.remove(job.token, handle)
            results.forget(job.token)
            MDC.remove("traceId")
            // Job cancellation must not poison the next claim. A stopped generation never resumes.
            if (generation.running.get()) Thread.interrupted()
        }
    }

    internal fun heartbeat() { current?.let { heartbeat(it) } }

    private fun heartbeat(generation: Generation) {
        generation.active.forEach { (token, handle) -> guarded {
            if (!generation.running.get()) return@guarded
            val renewed = database.execute {
                if (!generation.running.get()) throw InterruptedException("Worker stopping")
                store.renew(token)
            }
            if (renewed == 0) {
                val snapshot = database.execute {
                    if (!generation.running.get()) throw InterruptedException("Worker stopping")
                    store.snapshot(token)
                }
                if (generation.active.remove(token, handle) &&
                    (snapshot == null || !snapshot.completed(token) || !snapshot.identifies(handle.job)) &&
                    (snapshot == null || !results.isOwnFailure(snapshot, token))) {
                    handle.revoke()
                }
            }
        } }
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
