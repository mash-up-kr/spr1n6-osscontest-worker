/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.concurrent.ManagedTask
import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import com.osscontest.worker.indexing.retrieval.TemporaryDocumentFile
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import java.io.IOException
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/** 파싱을 전용 스레드풀에서 제한 시간 안에 실행하고 손상 파일의 I/O 실패를 영구 오류로 변환한다. */
@Component
class ParsingTimeoutGuard(
    @Value("\${indexing.limits.parse-timeout:PT60S}")
    private val parseTimeout: Duration,
    @Value("\${indexing.queue.concurrency:5}")
    concurrency: Int,
    private val meterRegistry: MeterRegistry,
) {
    init { require(concurrency > 0 && parseTimeout.toMillis() > 0) }
    private val lifecycle = Any()
    private val tasks = ConcurrentHashMap.newKeySet<ManagedTask<List<ParsedBlock>>>()
    // 제한 시간 뒤에도 끝나지 않은 파서가 스레드를 잡고 있을 수 있어 슬롯 수만큼만 대기시킨다.
    // 그 이상은 ParseCapacityException으로 돌려보내 다음 claim에서 다시 시도한다.
    private val parseExecutor = ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(concurrency))

    fun parse(
        parser: DocumentParser,
        file: TemporaryDocumentFile,
        mimeType: String,
    ): List<ParsedBlock> {
        val use = file.retain()
        lateinit var future: ManagedTask<List<ParsedBlock>>
        future = ManagedTask({
            try {
                Files.newInputStream(file.path).use { parser.parse(it).toList() }
            } catch (e: IOException) {
                throw CorruptedFileException(mimeType, e)
            }
        }, { try { use.close() } finally { tasks.remove(future); parseExecutor.remove(future) } })
        synchronized(lifecycle) {
            if (parseExecutor.isShutdown) {
                future.cancel(true)
                throw InterruptedException("Parser shutting down")
            }
            tasks.add(future)
            try { parseExecutor.execute(future) }
            catch (error: RejectedExecutionException) {
                future.cancel(true)
                throw ParseCapacityException(error)
            }
        }
        return try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            meterRegistry.counter("parse_timeout_total").increment()
            throw ParseTimeoutException(mimeType, parseTimeout)
        } catch (e: InterruptedException) {
            future.cancel(true)
            Thread.currentThread().interrupt()
            throw e
        } catch (_: CancellationException) {
            Thread.currentThread().interrupt()
            throw InterruptedException("Parsing cancelled")
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is Error) throw cause
            throw (cause as? Exception) ?: e
        }
    }

    @PreDestroy
    fun shutdown() {
        synchronized(lifecycle) {
            tasks.forEach { it.cancel(true) }
            parseExecutor.shutdownNow()
        }
    }
}

class ParseCapacityException(cause: Throwable) : RuntimeException("Parser capacity exhausted", cause) {
    val code: String = "PARSE_CAPACITY"
}
