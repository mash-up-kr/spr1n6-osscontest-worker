package com.osscontest.worker.indexing.parsing

import com.osscontest.worker.indexing.parsing.domain.ParsedBlock
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

// P0-2-b: 파싱이 hang되면 max.poll.interval.ms를 넘겨 불필요한 리밸런스가 난다. 전용
// 스레드풀에서 파싱을 실행하고 시간 안에 못 끝나면 future를 취소한 뒤 ParseTimeoutException을
// 던진다(재시도 가능 — isRetryable() 기본값을 그대로 따른다, 별도 화이트리스트 등록 불필요).
// IOException(손상 파일)은 CorruptedFileException(영구 실패)으로 여기서 감싼다.
@Component
class ParsingTimeoutGuard(
    @Value("\${indexing.limits.parse-timeout:PT60S}")
    private val parseTimeout: Duration,
    @Value("\${indexing.consumer.concurrency:5}")
    concurrency: Int,
    private val meterRegistry: MeterRegistry,
) {
    // IndexingBatchExecutorConfig의 executor(문서 처리 자체)와는 다른 풀이다 — 파싱이
    // 걸려도 취소된 스레드가 이 풀에만 누적된다(parse_thread_leaked, P1-2).
    private val parseExecutor = Executors.newFixedThreadPool(concurrency)
    private val leakedThreadCount = AtomicInteger(0)

    init {
        meterRegistry.gauge("parse_thread_leaked", leakedThreadCount)
    }

    fun parse(
        parser: DocumentParser,
        path: Path,
        mimeType: String,
    ): List<ParsedBlock> {
        val future =
            parseExecutor.submit(
                Callable {
                    try {
                        Files.newInputStream(path).use { parser.parse(it).toList() }
                    } catch (e: IOException) {
                        throw CorruptedFileException(mimeType, e)
                    }
                },
            )
        return try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            leakedThreadCount.incrementAndGet()
            throw ParseTimeoutException(mimeType, parseTimeout)
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is Error) throw cause
            throw (cause as? Exception) ?: e
        }
    }

    @PreDestroy
    fun shutdown() {
        parseExecutor.shutdownNow()
    }
}
