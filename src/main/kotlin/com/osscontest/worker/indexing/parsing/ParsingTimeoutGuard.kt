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

/** 파싱을 전용 스레드풀에서 제한 시간 안에 실행하고 손상 파일의 I/O 실패를 영구 오류로 변환한다. */
@Component
class ParsingTimeoutGuard(
    @Value("\${indexing.limits.parse-timeout:PT60S}")
    private val parseTimeout: Duration,
    @Value("\${indexing.queue.concurrency:5}")
    concurrency: Int,
    private val meterRegistry: MeterRegistry,
) {
    // 파싱을 별도 executor에서 수행해 호출 스레드에서 제한 시간과 취소를 관리한다.
    private val parseExecutor = Executors.newFixedThreadPool(concurrency)

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
            meterRegistry.counter("parse_timeout_total").increment()
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
