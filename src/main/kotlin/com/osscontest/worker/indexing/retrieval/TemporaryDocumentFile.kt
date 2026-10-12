package com.osscontest.worker.indexing.retrieval

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/** Deletes after the runner closes and every parser has actually stopped using the file. */
class TemporaryDocumentFile(val path: Path) : AutoCloseable {
    private var users = 1
    private var closed = false

    @Synchronized
    fun retain(): AutoCloseable {
        check(!closed) { "Document file already closed" }
        users++
        val released = AtomicBoolean()
        return AutoCloseable { if (released.compareAndSet(false, true)) release() }
    }

    override fun close() {
        synchronized(this) {
            if (closed) return
            closed = true
        }
        release()
    }

    private fun release() {
        val delete = synchronized(this) { check(users > 0); --users == 0 }
        if (delete) {
            try { Files.deleteIfExists(path) }
            catch (error: Exception) { log.warn("Could not delete temporary document: {}", path, error) }
        }
    }

    private companion object { val log = LoggerFactory.getLogger(TemporaryDocumentFile::class.java) }
}
