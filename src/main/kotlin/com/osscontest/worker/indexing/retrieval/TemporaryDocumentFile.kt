/*
 * Copyright (c) 2026 매쉬업스프링YB
 * SPDX-License-Identifier: MIT
 */

package com.osscontest.worker.indexing.retrieval

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/** 파이프라인이 닫고 이 파일을 쓰는 모든 파서가 실제로 멈춘 뒤에 삭제한다. */
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
