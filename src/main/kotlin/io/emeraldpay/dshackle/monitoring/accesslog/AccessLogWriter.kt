/**
 * Copyright (c) 2021 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.AccessLogConfig
import io.emeraldpay.dshackle.config.MainConfig
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Repository
class AccessLogWriter(
    @Autowired private val mainConfig: MainConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogWriter::class.java)
        private const val WRITE_BATCH_LIMIT = 5000
        private const val FLUSH_SLEEP_MS = 500L
        private const val START_SLEEP_MS = 2000L
        private val NL = "\n".toByteArray()
        private val ROTATE_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss").withZone(ZoneId.of("UTC"))
    }

    private val config = mainConfig.accessLogConfig
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private val objectMapper = Global.objectMapper
    private var lastErrorAt: Instant = Instant.ofEpochMilli(0)

    internal val targets = ArrayList<LogTarget>()

    private val runner = Runnable {
        flushRunner()
    }

    internal class LogTarget(
        val file: File,
        val errorsOnly: Boolean,
        val includeMessages: Boolean,
        val maxFileSize: Long?,
        val matcher: (Any) -> Boolean,
    ) {
        val queue = ConcurrentLinkedQueue<Any>()

        fun shouldLog(event: Any): Boolean {
            if (!matcher(event)) return false
            if (!errorsOnly) return true
            return event is Events.NativeCall && !event.succeed
        }
    }

    @PostConstruct
    fun start() {
        if (!config.enabled) {
            log.info("Access Log is disabled")
            return
        }

        // Global target
        config.filename?.let { filename ->
            val file = File(filename)
            val mode = if (config.errorsOnly) " (errors-only mode)" else ""
            log.info("Writing Access Log to ${file.absolutePath}$mode")
            targets.add(LogTarget(file, config.errorsOnly, config.includeMessages, config.fileSize) { true })
        }

        // Per-chain targets
        config.chainTargets.filter { it.enabled }.forEach { chainTarget ->
            val file = File(chainTarget.filename)
            val chain = chainTarget.chain
            log.info("Writing Access Log for chain $chain to ${file.absolutePath}")
            targets.add(
                LogTarget(file, chainTarget.errorsOnly, chainTarget.includeMessages, chainTarget.fileSize) { event ->
                    event is Events.ChainBase && event.blockchain == chain
                },
            )
        }

        // Per-upstream targets
        mainConfig.upstreams?.upstreams?.forEach { upstream ->
            val accessLog = upstream.accessLog ?: return@forEach
            if (!accessLog.enabled) return@forEach
            val upstreamId = upstream.id ?: return@forEach
            val file = File(accessLog.filename)
            log.info("Writing Access Log for upstream $upstreamId to ${file.absolutePath}")
            targets.add(
                LogTarget(file, accessLog.errorsOnly, accessLog.includeMessages, accessLog.fileSize) { event ->
                    event is Events.NativeCall && event.upstreamId?.split(",")?.any { it.trim() == upstreamId } == true
                },
            )
        }

        if (targets.isEmpty()) {
            log.info("Access Log is enabled but no targets are configured")
            return
        }

        rotateExistingLogs()
        scheduler.schedule(runner, START_SLEEP_MS, TimeUnit.MILLISECONDS)

        // Compute effective includeMessages: true if global or any enabled target needs it
        val anyTargetNeedsMessages = config.chainTargets.any { it.enabled && it.includeMessages } ||
            (mainConfig.upstreams?.upstreams?.any { it.accessLog?.let { al -> al.enabled && al.includeMessages } == true } ?: false)
        val effectiveIncludeMessages = config.includeMessages || anyTargetNeedsMessages
        if (effectiveIncludeMessages != config.includeMessages) {
            // Create a new config with includeMessages=true so EventsBuilder captures payloads
            val effectiveConfig = AccessLogConfig(config.enabled, true, config.errorsOnly)
            effectiveConfig.filename = config.filename
            effectiveConfig.chainTargets = config.chainTargets
            EventsBuilder.accessLogConfig = effectiveConfig
        } else {
            EventsBuilder.accessLogConfig = config
        }
    }

    private fun rotateExistingLogs() {
        val rotatedFiles = HashSet<String>()
        targets.forEach { target ->
            val path = target.file.absolutePath
            if (target.file.exists() && target.file.length() > 0 && rotatedFiles.add(path)) {
                rotateFile(target.file)
            }
        }
    }

    private fun rotateFile(file: File) {
        val ts = ROTATE_TS_FORMAT.format(Instant.now())
        val name = file.name
        val dotIdx = name.lastIndexOf('.')
        val backupName = if (dotIdx > 0) {
            name.substring(0, dotIdx) + ".$ts" + name.substring(dotIdx)
        } else {
            "$name.$ts"
        }
        val parent = file.absoluteFile.parentFile
        val backup = File(parent, backupName)
        try {
            Files.move(file.toPath(), backup.toPath(), StandardCopyOption.ATOMIC_MOVE)
            log.info("Rotated ${file.absolutePath} -> ${backup.name}")
        } catch (e: Exception) {
            log.warn("Failed to rotate ${file.absolutePath} -> ${backup.absolutePath}: ${e.message}")
        }
    }

    private fun flushRunner() {
        try {
            flush()
        } catch (t: Throwable) {
            logError {
                log.error("Failed to write logs. ${t.javaClass}:${t.message}")
            }
        } finally {
            scheduler.schedule(runner, FLUSH_SLEEP_MS, TimeUnit.MILLISECONDS)
        }
    }

    fun submit(event: Any) {
        targets.forEach { target ->
            if (target.shouldLog(event)) {
                target.queue.add(event)
            }
        }
    }

    fun submit(events: List<Any>) {
        events.forEach { event ->
            submit(event)
        }
    }

    fun logError(m: () -> Unit) {
        val now = Instant.now()
        if (lastErrorAt.isBefore(now - Duration.ofMinutes(1))) {
            lastErrorAt = now
            m()
        }
    }

    protected fun flush() {
        targets.forEach { target ->
            flushTarget(target)
        }
    }

    private fun flushTarget(target: LogTarget) {
        if (target.queue.isEmpty()) return
        if (!target.file.exists()) {
            target.file.parentFile?.mkdirs()
            if (!target.file.createNewFile()) {
                logError {
                    log.error("Cannot create Access Log file at ${target.file.absolutePath}")
                }
                return
            }
        }
        BufferedOutputStream(FileOutputStream(target.file, true)).use { wrt ->
            var limit = WRITE_BATCH_LIMIT
            while (limit > 0) {
                limit -= 1
                val next = target.queue.poll() ?: break
                val bytes: ByteArray? = try {
                    if (!target.includeMessages && next is Events.NativeCall) {
                        val tree = objectMapper.valueToTree<ObjectNode>(next)
                        tree.remove("responseBody")
                        tree.remove("errorMessage")
                        (tree.get("nativeCall") as? ObjectNode)?.remove("requestParams")
                        objectMapper.writeValueAsBytes(tree)
                    } else if (!target.includeMessages && next is Events.NativeSubscribe) {
                        val tree = objectMapper.valueToTree<ObjectNode>(next)
                        tree.remove("responseBody")
                        objectMapper.writeValueAsBytes(tree)
                    } else {
                        objectMapper.writeValueAsBytes(next)
                    }
                } catch (t: Throwable) {
                    logError {
                        log.warn("Failed to write an access log line. ${t.message}")
                    }
                    null
                }
                if (bytes != null && bytes.isNotEmpty()) {
                    wrt.write(bytes)
                    wrt.write(NL, 0, 1)
                }
            }
        }

        // Rotate if file exceeds configured size
        val maxSize = target.maxFileSize
        if (maxSize != null && target.file.exists() && target.file.length() > maxSize) {
            rotateFile(target.file)
        }
    }
}
