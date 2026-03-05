package io.emeraldpay.dshackle.upstream

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

class UpstreamRateLimiter(private val maxRps: Int) {

    private val window = ConcurrentLinkedDeque<Long>()
    private val size = AtomicInteger(0)

    fun tryAcquire(): Boolean {
        val nowNanos = System.nanoTime()
        val windowNanos = 1_000_000_000L
        synchronized(window) {
            while (true) {
                val oldest = window.peekFirst() ?: break
                if (nowNanos - oldest >= windowNanos) {
                    window.pollFirst(); size.decrementAndGet()
                } else break
            }
            return if (size.get() < maxRps) {
                window.addLast(nowNanos); size.incrementAndGet(); true
            } else false
        }
    }

    companion object {
        fun create(maxRps: Int?): UpstreamRateLimiter? =
            if (maxRps != null && maxRps > 0) UpstreamRateLimiter(maxRps) else null
    }
}
