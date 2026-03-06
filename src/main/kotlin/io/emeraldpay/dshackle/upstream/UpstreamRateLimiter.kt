package io.emeraldpay.dshackle.upstream

import java.util.concurrent.atomic.AtomicLong

class UpstreamRateLimiter(private val maxRps: Int) {

    // Packs (epochSecond << 32 | count) into a single AtomicLong for lock-free CAS updates.
    // epochSecond fits in 32 unsigned bits until year 2106.
    private val state = AtomicLong(0L)

    fun tryAcquire(): Boolean {
        val nowSec = System.currentTimeMillis() / 1000
        while (true) {
            val current = state.get()
            val windowSec = current ushr 32
            val count = current and 0xFFFFFFFFL
            if (windowSec != nowSec) {
                // New second: reset window and grant this request
                if (state.compareAndSet(current, (nowSec shl 32) or 1L)) return true
                // CAS lost — another thread updated concurrently, retry
            } else if (count < maxRps) {
                if (state.compareAndSet(current, (windowSec shl 32) or (count + 1L))) return true
            } else {
                return false
            }
        }
    }

    companion object {
        fun create(maxRps: Int?): UpstreamRateLimiter? =
            if (maxRps != null && maxRps > 0) UpstreamRateLimiter(maxRps) else null
    }
}
