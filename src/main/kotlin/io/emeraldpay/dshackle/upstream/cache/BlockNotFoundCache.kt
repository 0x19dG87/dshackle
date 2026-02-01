/**
 * Copyright (c) 2025 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.cache

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.upstream.rpcclient.CallParams
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.rpcclient.ObjectParams
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import java.time.Duration

/**
 * Cache for tracking "block not found" errors per upstream.
 * Prevents repeated requests to upstreams that recently failed for a specific block hash.
 */
object BlockNotFoundCache {

    private val cache = Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterWrite(Duration.ofSeconds(5))
        .build<String, Boolean>()

    private val cacheHits: Counter = Counter.builder("block.not.found.cache.hits")
        .description("Number of block-not-found cache hits (upstream skipped)")
        .register(Metrics.globalRegistry)

    private val cacheMisses: Counter = Counter.builder("block.not.found.cache.misses")
        .description("Number of block-not-found cache misses (upstream checked)")
        .register(Metrics.globalRegistry)

    /**
     * Record that an upstream doesn't have a specific block.
     */
    fun recordBlockNotFound(upstreamId: String, blockHash: String) {
        cache.put(cacheKey(upstreamId, blockHash), true)
    }

    /**
     * Check if an upstream recently reported "block not found" for a specific block.
     */
    fun hasRecentFailure(upstreamId: String, blockHash: String): Boolean {
        val hasFailure = cache.getIfPresent(cacheKey(upstreamId, blockHash)) != null
        if (hasFailure) {
            cacheHits.increment()
        } else {
            cacheMisses.increment()
        }
        return hasFailure
    }

    private fun cacheKey(upstreamId: String, blockHash: String): String {
        return "$upstreamId:$blockHash"
    }

    /**
     * Extract block hash from error message if present.
     * Matches patterns like "block not found: hash 0x..."
     */
    fun extractBlockHashFromError(errorMessage: String?): String? {
        if (errorMessage == null) return null
        if (!errorMessage.contains("block not found", ignoreCase = true)) return null

        // Look for a 66-character hex hash (0x + 64 chars)
        val hashRegex = Regex("0x[a-fA-F0-9]{64}")
        return hashRegex.find(errorMessage)?.value?.lowercase()
    }

    /**
     * Extract block hash from CallParams.
     */
    fun extractBlockHashFromCallParams(params: CallParams?): String? {
        return when (params) {
            is ListParams -> extractBlockHashFromList(params.list)
            is ObjectParams -> extractBlockHashFromMap(params.obj)
            else -> null
        }
    }

    private fun extractBlockHashFromList(list: List<Any>): String? {
        if (list.isEmpty()) return null

        val firstParam = list[0]
        // Check if first param is a block hash (66 chars: 0x + 64 hex)
        if (firstParam is String && isBlockHash(firstParam)) {
            return firstParam.lowercase()
        }
        // Check if it's an object with blockHash field
        if (firstParam is Map<*, *>) {
            extractBlockHashFromMap(firstParam)?.let { return it }
        }
        // Check second param (for eth_call, the block reference is second param)
        if (list.size > 1) {
            val secondParam = list[1]
            if (secondParam is String && isBlockHash(secondParam)) {
                return secondParam.lowercase()
            }
            if (secondParam is Map<*, *>) {
                extractBlockHashFromMap(secondParam)?.let { return it }
            }
        }
        return null
    }

    private fun extractBlockHashFromMap(map: Map<*, *>): String? {
        val blockHash = map["blockHash"]
        if (blockHash is String && isBlockHash(blockHash)) {
            return blockHash.lowercase()
        }
        return null
    }

    private fun isBlockHash(value: String): Boolean {
        return value.length == 66 && value.startsWith("0x")
    }
}
