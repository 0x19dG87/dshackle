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
package io.emeraldpay.dshackle.cache

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import java.util.concurrent.ConcurrentHashMap

/**
 * Centralized cache metrics for tracking cache hits and misses.
 *
 * Cached data types:
 * - blocks: eth_getBlockByHash
 * - tx: eth_getTransactionByHash
 * - receipts: eth_getTransactionReceipt
 * - height: eth_getBlockByNumber (height->hash lookup)
 * - block_not_found: Block not found error cache
 */
object CacheMetrics {
    private val counters = ConcurrentHashMap<String, Counter>()

    /**
     * Record a cache hit.
     *
     * @param cacheType The type of cache (blocks, tx, receipts, height, block_not_found)
     * @param chain The chain code
     */
    fun recordHit(cacheType: String, chain: String) {
        getCounter(cacheType, chain, "hit").increment()
    }

    /**
     * Record a cache miss.
     *
     * @param cacheType The type of cache (blocks, tx, receipts, height, block_not_found)
     * @param chain The chain code
     */
    fun recordMiss(cacheType: String, chain: String) {
        getCounter(cacheType, chain, "miss").increment()
    }

    private fun getCounter(cache: String, chain: String, result: String): Counter {
        val key = "$cache:$chain:$result"
        return counters.computeIfAbsent(key) {
            Counter.builder("cache.requests")
                .description("Number of cache requests by type, chain, and result")
                .tags(
                    "cache",
                    cache,
                    "chain",
                    chain,
                    "result",
                    result,
                )
                .register(Metrics.globalRegistry)
        }
    }
}
