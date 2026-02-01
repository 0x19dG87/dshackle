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
package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.Global
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import java.util.concurrent.ConcurrentHashMap

/**
 * Centralized upstream metrics for failures, selections, and failovers.
 * All metrics are only recorded when Global.metricsExtended is enabled.
 */
object UpstreamMetrics {
    private val failureCounters = ConcurrentHashMap<String, Counter>()
    private val selectionCounters = ConcurrentHashMap<String, Counter>()
    private val failoverCounters = ConcurrentHashMap<String, Counter>()

    /**
     * Record an upstream RPC failure with method and error code details.
     *
     * @param chain The chain code (e.g., "ethereum", "polygon")
     * @param upstream The upstream identifier
     * @param method The RPC method that failed (e.g., "eth_call", "eth_getLogs")
     * @param errorCode The JSON-RPC error code, or null if unknown
     */
    fun recordFailure(chain: String, upstream: String, method: String, errorCode: Int?) {
        if (!Global.metricsExtended) return
        val key = "$chain:$upstream:$method:${errorCode ?: "unknown"}"
        failureCounters.computeIfAbsent(key) {
            Counter.builder("upstream.rpc.fail")
                .description("Number of upstream RPC failures by method and error code")
                .tags(
                    "chain", chain,
                    "upstream", upstream,
                    "method", method,
                    "error_code", errorCode?.toString() ?: "unknown",
                )
                .register(Metrics.globalRegistry)
        }.increment()
    }

    /**
     * Record an upstream selection event.
     *
     * @param chain The chain code
     * @param upstream The selected upstream identifier
     * @param role The role of the upstream (primary, secondary, fallback)
     * @param method The RPC method being called
     */
    fun recordSelection(chain: String, upstream: String, role: String, method: String) {
        if (!Global.metricsExtended) return
        val key = "$chain:$upstream:$role:$method"
        selectionCounters.computeIfAbsent(key) {
            Counter.builder("upstream.selected")
                .description("Number of times an upstream was selected for a request")
                .tags(
                    "chain", chain,
                    "upstream", upstream,
                    "role", role,
                    "method", method,
                )
                .register(Metrics.globalRegistry)
        }.increment()
    }

    /**
     * Record an upstream failover event.
     *
     * @param chain The chain code
     * @param failedUpstream The upstream that failed
     * @param nextUpstream The upstream that will be tried next
     * @param method The RPC method being called
     */
    fun recordFailover(chain: String, failedUpstream: String, nextUpstream: String, method: String) {
        if (!Global.metricsExtended) return
        val key = "$chain:$failedUpstream:$nextUpstream:$method"
        failoverCounters.computeIfAbsent(key) {
            Counter.builder("upstream.failover")
                .description("Number of failovers from one upstream to another")
                .tags(
                    "chain", chain,
                    "failed_upstream", failedUpstream,
                    "next_upstream", nextUpstream,
                    "method", method,
                )
                .register(Metrics.globalRegistry)
        }.increment()
    }
}
