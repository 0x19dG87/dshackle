/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.reactivestreams.Subscriber
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.EnumMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class FilteredApis(
    val chain: Chain,
    private val allUpstreams: List<Upstream>,
    matcher: Selector.Matcher,
    private val pos: Int,
    private val retries: Int,
    sort: Selector.Sort = Selector.Sort.default,
) : ApiSource {
    private val internalMatcher: Selector.Matcher

    companion object {
        private val log = LoggerFactory.getLogger(FilteredApis::class.java)

        private const val DEFAULT_RETRY_LIMIT = 3

        private const val metricsCode = "select"

        // Throttle rate-limit log messages to at most once per second per upstream across all threads
        private val lastRateLimitLogTime = ConcurrentHashMap<String, Long>()
        private const val RATE_LIMIT_LOG_INTERVAL_MS = 1_000L

        private fun shouldLogRateLimit(upstreamId: String): Boolean {
            val now = System.currentTimeMillis()
            // compute() is atomic per key — only the first thread in each interval updates and returns `now`
            return lastRateLimitLogTime.compute(upstreamId) { _, last ->
                if (last == null || now - last >= RATE_LIMIT_LOG_INTERVAL_MS) now else last
            } == now
        }

        @JvmStatic
        fun <T> startFrom(upstreams: List<T>, pos: Int): List<T> {
            return if (upstreams.size <= 1 || pos == 0) {
                upstreams
            } else {
                val safePosition = pos % upstreams.size
                upstreams.subList(safePosition, upstreams.size) + upstreams.subList(0, safePosition)
            }
        }

        private val metrics = EnumMap<Chain, Monitoring>(Chain::class.java)
        private val metricsSetup: Lock = ReentrantLock()
    }

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        matcher: Selector.Matcher,
        pos: Int,
    ) : this(chain, allUpstreams, matcher, pos, DEFAULT_RETRY_LIMIT)

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        upstreamFilter: Selector.UpstreamFilter,
        pos: Int,
    ) : this(chain, allUpstreams, upstreamFilter.matcher, pos, DEFAULT_RETRY_LIMIT, upstreamFilter.sort)

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        matcher: Selector.Matcher,
    ) : this(chain, allUpstreams, matcher, 0, DEFAULT_RETRY_LIMIT)

    private val primaryUpstreams: List<Upstream> = allUpstreams.filter {
        it.getRole() == UpstreamsConfig.UpstreamRole.PRIMARY
    }.let {
        startFrom(it, pos)
    }.sortedWith(sort.comparator)
    private val secondaryUpstreams: List<Upstream> = allUpstreams.filter {
        it.getRole() == UpstreamsConfig.UpstreamRole.SECONDARY
    }.let {
        startFrom(it, pos)
    }.sortedWith(sort.comparator)
    private val fallbackUpstreams: List<Upstream>
    private val standardWithFallback: List<Upstream>

    private val counter: AtomicInteger = AtomicInteger(0)

    private var started = false
    private val control = Sinks.many().unicast().onBackpressureBuffer<Boolean>()
    private var upstreamsMatchesResponse: UpstreamsMatchesResponse? = UpstreamsMatchesResponse()

    // Track upstreams that have been tried to prioritize untried ones in retries
    private val triedUpstreams = mutableSetOf<String>()

    // Track upstreams confirmed rate-limited in this subscription to skip them silently on retries
    private val rateLimitedUpstreams = mutableSetOf<String>()
    private val syncRateLimitedUpstreams = mutableSetOf<String>()
    private val laggingRateLimitedUpstreams = mutableSetOf<String>()

    // Track providers that have failed to skip other upstreams from the same provider
    private val failedProviders = mutableSetOf<String>()

    init {
        fallbackUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.FALLBACK
        }.let {
            startFrom(it, pos)
        }.sortedWith(sort.comparator)
        standardWithFallback = emptyList<Upstream>()
            .plus(primaryUpstreams)
            .plus(secondaryUpstreams)
            .plus(fallbackUpstreams)

        if (Global.metricsExtended) {
            getMetrics(chain).let { monitoring ->
                monitoring.countPrimary.record(primaryUpstreams.size.toDouble())
                monitoring.countSecondary.record(secondaryUpstreams.size.toDouble())
                monitoring.countFallback.record(fallbackUpstreams.size.toDouble())
            }
        }
        internalMatcher = Selector.MultiMatcher(
            listOf(Selector.AvailabilityMatcher(), matcher),
        )
    }

    private fun getMetrics(chain: Chain): Monitoring {
        val existing = metrics[chain]
        return if (existing == null) {
            metricsSetup.withLock {
                val existingDoubleCheck = metrics[chain]
                if (existingDoubleCheck != null) {
                    existingDoubleCheck
                } else {
                    val created = Monitoring(chain)
                    metrics[chain] = created
                    created
                }
            }
        } else {
            existing
        }
    }

    override fun subscribe(subscriber: Subscriber<in Upstream>) {
        // Phase 1: try PRIMARY upstreams
        val first = Flux.fromIterable(primaryUpstreams.sortedBy { it.getStatus().grpcId })
        // Phase 2: try SECONDARY upstreams
        val second = Flux.fromIterable(secondaryUpstreams.sortedBy { it.getStatus().grpcId })
        // Phase 3: try FALLBACK upstreams (before any retries)
        val third = Flux.fromIterable(fallbackUpstreams.sortedBy { it.getStatus().grpcId })
        // Phase 4: retries - try all upstreams again, but filter will skip already-tried ones
        // until all unique upstreams have been tried at least once
        val retries = (0 until this.retries).map {
            Flux.fromIterable(standardWithFallback.sortedBy { up -> up.getStatus().grpcId })
        }.let { Flux.concat(it) }

        val size = primaryUpstreams.size + secondaryUpstreams.size + fallbackUpstreams.size + standardWithFallback.size * this.retries
        var result = Flux.concat(first, second, third, retries).take(size.toLong(), false)

        if (Global.metricsExtended) {
            var count = 0
            result = result
                .doOnNext { count++ }
                .doFinally { metrics[chain]?.tried?.record(count.toDouble()) }
        }

        val totalUniqueUpstreams = standardWithFallback.size

        control.asFlux()
            .zipWith(result)
            .map { it.t2 }
            .filter { up ->
                val upstreamId = up.getId()

                // Skip rate-limited upstreams.
                // Once an upstream is found rate-limited within this subscription, record it and skip
                // it silently on subsequent retry rounds to avoid log spam and redundant tryAcquire calls.
                val rateLimiter = up.getRateLimiter()
                if (rateLimiter != null) {
                    if (rateLimitedUpstreams.contains(upstreamId)) {
                        this.request(1)
                        return@filter false
                    }
                    if (!rateLimiter.tryAcquire()) {
                        rateLimitedUpstreams.add(upstreamId)
                        if (shouldLogRateLimit(upstreamId)) {
                            log.debug("Upstream [$upstreamId] is rate-limited, skipping")
                        }
                        this.request(1)
                        return@filter false
                    }
                }

                val status = up.getStatus()

                // SYNCING rate limiter
                if (status == UpstreamAvailability.SYNCING) {
                    val syncRateLimiter = up.getSyncRateLimiter()
                    if (syncRateLimiter != null) {
                        if (syncRateLimitedUpstreams.contains(upstreamId)) {
                            this.request(1)
                            return@filter false
                        }
                        if (!syncRateLimiter.tryAcquire()) {
                            syncRateLimitedUpstreams.add(upstreamId)
                            if (shouldLogRateLimit(upstreamId)) {
                                log.debug("Upstream [$upstreamId] is sync-rate-limited, skipping")
                            }
                            this.request(1)
                            return@filter false
                        }
                    }
                }

                // LAGGING rate limiter
                if (status == UpstreamAvailability.LAGGING) {
                    val laggingRateLimiter = up.getLaggingRateLimiter()
                    if (laggingRateLimiter != null) {
                        if (laggingRateLimitedUpstreams.contains(upstreamId)) {
                            this.request(1)
                            return@filter false
                        }
                        if (!laggingRateLimiter.tryAcquire()) {
                            laggingRateLimitedUpstreams.add(upstreamId)
                            if (shouldLogRateLimit(upstreamId)) {
                                log.debug("Upstream [$upstreamId] is lagging-rate-limited, skipping")
                            }
                            this.request(1)
                            return@filter false
                        }
                    }
                }

                // Skip upstream if its provider has already failed
                val provider = up.getLabels().firstOrNull()?.get("provider")
                if (provider != null && failedProviders.contains(provider)) {
                    this.request(1)
                    return@filter false
                }

                val alreadyTried = triedUpstreams.contains(upstreamId)
                val allUpstreamsTried = triedUpstreams.size >= totalUniqueUpstreams

                // Skip this upstream if:
                // - It was already tried AND
                // - There are still untried upstreams available
                if (alreadyTried && !allUpstreamsTried) {
                    this.request(1)
                    return@filter false
                }

                // Mark as tried before checking matcher
                triedUpstreams.add(upstreamId)

                val matchesResponse = internalMatcher.matchesWithCause(up)
                processMatchesResponse(upstreamId, matchesResponse)
                matchesResponse.matched()
                    .also {
                        if (!it) {
                            this.request(1)
                        }
                    }
            }
            .doOnNext {
                upstreamsMatchesResponse = null
                counter.incrementAndGet()
            }
            .doOnSubscribe {
                if (!started) {
                    // in addition to subscription the FilteredAPI should use request() method to prepare the control flow
                    log.warn("API Source subscribed before preparing a request")
                }
            }
            .subscribe(subscriber)
    }

    private fun processMatchesResponse(upstreamId: String, matchesResponse: MatchesResponse) {
        upstreamsMatchesResponse?.run {
            if (!matchesResponse.matched()) {
                addUpstreamMatchesResponse(upstreamId, matchesResponse)
            }
        }
    }

    override fun resolve() {
        control.tryEmitComplete()
    }

    override fun reportFailure(upstreamId: String) {
        allUpstreams.find { it.getId() == upstreamId }
            ?.getLabels()?.firstOrNull()?.get("provider")
            ?.let { failedProviders.add(it) }
    }

    override fun request(tries: Int) {
        started = true
        // TODO check the buffer size before submitting
        repeat(tries) {
            control.tryEmitNext(true)
        }
    }

    override fun attempts(): AtomicInteger =
        counter

    override fun upstreamsMatchesResponse(): UpstreamsMatchesResponse? = upstreamsMatchesResponse

    override fun toString(): String {
        return "Filter API: ${allUpstreams.size} upstreams with $internalMatcher"
    }

    class Monitoring(chain: Chain) {
        val countPrimary: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "primary")))
            .register(Metrics.globalRegistry)
        val countSecondary: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "secondary")))
            .register(Metrics.globalRegistry)
        val countFallback: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available fallback upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "fallback")))
            .register(Metrics.globalRegistry)
        val tried: DistributionSummary = DistributionSummary.builder("$metricsCode.tried")
            .description("How many upstreams were checked")
            .tags(listOf(Tag.of("chain", chain.chainCode)))
            .register(Metrics.globalRegistry)
    }
}
