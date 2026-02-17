/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionReceiptJson
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

open class Caches(
    private val memBlocksByHash: BlocksMemCache,
    private val blocksByHeight: HeightCache,
    private val memTxsByHash: TxMemCache,
    private val memReceipts: ReceiptMemCache,
    private var redisBlocksByHash: BlocksRedisCache?,
    private var redisTxsByHash: TxRedisCache?,
    private var redisReceipts: ReceiptRedisCache?,
    private var redisHeightByHashCache: HeightByHashRedisCache?,
    private val cacheEnabled: Boolean,
    private val chain: Chain = Chain.UNSPECIFIED,
) {

    companion object {
        private val log = LoggerFactory.getLogger(Caches::class.java)

        @JvmStatic
        fun newBuilder(): Builder {
            return Builder()
        }

        @JvmStatic
        fun default(): Caches {
            return newBuilder().build()
        }
    }

    private val memHeightByHash: HeightByHashMemCache = HeightByHashMemCache()

    @Volatile
    private var blocksByHash: Reader<BlockId, BlockContainer>
    @Volatile
    private var txsByHash: Reader<TxId, TxContainer>
    @Volatile
    private var receiptByHash: Reader<TxId, ByteArray>
    private val heightByNumber: Reader<Long, BlockId>

    private var head: Head? = null

    init {
        val chainCode = chain.chainCode
        val initRedisBlocks = redisBlocksByHash
        val baseBlocksByHash = if (initRedisBlocks == null) {
            memBlocksByHash
        } else {
            CompoundReader(memBlocksByHash, initRedisBlocks)
        }
        blocksByHash = MetricsTrackingReader(baseBlocksByHash, "blocks", chainCode)

        val initRedisTx = redisTxsByHash
        val baseTxsByHash = if (initRedisTx == null) {
            memTxsByHash
        } else {
            CompoundReader(memTxsByHash, initRedisTx)
        }
        txsByHash = MetricsTrackingReader(baseTxsByHash, "tx", chainCode)

        val initRedisReceipts = redisReceipts
        val baseReceiptByHash = if (initRedisReceipts == null) {
            memReceipts
        } else {
            CompoundReader(memReceipts, initRedisReceipts)
        }
        receiptByHash = MetricsTrackingReader(baseReceiptByHash, "receipts", chainCode)

        heightByNumber = MetricsTrackingReader(blocksByHeight, "height", chainCode)
    }

    /**
     * Wrapper reader that tracks cache hits and misses for metrics.
     */
    private class MetricsTrackingReader<K, V : Any>(
        private val delegate: Reader<K, V>,
        private val cacheType: String,
        private val chainCode: String,
    ) : Reader<K, V> {
        override fun read(key: K): Mono<V> {
            return delegate.read(key)
                .doOnNext {
                    // Record hit when we get a value
                    CacheMetrics.recordHit(cacheType, chainCode)
                }
                .switchIfEmpty(
                    Mono.defer {
                        // Record miss when the result is empty
                        CacheMetrics.recordMiss(cacheType, chainCode)
                        Mono.empty()
                    },
                )
        }
    }

    fun setHead(head: Head) {
        this.head = head
        redisTxsByHash?.head = head
        redisReceipts?.head = head
    }

    fun upgradeWithRedis(redisConnection: StatefulRedisConnection<String, ByteArray>) {
        val reactive = redisConnection.reactive()
        val chainCode = chain.chainCode

        val newRedisBlocks = BlocksRedisCache(reactive, chain)
        val newRedisTx = TxRedisCache(reactive, chain)
        val newRedisReceipts = ReceiptRedisCache(reactive, chain)
        val newRedisHeight = HeightByHashRedisCache(reactive, chain)

        redisBlocksByHash = newRedisBlocks
        redisTxsByHash = newRedisTx
        redisReceipts = newRedisReceipts
        redisHeightByHashCache = newRedisHeight

        // set head on new redis caches if already available
        head?.let { h ->
            newRedisTx.head = h
            newRedisReceipts.head = h
        }

        // rebuild compound readers so read operations also use Redis
        blocksByHash = MetricsTrackingReader(CompoundReader(memBlocksByHash, newRedisBlocks), "blocks", chainCode)
        txsByHash = MetricsTrackingReader(CompoundReader(memTxsByHash, newRedisTx), "tx", chainCode)
        receiptByHash = MetricsTrackingReader(CompoundReader(memReceipts, newRedisReceipts), "receipts", chainCode)

        log.info("Caches for $chain upgraded with Redis")
    }

    open fun cacheReceipt(tag: Tag, data: DefaultContainer<TransactionReceiptJson>) {
        if (!cacheEnabled) {
            return
        }
        val currentHeight = head?.getCurrentHeight()
        if (currentHeight != null && data.height != null && memReceipts.acceptsRecentBlocks(currentHeight - data.height)) {
            memReceipts.add(data).subscribe()
        }
        // TODO move subscription to the caller
        redisReceipts?.add(data)?.subscribe()
    }

    fun cache(tag: Tag, tx: TxContainer) {
        if (!cacheEnabled) {
            return
        }
        // do not cache transactions that are not in a block yet
        if (tx.blockId == null) {
            return
        }
        memTxsByHash.add(tx)
        // TODO move subscription to the caller
        getBlocksByHash().read(tx.blockId).flatMap { block ->
            redisTxsByHash?.add(tx, block) ?: Mono.empty()
        }.subscribe()
    }

    fun cache(tag: Tag, block: BlockContainer) {
        if (!cacheEnabled) {
            return
        }
        val job = ArrayList<Mono<Void>>()

        redisHeightByHashCache?.add(block)?.let(job::add)

        if (tag == Tag.LATEST) {
            // for LATEST data cache it in memory, it may be short living so better to avoid Redis
            memoizeBlock(block)
        } else if (tag == Tag.REQUESTED) {
            val blockOnlyContainer: BlockContainer?
            var jsonValue: BlockJson<*>? = null
            if (block.full) {
                jsonValue = Global.objectMapper.readValue<BlockJson<*>>(block.json, BlockJson::class.java)
                // shouldn't cache block json with transactions, separate txes and blocks with refs
                val blockOnly = jsonValue.withoutTransactionDetails()
                blockOnlyContainer = BlockContainer.from(blockOnly)
            } else {
                blockOnlyContainer = block
            }
            memoizeBlock(blockOnlyContainer)
            redisBlocksByHash?.add(blockOnlyContainer)?.let(job::add)

            // now cache only transactions
            jsonValue?.let { value ->
                val plainTransactions = value.transactions.filterIsInstance<TransactionJson>()
                if (plainTransactions.isNotEmpty()) {
                    val transactions = plainTransactions.map { tx ->
                        TxContainer.from(tx)
                    }
                    val currentRedisTx = redisTxsByHash
                    if (currentRedisTx != null) {
                        job.add(
                            Flux.fromIterable(transactions)
                                .doOnNext { memTxsByHash.add(it) }
                                .flatMap { currentRedisTx.add(it, block) }
                                .then(),
                        )
                    }
                }
            }
        }
        Flux.fromIterable(job).flatMap { it }.subscribe() // TODO move out to a caller
    }

    /**
     * Cache the block only in memory
     */
    fun memoizeBlock(block: BlockContainer) {
        memBlocksByHash.add(block)
        memHeightByHash.add(block)
        val replaced = blocksByHeight.add(block)
        // evict cached transactions if an existing block was updated
        replaced?.let { evict(it) }
    }

    fun evict(blockId: BlockId) {
        var evicted = false
        redisBlocksByHash?.evict(blockId)
        memBlocksByHash.get(blockId)?.let { block ->
            memTxsByHash.evict(block)
            redisTxsByHash?.evict(block)
            memReceipts.evict(block)
            evicted = true
        }
        if (!evicted) {
            memTxsByHash.evict(blockId)
        }
    }

    fun getBlocksByHash(): Reader<BlockId, BlockContainer> {
        return blocksByHash
    }

    fun getBlockHashByHeight(): Reader<Long, BlockId> {
        return heightByNumber
    }

    fun getBlocksByHeight(): Reader<Long, BlockContainer> {
        return BlockByHeight(heightByNumber, blocksByHash)
    }

    fun getTxByHash(): Reader<TxId, TxContainer> {
        return txsByHash
    }

    fun getReceipts(): Reader<TxId, ByteArray> {
        return receiptByHash
    }

    open fun getLastHeightByHash(): Reader<BlockId, Long> {
        return memHeightByHash
    }

    fun getRedisHeightByHash(): HeightByHashCache? {
        return redisHeightByHashCache
    }

    enum class Tag {
        /**
         * Latest data produced by blockchain
         */
        LATEST,

        /**
         * Data requested by client
         */
        REQUESTED,
    }

    class Builder {
        private var blocksByHash: BlocksMemCache? = null
        private var blocksByHeight: HeightCache? = null
        private var txsByHash: TxMemCache? = null
        private var receipts: ReceiptMemCache? = null
        private var redisBlocksByHash: BlocksRedisCache? = null
        private var redisTxsByHash: TxRedisCache? = null
        private var redisReceiptCache: ReceiptRedisCache? = null
        private var redisHeightByHashCache: HeightByHashRedisCache? = null
        private var cacheEnabled: Boolean = true
        private var chain: Chain = Chain.UNSPECIFIED

        fun setChain(chain: Chain): Builder {
            this.chain = chain
            return this
        }

        fun setBlockByHash(cache: BlocksMemCache): Builder {
            blocksByHash = cache
            return this
        }

        fun setBlockByHash(cache: BlocksRedisCache): Builder {
            redisBlocksByHash = cache
            return this
        }

        fun setBlockByHeight(cache: HeightCache): Builder {
            blocksByHeight = cache
            return this
        }

        fun setTxByHash(cache: TxMemCache): Builder {
            txsByHash = cache
            return this
        }

        fun setTxByHash(cache: TxRedisCache): Builder {
            redisTxsByHash = cache
            return this
        }

        fun setReceipts(cache: ReceiptRedisCache): Builder {
            redisReceiptCache = cache
            return this
        }

        fun setReceipts(cache: ReceiptMemCache): Builder {
            this.receipts = cache
            return this
        }

        fun setHeightByHash(cache: HeightByHashRedisCache): Builder {
            redisHeightByHashCache = cache
            return this
        }

        fun setCacheEnabled(cacheEnabled: Boolean): Builder {
            this.cacheEnabled = cacheEnabled
            return this
        }

        fun build(): Caches {
            if (blocksByHash == null) {
                blocksByHash = BlocksMemCache()
            }
            if (blocksByHeight == null) {
                blocksByHeight = HeightCache()
            }
            if (txsByHash == null) {
                txsByHash = TxMemCache()
            }
            if (receipts == null) {
                receipts = ReceiptMemCache()
            }
            return Caches(
                blocksByHash!!, blocksByHeight!!, txsByHash!!, receipts!!,
                redisBlocksByHash, redisTxsByHash, redisReceiptCache, redisHeightByHashCache, cacheEnabled, chain,
            )
        }
    }
}
