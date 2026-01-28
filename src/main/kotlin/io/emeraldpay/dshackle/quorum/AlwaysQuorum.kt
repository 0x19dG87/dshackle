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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner

open class AlwaysQuorum : CallQuorum {

    private var resolved = false
    private var result: ChainResponse? = null
    private var rpcError: ChainCallError? = null
    private var sig: ResponseSigner.Signature? = null
    private val resolvers = ArrayList<Upstream>()

    override fun isResolved(): Boolean {
        return resolved
    }

    override fun isFailed(): Boolean {
        val error = rpcError ?: return false
        // Stop only for non-retryable errors
        // For retryable errors, let FilteredApis exhaust all upstreams through its finite retry mechanism
        return isNonRetryableError(error)
    }

    private fun isNonRetryableError(error: ChainCallError): Boolean {
        // Check message first - execution reverted can come with various codes
        if (error.message.contains("execution reverted", ignoreCase = true)) {
            return true
        }
        // Non-retryable error codes
        return when (error.code) {
            3 -> true        // EIP-1474: execution reverted
            -32700 -> true   // Parse error
            -32600 -> true   // Invalid request
            -32601 -> true   // Method not found
            -32602 -> true   // Invalid params
            -32003 -> true   // Transaction rejected
            -32004 -> true   // Method not supported
            else -> false    // All others are retryable (-32000, -32001, -32002, -32005, etc.)
        }
    }

    override fun getSignature(): ResponseSigner.Signature? {
        return sig
    }

    override fun record(
        response: ChainResponse,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean {
        result = response
        resolved = true
        sig = signature
        resolvers.add(upstream)
        return true
    }

    override fun record(
        error: ChainException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        this.rpcError = error.error
        sig = signature
        resolvers.add(upstream)
    }

    override fun getResponse(): ChainResponse? {
        return result
    }

    override fun getError(): ChainCallError? {
        return rpcError
    }

    override fun getResolvedBy(): List<Upstream> = resolvers

    override fun toString(): String {
        return "Quorum: Accept Any"
    }
}
