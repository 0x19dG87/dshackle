package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import java.time.Duration

interface HttpFactory {
    fun create(id: String?, chain: Chain, timeout: Duration): HttpReader
}
