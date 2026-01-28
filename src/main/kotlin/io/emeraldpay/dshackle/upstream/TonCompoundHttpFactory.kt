package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.restclient.TonCompoundRestHttpReader
import java.time.Duration

class TonCompoundHttpFactory(
    private val tonHttpFactory: HttpFactory,
    private val tonV3HttpFactory: HttpFactory?,
) : HttpFactory {

    override fun create(id: String?, chain: Chain, timeout: Duration): HttpReader {
        val tonReader = tonHttpFactory.create(id, chain, timeout)
        if (tonV3HttpFactory != null) {
            return TonCompoundRestHttpReader(tonReader, tonV3HttpFactory.create(id, chain, timeout))
        }
        return tonReader
    }
}
