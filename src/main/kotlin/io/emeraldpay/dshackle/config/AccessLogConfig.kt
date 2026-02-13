package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain

class AccessLogConfig(
    val enabled: Boolean = false,
    val includeMessages: Boolean = false,
    val errorsOnly: Boolean = false,
) {

    var filename: String? = "./access_log.jsonl"
    var fileSize: Long? = null
    var chainTargets: List<ChainLogTarget> = emptyList()

    data class ChainLogTarget(
        val enabled: Boolean,
        val chain: Chain,
        val filename: String,
        val includeMessages: Boolean,
        val errorsOnly: Boolean,
        val fileSize: Long?,
    )

    companion object {

        fun default(): AccessLogConfig {
            return disabled()
        }

        fun disabled(): AccessLogConfig {
            return AccessLogConfig(
                enabled = false,
            )
        }
    }
}
