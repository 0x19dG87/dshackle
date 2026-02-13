package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode

class AccessLogReader : YamlConfigReader<AccessLogConfig>() {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogReader::class.java)
    }

    override fun read(input: MappingNode?): AccessLogConfig {
        return getMapping(input, "accessLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                AccessLogConfig.disabled()
            } else {
                val includeMessages = getValueAsBool(node, "include-messages") ?: false
                val errorsOnly = getValueAsBool(node, "errors-only") ?: false
                val config = AccessLogConfig(true, includeMessages, errorsOnly)
                config.filename = getValueAsString(node, "filename")
                config.chainTargets = readChainTargets(node, includeMessages, errorsOnly)
                config
            }
        } ?: AccessLogConfig.default()
    }

    private fun readChainTargets(
        node: MappingNode,
        globalIncludeMessages: Boolean,
        globalErrorsOnly: Boolean,
    ): List<AccessLogConfig.ChainLogTarget> {
        return getList<MappingNode>(node, "chains")?.value?.mapNotNull { conf ->
            val chainName = getValueAsString(conf, "chain")
            if (chainName == null) {
                log.warn("Chain is not specified for an access log chain target")
                return@mapNotNull null
            }
            val chain = Global.chainById(chainName)
            if (chain == io.emeraldpay.dshackle.Chain.UNSPECIFIED) {
                log.warn("Unknown chain '$chainName' in access log chain target")
                return@mapNotNull null
            }
            val filename = getValueAsString(conf, "filename")
            if (filename == null) {
                log.warn("Filename is not specified for access log chain target '$chainName'")
                return@mapNotNull null
            }
            val enabled = getValueAsBool(conf, "enabled") ?: true
            val includeMessages = getValueAsBool(conf, "include-messages") ?: globalIncludeMessages
            val errorsOnly = getValueAsBool(conf, "errors-only") ?: globalErrorsOnly
            AccessLogConfig.ChainLogTarget(
                enabled = enabled,
                chain = chain,
                filename = filename,
                includeMessages = includeMessages,
                errorsOnly = errorsOnly,
            )
        } ?: emptyList()
    }
}
