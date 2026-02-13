package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode

class AccessLogReader : YamlConfigReader<AccessLogConfig>() {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogReader::class.java)
        private val FILE_SIZE_RE = Regex("^(\\d+)\\s*(b|kb|k|mb|m|gb|g)?$", RegexOption.IGNORE_CASE)

        @JvmStatic
        fun parseFileSize(value: String): Long {
            val m = FILE_SIZE_RE.find(value.trim())
                ?: throw IllegalArgumentException("Not a valid file size: $value. Examples: '100mb', '1gb', '500kb'")
            val base = m.groups[1]!!.value.toLong()
            val multiplier = when (m.groups[2]?.value?.lowercase()) {
                "k", "kb" -> 1024L
                "m", "mb" -> 1024L * 1024
                "g", "gb" -> 1024L * 1024 * 1024
                else -> 1L
            }
            return base * multiplier
        }
    }

    override fun read(input: MappingNode?): AccessLogConfig {
        return getMapping(input, "accessLog")?.let { node ->
            val enabled = getValueAsBool(node, "enabled") ?: false
            if (!enabled) {
                AccessLogConfig.disabled()
            } else {
                val includeMessages = getValueAsBool(node, "include-messages") ?: false
                val errorsOnly = getValueAsBool(node, "errors-only") ?: false
                val globalFileSize = getValueAsString(node, "filesize")?.let { parseFileSize(it) }
                val config = AccessLogConfig(true, includeMessages, errorsOnly)
                config.filename = getValueAsString(node, "filename")
                config.fileSize = globalFileSize
                config.chainTargets = readChainTargets(node, includeMessages, errorsOnly, globalFileSize)
                config
            }
        } ?: AccessLogConfig.default()
    }

    private fun readChainTargets(
        node: MappingNode,
        globalIncludeMessages: Boolean,
        globalErrorsOnly: Boolean,
        globalFileSize: Long?,
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
            val fileSize = getValueAsString(conf, "filesize")?.let { parseFileSize(it) } ?: globalFileSize
            AccessLogConfig.ChainLogTarget(
                enabled = enabled,
                chain = chain,
                filename = filename,
                includeMessages = includeMessages,
                errorsOnly = errorsOnly,
                fileSize = fileSize,
            )
        } ?: emptyList()
    }
}
