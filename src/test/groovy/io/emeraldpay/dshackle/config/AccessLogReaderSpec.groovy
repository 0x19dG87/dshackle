package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import spock.lang.Specification

class AccessLogReaderSpec extends Specification {

    AccessLogReader reader = new AccessLogReader()

    def "Read legacy config (backward compat)"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-legacy.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.filename == "/var/log/dshackle/access_log.jsonl"
        !act.includeMessages
        !act.errorsOnly
        act.chainTargets.size() == 0
    }

    def "Read disabled config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-disabled.yaml")
        when:
        def act = reader.read(config)

        then:
        !act.enabled
    }

    def "Read config with chains"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-chains.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.filename == "/var/log/dshackle/access_log.jsonl"
        !act.includeMessages
        act.chainTargets.size() == 2

        with(act.chainTargets[0]) {
            it.chain == Chain.ETHEREUM__MAINNET
            it.enabled
            it.filename == "/var/log/dshackle/ethereum.jsonl"
            it.includeMessages
            !it.errorsOnly
        }

        with(act.chainTargets[1]) {
            it.chain == Chain.BITCOIN__MAINNET
            !it.enabled
            it.filename == "/var/log/dshackle/bitcoin.jsonl"
            !it.includeMessages
            it.errorsOnly
        }
    }

    def "Read config with no global filename"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-no-global.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.filename == null
        act.chainTargets.size() == 1

        with(act.chainTargets[0]) {
            it.chain == Chain.ETHEREUM__MAINNET
            it.enabled
            it.filename == "/var/log/dshackle/ethereum.jsonl"
        }
    }

    def "Chain target defaults to global include-messages value"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-chains.yaml")
        when:
        def act = reader.read(config)

        then:
        // Bitcoin chain target has no explicit include-messages, should default to global (false)
        !act.chainTargets[1].includeMessages
    }

    def "Chain target defaults to global errors-only value"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-chains.yaml")
        when:
        def act = reader.read(config)

        then:
        // Ethereum chain target has no explicit errors-only, should default to global (false)
        !act.chainTargets[0].errorsOnly
    }

    def "Read config with file-size"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-filesize.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.fileSize == 100L * 1024 * 1024

        // Ethereum has explicit file-size
        act.chainTargets[0].fileSize == 50L * 1024 * 1024

        // Bitcoin inherits global file-size
        act.chainTargets[1].fileSize == 100L * 1024 * 1024
    }

    def "file-size is null when not specified"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-accesslog-legacy.yaml")
        when:
        def act = reader.read(config)

        then:
        act.fileSize == null
    }

    def "parseFileSize handles various formats"() {
        expect:
        AccessLogReader.parseFileSize("100") == 100L
        AccessLogReader.parseFileSize("100b") == 100L
        AccessLogReader.parseFileSize("100kb") == 100L * 1024
        AccessLogReader.parseFileSize("100k") == 100L * 1024
        AccessLogReader.parseFileSize("100mb") == 100L * 1024 * 1024
        AccessLogReader.parseFileSize("100m") == 100L * 1024 * 1024
        AccessLogReader.parseFileSize("1gb") == 1024L * 1024 * 1024
        AccessLogReader.parseFileSize("1g") == 1024L * 1024 * 1024
        AccessLogReader.parseFileSize("100MB") == 100L * 1024 * 1024
    }
}
