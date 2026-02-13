package io.emeraldpay.dshackle.monitoring.accesslog


import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.AccessLogConfig
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import spock.lang.Specification

import java.time.Instant

class AccessLogWriterSpec extends Specification {

    def "writes log event"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        println("Write access log to $accessLog.absolutePath")
        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = accessLog.absolutePath
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        def event = new Events.Status(
                Chain.ETHEREUM__MAINNET, UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                new Events.StreamRequestDetails(
                        UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                        Instant.ofEpochMilli(1626746880123),
                        new Events.Remote(
                                ["127.0.0.1", "172.217.8.78"], "172.217.8.78", "UnitTest"
                        )
                )
        )
        logWriter.submit([event])
        logWriter.flush()
        def act = accessLog.readLines()
        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["version"] == "accesslog/v1beta"
            json["id"] == "9d8ecbf3-12fb-49cf-af9d-949a1050a000"
            json["method"] == "Status"
            json["blockchain"] == "ETHEREUM__MAINNET"
            json["request"]["start"] == "2021-07-20T02:08:00.123Z"
            json["request"]["id"] == "9d8ecbf3-12fb-49cf-af9d-949a1050a000"
            json["request"]["remote"]["ip"] == "172.217.8.78"
            json["request"]["remote"]["userAgent"] == "UnitTest"
        }
    }

    private Events.StreamRequestDetails defaultRequestDetails() {
        return new Events.StreamRequestDetails(
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                Instant.ofEpochMilli(1626746880123),
                new Events.Remote(
                        ["127.0.0.1", "172.217.8.78"], "172.217.8.78", "UnitTest"
                )
        )
    }

    private Events.NativeCall makeNativeCall(boolean succeed, String upstreamId = null) {
        return new Events.NativeCall(
                Chain.ETHEREUM__MAINNET,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                Events.Channel.GRPC,
                defaultRequestDetails(),
                1, 0, null, null, null, 100L,
                succeed, null, 256L,
                new Events.NativeCallItemDetails("eth_call", 1, 128L, 1L, null),
                null, null, null, null,
                upstreamId
        )
    }

    private Events.NativeCall makeNativeCallForChain(Chain chain, boolean succeed, String upstreamId = null) {
        return new Events.NativeCall(
                chain,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                Events.Channel.GRPC,
                defaultRequestDetails(),
                1, 0, null, null, null, 100L,
                succeed, null, 256L,
                new Events.NativeCallItemDetails("eth_call", 1, 128L, 1L, null),
                null, null, null, null,
                upstreamId
        )
    }

    private AccessLogWriter createWriter(File accessLog, boolean errorsOnly) {
        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, errorsOnly).tap {
            it.filename = accessLog.absolutePath
        }
        def writer = new AccessLogWriter(config)
        writer.start()
        return writer
    }

    def "errors-only drops successful NativeCall events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, true)

        when:
        logWriter.submit(makeNativeCall(true))
        logWriter.flush()
        def act = accessLog.exists() ? accessLog.readLines() : []

        then:
        act.size() == 0
    }

    def "errors-only keeps failed NativeCall events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, true)

        when:
        logWriter.submit(makeNativeCall(false))
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["method"] == "NativeCall"
            json["succeed"] == false
        }
    }

    def "errors-only drops non-NativeCall events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, true)

        when:
        def statusEvent = new Events.Status(
                Chain.ETHEREUM__MAINNET,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                defaultRequestDetails()
        )
        logWriter.submit(statusEvent)
        logWriter.flush()
        def act = accessLog.exists() ? accessLog.readLines() : []

        then:
        act.size() == 0
    }

    def "default mode passes all events through"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, false)

        when:
        def statusEvent = new Events.Status(
                Chain.ETHEREUM__MAINNET,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                defaultRequestDetails()
        )
        logWriter.submit(statusEvent)
        logWriter.submit(makeNativeCall(true))
        logWriter.submit(makeNativeCall(false))
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        act.size() == 3
    }

    def "errors-only filters list submission"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, true)

        when:
        def statusEvent = new Events.Status(
                Chain.ETHEREUM__MAINNET,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                defaultRequestDetails()
        )
        logWriter.submit([statusEvent, makeNativeCall(true), makeNativeCall(false)])
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["method"] == "NativeCall"
            json["succeed"] == false
        }
    }

    def "upstreamId is serialized in NativeCall event"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, false)

        when:
        logWriter.submit(makeNativeCall(true, "infura-opt"))
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["upstreamId"] == "infura-opt"
        }
    }

    def "upstreamId null is omitted from JSON"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        AccessLogWriter logWriter = createWriter(accessLog, false)

        when:
        logWriter.submit(makeNativeCall(true, null))
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            !json.containsKey("upstreamId")
        }
    }

    def "chain target routes events to separate file"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallForChain(Chain.ETHEREUM__MAINNET, true))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.readLines()

        then:
        globalLines.size() == 1
        ethLines.size() == 1
    }

    def "chain target does not receive events from other chains"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallForChain(Chain.BITCOIN__MAINNET, true))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.exists() ? ethLog.readLines() : []

        then:
        globalLines.size() == 1
        ethLines.size() == 0
    }

    def "upstream target routes events by upstreamId"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File upstreamLog = new File(dir, "infura.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
        }
        def upstreamsConfig = new UpstreamsConfig()
        def upstream = new UpstreamsConfig.Upstream()
        upstream.id = "infura-opt"
        upstream.chain = "optimism"
        upstream.accessLog = new UpstreamsConfig.UpstreamAccessLog(true, upstreamLog.absolutePath, false, false)
        upstreamsConfig.upstreams = [upstream]
        config.upstreams = upstreamsConfig

        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCall(true, "infura-opt"))
        logWriter.submit(makeNativeCall(true, "alchemy-opt"))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def upstreamLines = upstreamLog.readLines()

        then:
        globalLines.size() == 2
        upstreamLines.size() == 1
    }

    def "disabled chain target does not receive events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(false, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallForChain(Chain.ETHEREUM__MAINNET, true))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.exists() ? ethLog.readLines() : []

        then:
        globalLines.size() == 1
        ethLines.size() == 0
    }

    def "disabled upstream target does not receive events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File upstreamLog = new File(dir, "infura.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
        }
        def upstreamsConfig = new UpstreamsConfig()
        def upstream = new UpstreamsConfig.Upstream()
        upstream.id = "infura-opt"
        upstream.chain = "optimism"
        upstream.accessLog = new UpstreamsConfig.UpstreamAccessLog(false, upstreamLog.absolutePath, false, false)
        upstreamsConfig.upstreams = [upstream]
        config.upstreams = upstreamsConfig

        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCall(true, "infura-opt"))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def upstreamLines = upstreamLog.exists() ? upstreamLog.readLines() : []

        then:
        globalLines.size() == 1
        upstreamLines.size() == 0
    }

    def "no global file when filename is null"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = null
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallForChain(Chain.ETHEREUM__MAINNET, true))
        logWriter.flush()
        def ethLines = ethLog.readLines()

        then:
        logWriter.targets.size() == 1
        ethLines.size() == 1
    }

    def "backward compat - existing config still works"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = accessLog.absolutePath
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCall(true))
        logWriter.flush()
        def act = accessLog.readLines()

        then:
        logWriter.targets.size() == 1
        act.size() == 1
    }

    def "chain target with errors-only filters events"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, true)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallForChain(Chain.ETHEREUM__MAINNET, true))
        logWriter.submit(makeNativeCallForChain(Chain.ETHEREUM__MAINNET, false))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.readLines()

        then:
        globalLines.size() == 2
        ethLines.size() == 1
    }
}
