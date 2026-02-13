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

    private Events.NativeCall makeNativeCallWithMessages(Chain chain, boolean succeed, String upstreamId = null) {
        return new Events.NativeCall(
                chain,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                Events.Channel.GRPC,
                defaultRequestDetails(),
                1, 0, null, null, null, 100L,
                succeed, null, 256L,
                new Events.NativeCallItemDetails("eth_call", 1, 128L, 1L, '["0x1234"]'),
                '{"result":"0xabc"}', "some error", null, null,
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
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false, null)
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
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false, null)
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
        upstream.accessLog = new UpstreamsConfig.UpstreamAccessLog(true, upstreamLog.absolutePath, false, false, null)
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
                    new AccessLogConfig.ChainLogTarget(false, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false, null)
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
        upstream.accessLog = new UpstreamsConfig.UpstreamAccessLog(false, upstreamLog.absolutePath, false, false, null)
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
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false, null)
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

    def "chain target with include-messages false strips message fields"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, true, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, false, null)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallWithMessages(Chain.ETHEREUM__MAINNET, false))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.readLines()

        then:
        // Global target has includeMessages=true, so messages should be present
        globalLines.size() == 1
        with(globalLines[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["responseBody"] == '{"result":"0xabc"}'
            json["errorMessage"] == "some error"
            json["nativeCall"]["requestParams"] == '["0x1234"]'
        }

        // Chain target has includeMessages=false, so message fields should be stripped
        ethLines.size() == 1
        with(ethLines[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            !json.containsKey("responseBody")
            !json.containsKey("errorMessage")
            !json["nativeCall"].containsKey("requestParams")
            // Other fields should still be present
            json["method"] == "NativeCall"
            json["nativeCall"]["method"] == "eth_call"
        }
    }

    def "global include-messages false strips message fields even when chain target has them"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File globalLog = new File(dir, "global.jsonl")
        File ethLog = new File(dir, "ethereum.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = globalLog.absolutePath
            it.chainTargets = [
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, true, false, null)
            ]
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCallWithMessages(Chain.ETHEREUM__MAINNET, false))
        logWriter.flush()
        def globalLines = globalLog.readLines()
        def ethLines = ethLog.readLines()

        then:
        // Global target has includeMessages=false
        globalLines.size() == 1
        with(globalLines[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            !json.containsKey("responseBody")
            !json.containsKey("errorMessage")
        }

        // Chain target has includeMessages=true
        ethLines.size() == 1
        with(ethLines[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["responseBody"] == '{"result":"0xabc"}'
            json["errorMessage"] == "some error"
            json["nativeCall"]["requestParams"] == '["0x1234"]'
        }
    }

    def "rotates existing log file on start"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        // Write some pre-existing content
        accessLog.text = '{"old":"data"}\n'

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = accessLog.absolutePath
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        logWriter.submit(makeNativeCall(true))
        logWriter.flush()

        // Check that the original file was rotated
        def backupFiles = dir.listFiles().findAll { it.name.startsWith("accesslog.") && it.name != "accesslog.jsonl" }

        then:
        // A backup file was created with the old content
        backupFiles.size() == 1
        backupFiles[0].name ==~ /accesslog\.\d{8}T\d{6}\.jsonl/
        backupFiles[0].text.trim() == '{"old":"data"}'

        // The current log file has only the new event
        accessLog.exists()
        def lines = accessLog.readLines()
        lines.size() == 1
        with(lines[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["method"] == "NativeCall"
        }
    }

    def "does not rotate empty log file on start"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        // Create an empty file
        accessLog.createNewFile()

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = accessLog.absolutePath
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        def backupFiles = dir.listFiles().findAll { it.name.startsWith("accesslog.") && it.name != "accesslog.jsonl" }

        then:
        // No backup created for empty file
        backupFiles.size() == 0
    }

    def "does not rotate if log file does not exist"() {
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
        def allFiles = dir.listFiles()

        then:
        // No files created during start, only on first flush
        allFiles.length == 0
    }

    def "rotates log file when exceeding filesize limit"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")

        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, false).tap {
            it.filename = accessLog.absolutePath
            it.fileSize = 100L // 100 bytes limit - a single event exceeds this
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)
        logWriter.start()

        when:
        // Flush writes one event (exceeds 100 bytes), triggers rotation
        logWriter.submit(makeNativeCall(true))
        logWriter.flush()

        def backupFiles = dir.listFiles().findAll { it.name =~ /accesslog\.\d{8}T\d{6}\.jsonl/ }

        then:
        // The file was rotated after exceeding the size limit
        backupFiles.size() == 1
        // Rotated file contains the event
        backupFiles[0].readLines().size() == 1
        with(backupFiles[0].readLines()[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["method"] == "NativeCall"
        }
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
                    new AccessLogConfig.ChainLogTarget(true, Chain.ETHEREUM__MAINNET, ethLog.absolutePath, false, true, null)
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
