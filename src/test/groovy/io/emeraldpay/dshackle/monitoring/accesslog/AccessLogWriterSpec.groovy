package io.emeraldpay.dshackle.monitoring.accesslog


import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.AccessLogConfig
import io.emeraldpay.dshackle.config.MainConfig
import spock.lang.Specification

import java.time.Instant

class AccessLogWriterSpec extends Specification {

    def "writes log event"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "accesslog.jsonl")
        println("Write access log to $accessLog.absolutePath")
        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false).tap {
            it.filename = accessLog.absolutePath
        }
        AccessLogWriter logWriter = new AccessLogWriter(config)

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

    private Events.NativeCall makeNativeCall(boolean succeed) {
        return new Events.NativeCall(
                Chain.ETHEREUM__MAINNET,
                UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                Events.Channel.GRPC,
                defaultRequestDetails(),
                1, 0, null, null, null, 100L,
                succeed, null, 256L,
                new Events.NativeCallItemDetails("eth_call", 1, 128L, 1L, null),
                null, null, null, null
        )
    }

    private AccessLogWriter createWriter(File accessLog, boolean errorsOnly) {
        MainConfig config = new MainConfig()
        config.accessLogConfig = new AccessLogConfig(true, false, errorsOnly).tap {
            it.filename = accessLog.absolutePath
        }
        return new AccessLogWriter(config)
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
}
