package com.lynnyq.promgw.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import com.lynnyq.promgw.sink.StarRocksStreamLoadClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * DlqReplayerTest DLQ 重放工具单元测试。
 *
 * 覆盖:
 *   - resolveTable 表名解析(消息 table 优先,fallback 兜底)
 *   - buildBatchJson JSON 数组拼接
 *   - computeBackoffMs 指数退避计算
 *   - flushBuffer Stream Load 成功 / 重试 / 超限丢弃
 */
class DlqReplayerTest {

    /** 创建一个最小可用的配置对象(用于测试,不连真实 Kafka/SR)。 */
    private DlqReplayerConfig createConfig() throws IOException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("kafka.dlq.topic", "prom.test.dlq.sr.5m");
        props.setProperty("kafka.consumer.group.id", "dlq-replayer-test");
        props.setProperty("starrocks.fe.host", "localhost");
        props.setProperty("starrocks.fe.port", "8030");
        props.setProperty("starrocks.db", "observability");
        props.setProperty("starrocks.table", "metrics_default_5m");
        props.setProperty("replay.max.retry", "3");
        props.setProperty("replay.backoff.base.ms", "10");
        props.setProperty("replay.backoff.max.ms", "1000");
        props.setProperty("replay.backoff.multiplier", "2.0");
        props.setProperty("batch.size", "100");
        props.setProperty("batch.wait.ms", "5000");

        java.io.File tmp = java.io.File.createTempFile("dlq-replayer-test", ".properties");
        tmp.deleteOnExit();
        try (java.io.FileOutputStream out = new java.io.FileOutputStream(tmp)) {
            props.store(out, "test");
        }
        return new DlqReplayerConfig(tmp.getAbsolutePath());
    }

    @Test
    void testResolveTableUsesMessageTable() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        DlqMessage msg = new DlqMessage("{}", "metrics_s001_5m", "label1", "err", 0, 0L);

        String table = replayer.resolveTable(msg);

        assertThat(table).isEqualTo("metrics_s001_5m");
    }

    @Test
    void testResolveTableFallbackWhenMessageTableEmpty() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        DlqMessage msg = new DlqMessage("{}", "", "label1", "err", 0, 0L);

        String table = replayer.resolveTable(msg);

        assertThat(table).isEqualTo("metrics_default_5m");
    }

    @Test
    void testResolveTableFallbackWhenMessageTableNull() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        DlqMessage msg = new DlqMessage("{}", null, "label1", "err", 0, 0L);

        String table = replayer.resolveTable(msg);

        assertThat(table).isEqualTo("metrics_default_5m");
    }

    @Test
    void testBuildBatchJsonSingleRow() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        List<DlqMessage> msgs = List.of(
                new DlqMessage("{\"metric\":\"cpu\"}", "metrics_5m", "l1", "e", 0, 0L));

        String json = replayer.buildBatchJson(msgs);

        assertThat(json).isEqualTo("[{\"metric\":\"cpu\"}]");
    }

    @Test
    void testBuildBatchJsonMultipleRows() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        List<DlqMessage> msgs = List.of(
                new DlqMessage("{\"metric\":\"cpu\"}", "metrics_5m", "l1", "e", 0, 0L),
                new DlqMessage("{\"metric\":\"mem\"}", "metrics_5m", "l2", "e", 0, 0L),
                new DlqMessage("{\"metric\":\"net\"}", "metrics_5m", "l3", "e", 0, 0L));

        String json = replayer.buildBatchJson(msgs);

        assertThat(json).isEqualTo(
                "[{\"metric\":\"cpu\"},{\"metric\":\"mem\"},{\"metric\":\"net\"}]");
    }

    @Test
    void testComputeBackoffMsExponential() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        // base=10, multiplier=2
        assertThat(replayer.computeBackoffMs(0)).isEqualTo(10L);
        assertThat(replayer.computeBackoffMs(1)).isEqualTo(20L);
        assertThat(replayer.computeBackoffMs(2)).isEqualTo(40L);
        assertThat(replayer.computeBackoffMs(3)).isEqualTo(80L);
    }

    @Test
    void testComputeBackoffMsCappedAtMax() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        // max=1000, attempt=10 → 10*2^10=10240 → cap at 1000
        assertThat(replayer.computeBackoffMs(10)).isEqualTo(1000L);
    }

    /**
     * StubStarRocksClient 用于测试 flushBuffer 的成功/失败行为。
     * 可配置成功前的失败次数。
     */
    static class StubStarRocksClient extends StarRocksStreamLoadClient {
        int failBeforeSuccess;
        int loadCallCount = 0;
        String lastLabel;
        String lastJson;

        StubStarRocksClient(int failBeforeSuccess) {
            super("localhost", 8030, "observability", "metrics_5m", "root", "");
            this.failBeforeSuccess = failBeforeSuccess;
        }

        @Override
        public String load(String label, String jsonBody, boolean gzip) throws IOException {
            loadCallCount++;
            lastLabel = label;
            lastJson = jsonBody;
            if (loadCallCount <= failBeforeSuccess) {
                throw new IOException("simulated SR failure #" + loadCallCount);
            }
            return "{\"Status\":\"Success\",\"NumberLoadedRows\":1,\"NumberTotalRows\":1}";
        }
    }

    @Test
    void testFlushBufferSuccessOnFirstAttempt() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        StubStarRocksClient stub = new StubStarRocksClient(0);

        List<DlqMessage> buf = new ArrayList<>(List.of(
                new DlqMessage("{\"metric\":\"cpu\"}", "metrics_5m", "label_ok", "e", 0, 0L)));

        replayer.flushBufferWithClient("metrics_5m", buf, stub);

        assertThat(stub.loadCallCount).isEqualTo(1);
        assertThat(buf).isEmpty();
        assertThat(stub.lastLabel).isEqualTo("label_ok");
        assertThat(stub.lastJson).isEqualTo("[{\"metric\":\"cpu\"}]");
    }

    @Test
    void testFlushBufferRetriesThenSucceeds() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        // 失败 2 次后成功,maxRetry=3 所以会重试到成功
        StubStarRocksClient stub = new StubStarRocksClient(2);

        List<DlqMessage> buf = new ArrayList<>(List.of(
                new DlqMessage("{\"metric\":\"cpu\"}", "metrics_5m", "label_retry", "e", 0, 0L)));

        replayer.flushBufferWithClient("metrics_5m", buf, stub);

        assertThat(stub.loadCallCount).isEqualTo(3);
        assertThat(buf).isEmpty();
    }

    @Test
    void testFlushBufferExhaustsRetriesDropsBatch() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        // 一直失败,maxRetry=3 → 总共 4 次调用(1 次首次 + 3 次重试),然后丢弃
        StubStarRocksClient stub = new StubStarRocksClient(99);

        List<DlqMessage> buf = new ArrayList<>(List.of(
                new DlqMessage("{\"metric\":\"cpu\"}", "metrics_5m", "label_poison", "e", 0, 0L)));

        replayer.flushBufferWithClient("metrics_5m", buf, stub);

        // maxRetry=3 → attempt 0..3 共 4 次调用
        assertThat(stub.loadCallCount).isEqualTo(4);
        assertThat(buf).isEmpty(); // 毒丸批次被丢弃
    }

    @Test
    void testFlushBufferEmptyListNoOp() throws Exception {
        DlqReplayer replayer = new DlqReplayer(createConfig());
        StubStarRocksClient stub = new StubStarRocksClient(0);

        List<DlqMessage> buf = new ArrayList<>();
        replayer.flushBufferWithClient("metrics_5m", buf, stub);

        assertThat(stub.loadCallCount).isEqualTo(0);
    }
}
