package com.lynnyq.promgw.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.lynnyq.promgw.aggregate.AggResult;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * BufferingStarRocksSinkTest 验证 batch label 生成与行级 label 生成。
 *
 * 核心回归:
 *   1. buildBatchLabel 必须对每次 flush 生成不同 label(batchSeq 递增)
 *   2. buildRowLabel 必须对不同 series 生成不同 label(含完整 labels_hash)
 *   3. label 跨 subtask 不碰撞(taskIndex 参与)
 */
class BufferingStarRocksSinkTest {

    private BufferingStarRocksSink createSink() {
        return new BufferingStarRocksSink(
                "127.0.0.1", 8030, "prom", "metrics_5m",
                "root", "", true, "sz_5m", null,
                500, 10_000L);
    }

    private void initLabelFmt(BufferingStarRocksSink sink) throws Exception {
        Field f = BufferingStarRocksSink.class.getDeclaredField("batchLabelFmt");
        f.setAccessible(true);
        f.set(sink, new SimpleDateFormat("yyyyMMddHHmmssSSS"));
    }

    private void initTaskIndex(BufferingStarRocksSink sink, int idx) throws Exception {
        Field f = BufferingStarRocksSink.class.getDeclaredField("taskIndex");
        f.setAccessible(true);
        f.setInt(sink, idx);
    }

    private void initBatchSeq(BufferingStarRocksSink sink, long seq) throws Exception {
        Field f = BufferingStarRocksSink.class.getDeclaredField("batchSeq");
        f.setAccessible(true);
        f.setLong(sink, seq);
    }

    private AggResult makeResult(String business, String labelsHash) {
        AggResult r = new AggResult();
        r.setTs(new Date(1724544000000L));
        r.setBusiness(business);
        r.setLabelsHash(labelsHash);
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "up");
        r.setLabels(labels);
        return r;
    }

    @Test
    void buildBatchLabelIncrementsSeq() throws Exception {
        BufferingStarRocksSink sink = createSink();
        initLabelFmt(sink);
        initTaskIndex(sink, 0);

        String label1 = sink.buildBatchLabel();
        String label2 = sink.buildBatchLabel();
        String label3 = sink.buildBatchLabel();

        // 每次调用 batchSeq 递增 → label 不同
        assertThat(label1).isNotEqualTo(label2);
        assertThat(label2).isNotEqualTo(label3);
        assertThat(label1).isNotEqualTo(label3);
    }

    @Test
    void buildBatchLabelDifferentAcrossSubtasks() throws Exception {
        BufferingStarRocksSink sink0 = createSink();
        initLabelFmt(sink0);
        initTaskIndex(sink0, 0);
        initBatchSeq(sink0, 1);

        BufferingStarRocksSink sink1 = createSink();
        initLabelFmt(sink1);
        initTaskIndex(sink1, 1);
        initBatchSeq(sink1, 1);

        // 同时间同 seq,但 taskIndex 不同 → label 不同
        String label0 = sink0.buildBatchLabel();
        String label1 = sink1.buildBatchLabel();
        assertThat(label0).isNotEqualTo(label1);
    }

    @Test
    void buildRowLabelIncludesFullHash() throws Exception {
        BufferingStarRocksSink sink = createSink();
        AggResult r = makeResult("team_alpha", "a1b2c3d4e5f6a7b8");

        String label = sink.buildRowLabel(r);
        // 必须包含完整 16 字符 hash
        assertThat(label).endsWith("a1b2c3d4e5f6a7b8");
        assertThat(label).doesNotEndWith("a1b2c3d4");
    }

    @Test
    void buildRowLabelDifferentForDifferentSeries() {
        BufferingStarRocksSink sink = createSink();
        AggResult r1 = makeResult("team_alpha", "a1b2c3d4e5f6a7b8");
        AggResult r2 = makeResult("team_alpha", "b2c3d4e5f6a7b8c9");

        String label1 = sink.buildRowLabel(r1);
        String label2 = sink.buildRowLabel(r2);

        assertThat(label1).isNotEqualTo(label2);
    }

    @Test
    void buildRowLabelCollisionRegression() {
        // 前 8 字符相同但完整 hash 不同的 series
        BufferingStarRocksSink sink = createSink();
        AggResult r1 = makeResult("team_alpha", "a1b2c3d400000001");
        AggResult r2 = makeResult("team_alpha", "a1b2c3d400000002");

        String label1 = sink.buildRowLabel(r1);
        String label2 = sink.buildRowLabel(r2);

        assertThat(label1)
                .as("两个 series 前 8 字符相同但完整 hash 不同,必须生成不同 label")
                .isNotEqualTo(label2);
    }

    @Test
    void buildRowLabelHandlesNullHash() {
        BufferingStarRocksSink sink = createSink();
        AggResult r = makeResult("team_alpha", null);
        String label = sink.buildRowLabel(r);
        assertThat(label).endsWith("0000000000000000");
    }
}
