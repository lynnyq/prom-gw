package com.example.promgw.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.promgw.aggregate.AggResult;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * StarRocksSinkTest 验证 StarRocksSink 的 label 生成逻辑。
 *
 * 核心回归:buildLabel 必须对不同 series 生成不同 label,
 * 否则 StarRocks 会按 label 幂等去重 → 第二个 Stream Load 被拒绝 → 静默丢数。
 * 修复前 hashShort 只取前 8 字符(32-bit),修复后取完整 16 字符(64-bit)。
 */
class StarRocksSinkTest {

    private StarRocksSink createSink() {
        return new StarRocksSink(
                "127.0.0.1", 8030, "prom", "metrics_5m",
                "root", "", true, "sz_5m", null);
    }

    private AggResult makeResult(String business, String labelsHash) {
        AggResult r = new AggResult();
        r.setTs(new Date(1724544000000L)); // 2024-08-25 00:00:00 UTC
        r.setBusiness(business);
        r.setLabelsHash(labelsHash);
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "up");
        r.setLabels(labels);
        return r;
    }

    @Test
    void buildLabelIncludesFullHash() {
        StarRocksSink sink = createSink();
        AggResult r = makeResult("team_alpha", "a1b2c3d4e5f6a7b8");

        // open() 初始化 labelFmt;手动调用 buildLabel 前需初始化
        // buildLabel 只用 labelFmt,跳过 open() 直接反射设置
        try {
            java.lang.reflect.Field f = StarRocksSink.class.getDeclaredField("labelFmt");
            f.setAccessible(true);
            f.set(sink, new java.text.SimpleDateFormat("yyyyMMdd_HHmm"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String label = sink.buildLabel(r);
        // 必须包含完整 16 字符 hash,而非截断的前 8 字符
        assertThat(label).endsWith("a1b2c3d4e5f6a7b8");
        assertThat(label).doesNotEndWith("a1b2c3d4");
    }

    @Test
    void buildLabelDifferentForDifferentSeries() {
        StarRocksSink sink = createSink();
        try {
            java.lang.reflect.Field f = StarRocksSink.class.getDeclaredField("labelFmt");
            f.setAccessible(true);
            f.set(sink, new java.text.SimpleDateFormat("yyyyMMdd_HHmm"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 两个不同 series,同 business,不同 labels_hash
        AggResult r1 = makeResult("team_alpha", "a1b2c3d4e5f6a7b8");
        AggResult r2 = makeResult("team_alpha", "b2c3d4e5f6a7b8c9");

        String label1 = sink.buildLabel(r1);
        String label2 = sink.buildLabel(r2);

        // 修复前(只用前 8 字符):a1b2c3d4 vs b2c3d4e5 → 不同,不碰撞
        // 但如果两个 hash 前 8 字符相同:如 a1b2c3d4... 和 a1b2c3d4.....
        // 修复后:用完整 16 字符,不会碰撞
        assertThat(label1).isNotEqualTo(label2);
    }

    @Test
    void buildLabelCollisionRegression() {
        // 回归:两个 hash 前 8 字符相同但完整 hash 不同的 series
        // 修复前 → 生成相同 label → StarRocks 静默丢数
        // 修复后 → 生成不同 label → 安全
        StarRocksSink sink = createSink();
        try {
            java.lang.reflect.Field f = StarRocksSink.class.getDeclaredField("labelFmt");
            f.setAccessible(true);
            f.set(sink, new java.text.SimpleDateFormat("yyyyMMdd_HHmm"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 前 8 字符相同,但完整 hash 不同
        AggResult r1 = makeResult("team_alpha", "a1b2c3d400000001");
        AggResult r2 = makeResult("team_alpha", "a1b2c3d400000002");

        String label1 = sink.buildLabel(r1);
        String label2 = sink.buildLabel(r2);

        assertThat(label1)
                .as("两个 series 前 8 字符相同但完整 hash 不同,必须生成不同 label")
                .isNotEqualTo(label2);
    }

    @Test
    void buildLabelHandlesNullHash() {
        StarRocksSink sink = createSink();
        try {
            java.lang.reflect.Field f = StarRocksSink.class.getDeclaredField("labelFmt");
            f.setAccessible(true);
            f.set(sink, new java.text.SimpleDateFormat("yyyyMMdd_HHmm"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        AggResult r = makeResult("team_alpha", null);
        String label = sink.buildLabel(r);
        assertThat(label).endsWith("0000000000000000");
    }
}
