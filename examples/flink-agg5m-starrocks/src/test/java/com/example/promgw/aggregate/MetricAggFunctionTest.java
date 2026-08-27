package com.example.promgw.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * MetricAggFunctionTest 验证 5min 窗口聚合逻辑(sum/count/max/min/p50/p99)。
 */
class MetricAggFunctionTest {

    private final MetricAggFunction agg = new MetricAggFunction();

    @Test
    void testAccumulatorBasic() {
        MetricAggState acc = agg.createAccumulator();

        Map<String, String> labels = new HashMap<>();
        labels.put("job", "prometheus");

        // 加入 3 个 sample
        agg.add(new SampleWithMeta("biz1", "up", labels, 1.0, 1000L,
                "dc1", "bj", "dc1", 0L, ""), acc);
        agg.add(new SampleWithMeta("biz1", "up", labels, 2.0, 2000L,
                "dc1", "bj", "dc1", 0L, ""), acc);
        agg.add(new SampleWithMeta("biz1", "up", labels, 3.0, 3000L,
                "dc1", "bj", "dc1", 0L, ""), acc);

        MetricAggState result = agg.getResult(acc);

        assertThat(result.count).isEqualTo(3);
        assertThat(result.sum).isEqualTo(6.0);
        assertThat(result.max).isEqualTo(3.0);
        assertThat(result.min).isEqualTo(1.0);
        assertThat(result.business).isEqualTo("biz1");
        assertThat(result.metric).isEqualTo("up");
        // avg 在 window function 计算,这里只验证 sum/count
        assertThat(result.sum / result.count).isEqualTo(2.0);
    }

    @Test
    void testPercentileExact() {
        // 10 个值:1,2,3,...,10
        // 注意:不用 Arrays.asList 直接返回值(JDK 内部 Arrays$ArrayList 实现,
        // Kryo 反射序列化存在隐患),统一包装为普通 ArrayList。
        List<Double> sorted = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0));

        // p50: ceil(0.5 * 10) - 1 = 4 → 索引 4 → 5.0
        assertThat(MetricAggFunction.percentile(sorted, 0.50)).isEqualTo(5.0);

        // p99: ceil(0.99 * 10) - 1 = 9 → 索引 9 → 10.0
        assertThat(MetricAggFunction.percentile(sorted, 0.99)).isEqualTo(10.0);

        // p90: ceil(0.90 * 10) - 1 = 8 → 索引 8 → 9.0
        assertThat(MetricAggFunction.percentile(sorted, 0.90)).isEqualTo(9.0);
    }

    @Test
    void testPercentileEdgeCases() {
        List<Double> single = new ArrayList<>(Arrays.asList(42.0));
        assertThat(MetricAggFunction.percentile(single, 0.50)).isEqualTo(42.0);
        assertThat(MetricAggFunction.percentile(single, 0.99)).isEqualTo(42.0);

        List<Double> empty = new ArrayList<>(Arrays.asList());
        assertThat(MetricAggFunction.percentile(empty, 0.50)).isNaN();
    }

    @Test
    void testMergeAccumulators() {
        Map<String, String> labels = new HashMap<>();
        labels.put("job", "prom");

        MetricAggState a = agg.createAccumulator();
        agg.add(new SampleWithMeta("biz1", "up", labels, 1.0, 1000L, "dc1", "bj", "dc1", 0L, ""), a);
        agg.add(new SampleWithMeta("biz1", "up", labels, 2.0, 2000L, "dc1", "bj", "dc1", 0L, ""), a);

        MetricAggState b = agg.createAccumulator();
        agg.add(new SampleWithMeta("biz1", "up", labels, 3.0, 3000L, "dc1", "bj", "dc1", 0L, ""), b);

        MetricAggState merged = agg.merge(a, b);

        assertThat(merged.count).isEqualTo(3);
        assertThat(merged.sum).isEqualTo(6.0);
        assertThat(merged.max).isEqualTo(3.0);
        assertThat(merged.min).isEqualTo(1.0);
        assertThat(merged.samples).hasSize(3);
    }
}