package com.lynnyq.promgw.aggregate;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * MetricAggFunction 5min 窗口聚合函数。
 *
 * 聚合维度:同一 series(business + metric + sorted labels)在 5min 窗口内的所有 sample
 * 聚合输出:sample_count / sum / max / min / avg / p50 / p99
 *
 * p50/p99 用桶内排序精确计算(与 prom-gw downsample stage 对齐)。
 * 单 series 5min 内通常 ≤ 20 个 sample(15s 抓取间隔 × 20 = 5min),排序开销可忽略。
 */
public class MetricAggFunction implements AggregateFunction<SampleWithMeta, MetricAggState, MetricAggState> {

    @Override
    public MetricAggState createAccumulator() {
        return new MetricAggState();
    }

    @Override
    public MetricAggState add(SampleWithMeta rec, MetricAggState acc) {
        acc.count++;
        acc.sum += rec.getValue();
        if (rec.getValue() > acc.max) acc.max = rec.getValue();
        if (rec.getValue() < acc.min) acc.min = rec.getValue();
        acc.samples.add(rec.getValue());

        // 元数据从第一条 sample 取(同 series 后续应一致)
        if (acc.business == null) {
            acc.business = rec.getBusiness();
            acc.metric = rec.getMetric();
            acc.labels = rec.getLabels();
            acc.sourceDc = rec.getSourceDc();
            acc.ingestCity = rec.getIngestCity();
        }
        return acc;
    }

    @Override
    public MetricAggState getResult(MetricAggState acc) {
        // 计算 p50/p99(桶内排序精确计算)
        if (!acc.samples.isEmpty()) {
            Collections.sort(acc.samples);
        }
        return acc;
    }

    @Override
    public MetricAggState merge(MetricAggState a, MetricAggState b) {
        a.count += b.count;
        a.sum += b.sum;
        if (b.max > a.max) a.max = b.max;
        if (b.min < a.min) a.min = b.min;
        a.samples.addAll(b.samples);
        if (a.business == null) {
            a.business = b.business;
            a.metric = b.metric;
            a.labels = b.labels;
            a.sourceDc = b.sourceDc;
            a.ingestCity = b.ingestCity;
        }
        return a;
    }

    /** percentile 计算百分位数(输入必须已排序)。 */
    public static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return Double.NaN;
        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }
}