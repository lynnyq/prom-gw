package com.example.promgw.aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MetricAggState 窗口聚合累加器状态。
 *
 * 5min 窗口内同一 series 的所有 sample 累加到此结构,
 * 窗口触发时计算 avg/p50/p99 等聚合值。
 */
public class MetricAggState {
    /** 样本数 */
    public long count = 0;
    /** 值总和(用于 avg = sum/count) */
    public double sum = 0.0;
    /** 最大值 */
    public double max = Double.NEGATIVE_INFINITY;
    /** 最小值 */
    public double min = Double.POSITIVE_INFINITY;
    /** 所有原始值(用于 p50/p99 精确计算) */
    public List<Double> samples = new ArrayList<>();
    /** 元数据(来自第一条 sample,后续应一致) */
    public String tenant;
    public String metric;
    public Map<String, String> labels;
    public String sourceDc;
    public String ingestCity;
    public String ingestDc;
    public long ingestTimeMs;
    public String traceparent;

    /** reset 重置累加器(复用对象,减少 GC)。 */
    public void reset() {
        count = 0;
        sum = 0.0;
        max = Double.NEGATIVE_INFINITY;
        min = Double.POSITIVE_INFINITY;
        samples.clear();
        tenant = null;
        metric = null;
        labels = null;
        sourceDc = null;
        ingestCity = null;
        ingestDc = null;
        ingestTimeMs = 0;
        traceparent = null;
    }
}
