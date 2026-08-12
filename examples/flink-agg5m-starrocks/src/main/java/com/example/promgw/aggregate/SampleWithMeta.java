package com.example.promgw.aggregate;

import java.util.Map;

/**
 * SampleWithMeta 展开后的单条 sample(带元数据)。
 *
 * 由 ExpandWriteRequest 把 PromSample(含 N 个 series)展开为 N 条 SampleWithMeta,
 * 每条对应一个 series 的一个 sample point。
 */
public class SampleWithMeta {
    private String tenant;
    private String metric;
    private Map<String, String> labels;
    private double value;
    private long timestampMs;          // 事件时间,毫秒(prompb.Sample.timestamp)
    private String sourceDc;
    private String ingestCity;
    private String ingestDc;
    private long ingestTimeMs;
    private String traceparent;

    // Flink POJO 需要无参构造
    public SampleWithMeta() {}

    public SampleWithMeta(String tenant, String metric, Map<String, String> labels,
                          double value, long timestampMs,
                          String sourceDc, String ingestCity, String ingestDc,
                          long ingestTimeMs, String traceparent) {
        this.tenant = tenant;
        this.metric = metric;
        this.labels = labels;
        this.value = value;
        this.timestampMs = timestampMs;
        this.sourceDc = sourceDc;
        this.ingestCity = ingestCity;
        this.ingestDc = ingestDc;
        this.ingestTimeMs = ingestTimeMs;
        this.traceparent = traceparent;
    }

    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }

    public String getMetric() { return metric; }
    public void setMetric(String metric) { this.metric = metric; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    public long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }

    public String getSourceDc() { return sourceDc; }
    public void setSourceDc(String sourceDc) { this.sourceDc = sourceDc; }

    public String getIngestCity() { return ingestCity; }
    public void setIngestCity(String ingestCity) { this.ingestCity = ingestCity; }

    public String getIngestDc() { return ingestDc; }
    public void setIngestDc(String ingestDc) { this.ingestDc = ingestDc; }

    public long getIngestTimeMs() { return ingestTimeMs; }
    public void setIngestTimeMs(long ingestTimeMs) { this.ingestTimeMs = ingestTimeMs; }

    public String getTraceparent() { return traceparent; }
    public void setTraceparent(String traceparent) { this.traceparent = traceparent; }
}
