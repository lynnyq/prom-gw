package com.example.promgw.decoder;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * PromSample 解码后的 prom-gw 消息 POJO。
 *
 * 对应 prom-gw 内部 parser.Sample + 整个 WriteRequest:
 *   - timeseries: WriteRequest 中所有 TimeSeries(每个含 labels + samples)
 *   - tenant/sourceDc/ingestCity/...: 从 Kafka header 提取(不在 payload 里)
 *
 * 注意:一条 Kafka 消息的 payload 是整个 WriteRequest,包含 N 个 series。
 * 上游(prom-gw)为每个 sample 产生一条消息但 payload 相同,需在 DedupFunction 去重。
 *
 * 实现 {@link Serializable}:作为 DataStream 中的传输对象,需支持 Flink 序列化。
 */
public class PromSample implements Serializable {

    private static final long serialVersionUID = 1L;
    private List<ParsedSeries> timeseries;
    private String tenant;
    private String sourceDc;
    private String ingestCity;
    private String ingestDc;
    private long ingestTimeMs;
    private String traceparent;

    // Flink POJO 需要无参构造
    public PromSample() {
        this.timeseries = Collections.emptyList();
    }

    public PromSample(List<ParsedSeries> timeseries,
                      String tenant, String sourceDc, String ingestCity,
                      String ingestDc, long ingestTimeMs, String traceparent) {
        this.timeseries = timeseries != null ? timeseries : Collections.emptyList();
        this.tenant = tenant;
        this.sourceDc = sourceDc;
        this.ingestCity = ingestCity;
        this.ingestDc = ingestDc;
        this.ingestTimeMs = ingestTimeMs;
        this.traceparent = traceparent;
    }

    public List<ParsedSeries> getTimeseries() { return timeseries; }
    public void setTimeseries(List<ParsedSeries> timeseries) { this.timeseries = timeseries; }

    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }

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

    /** ParsedSeries WriteRequest 中单个 TimeSeries 解析结果。 */
    public static class ParsedSeries implements Serializable {

        private static final long serialVersionUID = 1L;
        private String metricName;
        private Map<String, String> labels;
        private List<ParsedSample> samples;

        public ParsedSeries() {}

        public ParsedSeries(String metricName, Map<String, String> labels, List<ParsedSample> samples) {
            this.metricName = metricName;
            this.labels = labels;
            this.samples = samples;
        }

        public String getMetricName() { return metricName; }
        public void setMetricName(String metricName) { this.metricName = metricName; }

        public Map<String, String> getLabels() { return labels; }
        public void setLabels(Map<String, String> labels) { this.labels = labels; }

        public List<ParsedSample> getSamples() { return samples; }
        public void setSamples(List<ParsedSample> samples) { this.samples = samples; }
    }

    /** ParsedSample 单个采样点。 */
    public static class ParsedSample implements Serializable {

        private static final long serialVersionUID = 1L;
        private double value;
        private long timestampMs;

        public ParsedSample() {}

        public ParsedSample(double value, long timestampMs) {
            this.value = value;
            this.timestampMs = timestampMs;
        }

        public double getValue() { return value; }
        public void setValue(double value) { this.value = value; }

        public long getTimestampMs() { return timestampMs; }
        public void setTimestampMs(long timestampMs) { this.timestampMs = timestampMs; }
    }
}
