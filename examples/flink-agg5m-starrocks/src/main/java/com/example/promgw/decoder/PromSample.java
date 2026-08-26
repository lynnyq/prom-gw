package com.example.promgw.decoder;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;

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
@TypeInfo(PromSample.Factory.class)
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
    // 注意:不用 Collections.emptyList()(JDK 内部不可变单例实现,
    // 与 Arrays$ArrayList 同属序列化隐患),统一用普通 ArrayList。
    public PromSample() {
        this.timeseries = new ArrayList<>();
    }

    public PromSample(List<ParsedSeries> timeseries,
                      String tenant, String sourceDc, String ingestCity,
                      String ingestDc, long ingestTimeMs, String traceparent) {
        this.timeseries = timeseries != null ? timeseries : new ArrayList<>();
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

    /**
     * typeInfo 显式声明 PromSample 的 Flink 序列化器。
     *
     * Flink 自动分析 POJO 时,List/Map 等接口类型字段会被推断为 GenericTypeInfo
     * 并落入 Kryo,JDK 17+ 强封装下 Kryo 初始化即抛 InaccessibleObjectException。
     * 显式声明后全部使用 Flink 原生序列化器,不触发 Kryo。
     */
    public static TypeInformation<PromSample> typeInfo() {
        Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
        fields.put("timeseries", Types.LIST(ParsedSeries.typeInfo()));
        fields.put("tenant", Types.STRING);
        fields.put("sourceDc", Types.STRING);
        fields.put("ingestCity", Types.STRING);
        fields.put("ingestDc", Types.STRING);
        fields.put("ingestTimeMs", Types.LONG);
        fields.put("traceparent", Types.STRING);
        return Types.POJO(PromSample.class, fields);
    }

    /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
    public static class Factory extends TypeInfoFactory<PromSample> {
        @Override
        public TypeInformation<PromSample> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return typeInfo();
        }
    }

    /** ParsedSeries WriteRequest 中单个 TimeSeries 解析结果。 */
    @TypeInfo(ParsedSeries.Factory.class)
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

        /** typeInfo 显式声明 ParsedSeries 的 Flink 序列化器(理由见 PromSample#typeInfo)。 */
        public static TypeInformation<ParsedSeries> typeInfo() {
            Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
            fields.put("metricName", Types.STRING);
            fields.put("labels", Types.MAP(Types.STRING, Types.STRING));
            fields.put("samples", Types.LIST(ParsedSample.typeInfo()));
            return Types.POJO(ParsedSeries.class, fields);
        }

        /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
        public static class Factory extends TypeInfoFactory<ParsedSeries> {
            @Override
            public TypeInformation<ParsedSeries> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
                return typeInfo();
            }
        }
    }

    /** ParsedSample 单个采样点。 */
    @TypeInfo(ParsedSample.Factory.class)
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

        /** typeInfo 显式声明 ParsedSample 的 Flink 序列化器(理由见 PromSample#typeInfo)。 */
        public static TypeInformation<ParsedSample> typeInfo() {
            Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
            fields.put("value", Types.DOUBLE);
            fields.put("timestampMs", Types.LONG);
            return Types.POJO(ParsedSample.class, fields);
        }

        /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
        public static class Factory extends TypeInfoFactory<ParsedSample> {
            @Override
            public TypeInformation<ParsedSample> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
                return typeInfo();
            }
        }
    }
}
