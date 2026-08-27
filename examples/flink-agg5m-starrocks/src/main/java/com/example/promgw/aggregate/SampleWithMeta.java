package com.example.promgw.aggregate;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;

/**
 * SampleWithMeta 展开后的单条 sample(带元数据)。
 *
 * 由 ExpandWriteRequest 把 PromSample(含 N 个 series)展开为 N 条 SampleWithMeta,
 * 每条对应一个 series 的一个 sample point。
 *
 * 实现 {@link Serializable}:作为 DataStream 中的传输对象,需支持 Flink 序列化。
 */
@TypeInfo(SampleWithMeta.Factory.class)
public class SampleWithMeta implements Serializable {

    private static final long serialVersionUID = 1L;
    private String business;
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

    public SampleWithMeta(String business, String metric, Map<String, String> labels,
                          double value, long timestampMs,
                          String sourceDc, String ingestCity, String ingestDc,
                          long ingestTimeMs, String traceparent) {
        this.business = business;
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

    public String getBusiness() { return business; }
    public void setBusiness(String business) { this.business = business; }

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

    /**
     * typeInfo 显式声明 SampleWithMeta 的 Flink 序列化器。
     *
     * Flink 自动分析 POJO 时,labels(Map 接口字段)会被推断为 GenericTypeInfo
     * 并落入 Kryo,JDK 17+ 强封装下 Kryo 初始化即抛 InaccessibleObjectException。
     * 显式声明后全部使用 Flink 原生序列化器,不触发 Kryo。
     */
    public static TypeInformation<SampleWithMeta> typeInfo() {
        Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
        fields.put("business", Types.STRING);
        fields.put("metric", Types.STRING);
        fields.put("labels", Types.MAP(Types.STRING, Types.STRING));
        fields.put("value", Types.DOUBLE);
        fields.put("timestampMs", Types.LONG);
        fields.put("sourceDc", Types.STRING);
        fields.put("ingestCity", Types.STRING);
        fields.put("ingestDc", Types.STRING);
        fields.put("ingestTimeMs", Types.LONG);
        fields.put("traceparent", Types.STRING);
        return Types.POJO(SampleWithMeta.class, fields);
    }

    /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
    public static class Factory extends TypeInfoFactory<SampleWithMeta> {
        @Override
        public TypeInformation<SampleWithMeta> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return typeInfo();
        }
    }
}