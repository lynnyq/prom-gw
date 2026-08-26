package com.example.promgw.aggregate;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;

/**
 * AggResult 5min 聚合结果,对应 StarRocks sr_bj_metrics_5m 表一行。
 *
 * 字段顺序与 StarRocks DDL 对齐:
 *   ts, metric, tenant, business, ingest_city, source_dc, labels_hash,
 *   labels, sample_count, value_sum, value_max, value_min, value_avg,
 *   value_p50, value_p99, ingest_time
 *
 * 实现 {@link Serializable}:本类用于 BufferingStarRocksSink 的 ListState<AggResult>
 * checkpoint 状态,Flink checkpoint 时会序列化状态对象。
 */
@TypeInfo(AggResult.Factory.class)
public class AggResult implements Serializable {

    private static final long serialVersionUID = 1L;
    /** 5min 窗口起始时间(UTC+8) */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private Date ts;
    private String metric;
    private String tenant;
    private String business;
    private String ingestCity;
    private String sourceDc;
    /** labels 的 XXH3 hash,作 PK 键列 */
    private String labelsHash;
    /** 原始 labels(非键列,仅查询用) */
    private Map<String, String> labels;
    private long sampleCount;
    private double valueSum;
    private Double valueMax;
    private Double valueMin;
    private Double valueAvg;
    private Double valueP50;
    private Double valueP99;
    /** 入 StarRocks 时间(DLQ 重放去重用) */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private Date ingestTime;

    // Flink POJO 需要无参构造
    public AggResult() {}

    public Date getTs() { return ts; }
    public void setTs(Date ts) { this.ts = ts; }

    public String getMetric() { return metric; }
    public void setMetric(String metric) { this.metric = metric; }

    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }

    public String getBusiness() { return business; }
    public void setBusiness(String business) { this.business = business; }

    public String getIngestCity() { return ingestCity; }
    public void setIngestCity(String ingestCity) { this.ingestCity = ingestCity; }

    public String getSourceDc() { return sourceDc; }
    public void setSourceDc(String sourceDc) { this.sourceDc = sourceDc; }

    public String getLabelsHash() { return labelsHash; }
    public void setLabelsHash(String labelsHash) { this.labelsHash = labelsHash; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    public long getSampleCount() { return sampleCount; }
    public void setSampleCount(long sampleCount) { this.sampleCount = sampleCount; }

    public double getValueSum() { return valueSum; }
    public void setValueSum(double valueSum) { this.valueSum = valueSum; }

    public Double getValueMax() { return valueMax; }
    public void setValueMax(Double valueMax) { this.valueMax = valueMax; }

    public Double getValueMin() { return valueMin; }
    public void setValueMin(Double valueMin) { this.valueMin = valueMin; }

    public Double getValueAvg() { return valueAvg; }
    public void setValueAvg(Double valueAvg) { this.valueAvg = valueAvg; }

    public Double getValueP50() { return valueP50; }
    public void setValueP50(Double valueP50) { this.valueP50 = valueP50; }

    public Double getValueP99() { return valueP99; }
    public void setValueP99(Double valueP99) { this.valueP99 = valueP99; }

    public Date getIngestTime() { return ingestTime; }
    public void setIngestTime(Date ingestTime) { this.ingestTime = ingestTime; }

    /**
     * typeInfo 显式声明 AggResult 的 Flink 序列化器。
     *
     * AggResult 既在算子间传输,也作为 BufferingStarRocksSink 的 ListState 状态。
     * Flink 自动分析 POJO 时,labels(Map 接口字段)会被推断为 GenericTypeInfo
     * 并落入 Kryo,JDK 17+ 强封装下 Kryo 初始化即抛 InaccessibleObjectException。
     * 显式声明后全部使用 Flink 原生序列化器(Date 用 BasicTypeInfo)。
     */
    public static TypeInformation<AggResult> typeInfo() {
        Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
        fields.put("ts", BasicTypeInfo.DATE_TYPE_INFO);
        fields.put("metric", Types.STRING);
        fields.put("tenant", Types.STRING);
        fields.put("business", Types.STRING);
        fields.put("ingestCity", Types.STRING);
        fields.put("sourceDc", Types.STRING);
        fields.put("labelsHash", Types.STRING);
        fields.put("labels", Types.MAP(Types.STRING, Types.STRING));
        fields.put("sampleCount", Types.LONG);
        fields.put("valueSum", Types.DOUBLE);
        fields.put("valueMax", Types.DOUBLE);
        fields.put("valueMin", Types.DOUBLE);
        fields.put("valueAvg", Types.DOUBLE);
        fields.put("valueP50", Types.DOUBLE);
        fields.put("valueP99", Types.DOUBLE);
        fields.put("ingestTime", BasicTypeInfo.DATE_TYPE_INFO);
        return Types.POJO(AggResult.class, fields);
    }

    /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
    public static class Factory extends TypeInfoFactory<AggResult> {
        @Override
        public TypeInformation<AggResult> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return typeInfo();
        }
    }
}
