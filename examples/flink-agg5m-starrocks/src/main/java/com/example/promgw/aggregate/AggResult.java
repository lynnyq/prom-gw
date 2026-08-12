package com.example.promgw.aggregate;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.util.Date;
import java.util.Map;

/**
 * AggResult 5min 聚合结果,对应 StarRocks sr_bj_metrics_5m 表一行。
 *
 * 字段顺序与 StarRocks DDL 对齐:
 *   ts, metric, tenant, business, ingest_city, source_dc, labels_hash,
 *   labels, sample_count, value_sum, value_max, value_min, value_avg,
 *   value_p50, value_p99, ingest_time
 */
public class AggResult {
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
}
