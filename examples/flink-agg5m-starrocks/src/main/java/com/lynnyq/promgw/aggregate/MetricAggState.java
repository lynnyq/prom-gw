package com.lynnyq.promgw.aggregate;

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
 * MetricAggState 窗口聚合累加器状态。
 *
 * 5min 窗口内同一 series 的所有 sample 累加到此结构,
 * 窗口触发时计算 avg/p50/p99 等聚合值。
 *
 * 实现 {@link Serializable}:作为 AggregateFunction 的累加器,Flink 会在
 * checkpoint 时序列化窗口状态,累加器必须可序列化。
 */
@TypeInfo(MetricAggState.Factory.class)
public class MetricAggState implements Serializable {

    private static final long serialVersionUID = 1L;
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
    public String business;
    public String metric;
    public Map<String, String> labels;
    public String sourceDc;
    public String ingestCity;

    /** reset 重置累加器(复用对象,减少 GC)。 */
    public void reset() {
        count = 0;
        sum = 0.0;
        max = Double.NEGATIVE_INFINITY;
        min = Double.POSITIVE_INFINITY;
        samples.clear();
        business = null;
        metric = null;
        labels = null;
        sourceDc = null;
        ingestCity = null;
    }

    /**
     * typeInfo 显式声明 MetricAggState 的 Flink 序列化器。
     *
     * MetricAggState 是窗口聚合累加器,checkpoint 时由状态后端序列化。
     * Flink 自动分析 POJO 时,samples(List)/labels(Map)接口字段会被推断为
     * GenericTypeInfo 并落入 Kryo,JDK 17+ 强封装下 Kryo 初始化即抛
     * InaccessibleObjectException。显式声明后全部使用 Flink 原生序列化器。
     */
    public static TypeInformation<MetricAggState> typeInfo() {
        Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
        fields.put("count", Types.LONG);
        fields.put("sum", Types.DOUBLE);
        fields.put("max", Types.DOUBLE);
        fields.put("min", Types.DOUBLE);
        fields.put("samples", Types.LIST(Types.DOUBLE));
        fields.put("business", Types.STRING);
        fields.put("metric", Types.STRING);
        fields.put("labels", Types.MAP(Types.STRING, Types.STRING));
        fields.put("sourceDc", Types.STRING);
        fields.put("ingestCity", Types.STRING);
        return Types.POJO(MetricAggState.class, fields);
    }

    /** Factory 供 Flink TypeExtractor 自动推导类型时复用 {@link #typeInfo()}。 */
    public static class Factory extends TypeInfoFactory<MetricAggState> {
        @Override
        public TypeInformation<MetricAggState> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return typeInfo();
        }
    }
}