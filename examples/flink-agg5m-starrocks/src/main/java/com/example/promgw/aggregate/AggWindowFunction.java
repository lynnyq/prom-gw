package com.example.promgw.aggregate;

import com.example.promgw.util.LabelsHasher;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AggWindowFunction 窗口触发时把 MetricAggState 组装为 AggResult。
 *
 * 计算内容:
 *   - ts:           窗口起始时间(UTC+8)
 *   - business:     从 labels 提取("team" 或 "tenant" 字段)
 *   - labelsHash:   labels 的 XXH3 hash(与 prom-gw 端对齐)
 *   - valueAvg:     sum / count
 *   - valueP50/P99: 桶内排序精确计算
 */
public class AggWindowFunction
        extends ProcessWindowFunction<MetricAggState, AggResult, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(AggWindowFunction.class);

    /** 北京时区(UTC+8) */
    private static final ZoneId BJ_ZONE = ZoneId.of("Asia/Shanghai");

    @Override
    public void process(String seriesKey,
                         Context ctx,
                         Iterable<MetricAggState> states,
                         Collector<AggResult> out) throws Exception {
        MetricAggState s = states.iterator().next();
        if (s == null || s.count == 0) {
            return;
        }

        AggResult r = new AggResult();
        // 窗口起始时间(UTC+8)
        r.setTs(toBJDate(ctx.window().getStart()));
        r.setMetric(s.metric);
        r.setTenant(s.tenant);
        r.setBusiness(extractBusiness(s.labels, s.tenant));
        r.setIngestCity(s.ingestCity);
        r.setSourceDc(s.sourceDc);
        r.setLabelsHash(LabelsHasher.hash(s.labels));
        r.setLabels(s.labels);
        r.setSampleCount(s.count);
        r.setValueSum(s.sum);
        r.setValueMax(Double.isInfinite(s.max) ? null : s.max);
        r.setValueMin(Double.isInfinite(s.min) ? null : s.min);
        r.setValueAvg(s.count > 0 ? s.sum / s.count : 0.0);

        // p50/p99(已排序)
        if (!s.samples.isEmpty()) {
            r.setValueP50(MetricAggFunction.percentile(s.samples, 0.50));
            r.setValueP99(MetricAggFunction.percentile(s.samples, 0.99));
        }

        r.setIngestTime(new Date());

        if (LOG.isDebugEnabled()) {
            LOG.debug("agg emitted: metric={}, tenant={}, count={}, ts={}",
                    r.getMetric(), r.getTenant(), r.getSampleCount(), r.getTs());
        }
        out.collect(r);
    }

    /** extractBusiness 从 labels 提取业务标识,优先级:team > service > tenant。 */
    private static String extractBusiness(Map<String, String> labels, String tenant) {
        if (labels != null) {
            String team = labels.get("team");
            if (team != null && !team.isEmpty()) return team;
            String service = labels.get("service");
            if (service != null && !service.isEmpty()) return service;
        }
        return tenant != null ? tenant : "unknown";
    }

    /** toBJDate 把毫秒时间戳转成北京时区的 Date。 */
    private static Date toBJDate(long epochMs) {
        ZonedDateTime bjt = Instant.ofEpochMilli(epochMs).atZone(BJ_ZONE);
        return Date.from(bjt.toInstant());
    }
}
