package com.example.promgw.sink;

import com.example.promgw.aggregate.AggResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StarRocksSink 把 AggResult 通过 Stream Load 写入 StarRocks。
 *
 * 特性:
 *   - 批量:单个 AggResult 一条 Stream Load(5min 窗口触发,频率不高)
 *     如需更高吞吐,可改为攒批后批量提交
 *   - 重试:最多 3 次,指数退避(1s/2s/4s)
 *   - DLQ:最终失败写回本城 Kafka DLQ topic,由运维工具重放
 *   - Label:全局唯一,格式 <city>_<windowStart>_<business>_<labelsHashShort>
 *     同窗口重试 → 同 label,StarRocks 自动去重(at-least-once)
 */
public class StarRocksSink extends RichSinkFunction<AggResult> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSink.class);

    private static final int MAX_RETRY = 3;
    private static final long BASE_BACKOFF_MS = 1000L;

    private final String feHost;
    private final int fePort;
    private final String db;
    private final String table;
    private final String user;
    private final String password;
    private final boolean gzip;
    private final String labelPrefix;
    private final DlqHandler dlqHandler;

    private transient StarRocksStreamLoadClient client;
    private transient ObjectMapper mapper;
    private transient SimpleDateFormat labelFmt;

    public StarRocksSink(String feHost, int fePort, String db, String table,
                          String user, String password, boolean gzip,
                          String labelPrefix, DlqHandler dlqHandler) {
        this.feHost = feHost;
        this.fePort = fePort;
        this.db = db;
        this.table = table;
        this.user = user;
        this.password = password;
        this.gzip = gzip;
        this.labelPrefix = labelPrefix;
        this.dlqHandler = dlqHandler;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new StarRocksStreamLoadClient(feHost, fePort, db, table, user, password);
        mapper = new ObjectMapper();
        labelFmt = new SimpleDateFormat("yyyyMMdd_HHmm");
        if (dlqHandler != null) {
            dlqHandler.open();
        }
        LOG.info("StarRocksSink opened: url=http://{}:{}/api/{}/{}", feHost, fePort, db, table);
    }

    @Override
    public void invoke(AggResult result, Context context) throws Exception {
        String json = toJson(result);
        String label = buildLabel(result);

        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            try {
                String resp = client.load(label, json, gzip);
                LOG.debug("Stream Load success: label={}, resp={}", label, resp);
                return;
            } catch (Exception e) {
                if (attempt < MAX_RETRY) {
                    long backoff = BASE_BACKOFF_MS * (1L << attempt);
                    LOG.warn("Stream Load retry {}/{}: label={}, err={}", attempt + 1, MAX_RETRY, label, e.getMessage());
                    Thread.sleep(backoff);
                } else {
                    LOG.error("Stream Load finally failed, sending to DLQ: label={}", label, e);
                    sendToDlq(result, label, e.getMessage());
                    return;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (dlqHandler != null) {
                dlqHandler.close();
            }
            super.close();
        }
    }

    /** toJson 把 AggResult 转为 StarRocks Stream Load 期望的 JSON。 */
    private String toJson(AggResult r) throws Exception {
        // 用 LinkedHashMap 保证字段顺序与 DDL 一致
        Map<String, Object> m = new LinkedHashMap<>(16);
        m.put("ts", formatTs(r.getTs()));
        m.put("metric", r.getMetric());
        m.put("tenant", r.getTenant());
        m.put("business", r.getBusiness());
        m.put("ingest_city", r.getIngestCity());
        m.put("source_dc", r.getSourceDc());
        m.put("labels_hash", r.getLabelsHash());
        m.put("labels", r.getLabels());
        m.put("sample_count", r.getSampleCount());
        m.put("value_sum", r.getValueSum());
        m.put("value_max", r.getValueMax());
        m.put("value_min", r.getValueMin());
        m.put("value_avg", r.getValueAvg());
        m.put("value_p50", r.getValueP50());
        m.put("value_p99", r.getValueP99());
        m.put("ingest_time", formatTs(r.getIngestTime()));
        return mapper.writeValueAsString(m);
    }

    /**
     * buildLabel 生成全局唯一 label,格式:<prefix>_<yyyyMMddHHmm>_<business>_<labelsHash>。
     *
     * label 必须在同窗口内对不同 series 唯一,否则 StarRocks 会按 label 幂等去重,
     * 导致同 label 的第二个 Stream Load 被拒绝 → 静默丢数。
     * 使用完整 16 字符 labels_hash(64-bit SHA-1)而非截断前 8 字符(32-bit),
     * 将同 business 内碰撞概率从 ~N²/2³³ 降至 ~N²/2⁶³(N=series 数)。
     */
    /** buildLabel 生成全局唯一 label(package-private 便于测试)。 */
    String buildLabel(AggResult r) {
        String ts = labelFmt.format(r.getTs());
        String hash = r.getLabelsHash() != null && !r.getLabelsHash().isEmpty()
                ? r.getLabelsHash() : "0000000000000000";
        return String.format("%s_%s_%s_%s", labelPrefix, ts, r.getBusiness(), hash);
    }

    private void sendToDlq(AggResult result, String label, String error) throws Exception {
        if (dlqHandler == null) {
            LOG.warn("no DLQ handler configured, dropping failed batch: label={}", label);
            return;
        }
        String payload = mapper.writeValueAsString(result);
        dlqHandler.send(label, payload, error);
    }

    private static String formatTs(Date d) {
        if (d == null) return null;
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d);
    }
}
