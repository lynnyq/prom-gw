package com.example.promgw.sink;

import com.example.promgw.aggregate.AggResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BufferingStarRocksSink 攒批写入 StarRocks,大幅减少 HTTP 请求数。
 *
 * 优化前(StarRocksSink):每条 AggResult 一次 HTTP PUT → 10K series/5min = 10K 请求。
 * 优化后:攒批 N 条 → 单次 Stream Load 发 JSON 数组,请求数降至 1/N。
 *
 * flush 触发条件:
 *   1. batch 行数达到 batchSize(默认 500)
 *   2. 距上次 flush 超过 batchIntervalMs(默认 10s)
 *   3. checkpoint 触发(snapshotState 强制 flush)
 *
 * 容错语义:
 *   - 实现 CheckpointedFunction,snapshotState 时先 flush 再保存空 buffer
 *   - flush 失败 → snapshotState 抛异常 → checkpoint 失败 → 从 last checkpoint 重启
 *   - 重启后 initializeState 恢复 buffer(last checkpoint 时已 flush,buffer 为空)
 *   - Stream Load label 含 taskIndex + 毫秒时间戳 + batchSeq,跨重启不碰撞
 *   - 同 label 重试安全:StarRocks 按 label 幂等去重
 *
 * DLQ 策略:
 *   - 批量 flush 重试 MAX_RETRY 次后仍失败 → 按行发 DLQ(用行级 label)
 *   - DLQ send 同步等待 broker ack(KafkaDlqHandler 已修复为同步)
 */
public class BufferingStarRocksSink extends RichSinkFunction<AggResult>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(BufferingStarRocksSink.class);
    private static final int MAX_RETRY = 3;
    private static final long BASE_BACKOFF_MS = 1000L;

    // --- 配置(不可变,构造时确定) ---
    private final String srHost;
    private final int srPort;
    private final String db;
    private final String table;
    private final String user;
    private final String password;
    private final boolean gzip;
    private final String labelPrefix;
    private final DlqHandler dlqHandler;
    private final int batchSize;
    private final long batchIntervalMs;

    // --- 运行时状态(transient,open 时初始化) ---
    private transient StarRocksStreamLoadClient client;
    private transient ObjectMapper mapper;
    private transient SimpleDateFormat batchLabelFmt;  // batch label 时间格式
    private transient SimpleDateFormat tsFmt;           // 数据 ts 格式(与 StarRocksSink 对齐)
    private transient List<AggResult> buffer;
    private transient long lastFlushTime;
    private transient long batchSeq;
    private transient int taskIndex;
    private transient Counter flushSuccessCounter;
    private transient Counter flushFailureCounter;
    private transient Counter dlqRowsCounter;
    private transient Counter dlqDropRowsCounter;

    // --- checkpoint state ---
    private transient ListState<AggResult> checkpointBuffer;

    public BufferingStarRocksSink(
            String srHost, int srPort, String db, String table,
            String user, String password, boolean gzip,
            String labelPrefix, DlqHandler dlqHandler,
            int batchSize, long batchIntervalMs) {
        this.srHost = srHost;
        this.srPort = srPort;
        this.db = db;
        this.table = table;
        this.user = user;
        this.password = password;
        this.gzip = gzip;
        this.labelPrefix = labelPrefix;
        this.dlqHandler = dlqHandler;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new StarRocksStreamLoadClient(srHost, srPort, db, table, user, password);
        mapper = new ObjectMapper();
        batchLabelFmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        tsFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        buffer = new ArrayList<>(batchSize);
        lastFlushTime = System.currentTimeMillis();
        batchSeq = 0;
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        flushSuccessCounter = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("srFlushSuccess");
        flushFailureCounter = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("srFlushFailure");
        dlqRowsCounter = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("srDlqRows");
        dlqDropRowsCounter = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("srDlqDropRows");

        if (dlqHandler != null) {
            dlqHandler.open();
        }
        LOG.info("BufferingStarRocksSink opened: host={}:{}, table={}.{}, batchSize={}, batchIntervalMs={}, taskIndex={}",
                srHost, srPort, db, table, batchSize, batchIntervalMs, taskIndex);
    }

    @Override
    public void invoke(AggResult result, Context context) throws Exception {
        buffer.add(result);

        boolean sizeReached = buffer.size() >= batchSize;
        boolean timeElapsed = (System.currentTimeMillis() - lastFlushTime) >= batchIntervalMs;

        if (sizeReached || timeElapsed) {
            flush();
        }
    }

    /**
     * flush 把 buffer 中所有行作为单个 JSON 数组发送到 StarRocks。
     *
     * 失败处理:
     *   1. 重试 MAX_RETRY 次(指数退避 1s/2s/4s)
     *   2. 最终失败 → 按行发 DLQ(用行级 buildRowLabel)
     *   3. DLQ 也失败 → 抛异常 → Flink 从 last checkpoint 重启
     */
    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        // 构建 JSON 数组:[{...},{...},...]
        StringBuilder sb = new StringBuilder(buffer.size() * 256);
        sb.append("[");
        for (int i = 0; i < buffer.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(toJson(buffer.get(i)));
        }
        sb.append("]");
        String json = sb.toString();

        // 生成唯一 batch label
        String label = buildBatchLabel();
        int rowCount = buffer.size();

        // 重试循环
        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            try {
                String resp = client.load(label, json, gzip);
                LOG.debug("Stream Load batch success: label={}, rows={}", label, rowCount);
                flushSuccessCounter.inc();
                buffer.clear();
                lastFlushTime = System.currentTimeMillis();
                return;
            } catch (Exception e) {
                if (attempt < MAX_RETRY) {
                    long backoff = BASE_BACKOFF_MS * (1L << attempt);
                    LOG.warn("Stream Load batch retry {}/{}: label={}, rows={}, err={}",
                            attempt + 1, MAX_RETRY, label, rowCount, e.getMessage());
                    Thread.sleep(backoff);
                } else {
                    flushFailureCounter.inc();
                    LOG.error("Stream Load batch finally failed, sending to DLQ: label={}, rows={}",
                            label, rowCount, e);
                    sendRowsToDlq(e.getMessage());
                    buffer.clear();
                    lastFlushTime = System.currentTimeMillis();
                    return;
                }
            }
        }
    }

    /**
     * sendRowsToDlq 把 buffer 中每行单独发到 DLQ。
     * 用行级 label(含完整 labels_hash),DLQ 消费者可按行重试。
     *
     * 容错策略(修复:DLQ 故障不能拖死主链路):
     *   逐行 try/catch,单行 DLQ 发送失败仅记 ERROR + srDlqDropRows 计数器,
     *   不向上抛异常。修复前 send 失败直接传播 → invoke/snapshotState 抛异常 →
     *   checkpoint 失败 → 作业无限重启,主链路完全停摆(线上已发生:
     *   DLQ broker 配错为 localhost:9092,任何一批写入失败即卡死)。
     *   DLQ 是兜底通道,其可用性通过 srDlqDropRows 指标告警保障,不应阻塞写入。
     */
    private void sendRowsToDlq(String error) throws Exception {
        if (dlqHandler == null) {
            LOG.warn("no DLQ handler configured, dropping {} failed rows", buffer.size());
            dlqRowsCounter.inc(buffer.size());
            return;
        }
        int dropped = 0;
        for (AggResult r : buffer) {
            String rowLabel = buildRowLabel(r);
            String rowJson = mapper.writeValueAsString(r);
            try {
                dlqHandler.send(rowLabel, rowJson, error);
                dlqRowsCounter.inc();
            } catch (Exception dlqErr) {
                dropped++;
                LOG.error("DLQ send failed, row dropped (watch srDlqDropRows metric): label={}",
                        rowLabel, dlqErr);
            }
        }
        if (dropped > 0) {
            dlqDropRowsCounter.inc(dropped);
            LOG.error("DLQ unavailable, dropped {}/{} rows in this batch", dropped, buffer.size());
        }
    }

    // --- CheckpointedFunction ---

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // checkpoint 前强制 flush,确保 buffer 中数据已写入 StarRocks 或 DLQ
        flush();
        // flush 成功后 buffer 为空,清空 state
        checkpointBuffer.clear();
        // 保存当前 buffer(flush 后通常为空,除非 flush 全部走了 DLQ)
        for (AggResult r : buffer) {
            checkpointBuffer.add(r);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<AggResult> desc = new ListStateDescriptor<>(
                "bufferingStarRocksSinkBuffer", TypeInformation.of(AggResult.class));
        checkpointBuffer = context.getOperatorStateStore().getListState(desc);

        buffer = new ArrayList<>(batchSize);
        // 从 checkpoint 恢复(上次 checkpoint 时已 flush,buffer 通常为空)
        if (context.isRestored()) {
            int restored = 0;
            for (AggResult r : checkpointBuffer.get()) {
                buffer.add(r);
                restored++;
            }
            if (restored > 0) {
                LOG.warn("restored {} unflushed rows from checkpoint, will flush on first invoke", restored);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            // 关闭前 flush 剩余数据
            flush();
        } finally {
            if (client != null) {
                client.close();
            }
            if (dlqHandler != null) {
                dlqHandler.close();
            }
            super.close();
        }
    }

    // --- helpers ---

    /**
     * buildBatchLabel 生成全局唯一 batch label。
     *
     * 格式:<prefix>_<yyyyMMddHHmmssSSS>_<taskIndex>_<batchSeq>
     * - taskIndex:不同 subtask 不碰撞
     * - 毫秒时间戳:跨重启不碰撞
     * - batchSeq:同毫秒内多次 flush 不碰撞
     */
    String buildBatchLabel() {
        String ts = batchLabelFmt.format(new Date());
        batchSeq++;
        return String.format("%s_%s_%d_%d", labelPrefix, ts, taskIndex, batchSeq);
    }

    /**
     * buildRowLabel 生成行级 label,用于 DLQ。
     * 与 StarRocksSink.buildLabel 格式一致,含完整 labels_hash。
     */
    String buildRowLabel(AggResult r) {
        String ts = new SimpleDateFormat("yyyyMMdd_HHmm").format(r.getTs());
        String hash = r.getLabelsHash() != null && !r.getLabelsHash().isEmpty()
                ? r.getLabelsHash() : "0000000000000000";
        return String.format("%s_%s_%s_%s", labelPrefix, ts, r.getBusiness(), hash);
    }

    /** toJson 把 AggResult 转为 StarRocks Stream Load 期望的 JSON(与 StarRocksSink 对齐)。 */
    private String toJson(AggResult r) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>(16);
        m.put("ts", tsFmt.format(r.getTs()));
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
        m.put("ingest_time", tsFmt.format(r.getIngestTime()));
        return mapper.writeValueAsString(m);
    }
}
