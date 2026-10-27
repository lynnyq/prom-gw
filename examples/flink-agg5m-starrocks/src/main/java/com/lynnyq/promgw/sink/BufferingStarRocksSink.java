package com.lynnyq.promgw.sink;

import com.lynnyq.promgw.aggregate.AggResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
 * sysid 表路由(核心特性):
 *   - 每条 AggResult 携带 sysid(从 Prometheus 公共标签提取)
 *   - 根据 sysidTableMap(sysid → 表名)把数据写入不同 StarRocks 表
 *   - 每个表维护独立的 buffer 与 StarRocksStreamLoadClient
 *   - sysid 不在映射表中时,fallback 到 defaultTable;若 defaultTable 也为空则丢弃并计数
 *   - 新增 S 系统只需在 sysidTableMap 中追加 sysid=表名 映射,无需改代码
 *
 * flush 触发条件:
 *   1. 任一表的 batch 行数达到 batchSize(默认 500)
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
    /** 单表模式下表名;sysid 路由模式下作为 defaultTable 的 fallback */
    private final String table;
    private final String user;
    private final String password;
    private final boolean gzip;
    private final String labelPrefix;
    private final DlqHandler dlqHandler;
    private final int batchSize;
    private final long batchIntervalMs;
    /** sysid → StarRocks 表名 映射;为空时退化为单表模式(全部写入 table) */
    private final Map<String, String> sysidTableMap;
    /** sysid 不在 sysidTableMap 中时的兜底表;为空则丢弃未知 sysid 的数据 */
    private final String defaultTable;

    // --- 运行时状态(transient,open 时初始化) ---
    /** 单表模式下的 client(sysidTableMap 为空时使用) */
    private transient StarRocksStreamLoadClient client;
    /** 多表模式下的 client 缓存:table → client */
    private transient Map<String, StarRocksStreamLoadClient> clientMap;
    private transient ObjectMapper mapper;
    private transient SimpleDateFormat batchLabelFmt;  // batch label 时间格式
    private transient SimpleDateFormat tsFmt;           // 数据 ts 格式(与 StarRocksSink 对齐)
    /** 单表模式下的 buffer */
    private transient List<AggResult> buffer;
    /** 多表模式下的 buffer:table → buffer */
    private transient Map<String, List<AggResult>> bufferMap;
    private transient long lastFlushTime;
    private transient long batchSeq;
    private transient int taskIndex;
    private transient Counter flushSuccessCounter;
    private transient Counter flushFailureCounter;
    private transient Counter dlqRowsCounter;
    private transient Counter dlqDropRowsCounter;
    private transient Counter unknownSysidDropCounter;

    // --- checkpoint state ---
    private transient ListState<AggResult> checkpointBuffer;

    /**
     * 单表模式构造器(向后兼容)。
     * 所有数据写入固定 table,不做 sysid 路由。
     */
    public BufferingStarRocksSink(
            String srHost, int srPort, String db, String table,
            String user, String password, boolean gzip,
            String labelPrefix, DlqHandler dlqHandler,
            int batchSize, long batchIntervalMs) {
        this(srHost, srPort, db, table, user, password, gzip, labelPrefix, dlqHandler,
                batchSize, batchIntervalMs, null, null);
    }

    /**
     * sysid 表路由构造器。
     *
     * @param sysidTableMap sysid → 表名映射;为 null 或空时退化为单表模式
     * @param defaultTable  sysid 不在映射表中时的兜底表;为 null/空时丢弃未知 sysid 数据
     */
    public BufferingStarRocksSink(
            String srHost, int srPort, String db, String table,
            String user, String password, boolean gzip,
            String labelPrefix, DlqHandler dlqHandler,
            int batchSize, long batchIntervalMs,
            Map<String, String> sysidTableMap, String defaultTable) {
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
        this.sysidTableMap = sysidTableMap != null ? sysidTableMap : new HashMap<>();
        this.defaultTable = defaultTable;
    }

    /** isMultiTable 是否为 sysid 多表路由模式。 */
    private boolean isMultiTable() {
        return !sysidTableMap.isEmpty();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = new ObjectMapper();
        batchLabelFmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        tsFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        unknownSysidDropCounter = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("srUnknownSysidDropRows");

        if (isMultiTable()) {
            clientMap = new HashMap<>();
            bufferMap = new HashMap<>();
            // 预创建已知 sysid 对应的 client 与 buffer
            for (Map.Entry<String, String> e : sysidTableMap.entrySet()) {
                String tbl = e.getValue();
                if (!clientMap.containsKey(tbl)) {
                    clientMap.put(tbl, new StarRocksStreamLoadClient(srHost, srPort, db, tbl, user, password));
                }
                if (!bufferMap.containsKey(tbl)) {
                    bufferMap.put(tbl, new ArrayList<AggResult>(batchSize));
                }
            }
            // defaultTable 也预创建(若配置)
            if (defaultTable != null && !defaultTable.isEmpty() && !clientMap.containsKey(defaultTable)) {
                clientMap.put(defaultTable, new StarRocksStreamLoadClient(srHost, srPort, db, defaultTable, user, password));
                bufferMap.put(defaultTable, new ArrayList<AggResult>(batchSize));
            }
            LOG.info("BufferingStarRocksSink opened (multi-table): host={}:{}, db={}, sysidCount={}, defaultTable={}, batchSize={}, taskIndex={}",
                    srHost, srPort, db, sysidTableMap.size(), defaultTable, batchSize, taskIndex);
        } else {
            client = new StarRocksStreamLoadClient(srHost, srPort, db, table, user, password);
            buffer = new ArrayList<>(batchSize);
            LOG.info("BufferingStarRocksSink opened (single-table): host={}:{}, table={}.{}, batchSize={}, batchIntervalMs={}, taskIndex={}",
                    srHost, srPort, db, table, batchSize, batchIntervalMs, taskIndex);
        }

        if (dlqHandler != null) {
            dlqHandler.open();
        }
    }

    @Override
    public void invoke(AggResult result, Context context) throws Exception {
        if (isMultiTable()) {
            String targetTable = resolveTable(result);
            if (targetTable == null) {
                // sysid 不在映射表中且无 defaultTable → 丢弃
                unknownSysidDropCounter.inc();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("drop row: unknown sysid={}, no default table configured", result.getSysid());
                }
                return;
            }
            List<AggResult> tblBuffer = bufferMap.get(targetTable);
            if (tblBuffer == null) {
                // 运行时新出现的 table(理论上不会,因为 resolveTable 只返回已知 table),惰性创建
                tblBuffer = new ArrayList<>(batchSize);
                bufferMap.put(targetTable, tblBuffer);
                if (!clientMap.containsKey(targetTable)) {
                    clientMap.put(targetTable, new StarRocksStreamLoadClient(srHost, srPort, db, targetTable, user, password));
                }
            }
            tblBuffer.add(result);

            // 任一表达到 batchSize 或全局超时 → flush 所有表
            boolean sizeReached = tblBuffer.size() >= batchSize;
            boolean timeElapsed = (System.currentTimeMillis() - lastFlushTime) >= batchIntervalMs;
            if (sizeReached || timeElapsed) {
                flush();
            }
        } else {
            buffer.add(result);
            boolean sizeReached = buffer.size() >= batchSize;
            boolean timeElapsed = (System.currentTimeMillis() - lastFlushTime) >= batchIntervalMs;
            if (sizeReached || timeElapsed) {
                flush();
            }
        }
    }

    /**
     * resolveTable 根据 AggResult.sysid 解析目标 StarRocks 表名。
     *
     * @return 目标表名;若 sysid 不在映射中且无 defaultTable 则返回 null
     */
    String resolveTable(AggResult r) {
        String sysid = r.getSysid();
        if (sysid != null && !sysid.isEmpty()) {
            String tbl = sysidTableMap.get(sysid);
            if (tbl != null && !tbl.isEmpty()) {
                return tbl;
            }
        }
        // fallback 到 defaultTable
        if (defaultTable != null && !defaultTable.isEmpty()) {
            return defaultTable;
        }
        return null;
    }

    /**
     * flush 把所有 buffer(单表或多表)中的行作为 JSON 数组发送到 StarRocks。
     *
     * 失败处理:
     *   1. 重试 MAX_RETRY 次(指数退避 1s/2s/4s)
     *   2. 最终失败 → 按行发 DLQ(用行级 buildRowLabel)
     *   3. DLQ 也失败 → 抛异常 → Flink 从 last checkpoint 重启
     */
    private void flush() throws Exception {
        if (isMultiTable()) {
            for (Map.Entry<String, List<AggResult>> entry : bufferMap.entrySet()) {
                String tbl = entry.getKey();
                List<AggResult> tblBuffer = entry.getValue();
                if (tblBuffer == null || tblBuffer.isEmpty()) {
                    continue;
                }
                StarRocksStreamLoadClient tblClient = clientMap.get(tbl);
                flushBuffer(tblClient, tblBuffer, tbl);
            }
        } else {
            if (buffer == null || buffer.isEmpty()) {
                return;
            }
            flushBuffer(client, buffer, table);
        }
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * flushBuffer 把单个 buffer 发送到指定 client 对应的表。
     *
     * @param tblClient 目标表的 Stream Load client
     * @param buf       待发送的行
     * @param tblName   表名(仅用于日志 / label)
     */
    private void flushBuffer(StarRocksStreamLoadClient tblClient, List<AggResult> buf, String tblName) throws Exception {
        if (buf.isEmpty()) {
            return;
        }

        // 构建 JSON 数组:[{...},{...},...]
        StringBuilder sb = new StringBuilder(buf.size() * 256);
        sb.append("[");
        for (int i = 0; i < buf.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(toJson(buf.get(i)));
        }
        sb.append("]");
        String json = sb.toString();

        // 生成唯一 batch label(含表名,避免多表 label 冲突)
        String label = buildBatchLabel(tblName);
        int rowCount = buf.size();

        // 重试循环
        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            try {
                String resp = tblClient.load(label, json, gzip);
                LOG.debug("Stream Load batch success: table={}, label={}, rows={}", tblName, label, rowCount);
                flushSuccessCounter.inc();
                buf.clear();
                return;
            } catch (Exception e) {
                if (attempt < MAX_RETRY) {
                    long backoff = BASE_BACKOFF_MS * (1L << attempt);
                    LOG.warn("Stream Load batch retry {}/{}: table={}, label={}, rows={}, err={}",
                            attempt + 1, MAX_RETRY, tblName, label, rowCount, e.getMessage());
                    Thread.sleep(backoff);
                } else {
                    flushFailureCounter.inc();
                    LOG.error("Stream Load batch finally failed, sending to DLQ: table={}, label={}, rows={}",
                            tblName, label, rowCount, e);
                    sendRowsToDlq(buf, tblName, e.getMessage());
                    buf.clear();
                    return;
                }
            }
        }
    }

    /**
     * sendRowsToDlq 把指定 buffer 中每行单独发到 DLQ。
     * 用行级 label(含完整 labels_hash),DLQ 消费者可按行重试。
     *
     * 容错策略:
     *   逐行 try/catch,单行 DLQ 发送失败仅记 ERROR + srDlqDropRows 计数器,
     *   不向上抛异常。DLQ 是兜底通道,其可用性通过 srDlqDropRows 指标告警保障,不应阻塞写入。
     */
    private void sendRowsToDlq(List<AggResult> buf, String tblName, String error) throws Exception {
        if (dlqHandler == null) {
            LOG.warn("no DLQ handler configured, dropping {} failed rows", buf.size());
            dlqRowsCounter.inc(buf.size());
            return;
        }
        int dropped = 0;
        for (AggResult r : buf) {
            String rowLabel = buildRowLabel(r);
            String rowJson = mapper.writeValueAsString(r);
            try {
                dlqHandler.send(rowLabel, rowJson, tblName, error);
                dlqRowsCounter.inc();
            } catch (Exception dlqErr) {
                dropped++;
                LOG.error("DLQ send failed, row dropped (watch srDlqDropRows metric): label={}",
                        rowLabel, dlqErr);
            }
        }
        if (dropped > 0) {
            dlqDropRowsCounter.inc(dropped);
            LOG.error("DLQ unavailable, dropped {}/{} rows in this batch", dropped, buf.size());
        }
    }

    // --- CheckpointedFunction ---

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // checkpoint 前强制 flush,确保 buffer 中数据已写入 StarRocks 或 DLQ
        flush();
        // flush 成功后 buffer 为空,清空 state
        checkpointBuffer.clear();
        // 保存当前未 flush 的行(flush 后通常为空,除非 flush 全部走了 DLQ)
        if (isMultiTable()) {
            for (List<AggResult> buf : bufferMap.values()) {
                for (AggResult r : buf) {
                    checkpointBuffer.add(r);
                }
            }
        } else {
            for (AggResult r : buffer) {
                checkpointBuffer.add(r);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<AggResult> desc = new ListStateDescriptor<>(
                "bufferingStarRocksSinkBuffer", TypeInformation.of(AggResult.class));
        checkpointBuffer = context.getOperatorStateStore().getListState(desc);

        // 从 checkpoint 恢复(上次 checkpoint 时已 flush,buffer 通常为空)
        if (context.isRestored()) {
            int restored = 0;
            // 单表模式:全部塞进 buffer;多表模式:按 sysid 重新分桶
            List<AggResult> restoredRows = new ArrayList<>();
            for (AggResult r : checkpointBuffer.get()) {
                restoredRows.add(r);
                restored++;
            }
            if (restored > 0) {
                if (isMultiTable()) {
                    for (AggResult r : restoredRows) {
                        String tbl = resolveTable(r);
                        if (tbl != null) {
                            List<AggResult> buf = bufferMap.get(tbl);
                            if (buf == null) {
                                buf = new ArrayList<>(batchSize);
                                bufferMap.put(tbl, buf);
                                if (!clientMap.containsKey(tbl)) {
                                    clientMap.put(tbl, new StarRocksStreamLoadClient(srHost, srPort, db, tbl, user, password));
                                }
                            }
                            buf.add(r);
                        }
                    }
                } else {
                    buffer.addAll(restoredRows);
                }
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
            if (clientMap != null) {
                for (StarRocksStreamLoadClient c : clientMap.values()) {
                    try { c.close(); } catch (Exception e) { LOG.warn("close client failed", e); }
                }
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
     * 格式:<prefix>_<table>_<yyyyMMddHHmmssSSS>_<taskIndex>_<batchSeq>
     * - table:区分不同表的 label(多表模式下必需)
     * - taskIndex:不同 subtask 不碰撞
     * - 毫秒时间戳:跨重启不碰撞
     * - batchSeq:同毫秒内多次 flush 不碰撞
     */
    String buildBatchLabel(String tblName) {
        String ts = batchLabelFmt.format(new Date());
        batchSeq++;
        return String.format("%s_%s_%s_%d_%d", labelPrefix, tblName, ts, taskIndex, batchSeq);
    }

    /**
     * buildRowLabel 生成行级 label,用于 DLQ。
     * 与 StarRocksSink.buildLabel 格式一致,含完整 labels_hash。
     */
    String buildRowLabel(AggResult r) {
        String ts = new SimpleDateFormat("yyyyMMdd_HHmm").format(r.getTs());
        String hash = r.getLabelsHash() != null && !r.getLabelsHash().isEmpty()
                ? r.getLabelsHash() : "0000000000000000";
        String sysid = r.getSysid() != null ? r.getSysid() : "unknown";
        return String.format("%s_%s_%s_%s_%s", labelPrefix, ts, sysid, r.getBusiness(), hash);
    }

    /** toJson 把 AggResult 转为 StarRocks Stream Load 期望的 JSON(与 StarRocksSink 对齐)。 */
    private String toJson(AggResult r) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>(16);
        m.put("ts", tsFmt.format(r.getTs()));
        m.put("metric", r.getMetric());
        m.put("business", r.getBusiness());
        m.put("ingest_city", r.getIngestCity());
        m.put("source_dc", r.getSourceDc());
        m.put("sysid", r.getSysid());
        m.put("labels_hash", r.getLabelsHash());
        m.put("labels", r.getLabels());
        m.put("sample_count", r.getSampleCount());
        m.put("value_sum", r.getValueSum());
        m.put("value_max", r.getValueMax());
        m.put("value_min", r.getValueMin());
        m.put("value_avg", r.getValueAvg());
        m.put("value_p50", r.getValueP50());
        m.put("value_p99", r.getValueP99());
        return mapper.writeValueAsString(m);
    }
}
