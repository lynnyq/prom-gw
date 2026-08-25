package com.example.promgw.decoder;

import java.time.Duration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DedupFunction 按 payload hash 去重。
 *
 * 背景(见 PromWriteRequestDecoder 注释):
 *   prom-gw 为 WriteRequest 中每个 sample 产生一条 Kafka 消息,但 N 条消息的 payload
 *   完全相同。若不去重,同一 WriteRequest 会被解码 N 次,造成 N 倍重复处理。
 *
 * 策略:
 *   - keyBy(payloadHash) 让同 payload 落同 subtask
 *   - 用 ValueState<Long> 记录最近处理时间
 *   - 60s 内同 hash 视为重复,跳过(一个 WriteRequest 的 N 条消息通常在毫秒级到达)
 *   - 60s 后清除状态,允许后续新 payload 处理
 */
public class DedupFunction extends KeyedProcessFunction<Integer, KafkaRecord, PromSample> {

    private static final Logger LOG = LoggerFactory.getLogger(DedupFunction.class);

    /** 去重窗口:60s 内同 payload hash 视为重复。 */
    private static final long DEDUP_WINDOW_MS = 60_000L;

    private transient ValueState<Long> lastProcessedTs;
    private transient Counter decodeFailures;
    private final PromWriteRequestDecoder decoder = new PromWriteRequestDecoder();

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>(
                "lastProcessedTs", Types.LONG);
        lastProcessedTs = getRuntimeContext().getState(desc);
        decodeFailures = getRuntimeContext().getMetricGroup()
                .addGroup("promgw").counter("decodeFailures");
    }

    @Override
    public void processElement(KafkaRecord record, Context ctx, Collector<PromSample> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        Long last = lastProcessedTs.value();

        if (last != null && (now - last) < DEDUP_WINDOW_MS) {
            // 60s 内同 payload hash,视为重复,跳过
            if (LOG.isDebugEnabled()) {
                LOG.debug("dup skipped: topic={}, offset={}, hash={}",
                        record.getTopic(), record.getOffset(), ctx.getCurrentKey());
            }
            return;
        }

        // 首次出现或已过期,更新时间戳
        lastProcessedTs.update(now);
        // 注册 60s 后的清理 timer,触发后清除状态
        ctx.timerService().registerProcessingTimeTimer(now + DEDUP_WINDOW_MS);

        // 解码并输出
        PromSample sample;
        try {
            sample = decoder.decode(record.getValue(), record.getHeaders());
        } catch (Exception e) {
            // 修复前:只记 LOG.warn,decode 失败不可见 → 无法告警。
            // 修复后:增加 Flink counter,可在 metric dashboard 上告警 decode 失败率。
            decodeFailures.inc();
            LOG.warn("decode failed, skipping: topic={}, offset={}, err={}",
                    record.getTopic(), record.getOffset(), e.getMessage());
            return;
        }
        if (sample != null) {
            out.collect(sample);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PromSample> out) throws Exception {
        // 清除过期状态,避免长期占用内存
        lastProcessedTs.clear();
    }
}
