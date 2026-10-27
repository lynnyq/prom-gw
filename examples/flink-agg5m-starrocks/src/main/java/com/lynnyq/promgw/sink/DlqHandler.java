package com.lynnyq.promgw.sink;

import java.io.Serializable;

/**
 * DlqHandler DLQ 处理器接口,由 DlqSink 实现。
 *
 * 分离接口的原因:StarRocksSink 依赖 DLQ 能力,但 DLQ 具体实现
 * (写 Kafka / 写文件 / 写告警)可替换,便于测试。
 *
 * 实现 {@link Serializable}:Flink 需要把 Sink 闭包序列化后分发到 TaskManager,
 * DlqHandler 作为 StarRocksSink 的字段也必须可序列化。
 */
public interface DlqHandler extends Serializable {

    /** open 初始化 DLQ 资源(在 Sink open 时调用)。 */
    void open() throws Exception;

    /**
     * send 把失败的消息发到 DLQ。
     *
     * @param label   Stream Load label(重放时复用,保证幂等)
     * @param payload 原始 AggResult 的 JSON 字符串
     * @param table   目标 StarRocks 表名(重放工具据此路由到对应表)
     * @param error   失败原因
     */
    void send(String label, String payload, String table, String error) throws Exception;

    /** close 释放资源。 */
    void close() throws Exception;
}
