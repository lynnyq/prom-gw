package com.example.promgw.sink;

import org.apache.flink.configuration.Configuration;

/**
 * DlqHandler DLQ 处理器接口,由 DlqSink 实现。
 *
 * 分离接口的原因:StarRocksSink 依赖 DLQ 能力,但 DLQ 具体实现
 * (写 Kafka / 写文件 / 写告警)可替换,便于测试。
 */
public interface DlqHandler {

    /** open 初始化 DLQ 资源(在 Sink open 时调用)。 */
    void open() throws Exception;

    /** send 把失败的消息发到 DLQ。 */
    void send(String label, String payload, String error) throws Exception;

    /** close 释放资源。 */
    void close() throws Exception;
}
