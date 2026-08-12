package com.example.promgw.dlq;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * DlqMessage 写入 Kafka DLQ topic 的消息封装。
 *
 * 字段:
 *   - original:  原始 AggResult 的 JSON 字符串
 *   - label:     对应的 Stream Load label(重放时复用,保证幂等)
 *   - error:     失败原因
 *   - retryCount: 已重试次数(重放工具递增)
 *   - timestamp: 写入 DLQ 的时间
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DlqMessage {
    private String original;
    private String label;
    private String error;
    private int retryCount;
    private long timestamp;

    public DlqMessage() {}

    public DlqMessage(String original, String label, String error, int retryCount, long timestamp) {
        this.original = original;
        this.label = label;
        this.error = error;
        this.retryCount = retryCount;
        this.timestamp = timestamp;
    }

    public String getOriginal() { return original; }
    public void setOriginal(String original) { this.original = original; }

    public String getLabel() { return label; }
    public void setLabel(String label) { this.label = label; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
