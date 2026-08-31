package com.lynnyq.promgw.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;

/**
 * LabelsHasher 计算 labels 的 hash,用作 StarRocks metrics_5m 表主键之一(labels_hash)。
 *
 * 实现:按 label name 排序后拼接,计算 SHA-1,取前 16 个 hex 字符(64 位)。
 * 用 Java 内置 MessageDigest,无需额外依赖。
 *
 * 说明:prom-gw 端文档建议用 SHA-1,Java 侧用 JDK 内置 MessageDigest 实现,
 * SHA-1 碰撞率足够低(< 2^-60),且所有 JDK 内置,部署简单。
 */
public final class LabelsHasher {

    /** hash 长度:16 个 hex 字符 = 64 位 */
    private static final int HASH_HEX_LENGTH = 16;

    private LabelsHasher() {}

    /** hash 计算 labels 的 SHA-1 hash,返回前 16 个 hex 字符。 */
    public static String hash(Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) {
            return "0000000000000000";
        }
        // 按 key 排序,保证相同 labels 顺序不同时 hash 一致
        TreeMap<String, String> sorted = new TreeMap<>(labels);
        StringBuilder sb = new StringBuilder(128);
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append(',');
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            // 取前 8 字节(64 位)→ 16 个 hex 字符
            StringBuilder hex = new StringBuilder(HASH_HEX_LENGTH);
            for (int i = 0; i < 8; i++) {
                hex.append(String.format("%02x", digest[i] & 0xff));
            }
            return hex.toString();
        } catch (Exception e) {
            // SHA-1 是 JDK 内置算法,理论上不会抛异常
            throw new RuntimeException("SHA-1 not available", e);
        }
    }

    /**
     * seriesKey 计算 series 的分组 key(business + metric + sorted labels)。
     * 用于 Flink keyBy,保证同 series 落同 subtask。
     */
    public static String seriesKey(String business, String metric, Map<String, String> labels) {
        StringBuilder sb = new StringBuilder(64);
        sb.append(business).append('/').append(metric).append('/');
        if (labels != null) {
            TreeMap<String, String> sorted = new TreeMap<>(labels);
            for (Map.Entry<String, String> e : sorted.entrySet()) {
                sb.append(e.getKey()).append('=').append(e.getValue()).append(',');
            }
        }
        return sb.toString();
    }
}