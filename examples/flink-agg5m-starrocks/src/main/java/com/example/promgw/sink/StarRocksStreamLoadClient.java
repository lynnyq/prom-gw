package com.example.promgw.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StarRocksStreamLoadClient StarRocks Stream Load HTTP 客户端。
 *
 * Stream Load 接口:HTTP PUT 到 /api/<db>/<table>/_stream_load
 *   - Label 必须全局唯一(同 label 重试会幂等去重,at-least-once 语义)
 *   - 支持 gzip 压缩(Content-Encoding: gzip),跨城带宽减半
 *   - PK 模型表自动按主键去重
 */
public class StarRocksStreamLoadClient {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadClient.class);

    private final String loadUrl;
    private final String authHeader;
    private final RequestConfig requestConfig;

    /** 默认超时 60s(Stream Load 大批量写入可能较慢) */
    private static final int DEFAULT_TIMEOUT_MS = 60_000;

    /**
     * @param feHost  StarRocks FE VIP host
     * @param port    Stream Load 端口(通常 8070 或 8030)
     * @param db      数据库名
     * @param table   表名
     * @param user    用户名
     * @param password 密码(可为空)
     */
    public StarRocksStreamLoadClient(String feHost, int port,
                                      String db, String table,
                                      String user, String password) {
        this.loadUrl = String.format("http://%s:%d/api/%s/%s/_stream_load",
                feHost, port, db, table);
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + (password == null ? "" : password)).getBytes(StandardCharsets.UTF_8));
        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(DEFAULT_TIMEOUT_MS)
                .setSocketTimeout(DEFAULT_TIMEOUT_MS)
                .build();
    }

    /**
     * load 提交一批数据到 StarRocks。
     *
     * @param label    全局唯一 label(同 label 重试幂等)
     * @param jsonBody JSON 格式数据(strip_outer_array=true)
     * @param gzip     是否启用 gzip 压缩(跨城建议 true)
     * @return StarRocks 返回的 JSON 响应
     * @throws IOException HTTP 非 200 或网络错误
     */
    public String load(String label, String jsonBody, boolean gzip) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setConfig(requestConfig);
            put.setHeader("Authorization", authHeader);
            put.setHeader("Label", label);
            put.setHeader("Format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("Expect", "100-continue");

            byte[] body = jsonBody.getBytes(StandardCharsets.UTF_8);
            if (gzip) {
                body = gzipCompress(body);
                put.setHeader("Content-Encoding", "gzip");
            }
            put.setEntity(new ByteArrayEntity(body));

            try (CloseableHttpResponse resp = client.execute(put)) {
                String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                int code = resp.getStatusLine().getStatusCode();
                if (code != 200) {
                    throw new IOException("Stream Load failed: HTTP " + code + ", resp=" + result);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stream Load ok: label={}, respLen={}", label, result.length());
                }
                return result;
            }
        }
    }

    /** gzipCompress gzip 压缩字节数组。 */
    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length / 2);
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }
}
