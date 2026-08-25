package com.example.promgw.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;
import org.apache.http.Header;
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
 *
 * 重定向处理:StarRocks FE 收到 Stream Load 请求后会返回 HTTP 307
 * Temporary Redirect,通过 Location 响应头指向具体 BE。Apache HttpClient
 * 默认不对 PUT 做 307 跟随(仅 GET/HEAD),因此本客户端手动处理 307:
 * 读取 Location 头后用相同 body 与 headers 向 BE 重新发起 PUT。
 */
public class StarRocksStreamLoadClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadClient.class);

    /** 最大重定向次数(防止循环重定向,StarRocks 正常只有一次 FE→BE) */
    private static final int MAX_REDIRECTS = 5;

    private final String loadUrl;
    private final String authHeader;
    private final RequestConfig requestConfig;
    private final CloseableHttpClient client;
    private final ObjectMapper responseMapper = new ObjectMapper();

    /** 默认超时 60s(Stream Load 大批量写入可能较慢) */
    private static final int DEFAULT_TIMEOUT_MS = 60_000;

    /**
     * @param feHost  StarRocks FE VIP host
     * @param port    FE HTTP 端口(默认 8030,与 fe.conf 的 http_port 一致;
     *                StarRocks 没有 8070 端口,请勿使用)
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
                // 关闭自动重定向:由本客户端手动处理 307,
                // 确保 PUT 方法、body 和 Authorization 头正确转发到 BE
                .setRedirectsEnabled(false)
                .build();
        // 复用 HttpClient(连接池),避免每次 load 都新建 client 导致大量 TIME_WAIT
        // socket 和 DNS 解析开销。Flink sink 是单线程串行调用,无需同步。
        this.client = HttpClients.createDefault();
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
        byte[] body = jsonBody.getBytes(StandardCharsets.UTF_8);
        if (gzip) {
            body = gzipCompress(body);
        }

        String targetUrl = loadUrl;
        for (int redirect = 0; redirect <= MAX_REDIRECTS; redirect++) {
            HttpPut put = buildPutRequest(targetUrl, label, body, gzip);
            try (CloseableHttpResponse resp = client.execute(put)) {
                int code = resp.getStatusLine().getStatusCode();

                // StarRocks FE 返回 307 Temporary Redirect,Location 指向 BE
                if (code == 307) {
                    Header locationHeader = resp.getFirstHeader("Location");
                    EntityUtils.consumeQuietly(resp.getEntity());
                    if (locationHeader == null || locationHeader.getValue() == null
                            || locationHeader.getValue().isEmpty()) {
                        throw new IOException(
                                "Stream Load 307 redirect missing Location header, label=" + label);
                    }
                    targetUrl = locationHeader.getValue();
                    LOG.debug("Stream Load 307 redirect: label={}, count={}, target={}",
                            label, redirect + 1, targetUrl);
                    continue;
                }

                String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                if (code != 200) {
                    throw new IOException("Stream Load failed: HTTP " + code + ", resp=" + result);
                }
                // 校验 Stream Load 业务状态。
                // StarRocks 即使数据写入失败(格式错误、schema 不匹配)也返回 HTTP 200,
                // 实际成功/失败在 JSON body 的 Status 字段:
                //   "Success"        — 全部写入
                //   "Publish Timeout" — 事务提交超时,数据可能不可见
                //   "Fail"           — 写入失败
                // 不校验会静默丢数。
                validateLoadResult(result, label);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stream Load ok: label={}, respLen={}", label, result.length());
                }
                return result;
            }
        }
        throw new IOException("Stream Load exceeded max redirects (" + MAX_REDIRECTS
                + "), label=" + label);
    }

    /**
     * validateLoadResult 校验 Stream Load 响应 JSON 的 Status 字段。
     *
     * StarRocks 返回 HTTP 200 时,实际结果在 body 的 Status 中:
     *   "Success"        — 全部行写入成功
     *   "Publish Timeout" — 事务已提交但发布到 BE 超时,数据可能不可见
     *   "Fail"           — 写入失败(数据被拒绝)
     *
     * Status 非 Success 时抛 IOException,由 StarRocksSink.invoke 重试或写 DLQ。
     * 同 label 重试是安全的:StarRocks 对同 label 请求做幂等去重。
     *
     * 额外检查 NumberLoadedRows < NumberTotalRows(质量过滤导致的部分行丢弃),
     * 记 warn 但不抛异常(这是 StarRocks 的正常 quality filter 行为)。
     */
    private void validateLoadResult(String result, String label) throws IOException {
        JsonNode root;
        try {
            root = responseMapper.readTree(result);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("Stream Load response parse error: label=" + label
                    + ", resp=" + result, e);
        }
        String status = root.path("Status").asText("");
        if (status.isEmpty()) {
            // 老版本 StarRocks 可能不返回 Status 字段,以 HTTP 200 为准
            LOG.warn("Stream Load response missing Status field, assuming success: label={}", label);
            return;
        }
        if (!"Success".equalsIgnoreCase(status)) {
            String message = root.path("Message").asText("");
            throw new IOException("Stream Load failed: label=" + label
                    + ", Status=" + status + ", Message=" + message
                    + ", resp=" + result);
        }
        // 部分行被质量过滤丢弃(正常行为,记 warn)
        long totalRows = root.path("NumberTotalRows").asLong(0);
        long loadedRows = root.path("NumberLoadedRows").asLong(totalRows);
        if (totalRows > 0 && loadedRows < totalRows) {
            long filtered = root.path("NumberFilteredRows").asLong(0);
            LOG.warn("Stream Load partial success: label={}, loaded={}/{}, filtered={}",
                    label, loadedRows, totalRows, filtered);
        }
    }

    /** buildPutRequest 构造 Stream Load PUT 请求,含统一的 headers 与 body。 */
    private HttpPut buildPutRequest(String url, String label, byte[] body, boolean gzip) {
        HttpPut put = new HttpPut(url);
        put.setConfig(requestConfig);
        put.setHeader("Authorization", authHeader);
        put.setHeader("Label", label);
        put.setHeader("Format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("Expect", "100-continue");
        if (gzip) {
            put.setHeader("Content-Encoding", "gzip");
        }
        put.setEntity(new ByteArrayEntity(body));
        return put;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
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
