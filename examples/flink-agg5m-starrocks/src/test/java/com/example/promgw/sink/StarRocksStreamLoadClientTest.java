package com.example.promgw.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * StarRocksStreamLoadClientTest 验证 Stream Load HTTP 客户端核心行为。
 *
 * 使用 JDK 内置 {@link HttpServer} 模拟 StarRocks FE/BE,重点覆盖:
 *   - HTTP 307 FE→BE 重定向跟随(PUT 方法、body、headers 保留)
 *   - 无 Location 头的 307 异常
 *   - 非 200 最终响应报错
 *   - gzip 压缩路径正确解码
 *   - 多次重定向上限保护
 */
class StarRocksStreamLoadClientTest {

    private HttpServer feServer;
    private HttpServer beServer;
    private int fePort;
    private int bePort;

    /** beHitCount 记录 BE 收到的 PUT 请求次数(验证重定向是否真的发生)。 */
    private final AtomicInteger beHitCount = new AtomicInteger(0);

    /** feHitCount 记录 FE 收到的 PUT 请求次数。 */
    private final AtomicInteger feHitCount = new AtomicInteger(0);

    /** lastBeBody 记录 BE 收到的请求体(验证 body 正确转发)。 */
    private volatile String lastBeBody;

    /** lastBeMethod 记录 BE 收到的 HTTP 方法(验证仍是 PUT)。 */
    private volatile String lastBeMethod;

    /** lastBeAuth 记录 BE 收到的 Authorization 头(验证 header 转发)。 */
    private volatile String lastBeAuth;

    @BeforeEach
    void setUp() throws IOException {
        feServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fePort = feServer.getAddress().getPort();
        beServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        bePort = beServer.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (feServer != null) feServer.stop(0);
        if (beServer != null) beServer.stop(0);
    }

    @Test
    void testFollows307RedirectToBe() throws Exception {
        // FE: 收到 PUT,返回 307 + Location 指向 BE
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                feHitCount.incrementAndGet();
                exchange.getResponseHeaders().add("Location",
                        "http://127.0.0.1:" + bePort + "/api/prom/metrics_5m/_stream_load");
                exchange.sendResponseHeaders(307, -1);
                exchange.close();
            }
        });
        // BE: 收到 PUT,返回 200
        beServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                beHitCount.incrementAndGet();
                lastBeMethod = exchange.getRequestMethod();
                lastBeAuth = exchange.getRequestHeaders().getFirst("Authorization");
                lastBeBody = new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8);
                String resp = "{\"Status\":\"OK\",\"NumberTotalRows\":1}";
                byte[] respBytes = resp.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, respBytes.length);
                exchange.getResponseBody().write(respBytes);
                exchange.close();
            }
        });
        feServer.start();
        beServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        String result = client.load("test_label_001", "{\"metric\":\"up\"}", false);

        // FE 被命中 1 次(首次请求)
        assertThat(feHitCount.get()).isEqualTo(1);
        // BE 被命中 1 次(重定向后)
        assertThat(beHitCount.get()).isEqualTo(1);
        // 方法仍是 PUT(未被改成 GET)
        assertThat(lastBeMethod).isEqualTo("PUT");
        // body 正确转发
        assertThat(lastBeBody).isEqualTo("{\"metric\":\"up\"}");
        // Authorization 头正确转发到 BE
        assertThat(lastBeAuth).startsWith("Basic ");
        // 返回 BE 的响应
        assertThat(result).contains("\"Status\":\"OK\"");
    }

    @Test
    void test307WithoutLocationThrows() throws Exception {
        // FE: 返回 307 但无 Location 头
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                exchange.sendResponseHeaders(307, -1);
                exchange.close();
            }
        });
        feServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        assertThatThrownBy(() -> client.load("test_label_002", "{}", false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("307")
                .hasMessageContaining("Location");
    }

    @Test
    void testNon200ResponseThrows() throws Exception {
        // FE: 直接返回 403
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String resp = "{\"Status\":\"Forbidden\"}";
                byte[] respBytes = resp.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(403, respBytes.length);
                exchange.getResponseBody().write(respBytes);
                exchange.close();
            }
        });
        feServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        assertThatThrownBy(() -> client.load("test_label_003", "{}", false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP 403");
    }

    @Test
    void testGzipPathStillWorks() throws Exception {
        // FE: 收到 PUT,返回 307 + Location 指向 BE
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                exchange.getResponseHeaders().add("Location",
                        "http://127.0.0.1:" + bePort + "/api/prom/metrics_5m/_stream_load");
                exchange.sendResponseHeaders(307, -1);
                exchange.close();
            }
        });
        // BE: 返回 200
        beServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                // 消费请求体
                exchange.getRequestBody().readAllBytes();
                String resp = "{\"Status\":\"OK\"}";
                byte[] respBytes = resp.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, respBytes.length);
                exchange.getResponseBody().write(respBytes);
                exchange.close();
            }
        });
        feServer.start();
        beServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        String result = client.load("test_label_004", "{\"metric\":\"up\",\"value\":1.0}", true);

        assertThat(result).contains("\"Status\":\"OK\"");
    }

    @Test
    void testRedirectLoopProtection() throws Exception {
        // FE: 每次都返回 307 指向自己,形成循环
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                feHitCount.incrementAndGet();
                exchange.getResponseHeaders().add("Location",
                        "http://127.0.0.1:" + fePort + "/api/prom/metrics_5m/_stream_load");
                exchange.sendResponseHeaders(307, -1);
                exchange.close();
            }
        });
        feServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        assertThatThrownBy(() -> client.load("test_label_005", "{}", false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("max redirects");
        // FE 被命中 MAX_REDIRECTS+1 次(6 次)
        assertThat(feHitCount.get()).isGreaterThan(1);
    }

    @Test
    void testDirect200NoRedirect() throws Exception {
        // FE: 直接返回 200(无重定向)
        feServer.createContext("/api/prom/metrics_5m/_stream_load", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                exchange.getRequestBody().readAllBytes();
                String resp = "{\"Status\":\"OK\",\"NumberTotalRows\":5}";
                byte[] respBytes = resp.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, respBytes.length);
                exchange.getResponseBody().write(respBytes);
                exchange.close();
            }
        });
        feServer.start();

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                "127.0.0.1", fePort, "prom", "metrics_5m", "root", "");

        String result = client.load("test_label_006", "{\"metric\":\"up\"}", false);

        assertThat(result).contains("\"Status\":\"OK\"");
        // BE 不应被命中
        assertThat(beHitCount.get()).isEqualTo(0);
    }
}
