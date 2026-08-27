# Flink 消费 Kafka 开发指南
> 本文档面向下游 Flink 开发者,描述如何消费 prom-gw 写入 Kafka 的指标数据,完成 5 min 滚动聚合后通过 Stream Load 写入北京 StarRocks。
>
> **前置阅读**:**local-dev-guide.md**(见 §10)(prom-gw 本地部署)、**production-guide.md**(见 §1)(生产架构)、**设计文档 §4.5/§4.6**(三独立表 + 级联聚合方案)


---

## 1. 整体架构与定位

### 1.1 在全链路中的位置

```
Prometheus ─remote_write─> prom-gw ─> Kafka ─> Flink(本文档) ─Stream Load─> StarRocks
                                       ↑                          ↑
                                  snappy+protobuf            5min 聚合 + gzip
                                  zstd(Kafka 端)             跨城专线(仅 5m 主体)
```

Flink 在本城完成 **5 min 滚动窗口聚合**,跨城写入北京 StarRocks `metrics_5m` 表;**1h / 1d 聚合由 StarRocks 周期任务从 5m 表级联聚合**,不在 Flink 端独立输出。

### 1.2 Flink 作业划分

| 作业 | 职责 | 必需 |
|---|---|---|
| **A 作业:5 min 聚合 + Stream Load** | 消费本城 Kafka → 5 min 窗口聚合 → 跨城写 StarRocks | ✅ |
| B 作业:跨指标 join | 跨指标关联(如 `kube_pod_info` × 业务指标) | 可选 |
| C 作业:DLQ 重放 | 监听 `prom.<city>.dlq.sr.5m` 重放失败批次 | 运维工具 |

本文档聚焦 **A 作业** 的完整开发步骤。

### 1.3 数据规模假设

参考 **设计文档 §2.2.6**:

| 项 | 数值 |
|---|---|
| 单城 series 数 | ≈ 1000 万 |
| 5 min 单城输出 | ≈ 2.3 GB/批,288 批/天 |
| 5 min 跨城(gzip 后) | ≈ 345 GB/天/城 |
| 三城合计跨城 | ≈ 1 TB/天,占 1G 专线 9.3% |

---

## 2. Kafka 消息格式约定

> ⚠️ **这是 Flink 开发者必须首先理解的关键章节**。prom-gw 写入 Kafka 的消息格式有特殊设计,直接处理会导致数据重复消费。

### 2.1 消息整体结构

```
┌─────────────────────────────────────────────────────────────┐
│ Kafka Message                                               │
│                                                             │
│   Topic:    prom.<city>.<stage>.<business>                  │
│   Key:      <SeriesKey 十进制字符串>  (uint64 FNV-1a hash)  │
│   Value:    <snappy 压缩的 prompb.WriteRequest 字节>        │
│   Headers:  business / source_dc / ingest_city / ...        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Value(payload)编码

**两层压缩 + protobuf**:

```
Kafka 端 zstd 压缩  →  解后是 snappy 压缩字节  →  解后是 prompb.WriteRequest v1 protobuf
```

- **外层 zstd**:Kafka producer 端配置 `compression.type=zstd`(见 prom-gw [internal/kafkasink/producer.go](../../internal/kafkasink/producer.go) `main.go:155`)
- **内层 snappy**:Prometheus remote_write v1 协议原生编码,Flink 解外层 zstd 后必须再解一次 snappy
- **protobuf schema**:`prometheus.WriteRequest` v1(不是 v2),定义在 [api/proto/remote.proto](../../api/proto/remote.proto) + [api/proto/types.proto](../../api/proto/types.proto)

```protobuf
message WriteRequest {
  repeated prometheus.TimeSeries timeseries = 1;
  reserved 2;
  repeated prometheus.MetricMetadata metadata = 3;
}

message TimeSeries {
  repeated Label labels = 1;       // 含 __name__
  repeated Sample samples = 2;     // 通常 1 个
  repeated Exemplar exemplars = 3;
  repeated Histogram histograms = 4;
}

message Sample {
  double value = 1;
  int64 timestamp = 2;              // 毫秒
}

message Label {
  string name = 1;
  string value = 2;
}
```

### 2.3 ⚠️ 关键设计:一条 Kafka 消息 ≠ 一个 sample

prom-gw 在 [internal/ruleengine/pipeline.go:225-242](../../internal/ruleengine/pipeline.go) 中按如下逻辑分发:

```go
// 每 sample 一次 out,key 各自用 seriesKey
for _, s := range cur {
    m := msg
    m.Topic = topic
    m.Key = []byte(strconv.FormatUint(s.SeriesKey(), 10))
    m.Payload = raw   // ← 整个 WriteRequest 的 snappy 字节
    p.out(ctx, m)
}
```

**含义**:一个 WriteRequest 含 N 个 sample → 产生 **N 条 Kafka 消息**,这 N 条消息的 **payload 完全相同**,但 **Key 不同**。

**Flink 必须去重**,二选一:

| 方案 | 做法 | 优点 | 缺点 |
|---|---|---|---|
| **方案 1(推荐):payload hash 去重** | 用 `Arrays.hashCode(payload)` 作 key,state 中标记已处理 | 简单,无需复刻 SeriesKey | 丢失"这条消息对应哪个 series"的映射 |
| 方案 2:SeriesKey 精确匹配 | Flink 端复刻 SeriesKey 算法,解码 payload 后用 Key 匹配出对应 sample | 精确,只处理目标 sample | 需要复刻 FNV-1a 64 哈希(见 [internal/parser/sample.go:46-59](../../internal/parser/sample.go)) |

**推荐方案 1**:Flink 消费后按 payload hash 去重,每个唯一 payload 只解码一次,然后处理 WriteRequest 中的所有 sample。

### 2.4 Kafka Headers(必读)

**业务和机房信息不在 payload 里,在 Kafka header**。payload 是 Prometheus 发来的原始字节,Prometheus 不知道这些信息。

构造点:[cmd/prom-gw/main.go:419-427](../../cmd/prom-gw/main.go):

| Header 名 | 类型 | 说明 | 示例 |
|---|---|---|---|
| `business` | string | 业务名(来自 token 鉴权) | `app-business` |
| `source_dc` | string | 来源机房(来自 `X-Source-DC` 头或 `--source-dc` flag) | `五联` |
| `ingest_city` | string | 城市:`bj` / `sz` / `hf` | `sz` |
| `ingest_dc` | string | 写入 prom-gw 的机房标识 | `dc-sz-5union` |
| `ingest_time_ms` | string | 进入 prom-gw 时刻,**毫秒**(Unix ms) | `1786431389413` |
| `traceparent` | string | W3C Trace Context | `00-<trace-id>-<span-id>-01` |

Flink 必须从 header 提取这些字段写入 StarRocks,不能从 payload 解析。

### 2.5 Key(SeriesKey)说明

- **格式**:`uint64` 十进制字符串(如 `"12345678901234567890"`)
- **算法**:FNV-1a 64,基于 `business + metric + sorted labels` 拼接哈希
- **用途**:同 series 落同 partition,保证时间顺序
- **不可反解**:Key 只是哈希值,要拿 series 信息必须解码 payload
- **Flink 用途**:作为 keyBy 的依据,同 series 路由到同一 subtask,保证窗口内状态一致

### 2.6 Topic 命名规则

| 环境 | 原始 topic | 路由后 topic |
|---|---|---|
| 本地 | `prom.local.routed.<business>` | `prom.local.routed.<biz>` |
| 生产 | `prom.<city>.raw.<business>` | `prom.<city>.routed.<biz>` 或 `prom.<city>.cleaned.<biz>` |

Flink 通常消费 **路由后(cleaned/routed)的 topic**,因为这些数据已经过 relabel/route 清洗。若要消费原始数据,订阅 raw topic。

---

## 3. 前置条件

### 3.1 Kafka 集群

- **生产**:每城 3 Broker KRaft 集群,端口 `9094`(SSL/SASL),3 副本
- **本地开发**:单节点 KRaft,见 **local-dev-guide.md §3**(见 §10)
- **必要 topic**:
  - 消费源:`prom.<city>.routed.<business>`(或本地 `prom.local.routed.app_business`)
  - DLQ:`prom.<city>.dlq.sr.5m`(Flink 创建)
  - 本地兜底输出(可选):`prom.<city>.agg5m.<business>`

### 3.2 StarRocks 集群

- **生产**:北京 3 节点(FE+BE 混合,64C/512G/1.92T×22 SSD)
- **端口**:
  - `8030`:FE HTTP(Web UI + REST API + Stream Load)
  - `9030`:MySQL 协议(查询)
  - `8040`:BE HTTP(FE 收到 Stream Load 后 307 redirect 到此端口)
  - 无 `8070` 端口,旧文档中的 8070 引用已废弃
- **FE VIP**:负载均衡,所有 Stream Load 请求走 VIP

### 3.3 Flink 集群

- **生产**:每城 JM×2(1 Active + 1 Standby)+ ZK×3 + TM×2~6
- **版本**:Flink 1.19+(建议 1.19.2)
- **JDK**:Java 17
- **每 TM**:16C/32G/500G SSD,4 slot
- **状态后端**:RocksDB(增量 checkpoint)

### 3.4 跨城专线

- 深圳 ⇄ 北京:1G×2(主备),P95 ≤ 30ms
- 合肥 ⇄ 北京:1G×1,P95 ≤ 25ms
- 跨城带宽峰值预估:36 MB/s(3× 均值),占 1G 专线 28%

---

## 4. StarRocks 表结构准备

### 4.1 三独立表 DDL

完整 DDL 见 **设计文档 §4.6.1**。这里给出 5m 表(Flink 唯一写入点):

```sql
-- ===== 5 min 表:Flink 跨城 Stream Load 唯一写入点,留存 7 天 =====
CREATE TABLE metrics_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间(UTC+8)',
  metric        VARCHAR(128) NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL COMMENT 'bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 XXH3 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询用)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数(5 min × 15s = 20 个)',
  value_sum     DOUBLE       NOT NULL,
  value_max     DOUBLE,
  value_min     DOUBLE,
  value_avg     DOUBLE,
  value_p50     DOUBLE,
  value_p99     DOUBLE,
  ingest_time   DATETIME     NOT NULL COMMENT '入 StarRocks 时间(DLQ 重放去重用)'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, labels_hash)
  PARTITION BY RANGE(ts) ()
  DISTRIBUTED BY HASH(metric, business) BUCKETS 32
  PROPERTIES (
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "compression" = "LZ4",
    "replicated_storage" = "true"
  );
```

> 1h / 1d 表 DDL 结构相同,区别仅 `dynamic_partition.start` 分别为 `-90` / `-1095`,BUCKETS 为 16 / 8。由 StarRocks 周期任务级联聚合,Flink 不写入。

### 4.2 周期聚合任务(StarRocks 端,非 Flink)

```sql
-- 每小时执行:从 5m 表聚合到 1h 表
INSERT OVERWRITE metrics_1h
SELECT
  date_trunc('hour', ts) AS ts,
  metric, business, ingest_city, source_dc, labels_hash,
  max(labels) AS labels,
  sum(sample_count) AS sample_count,
  sum(value_sum) AS value_sum,
  max(value_max) AS value_max,
  min(value_min) AS value_min,
  sum(value_sum) / sum(sample_count) AS value_avg,
  percentile_approx(value_p50, sample_count) AS value_p50,
  percentile_approx(value_p99, sample_count) AS value_p99,
  max(ingest_time) AS ingest_time
FROM metrics_5m
WHERE ts >= date_trunc('hour', now() - interval 1 hour)
  AND ts <  date_trunc('hour', now())
GROUP BY date_trunc('hour', ts), metric, business, ingest_city, source_dc, labels_hash;

-- 每天执行:从 1h 表聚合到 1d 表(级联,不跳级)
-- SQL 结构同上,改 date_trunc('day', ...) 和表名
```

通过 StarRocks 的 `JOB` 或外部调度(dolphinscheduler/airflow)定时执行。

---

## 5. Flink 项目搭建

### 5.1 Maven 依赖

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example.promgw</groupId>
  <artifactId>flink-agg5m-starrocks</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <flink.version>1.19.2</flink.version>
    <java.version>17</java.version>
    <prometheus.version>0.16.0</prometheus.version>
  </properties>

  <dependencies>
    <!-- Flink core(标注 provided,生产环境由集群提供) -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java_2.12</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients_2.12</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Kafka connector -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka_2.12</artifactId>
      <version>${flink.version}</version>
    </dependency>

    <!-- Prometheus protobuf schema(prompb.WriteRequest v1) -->
    <dependency>
      <groupId>io.prometheus</groupId>
      <artifactId>simpleclient</artifactId>
      <version>${prometheus.version}</version>
    </dependency>
    <!-- 或直接使用 prometheus-protoc 生成的 Java protobuf -->

    <!-- Snappy / Zstd 解压(Kafka 端 zstd 通常由 connector 自动解,snappy 需手动) -->
    <dependency>
      <groupId>org.xerial.snappy</groupId>
      <artifactId>snappy-java</artifactId>
      <version>1.1.10.5</version>
    </dependency>

    <!-- XXH3 hash(labels_hash 计算) -->
    <dependency>
      <groupId>net.openhft</groupId>
      <artifactId>zero-allocation-hash</artifactId>
      <version>0.16</version>
    </dependency>

    <!-- StarRocks Stream Load connector(可选,也可手写 HTTP) -->
    <dependency>
      <groupId>com.starrocks.connector</groupId>
      <artifactId>flink-connector-starrocks</artifactId>
      <version>1.2.9_flink-1.19</version>
    </dependency>

    <!-- 日志 -->
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.36</version>
    </dependency>
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>1.2.11</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <artifactSet>
                <excludes>
                  <exclude>org.apache.flink:flink-streaming-java_2.12</exclude>
                  <exclude>org.apache.flink:flink-clients_2.12</exclude>
                </excludes>
              </artifactSet>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

### 5.2 项目结构

```
flink-agg5m-starrocks/
├── pom.xml
└── src/main/java/com/example/promgw/
    ├── Agg5mJob.java              # 主作业入口
    ├── decoder/
    │   ├── PromWriteRequestDecoder.java   # Kafka 反序列化器(zstd+snappy+protobuf)
    │   └── PromSample.java                # 解码后的 sample POJO
    ├── aggregate/
    │   ├── MetricAggFunction.java         # 窗口聚合函数(sum/count/p50/p99)
    │   ├── MetricAggState.java            # 状态定义
    │   └── AggResult.java                # 聚合输出 POJO
    ├── sink/
    │   ├── BufferingStarRocksSink.java   # 攒批 Stream Load 写入(checkpoint 集成)
    │   ├── StarRocksSink.java            # 单行 Stream Load(已被 Buffering 替代,保留兜底)
    │   └── StarRocksStreamLoadClient.java # HTTP 客户端(含响应校验)
    ├── util/
    │   ├── LabelsHasher.java             # XXH3 labels hash
    │   └── HeaderExtractor.java          # Kafka header 提取
    └── dlq/
        └── KafkaDlqHandler.java          # 失败消息同步写回 Kafka DLQ
```

---

## 6. Protobuf 解码器实现

### 6.1 Kafka 反序列化器

```java
package com.example.promgw.decoder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.xerial.snappy.Snappy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PromWriteRequestDecoder
 *
 * prom-gw 写入 Kafka 的消息格式:
 *   Value  = snappy(protobuf(WriteRequest))  (Kafka 端 zstd 由 connector 自动解)
 *   Key    = SeriesKey 十进制字符串(uint64 FNV-1a hash)
 *   Headers: business / source_dc / ingest_city / ingest_dc / ingest_time_ms / traceparent
 *
 * 关键:一条 Kafka 消息 = 一个 sample 的 Key + 整个 WriteRequest 的 payload。
 * 一个 WriteRequest 含 N 个 sample 会产生 N 条消息,payload 相同,Key 不同。
 * 本解码器按 payload hash 去重,每个唯一 payload 只解码一次,输出所有 sample。
 */
public class PromWriteRequestDecoder
        implements DeserializationSchema<PromSample> {

    @Override
    public PromSample deserialize(byte[] message) throws IOException {
        return deserialize(null, message, null, null, null);
    }

    /**
     * Flink KafkaSource 支持传递 key/headers 的重载(需用 KafkaRecordDeserializationSchema)。
     * 这里给出完整签名,实际使用时实现 KafkaRecordDeserializationSchema 接口。
     */
    public PromSample deserialize(
            byte[] key, byte[] value,
            String topic, Long offset,
            Map<String, String> headers) throws IOException {

        if (value == null || value.length == 0) {
            return null;
        }

        // 1. 解 snappy(Kafka 端 zstd 已由 connector 自动解压)
        byte[] protobufBytes = Snappy.uncompress(value);

        // 2. protobuf Unmarshal → prompb.WriteRequest
        // 使用 prometheus 官方 Java protobuf 或自行生成的 stub
        WriteRequestParser.ParsedWriteRequest parsed =
                WriteRequestParser.parse(protobufBytes);

        // 3. 从 header 提取业务/机房信息
        String business    = headers != null ? headers.get("business")       : "";
        String sourceDc     = headers != null ? headers.get("source_dc")    : "";
        String ingestCity   = headers != null ? headers.get("ingest_city")   : "";
        String ingestDc     = headers != null ? headers.get("ingest_dc")     : "";
        String ingestTimeMs = headers != null ? headers.get("ingest_time_ms"): "";
        String traceparent  = headers != null ? headers.get("traceparent")   : "";

        // 4. 返回 POJO(包含整个 WriteRequest 的所有 sample + 元数据)
        return PromSample.builder()
                .timeseries(parsed.getTimeseries())
                .business(business)
                .sourceDc(sourceDc)
                .ingestCity(ingestCity)
                .ingestDc(ingestDc)
                .ingestTimeMs(parseLongSafe(ingestTimeMs))
                .traceparent(traceparent)
                .build();
    }

    @Override
    public boolean isEndOfStream(PromSample nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PromSample> getProducedType() {
        return TypeInformation.of(PromSample.class);
    }

    private long parseLongSafe(String s) {
        if (s == null || s.isEmpty()) return 0L;
        try { return Long.parseLong(s); } catch (Exception e) { return 0L; }
    }
}
```

### 6.2 Payload 去重(关键)

由于同一 WriteRequest 会产生 N 条 payload 宍余消息,必须用算子状态去重:

```java
// 在 map 之前加一步:按 payload hash 去重
DataStream<PromSample> deduped = kafkaSource
    .keyBy(record -> Arrays.hashCode(record.getValue()))   // payload hash 作 key
    .process(new DedupFunction())                          // 同 hash 只处理第一条
    .name("dedup-by-payload");

/**
 * DedupFunction:同 payload hash 在短窗口内(如 60s)只处理一次。
 * 用 ValueState 记录最近处理时间,超时清除。
 */
public class DedupFunction extends KeyedProcessFunction<Integer, KafkaRecord, PromSample> {

    private ValueState<Long> lastProcessedTs;

    @Override
    public void open(Configuration parameters) {
        lastProcessedTs = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastTs", Types.LONG));
    }

    @Override
    public void processElement(KafkaRecord record, Context ctx, Collector<PromSample> out) throws Exception {
        Long last = lastProcessedTs.value();
        long now = ctx.timerService().currentProcessingTime();
        // 60s 内同 payload hash 视为重复,跳过
        if (last != null && (now - last) < 60_000L) {
            return;
        }
        lastProcessedTs.update(now);
        // 注册 60s 后的清理 timer
        ctx.timerService().registerProcessingTimeTimer(now + 60_000L);

        // 解码并输出
        out.collect(decoder.deserialize(
            record.getKey(), record.getValue(),
            record.getTopic(), record.getOffset(),
            record.getHeaders()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PromSample> out) throws Exception {
        lastProcessedTs.clear();
    }
}
```

> **替代方案**:如果 series 数量可控,也可以用 `KeyBy(SeriesKey)` 直接按 series 分组,但需要 Flink 端复刻 [SeriesKey 算法](../../internal/parser/sample.go)(FNV-1a 64)。payload hash 去重实现更简单,推荐首选。

### 6.3 解析 WriteRequest(使用 prometheus-protoc 生成的 stub)

```java
package com.example.promgw.decoder;

import io.prometheus.client.remote.WriteRequest;  // 来自 prometheus protobuf
import io.prometheus.client.remote.LabelPair;
import io.prometheus.client.remote.Sample;
import io.prometheus.client.remote.TimeSeries;
import java.util.ArrayList;
import java.util.List;

public class WriteRequestParser {

    public static ParsedWriteRequest parse(byte[] protobufBytes) throws IOException {
        WriteRequest req = WriteRequest.parseFrom(protobufBytes);

        List<ParsedSeries> series = new ArrayList<>(req.getTimeseriesCount());
        for (TimeSeries ts : req.getTimeseriesList()) {
            // 提取 metric name(__name__ label)
            String metricName = "";
            Map<String, String> labels = new HashMap<>();
            for (LabelPair lp : ts.getLabelsList()) {
                if ("__name__".equals(lp.getName())) {
                    metricName = lp.getValue();
                } else {
                    labels.put(lp.getName(), lp.getValue());
                }
            }

            List<ParsedSample> samples = new ArrayList<>(ts.getSamplesCount());
            for (Sample s : ts.getSamplesList()) {
                samples.add(new ParsedSample(
                    s.getValue(),
                    s.getTimestamp()   // 毫秒
                ));
            }
            series.add(new ParsedSeries(metricName, labels, samples));
        }
        return new ParsedWriteRequest(series);
    }
}
```

---

## 7. Kafka Source 配置

### 7.1 KafkaSource 构造

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

Properties kafkaProps = new Properties();
kafkaProps.setProperty("bootstrap.servers", "kafka-1:9094,kafka-2:9094,kafka-3:9094");
// SSL/SASL(生产)
kafkaProps.setProperty("security.protocol", "SASL_SSL");
kafkaProps.setProperty("sasl.mechanism", "SCRAM-SHA-512");
kafkaProps.setProperty("sasl.jaas.config",
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"flink\" password=\"<secret>\";");
kafkaProps.setProperty("ssl.truststore.location", "/etc/flink/kafka.truststore.jks");
kafkaProps.setProperty("ssl.truststore.password", "<secret>");

KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
    .setBootstrapServers("kafka-1:9094,kafka-2:9094,kafka-3:9094")
    .setTopics("prom.sz.routed.app_business")    // 按本城/业务订阅
    .setGroupId("flink-agg5m-sz-app-business")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
    .setValueOnlyDeserializer(new KafkaRecordDeserializer())  // 自定义,保留 key+headers
    .setProperties(kafkaProps)
    .build();
```

### 7.2 保留 key 和 headers 的 Deserializer

Flink 的 `KafkaRecordDeserializationSchema` 可同时拿到 key/value/headers:

```java
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class KafkaRecordDeserializer implements KafkaRecordDeserializationSchema<KafkaRecord> {

    private final PromWriteRequestDecoder decoder = new PromWriteRequestDecoder();

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<KafkaRecord> out) throws IOException {
        // 提取 headers
        Map<String, String> headers = new HashMap<>();
        Headers hdrs = record.headers();
        for (Header h : hdrs) {
            headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        out.collect(new KafkaRecord(
            record.key(), record.value(),
            record.topic(), record.offset(), record.partition(),
            record.timestamp(), headers
        ));
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
```

### 7.3 Source 并行度

- **并行度 = Kafka partition 数**(确保 1:1 消费)
- 本地 topic 4 partition → source parallelism = 4
- 生产 topic 建议 12~24 partition(三城合计 1000 万 series 时的吞吐)

---

## 8. 5 min 窗口聚合实现

### 8.1 窗口分配

按 `metric + business + sortedLabels` 作 key,5 min 滚动窗口:

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

// 1. 展开 WriteRequest → 单条 sample 流(每个 series 1 条)
DataStream<SampleWithMeta> samples = deduped
    .flatMap(new ExpandWriteRequest())     // 把 N 个 series 展开为 N 条 record
    .name("expand-samples");

// 2. 分配 watermark(prompb.Sample.timestamp 是事件时间,单位 ms)
DataStream<SampleWithMeta> withWatermark = samples
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<SampleWithMeta>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withTimestampAssigner((rec, ts) -> rec.getTimestampMs())
    );

// 3. keyBy(seriesKey) + 5min tumbling window
DataStream<AggResult> aggStream = withWatermark
    .keyBy(rec -> LabelsHasher.seriesKey(
        rec.getBusiness(), rec.getMetric(), rec.getLabels()))
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new MetricAggFunction(), new AggWindowFunction())
    .name("agg-5min");
```

### 8.2 聚合函数(sum/count/max/min/p50/p99)

```java
import org.apache.flink.api.common.functions.AggregateFunction;

public class MetricAggFunction
        implements AggregateFunction<SampleWithMeta, MetricAggState, MetricAggState> {

    @Override
    public MetricAggState createAccumulator() {
        return new MetricAggState();
    }

    @Override
    public MetricAggState add(SampleWithMeta rec, MetricAggState acc) {
        acc.count++;
        acc.sum += rec.getValue();
        acc.max = Math.max(acc.max, rec.getValue());
        acc.min = acc.count == 1 ? rec.getValue() : Math.min(acc.min, rec.getValue());
        acc.samples.add(rec.getValue());   // 用于 p50/p99 计算
        if (acc.business == null) {
            acc.business    = rec.getBusiness();
            acc.metric     = rec.getMetric();
            acc.labels     = rec.getLabels();
            acc.sourceDc   = rec.getSourceDc();
            acc.ingestCity = rec.getIngestCity();
            acc.ingestTimeMs = rec.getIngestTimeMs();
        }
        return acc;
    }

    @Override
    public MetricAggState getResult(MetricAggState acc) {
        // 计算 p50/p99(简单排序,series 内 5min 通常 ≤ 20 个 sample)
        if (!acc.samples.isEmpty()) {
            Collections.sort(acc.samples);
            acc.p50 = percentile(acc.samples, 0.50);
            acc.p99 = percentile(acc.samples, 0.99);
        }
        acc.avg = acc.sum / acc.count;
        return acc;
    }

    @Override
    public MetricAggState merge(MetricAggState a, MetricAggState b) {
        a.count += b.count;
        a.sum += b.sum;
        a.max = Math.max(a.max, b.max);
        a.min = Math.min(a.min, b.min);
        a.samples.addAll(b.samples);
        return a;
    }

    private double percentile(List<Double> sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        return sorted.get(Math.max(0, idx));
    }
}
```

> **大规模优化**:若单 series 5min 内 sample 数很多(>100),用 t-digest 替代排序(参考设计文档 §2.2.6 state 估算)。Java 库可用 [Caffeine t-digest](https://github.com/tdunning/t-digest)。

### 8.3 窗口函数(组装 AggResult)

```java
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AggWindowFunction
        extends ProcessWindowFunction<MetricAggState, AggResult, String, TimeWindow> {

    @Override
    public void process(
            String seriesKey,
            Context ctx,
            Iterable<MetricAggState> states,
            Collector<AggResult> out) throws Exception {
        MetricAggState s = states.iterator().next();
        if (s.count == 0) return;

        AggResult r = new AggResult();
        // 窗口起始时间(UTC+8,转 datetime)
        r.setTs(windowStartToLocalDateTime(ctx.window().getStart()));
        r.setMetric(s.metric);
        r.setBusiness(s.business);
        r.setBusiness(extractBusiness(s.labels));   // 从 labels 提取 business 字段
        r.setIngestCity(s.ingestCity);
        r.setSourceDc(s.sourceDc);
        r.setLabelsHash(LabelsHasher.xxh3(s.labels)); // XXH3 hash
        r.setLabels(s.labels);
        r.setSampleCount(s.count);
        r.setValueSum(s.sum);
        r.setValueMax(s.max);
        r.setValueMin(s.min);
        r.setValueAvg(s.avg);
        r.setValueP50(s.p50);
        r.setValueP99(s.p99);
        r.setIngestTime(new java.util.Date());       // 写入 StarRocks 时刻
        out.collect(r);
    }
}
```

---

## 9. Stream Load 写入 StarRocks

### 9.1 方案选择

| 方案 | 实现 | 适用 |
|---|---|---|
| **BufferingStarRocksSink(推荐)** | 攒批 + checkpoint 集成 + DLQ,见 [§9.3](#93-攒批-stream-loadbufferingstarrockssink推荐) | **生产默认**,单 HTTP 请求写 N 行,吞吐高、HTTP 请求数低 |
| 官方 connector | `flink-connector-starrocks` | 不想维护 sink 代码时备选,但 label/重试/DLQ 控制力弱 |
| StarRocksSink(兜底) | 单行 Stream Load,见 [sink/StarRocksSink.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/sink/StarRocksSink.java) | 调试/单条精细控制,生产已被 Buffering 替代 |

> **推荐选 BufferingStarRocksSink**:5min 窗口单城 ≈ 1000 万 series 时,单行 sink 会产生 10K 次 HTTP 请求,攒批后(batchSize=500)降至 20 次,FE/BE 压力大幅降低。

### 9.2 官方 connector 配置(备选)

```java
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

StarRocksSinkOptions options = StarRocksSinkOptions.builder()
    .withProperty("jdbc-url", "jdbc:mysql://<fe-vip>:9030")
    .withProperty("load-url", "http://<fe-vip>:8030")
    .withProperty("database-name", "default_cluster:prom")
    .withProperty("table-name", "metrics_5m")
    .withProperty("username", "root")
    .withProperty("password", "")
    .withProperty("sink.label-prefix", "sz_5m")     // 每城唯一,避免跨城冲突
    .withProperty("sink.properties.format", "json")
    .withProperty("sink.properties.strip_outer_array", "true")
    .withProperty("sink.properties.columns",
        "ts,metric,business,ingest_city,source_dc,labels_hash," +
        "labels,sample_count,value_sum,value_max,value_min,value_avg," +
        "value_p50,value_p99,ingest_time")
    .withProperty("sink.buffer-flush.max-rows", "50000")
    .withProperty("sink.buffer-flush.max-bytes", "104857600")  // 100MB
    .withProperty("sink.buffer-flush.interval-ms", "30000")
    .withProperty("sink.max-retries", "3")
    .withProperty("sink.connect.timeout-ms", "60000")
    .build();

aggStream.addSink(StarRocksSink.sink(options));
```

> 若使用官方 connector,`Agg5mJob` 中应改用上面的 sink,不再构造 `BufferingStarRocksSink`。

### 9.3 攒批 Stream Load(BufferingStarRocksSink,推荐)

**核心思路**:把窗口输出的 `AggResult` 在 sink 端攒批,N 条合并成单个 JSON 数组,一次 HTTP PUT 提交到 StarRocks。flush 由三种条件触发:

| 触发条件 | 参数 | 默认 | 说明 |
|---|---|---|---|
| 行数达上限 | `--sr-batch-size` | 500 | buffer.size() ≥ batchSize → flush |
| 时间达上限 | `--sr-batch-interval-ms` | 10000 | 距上次 flush 超过此值 → flush |
| checkpoint | (自动) | — | snapshotState 强制 flush,保证 at-least-once |

**容错语义**:实现 `CheckpointedFunction`,checkpoint 前 flush,buffer 状态存入 `ListState`;重启后从 state 恢复未 flush 的行;flush 失败 → 抛异常 → checkpoint 失败 → 从 last checkpoint 重启。

**作业接入**(见 [Agg5mJob.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/Agg5mJob.java)):

```java
DlqHandler dlqHandler = cfg.dlqEnabled
        ? new KafkaDlqHandler(cfg.dlqBootstrapServers, cfg.dlqTopic)
        : null;

aggStream.addSink(new BufferingStarRocksSink(
        cfg.srHost, cfg.srPort, cfg.srDb, cfg.srTable,
        cfg.srUser, cfg.srPassword, cfg.srGzip,
        cfg.srLabelPrefix, dlqHandler,
        cfg.srBatchSize, cfg.srBatchIntervalMs
)).name("starrocks-stream-load-batch");
```

**核心实现**(见 [sink/BufferingStarRocksSink.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/sink/BufferingStarRocksSink.java),关键片段):

```java
public class BufferingStarRocksSink extends RichSinkFunction<AggResult>
        implements CheckpointedFunction {

    private static final int MAX_RETRY = 3;
    private static final long BASE_BACKOFF_MS = 1000L;

    // 运行时状态
    private transient StarRocksStreamLoadClient client;
    private transient List<AggResult> buffer;
    private transient long lastFlushTime;
    private transient long batchSeq;
    private transient int taskIndex;
    private transient Counter flushSuccessCounter;
    private transient Counter flushFailureCounter;
    private transient Counter dlqRowsCounter;

    // checkpoint state
    private transient ListState<AggResult> checkpointBuffer;

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
     * flush:把 buffer 中所有行作为单个 JSON 数组提交到 StarRocks。
     * 失败处理:重试 MAX_RETRY 次(指数退避 1s/2s/4s)→ 最终失败按行发 DLQ。
     */
    private void flush() throws Exception {
        if (buffer.isEmpty()) return;

        // 构建 JSON 数组:[{...},{...},...]
        StringBuilder sb = new StringBuilder(buffer.size() * 256);
        sb.append("[");
        for (int i = 0; i < buffer.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(toJson(buffer.get(i)));
        }
        sb.append("]");
        String json = sb.toString();

        String label = buildBatchLabel();  // 全局唯一 batch label
        int rowCount = buffer.size();

        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            try {
                client.load(label, json, gzip);  // 复用 HttpClient,自动处理 307 + 响应校验
                flushSuccessCounter.inc();
                buffer.clear();
                lastFlushTime = System.currentTimeMillis();
                return;
            } catch (Exception e) {
                if (attempt < MAX_RETRY) {
                    Thread.sleep(BASE_BACKOFF_MS * (1L << attempt));
                } else {
                    flushFailureCounter.inc();
                    sendRowsToDlq(e.getMessage());  // 按行发 DLQ(同步 send)
                    buffer.clear();
                    lastFlushTime = System.currentTimeMillis();
                    return;
                }
            }
        }
    }

    // --- CheckpointedFunction ---

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();                // checkpoint 前强制 flush
        checkpointBuffer.clear();
        for (AggResult r : buffer) {  // flush 后通常为空
            checkpointBuffer.add(r);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<AggResult> desc = new ListStateDescriptor<>(
                "bufferingStarRocksSinkBuffer", TypeInformation.of(AggResult.class));
        checkpointBuffer = context.getOperatorStateStore().getListState(desc);
        buffer = new ArrayList<>(batchSize);
        if (context.isRestored()) {  // 从 checkpoint 恢复未 flush 的行
            for (AggResult r : checkpointBuffer.get()) {
                buffer.add(r);
            }
        }
    }
}
```

**Flink metrics**(subtask 级,接入 Prometheus 抓取):

| Metric | 类型 | 说明 |
|---|---|---|
| `flink_taskmanager_job_task_promgw_srFlushSuccess` | Counter | 攒批 flush 成功次数 |
| `flink_taskmanager_job_task_promgw_srFlushFailure` | Counter | 攒批 flush 最终失败次数(已走 DLQ) |
| `flink_taskmanager_job_task_promgw_srDlqRows` | Counter | 进入 DLQ 的行数 |

### 9.4 StarRocksStreamLoadClient(HTTP 客户端)

`BufferingStarRocksSink` 和 `StarRocksSink` 都复用 `StarRocksStreamLoadClient`(见 [sink/StarRocksStreamLoadClient.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/sink/StarRocksStreamLoadClient.java)),关键特性:

1. **HttpClient 复用**:构造时创建一个 `CloseableHttpClient`,避免每条请求新建 client 导致大量 TIME_WAIT socket 和 DNS 开销。Flink sink 是单线程串行调用,无需同步。
2. **307 重定向手动处理**:StarRocks FE 收到 Stream Load 后返回 HTTP 307 Temporary Redirect,通过 `Location` 头指向具体 BE。Apache HttpClient 默认不对 PUT 做 307 跟随,因此本客户端关闭自动重定向(`setRedirectsEnabled(false)`),手动读取 `Location` 后用相同 body 和 headers 向 BE 重新发起 PUT,最多重试 5 次防循环。
3. **响应业务状态校验**:StarRocks 即使数据写入失败(schema 不匹配、格式错误)也返回 HTTP 200,实际结果在 JSON body 的 `Status` 字段:
   - `Success` — 全部行写入
   - `Publish Timeout` — 事务提交超时,数据可能不可见
   - `Fail` — 写入失败

   `validateLoadResult` 解析 JSON 后校验 `Status`,非 `Success` 抛 `IOException`,由上层 sink 重试或写 DLQ。**不校验会静默丢数**。
4. **部分行丢弃告警**:若 `NumberLoadedRows < NumberTotalRows`(质量过滤导致),记 warn 但不抛异常(StarRocks 的正常行为)。

```java
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

            // 1. 307 重定向:读取 Location,向 BE 重新发起 PUT
            if (code == 307) {
                Header locationHeader = resp.getFirstHeader("Location");
                EntityUtils.consumeQuietly(resp.getEntity());
                if (locationHeader == null || locationHeader.getValue().isEmpty()) {
                    throw new IOException("Stream Load 307 redirect missing Location header, label=" + label);
                }
                targetUrl = locationHeader.getValue();
                continue;
            }

            String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            if (code != 200) {
                throw new IOException("Stream Load failed: HTTP " + code + ", resp=" + result);
            }
            // 2. 校验业务状态(Status 非 Success 抛异常,避免静默丢数)
            validateLoadResult(result, label);
            return result;
        }
    }
    throw new IOException("Stream Load exceeded max redirects (" + MAX_REDIRECTS + "), label=" + label);
}

private void validateLoadResult(String result, String label) throws IOException {
    JsonNode root = responseMapper.readTree(result);
    String status = root.path("Status").asText("");
    if (status.isEmpty()) {
        LOG.warn("Stream Load response missing Status field, assuming success: label={}", label);
        return;
    }
    if (!"Success".equalsIgnoreCase(status)) {
        String message = root.path("Message").asText("");
        throw new IOException("Stream Load failed: label=" + label
                + ", Status=" + status + ", Message=" + message + ", resp=" + result);
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
```

### 9.5 Label 命名规则(关键)

Stream Load 的 `Label` 是全局唯一的去重 key。**同 label 重试 → StarRocks 幂等去重**,因此 label 必须跨重启、跨 subtask、跨窗口唯一。`BufferingStarRocksSink` 区分 batch label 和行级 label:

| 类型 | 格式 | 用途 |
|---|---|---|
| **batch label** | `<prefix>_<yyyyMMddHHmmssSSS>_<taskIndex>_<batchSeq>` | 攒批 flush 用,例:`sz_5m_20260811143012345_2_17` |
| **row label** | `<prefix>_<yyyyMMdd_HHmm>_<business>_<labels_hash>` | DLQ 按行重试用,例:`sz_5m_20260811_1430_app_business_a3f5e1c2d4e5f6a7` |

- **batch label** 含毫秒时间戳 + taskIndex + batchSeq,保证跨重启、跨 subtask、同毫秒多次 flush 都不碰撞
- **row label** 含**完整 16 字符 labels_hash**(64-bit),而非截断前 8 字符(32-bit),将同 business 内碰撞概率从 ~N²/2³³ 降至 ~N²/2⁶³(N=series 数),避免 label 碰撞导致的静默丢数
- 不同城 → `labelPrefix` 不同(如 `sz_5m` / `hf_5m`),避免跨城冲突
- 不同窗口 → 时间戳不同,label 各自独立

> ⚠️ **label 碰撞会导致静默丢数**:同 label 的第二个 Stream Load 会被 StarRocks 当重复请求拒绝。务必用完整 16 字符 hash,不要截断。

---

## 10. DLQ 与重试机制

### 10.1 DLQ Topic 设计

```
prom.<city>.dlq.sr.5m     # Stream Load 失败的批次写回本城 Kafka,等待重放
```

每城独立 DLQ,由 C 作业(重放工具)定期消费并重新写 StarRocks。DLQ 消息结构见 [dlq/DlqMessage.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/dlq/DlqMessage.java),含 `payload`(原始 AggResult JSON)、`label`、`error`、`retryCount`、`timestamp`。

### 10.2 失败处理策略(BufferingStarRocksSink 集成)

`BufferingStarRocksSink.flush()` 的失败处理流程:

```
flush 失败
   │
   ├─ 重试 1/3(退避 1s)
   ├─ 重试 2/3(退避 2s)
   ├─ 重试 3/3(退避 4s)
   │
   └─ 最终失败 → sendRowsToDlq(error)
                    │
                    ├─ 对 buffer 中每行:
                    │   - 构造行级 label(含完整 16 字符 labels_hash)
                    │   - 调 dlqHandler.send(label, rowJson, error)
                    │   - dlqRowsCounter.inc()
                    │
                    ├─ buffer.clear()
                    └─ return(flush 视为完成,checkpoint 可推进)
```

> **关键**:DLQ 失败不会静默丢数。若 `dlqHandler.send` 抛异常,异常会传播到 Flink,触发 task 从 last checkpoint 重启(见 [§10.3](#103-kafkadlqhandler-同步发送关键修复))。

### 10.3 KafkaDlqHandler 同步发送(关键修复)

`KafkaDlqHandler`(见 [dlq/KafkaDlqHandler.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/dlq/KafkaDlqHandler.java))实现了 `DlqHandler` 接口,把失败批次写回本城 Kafka DLQ topic。

> ⚠️ **关键修复:同步 send**。早期版本用 `producer.send(record, callback)` 异步发送,`send()` 立即返回 → `BufferingStarRocksSink` 认为 DLQ 成功 → checkpoint 推进 → Kafka ack 实际失败 → 数据**静默丢失**(只记 LOG.error)。修复后用 `producer.send(record).get()` 同步等待 broker ack,失败抛异常传播到 Flink,保证 at-least-once 语义。

```java
public class KafkaDlqHandler implements DlqHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDlqHandler.class);

    private final String bootstrapServers;
    private final String dlqTopic;
    private final ObjectMapper mapper = new ObjectMapper();
    private transient KafkaProducer<byte[], byte[]> producer;

    @Override
    public void open() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");                  // 等所有副本 ack
        props.put("retries", 3);
        props.put("enable.idempotence", "true");   // 幂等生产者,防重
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void send(String label, String payload, String error) throws Exception {
        DlqMessage msg = new DlqMessage(payload, label, error, 0, System.currentTimeMillis());
        byte[] value = mapper.writeValueAsBytes(msg);
        // 用 label 作 key,保证同 label 重试落同 partition
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                dlqTopic, label.getBytes(StandardCharsets.UTF_8), value);
        // 同步发送:.get() 阻塞等待 broker ack
        // 失败抛 Exception → 由 BufferingStarRocksSink 传播到 Flink →
        // 触发 task 从 last checkpoint 重启,保证 at-least-once
        try {
            producer.send(record).get();
            LOG.warn("DLQ sent: label={}, topic={}", label, dlqTopic);
        } catch (Exception e) {
            LOG.error("DLQ send failed, propagating to ensure at-least-once: label={}", label, e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
```

**配置要点**:
- `acks=all`:等待所有 ISR 副本 ack,防止单副本故障丢消息
- `enable.idempotence=true`:幂等生产者,同 label 重试不产生重复消息
- `label` 作 Kafka key:同 label 的重试落同 partition,C 作业重放时顺序消费

### 10.4 DLQ 重放作业(C 作业,运维工具)

C 作业消费 DLQ topic,重新写 StarRocks:

```java
// 简单实现:消费 DLQ topic,重试 N 次,成功则提交 offset,失败则累加 retry_count
// 超过 max_retry(如 5 次)发到 dead-letter-syslog 告警
//
// 关键:重放时用 DlqMessage.label 作 Stream Load label,
// StarRocks 会按 label 幂等去重,避免重复写入
```

---

## 11. 本地开发与测试

### 11.1 本地环境

参照 **local-dev-guide.md**(见 §10) 部署本地 Kafka + prom-gw + Prometheus,并验证 Kafka 已有数据:

```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 5 --timeout-ms 10000 | xxd | head -20
# 期望:看到二进制数据(zstd + snappy + protobuf)
```

### 11.2 Flink 本地运行

`Agg5mJob.main` 直接读 `JobConfig.fromArgs(args)`,默认值即本地配置,无需传参:

```bash
mvn clean package
java -jar target/flink-agg5m-starrocks-1.0.0.jar \
  --env local \
  --kafka-brokers localhost:9092 \
  --topic prom.local.routed.app_business \
  --starrocks-host localhost \
  --starrocks-port 8030 \
  --label-prefix local_5m \
  --dlq-topic prom.local.dlq.sr.5m \
  --sr-batch-size 100 \
  --sr-batch-interval-ms 5000
```

> 本地建议调小 `--sr-batch-size` 和 `--sr-batch-interval-ms`,便于快速看到写入效果,不用等攒满 500 行。

### 11.3 验证步骤

```bash
# 1. 启动本地 Kafka + prom-gw + Prometheus(见 local-dev-guide.md §6.1)
# 2. 验证 Kafka 有数据
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic prom.local.routed.app_business
# 期望:offset > 0

# 3. 本地运行 Flink
mvn clean package
java -jar target/flink-agg5m-starrocks-1.0.0.jar --env local

# 4. 验证 StarRocks 收到数据
mysql -h <starrocks-fe> -P 9060 -u root -e "
  SELECT count(*), min(ts), max(ts)
  FROM prom.metrics_5m
  WHERE ingest_city = 'local';"

# 5. 验证聚合正确性(对比 Prometheus 原始值)
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq
# 对比 StarRocks 中近 5 分钟的 value_avg
```

### 11.4 单元测试

```java
public class PromWriteRequestDecoderTest {

    @Test
    void testDecodeSnappyProtobuf() throws Exception {
        // 构造一个 WriteRequest → snappy 编码
        byte[] raw = WriteRequest.newBuilder()
            .addTimeseries(TimeSeries.newBuilder()
                .addLabels(LabelPair.newBuilder().setName("__name__").setValue("up").build())
                .addLabels(LabelPair.newBuilder().setName("job").setValue("prometheus").build())
                .addSamples(Sample.newBuilder().setValue(1.0).setTimestamp(1786431389000L).build())
                .build())
            .build().toByteArray();
        byte[] snappyBytes = Snappy.compress(raw);

        // 模拟 Kafka header
        Map<String, String> headers = Map.of(
            "business", "app-business",
            "source_dc", "dc-local-dev",
            "ingest_city", "local",
            "ingest_time_ms", "1786431389413"
        );

        PromSample sample = new PromWriteRequestDecoder()
            .deserialize(null, snappyBytes, "test", 0L, headers);

        assertEquals("app-business", sample.getBusiness());
        assertEquals("local", sample.getIngestCity());
        assertEquals(1, sample.getTimeseries().size());
        assertEquals("up", sample.getTimeseries().get(0).getMetricName());
        assertEquals(1.0, sample.getTimeseries().get(0).getSamples().get(0).getValue(), 0.001);
    }
}
```

---

## 12. 生产部署

### 12.1 集群部署

```bash
# 1. 打包
mvn clean package -Pprod
# 产物:target/flink-agg5m-starrocks-1.0.0.jar

# 2. 提交到 Flink 集群(参数对应 JobConfig.java)
flink run \
  -d \                                  # detached 模式
  -p 24 \                               # 全局并行度
  -c com.example.promgw.Agg5mJob \
  /appdata/flink/usrlib/flink-agg5m-starrocks-1.0.0.jar \
  --env prod \
  --kafka-brokers kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094 \
  --topic prom.sz.routed.app_business \
  --group-id flink-agg5m-sz-app-business \
  --starrocks-host <beijing-fe-vip> \
  --starrocks-port 8030 \
  --starrocks-db prom \
  --starrocks-table metrics_5m \
  --starrocks-user root \
  --label-prefix sz_5m \
  --sr-batch-size 500 \
  --sr-batch-interval-ms 10000 \
  --dlq-topic prom.sz.dlq.sr.5m \
  --dlq-enabled true \
  --source-parallelism 24 \
  --agg-parallelism 24 \
  --checkpoint-path hdfs:///flink/checkpoints/agg5m-sz \
  --checkpoint-interval-ms 60000 \
  --window-minutes 5 \
  --allowed-lateness-ms 30000 \
  --kafka-start-from committed \
  --kafka-offset-reset latest

# 3. 配置 JM HA(见生产部署文档 §1.1)
```

> 完整参数列表见 [JobConfig.java](../../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/JobConfig.java)。`--env prod` 会自动启用 SASL_SSL + SCRAM-SHA-512 + gzip + hdfs checkpoint path,无需手动传 SASL/SSL 参数(由 kafka.client 配置文件提供)。

### 12.2 参数模板

| 参数 | 深圳示例 | 合肥示例 | 说明 |
|---|---|---|---|
| `--kafka-brokers` | `kafka-1.sz:9094,...` | `kafka-1.hf:9094,...` | 本城 3 broker |
| `--topic` | `prom.sz.routed.app_business` | `prom.hf.routed.app_business` | 路由后 topic |
| `--group-id` | `flink-agg5m-sz-app-business` | `flink-agg5m-hf-app-business` | 消费组 |
| `--starrocks-host` | `<beijing-fe-vip>` | 同 | 跨城写北京 StarRocks |
| `--starrocks-port` | `8030` | `8030` | FE http_port(非 8070) |
| `--label-prefix` | `sz_5m` | `hf_5m` | 每城唯一,避免跨城 label 冲突 |
| `--sr-batch-size` | `500` | `500` | 攒批行数上限 |
| `--sr-batch-interval-ms` | `10000` | `10000` | 攒批时间上限(ms) |
| `--dlq-topic` | `prom.sz.dlq.sr.5m` | `prom.hf.dlq.sr.5m` | 本城 DLQ |
| `--source-parallelism` | `24` | `8` | = Kafka partition 数 |
| `--agg-parallelism` | `24` | `8` | 聚合算子并行度 |

### 12.3 Checkpoint 配置(关键)

```java
env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);  // 1min
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
env.getCheckpointConfig().setCheckpointTimeout(300_000L);
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints/agg5m-sz");
```

> **攒批与 checkpoint 的关系**:`BufferingStarRocksSink` 实现 `CheckpointedFunction`,`snapshotState` 时强制 flush buffer,确保 checkpoint 完成时数据已写入 StarRocks 或 DLQ。checkpoint 间隔(60s)应 ≥ `sr-batch-interval-ms`(10s),避免 checkpoint 频繁打断攒批。

### 12.4 资源调优建议

| 维度 | 建议值 | 说明 |
|---|---|---|
| TM 内存 | 32G | t-digest state 较大 |
| TM slot | 4 | 每 TM 跑 4 个 subtask |
| RocksDB write buffer | 256MB | 减少 SST flush 频率 |
| t-digest compression | 50 | state 减半,精度可接受(见设计文档 §2.2.6) |
| 窗口允许延迟 | 30s | 超过则丢弃,走 DLQ |
| Kafka offset 提交 | 关闭自动提交,checkpoint 时提交 | at-least-once 语义 |
| `--sr-batch-size` | 500 | 单批 500 行,HTTP 请求数降至 1/500 |
| `--sr-batch-interval-ms` | 10000 | 10s 强制 flush,避免低频 metric 长时间缓冲 |
| StarRocksStreamLoadClient 超时 | 60s | 大批量写入可能较慢,见 [§9.4](#94-starrocksstreamloadclienthttp-客户端) |

---

## 13. 监控与告警

### 13.1 关键指标

| 指标 | 来源 | 告警阈值 |
|---|---|---|
| `flink_job_numRecordsInPerSecond` | Flink metrics | 突降 50% 告警 |
| `flink_job_numRecordsOutPerSecond` | Flink metrics | 突降 50% 告警 |
| `flink_job_currentEventTimeLag` | Flink metrics | > 60s 告警(消费滞后) |
| `flink_job_lastCheckpointDuration` | Flink metrics | > 60s 告警 |
| `flink_job_numFailedCheckpoints` | Flink metrics | > 0 告警 |
| Kafka consumer lag | Kafka exporter | > 10000 告警 |
| `flink_taskmanager_job_task_promgw_srFlushSuccess` | 自定义 Counter | 突降 50% 告警(攒批写入停滞) |
| `flink_taskmanager_job_task_promgw_srFlushFailure` | 自定义 Counter | > 0 告警(批次写失败,已走 DLQ) |
| `flink_taskmanager_job_task_promgw_srDlqRows` | 自定义 Counter | > 0 告警(需重放 DLQ) |
| StarRocks 写入 QPS | StarRocks FE | 突降 50% 告警 |

> 攒批相关 metric 由 `BufferingStarRocksSink.open()` 注册到 `MetricGroup.addGroup("promgw")`,subtask 级聚合。告警规则示例见生产部署文档 [06-flink-deployment.md](../production/06-flink-deployment.md)。

### 13.2 Prometheus 抓取配置

```yaml
- job_name: flink
  static_configs:
    - targets:
      - 'flink-jm-1.sz:9999'
      - 'flink-jm-2.sz:9999'
  metrics_path: /prom
```

### 13.3 Grafana 看板

参考 [deploy/grafana/dashboards/prom-gw.json](../../deploy/grafana/dashboards/prom-gw.json),新增 "Flink 消费链路" 面板组,包含:
- Kafka 消费速率 + lag
- 窗口触发频率
- Stream Load 成功率 / 延迟
- Checkpoint 耗时 / 失败率
- DLQ 队列深度

---

## 14. 常见问题

| 现象 | 排查 |
|---|---|
| Flink 解码报 `Snappy decoding failed` | Kafka connector 未启用 zstd 自动解压,或消息 payload 不是 snappy 编码。确认 prom-gw 端 `compression.type=zstd` 且 Flink `value.deserializer` 正确 |
| 数据重复入 StarRocks | (1) payload hash 去重未生效;(2) Stream Load label 重复导致幂等去重未命中。检查 label 命名规则 |
| 数据丢失 | (1) checkpoint 未启用 → 重启后 offset 回滚;(2) DLQ 未消费 → 失败批次积压;(3) DLQ send 异步失败被吞 → 确认 KafkaDlqHandler 已用同步 `.get()`(见 [§10.3](#103-kafkadlqhandler-同步发送关键修复)) |
| `currentEventTimeLag` 持续增大 | (1) Kafka 消费滞后 → 扩 partition / TM;(2) watermark 策略过严 → 调整 `boundedOutOfOrderness` |
| Checkpoint 超时 | (1) state 过大 → 调小 t-digest compression;(2) RocksDB IOPS 不足 → 用 SSD;(3) `snapshotState` 时 flush 卡住 → 看 `srFlushFailure` 是否增长,StarRocks 不可达 |
| Stream Load 失败率上升 | (1) StarRocks BE 压力大 → 扩 BE;(2) FE VIP 不可达 → 检查专线;(3) label 碰撞 → 确认用完整 16 字符 hash;(4) `Status=Fail` 但 HTTP 200 → 确认 `validateLoadResult` 已生效 |
| `Stream Load 307 redirect missing Location header` | FE 返回 307 但无 Location 头,通常为 StarRocks 版本异常或 FE 配置错误。检查 FE `http_port=8030` 且 BE `be_http_port=8040` 可达 |
| `Stream Load exceeded max redirects` | 307 重定向超过 5 次,出现 FE↔BE 循环重定向。检查 FE `frontend_address` 和 BE `backend_host` 配置,确认无回环 |
| HTTP 大量 TIME_WAIT socket | 旧版本每次 load 新建 HttpClient。确认 `StarRocksStreamLoadClient` 复用单例 client(见 [§9.4](#94-starrocksstreamloadclienthttp-客户端)) |
| 攒批不 flush,数据延迟 | (1) `--sr-batch-interval-ms` 过大 → 调小;(2) subtask 无数据 → 检查 keyBy 分配;(3) checkpoint 间隔过长 → 缩短到 60s |
| 跨城专线带宽告警 | 5 min 聚合 gzip 后仍 > 专线 30% → 降级为 1h 跨城(见设计文档 §4.5) |
| Prometheus 指标值与 StarRocks 不一致 | (1) 窗口触发延迟 → 看 watermark;(2) sample stage 采样(prom-gw 端 `rate=0.1`)→ 检查 ruleset;(3) downsample stage 修改了 value → 看 ruleset 是否含 downsample |
| Kafka header 缺失 | (1) 消费 raw topic 而非 routed topic(老消息可能没 header);(2) prom-gw 版本过旧,升级到最新 |
| `labels_hash` 碰撞 | XXH3 64 位 hash 碰撞概率 < 10⁻¹⁵,实际可忽略。若必须避免,改用 SHA-1(labels 全量拼接) |

---

## 附录

### A. 数据流完整时序

```
T+0s      Prometheus 抓取 → remote_write
T+0.1s    prom-gw 接收 → 鉴权 → ruleset 清洗 → Kafka
T+0.2s    Kafka 落盘(zstd 压缩)
T+0.3s    Flink 消费 → 解码 → 展开为 sample
T+0.3s    Flink keyBy(seriesKey) → 状态累加
T+5min    5min 窗口触发 → 计算 p50/p99
T+5min+1s Stream Load gzip → 北京 StarRocks
T+5min+2s StarRocks PK 模型 REPLACE 去重 → 落盘
T+1h      StarRocks 周期任务:5m → 1h 表
T+1d      StarRocks 周期任务:1h → 1d 表
```



---

