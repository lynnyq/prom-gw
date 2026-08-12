# flink-agg5m-starrocks

prom-gw 下游 Flink 作业:消费 Kafka 中的 Prometheus 指标数据,5min 聚合后通过 Stream Load 写入 StarRocks。

## 快速开始

### 1. 构建

```bash
cd examples/flink-agg5m-starrocks
mvn clean package
# 产物:target/flink-agg5m-starrocks-1.0.0.jar
```

### 2. 本地运行

前置:已按 [local-dev-guide.md](../../docs/operations/local-dev-guide.md) 启动 Kafka + prom-gw + Prometheus,Kafka 中已有数据。

```bash
java -jar target/flink-agg5m-starrocks-1.0.0.jar --env local
```

### 3. 生产提交

```bash
flink run -d -p 24 \
  -c com.example.promgw.Agg5mJob \
  target/flink-agg5m-starrocks-1.0.0.jar \
  --env prod \
  --kafka-brokers kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094 \
  --topic prom.sz.routed.app_business \
  --starrocks-host <beijing-fe-vip> \
  --label-prefix sz_5m \
  --dlq-topic prom.sz.dlq.sr.5m
```

## 测试验证

工程已通过完整的编译、单元测试、打包验证。以下步骤可本地复现。

### 环境要求

- JDK 11(建议 Oracle 11.0.2 或以上)
- Maven 3.9+(已验证 3.9.16)

```bash
# 验证 JDK / Maven 版本
java -version       # 11.0.2
mvn -v              # Apache Maven 3.9.16
```

### 步骤 1:编译验证

```bash
cd examples/flink-agg5m-starrocks
mvn clean compile -U
```

预期输出:
```
[INFO] Compiling 21 source files with javac [debug target 11] to target/classes
[INFO] BUILD SUCCESS
```

说明:
- 21 个 Java 源文件全部编译通过
- protobuf-maven-plugin 自动生成 `com.example.promgw.proto.PromProtos` 系列类
- 依赖从阿里云镜像下载(缺失时 fallback 到 Maven Central)

### 步骤 2:单元测试验证

```bash
mvn test
```

预期输出:
```
[INFO] Running com.example.promgw.util.LabelsHasherTest
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.example.promgw.aggregate.MetricAggFunctionTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.example.promgw.decoder.PromWriteRequestDecoderTest
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
[INFO] Results:
[INFO] Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

测试覆盖:
| 测试类 | 用例数 | 覆盖点 |
|---|---|---|
| `LabelsHasherTest` | 7 | SHA-1 哈希稳定性、空 labels、排序一致性 |
| `MetricAggFunctionTest` | 4 | 5min 窗口聚合 sum/count/max/min/avg/p50/p99 |
| `PromWriteRequestDecoderTest` | 6 | snappy 解压 + protobuf 解码 + 样本提取 |

### 步骤 3:打包验证

```bash
mvn clean package -DskipTests
```

预期输出:
```
[INFO] Replacing original artifact with shaded artifact.
[INFO] BUILD SUCCESS
```

产物:
```bash
ls -lh target/*.jar
# -rw-r--r--  35M  target/flink-agg5m-starrocks-1.0.0.jar        # fat jar(可提交集群)
# -rw-r--r-- 167K  target/original-flink-agg5m-starrocks-1.0.0.jar
```

验证主类与 Protobuf 生成类:
```bash
unzip -p target/flink-agg5m-starrocks-1.0.0.jar META-INF/MANIFEST.MF | grep Main-Class
# Main-Class: com.example.promgw.Agg5mJob

jar tf target/flink-agg5m-starrocks-1.0.0.jar | grep PromProtos | head -5
# com/example/promgw/proto/PromProtos$WriteRequest.class
# com/example/promgw/proto/PromProtos$TimeSeries.class
# com/example/promgw/proto/PromProtos$Sample.class
# com/example/promgw/proto/PromProtos$Label.class
# ...
```

### 步骤 4:本地启动 Flink 作业消费 Kafka

前置:已按 [local-dev-guide.md](../../docs/operations/local-dev-guide.md) 完成:
1. Kafka(KRaft 模式)已启动,`config/local.properties` 配置正确
2. prom-gw 已启动,topic 自动创建:
   - `prom.local.raw.app_business`
   - `prom.local.routed.app_business`
3. Prometheus 已启动并持续 remote_write 到 prom-gw
4. Kafka 中已有路由后的数据(可用 `kafka-console-consumer.sh` 验证)

```bash
# 1) 确认 Kafka 中 prom.local.routed.app_business 有数据
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 3 \
  --property print.headers=true

# 2) 启动 Flink 作业(本地模式)
java -jar target/flink-agg5m-starrocks-1.0.0.jar --env local
```

启动后日志关键字(表示消费正常):
```
[main] INFO com.example.promgw.Agg5mJob - Starting Agg5mJob env=local
[main] INFO com.example.promgw.Agg5mJob - Kafka source: brokers=localhost:9092, topic=prom.local.routed.app_business
[main] INFO com.example.promgw.Agg5mJob - StarRocks sink: host=localhost:8070, db=prom, table=metrics_5m
[flink-...-source] INFO com.example.promgw.decoder.PromWriteRequestDecoder - decoded 12 samples from 1 WriteRequest
```

### 步骤 5:消费结果验证

#### 5.1 查看 Flink 作业消费位点

作业运行后,通过 Flink Web UI(http://localhost:8081,若启用了 mini cluster)或日志确认:
- Kafka source 的 records consumed 持续增长
- 5min 窗口触发后,Stream Load 请求发出

#### 5.2 查看 StarRocks 数据落库

```sql
-- 1) 确认表存在(DDL 见 docs/operations/flink-consumer-guide.md)
SHOW CREATE TABLE prom.metrics_5m;

-- 2) 查询最近 10 分钟的聚合结果
SELECT
  metric_name,
  labels_hash,
  window_start,
  window_end,
  sample_count,
  sum_value,
  avg_value,
  p99_value
FROM prom.metrics_5m
WHERE window_start >= NOW() - INTERVAL 10 MINUTE
ORDER BY window_start DESC, metric_name
LIMIT 20;
```

预期:每个 5min 窗口、每个 metric + labels 组合对应一行聚合结果,`labels_hash` 为 16 位十六进制 SHA-1 摘要。

#### 5.3 验证幂等(重复运行不产生重复数据)

同一窗口内多次重试,Stream Load Label 相同,StarRocks 自动去重:
```sql
-- 同 label 多次写入后,记录数不变
SELECT labels_hash, window_start, COUNT(*) AS cnt
FROM prom.metrics_5m
WHERE window_start = '<某个窗口起点>'
GROUP BY labels_hash, window_start
HAVING cnt > 1;  -- 期望返回 0 行
```

#### 5.4 验证 DLQ(失败重放)

若 StarRocks 不可达或写入失败,作业会将失败批次发送到 DLQ topic:
```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.dlq.sr.5m \
  --from-beginning --max-messages 5
```

修复 StarRocks 后,可重放 DLQ topic 中的消息(通过单独的 DLQ 重放作业或脚本)。

### 已知问题与排查

| 现象 | 原因 | 解决 |
|---|---|---|
| `Could not find artifact ... flink-streaming-java_2.12` | Flink 1.15+ 去掉 Scala 后缀 | pom.xml 使用 `flink-streaming-java`(无 `_2.12`),已修复 |
| `Tried to write the same file twice` | 两个 proto 文件 `java_outer_classname` 相同 | 合并为单个 `prom.proto`,已修复 |
| `Could not find artifact net.openhift:zero-allocation-hash` | 依赖在 Maven Central 不存在 | 改用 JDK 内置 SHA-1,已修复 |
| Kafka 消费位点不增长 | topic 名错误或无数据 | 确认 prom-gw 路由配置和 `--topic` 参数一致 |
| Stream Load 401 / 403 | StarRocks 用户名/密码错误 | 检查 `--starrocks-user` / `--starrocks-password` 参数 |
| Stream Load Label 冲突 | 同窗口重复提交(正常) | StarRocks 自动去重,属预期行为 |

## 工程结构

```
flink-agg5m-starrocks/
├── pom.xml                          # Maven 依赖 + protobuf 自动生成 + shade 打包
├── README.md
├── src/main/proto/                  # Prometheus remote_write v1 proto(合并版,避免生成冲突)
│   └── prom.proto
├── src/main/resources/
│   └── logback.xml
└── src/main/java/com/example/promgw/
    ├── Agg5mJob.java                # 主作业入口
    ├── JobConfig.java               # 命令行参数解析
    ├── decoder/                     # Kafka 反序列化 + 去重
    │   ├── KafkaRecord.java
    │   ├── KafkaRecordDeserializer.java
    │   ├── PromSample.java
    │   ├── PromWriteRequestDecoder.java
    │   ├── WriteRequestParser.java
    │   └── DedupFunction.java
    ├── aggregate/                   # 5min 窗口聚合
    │   ├── SampleWithMeta.java
    │   ├── ExpandWriteRequest.java
    │   ├── MetricAggState.java
    │   ├── MetricAggFunction.java
    │   ├── AggResult.java
    │   └── AggWindowFunction.java
    ├── sink/                        # StarRocks Stream Load
    │   ├── StarRocksStreamLoadClient.java
    │   ├── StarRocksSink.java
    │   └── DlqHandler.java
    ├── dlq/                         # DLQ(失败重放)
    │   ├── DlqMessage.java
    │   └── KafkaDlqHandler.java
    └── util/
        └── LabelsHasher.java        # SHA-1 labels hash(用作 StarRocks 主键)
```

## 关键设计

### 1. payload 去重(核心)

prom-gw 写入 Kafka 时,一个 WriteRequest 含 N 个 sample 会产生 N 条消息,payload 完全相同。
本工程用 `DedupFunction` 按 payload hash 去重,60s 内同 hash 只解码一次,避免 N 倍重复处理。

### 2. 两层解压

Kafka 端 zstd(由 connector 自动解)→ snappy(本工程解)→ protobuf → WriteRequest

### 3. 租户信息从 header 提取

payload 是 Prometheus 原始字节,不含租户信息。tenant/source_dc/ingest_city 等从 Kafka header 提取。

### 4. SHA-1 labels hash

对 labels 排序后用 SHA-1 计算 8 字节摘要,作为 StarRocks 主键之一,保证相同 labels 的 hash 一致。无需第三方依赖,纯 JDK 实现。

### 5. Stream Load Label 幂等

格式 `<city>_<windowStart>_<business>_<hashShort>`,同窗口重试同 label,StarRocks 自动去重。

## 参数

完整参数见 [configuration-reference.md](../../docs/operations/configuration-reference.md),常用:

| 参数 | 说明 | 默认 |
|---|---|---|
| `--env` | 环境(local/prod) | local |
| `--kafka-brokers` | Kafka broker 列表 | localhost:9092 |
| `--topic` | 消费的 Kafka topic | prom.local.routed.app_business |
| `--starrocks-host` | StarRocks FE VIP | localhost |
| `--starrocks-port` | Stream Load 端口 | 8070 |
| `--label-prefix` | Stream Load label 前缀 | local_5m |
| `--window-minutes` | 聚合窗口(分钟) | 5 |
| `--checkpoint-path` | checkpoint 存储路径 | file:///tmp/flink-checkpoints |

## 相关文档

- [Flink 消费 Kafka 写 StarRocks 开发指南](../../docs/operations/flink-consumer-guide.md)
- [prom-gw 参数与配置完整说明](../../docs/operations/configuration-reference.md)
- [本地开发部署指南](../../docs/operations/local-dev-guide.md)
