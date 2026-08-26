# Flink 生产部署与配置详解
> 本文档覆盖 prom-gw 配套 Flink 集群的生产环境完整部署,包括集群搭建、JM HA、TM 配置、作业提交管理、Checkpoint/Savepoint、监控告警和运维操作。
>
> Flink 作业的开发实现见 **Flink 消费 Kafka 写入 StarRocks 开发指南**(见 §6),本文档聚焦**集群部署与运维**。
>
> 配套文档:**Kafka 生产部署**(见 §2)、**生产部署指南**(见 §1)、**压力测试指南**(见 §8)、**故障剧本**(见 §11)


---

## 1. 部署架构

### 1.1 单机房标准拓扑

每机房部署 JM×2(1 Active + 1 Standby)+ TM×2~6,Flink 作业消费本城 Kafka,跨城写北京 StarRocks:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  ┌─────────────┐         ┌─────────────┐                │
│  │ JM-1 (Active)│ ← HA → │ JM-2 (Standby)│               │
│  │ 10.0.1.31    │  ZK    │ 10.0.1.32    │                │
│  └──────┬───────┘         └──────┬───────┘                │
│         │                        │                        │
│  ┌──────┴────────────────────────┴──────┐                │
│  │            ZK Quorum × 3             │                │
│  │      10.0.1.41 / 42 / 43             │                │
│  └──────────────────────────────────────┘                │
│         │                                                │
│  ┌──────┼──────────────────────────┐                    │
│  │     │                          │                    │
│  ▼     ▼                          ▼                    │
│ TM-1  TM-2        ...          TM-6                    │
│ 16C/32G            16C/32G            16C/32G           │
│ 4 slot             4 slot             4 slot             │
│                                                          │
│  消费 → Kafka (本城 9094)                                 │
│  写入 → StarRocks (北京 FE VIP:8030)    ──跨城专线──→    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 1.2 资源规划

| 角色 | 规格 | 数量 | 说明 |
|---|---|---|---|
| JobManager | 8C/16G/200G | 2 | 1 Active + 1 Standby |
| TaskManager | 16C/32G/500G SSD | 2-6 | 每 TM 4 slot,按 series 数扩展 |
| ZooKeeper | 4C/8G/100G | 3 | JM HA 依赖,可与 Kafka 共用 |

### 1.3 端口规划

| 端口 | 组件 | 用途 | 暴露范围 |
|---|---|---|---|
| 8081 | JobManager Web UI | 作业管理界面 | 运维网段 |
| 6123 | JobManager RPC | TM ↔ JM 通信 | Flink 内部 |
| 6124 | JobManager Blob Server | JAR 分发 | Flink 内部 |
| 9999 | Metrics | Prometheus 抓取 | Prometheus 网段 |
| 2181 | ZooKeeper | JM HA 选主 | Flink 内部 |

---

## 2. 前置准备

### 2.1 操作系统

```bash
# CentOS / RHEL 8+ 或 Ubuntu / Debian 22+
# 所有 JM / TM 节点执行
```

### 2.2 JDK 17 安装

```bash
# CentOS / RHEL
sudo yum install -y java-17-openjdk java-17-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-17-jdk

java -version   # 期望: openjdk version "17.x.x"
```

### 2.3 创建 Flink 目录

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/flink /appdata/flink/checkpoints /appdata/flink/savepoints /applog/flink
sudo chown -R bdops:bdops /appdata/flink /applog/flink
```

### 2.4 内核参数调优

**`/etc/sysctl.d/99-flink.conf`**:

```ini
# 内存
vm.swappiness=10                        # Flink 用堆外内存,适度使用 swap
vm.max_map_count=262144                 # RocksDB 需要
vm.overcommit_memory=1                  # 允许内存超分

# 网络
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
net.ipv4.ip_local_port_range=10000 65535

# 文件句柄
fs.file-max=1000000
```

```bash
sudo sysctl --system
```

**`/etc/security/limits.d/flink.conf`**:

```
flink  soft  nofile  100000
flink  hard  nofile  100000
flink  soft  nproc   100000
flink  hard  nproc   100000
```

### 2.5 SSH 免密(JM 到 TM)

```bash
# JM 节点上生成密钥
ssh-keygen -t rsa -b 4096

# 分发到所有 TM 节点
for host in tm-1 tm-2 tm-3 tm-4 tm-5 tm-6 jm-2; do
    ssh-copy-id flink@${host}
done
```

### 2.6 下载并安装 Flink

```bash
cd /opt
sudo wget https://archive.apache.org/dist/flink/flink-1.19.2/flink-1.19.2-bin-scala_2.12.tgz
sudo tar -xzf flink-1.19.2-bin-scala_2.12.tgz
sudo ln -s flink-1.19.2 flink
sudo chown -R bdops:bdops /appdata/flink
ls /appdata/flink/bin/flink   # 确认解压成功
```

### 2.7 安装 Hadoop(可选,仅 Checkpoint/Savepoint 用 HDFS 时)

```bash
# 如果 Checkpoint 用本地文件系统(小规模)或 NFS,可跳过 Hadoop
# 如果用 HDFS(推荐,大规模生产):
# 安装 Hadoop 3.3+ 并配置 HDFS,见 Hadoop 官方文档
```

---

## 3. Flink 集群安装

### 3.1 目录结构

```
/appdata/flink/
├── conf/
│   ├── flink-conf.yaml          # 主配置
│   ├── masters                  # JM 节点列表
│   ├── workers                  # TM 节点列表
│   └── log4j2.xml              # 日志配置
├── bin/
│   ├── start-cluster.sh         # 启动集群
│   ├── stop-cluster.sh          # 停止集群
│   ├── flink                    # 作业提交 CLI
│   └── jobmanager.sh            # 单独启动 JM
├── lib/                         # 依赖 JAR
│   └── flink-dist-1.19.2.jar
├── plugins/                     # 插件(如 RocksDB)
└── log/                         # 日志输出
```

### 3.2 配置 masters 和 workers

**`/appdata/flink/conf/masters`**:

```
jm-1:8081
jm-2:8081
```

**`/appdata/flink/conf/workers`**:

```
tm-1
tm-2
tm-3
tm-4
tm-5
tm-6
```

### 3.3 分发到所有节点

```bash
# 从 JM-1 分发到所有节点
for host in jm-2 tm-1 tm-2 tm-3 tm-4 tm-5 tm-6; do
    echo "同步到 ${host}..."
    rsync -avz /appdata/flink/ flink@${host}:/appdata/flink/
done
```

---

## 4. JM HA 配置

### 4.1 ZooKeeper 部署

JM HA 依赖 ZooKeeper 进行主备选举。在 3 台节点上部署 ZK:

```bash
# 下载 ZooKeeper 3.8+
cd /opt
sudo wget https://archive.apache.org/dist/zookeeper/zookeeper-3.8.3/apache-zookeeper-3.8.3-bin.tar.gz
sudo tar -xzf apache-zookeeper-3.8.3-bin.tar.gz
sudo ln -s apache-zookeeper-3.8.3 zookeeper
sudo chown -R bdops:bdops /appdata/zookeeper

# 配置 /appdata/zookeeper/conf/zoo.cfg
cat > /appdata/zookeeper/conf/zoo.cfg << 'EOF'
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data/zookeeper
clientPort=2181
server.1=zk-1:2888:3888
server.2=zk-2:2888:3888
server.3=zk-3:2888:3888
EOF

# 创建 myid 文件(每台不同)
echo 1 > /data/zookeeper/myid   # zk-1
echo 2 > /data/zookeeper/myid   # zk-2
echo 3 > /data/zookeeper/myid   # zk-3

# 启动
/appdata/zookeeper/bin/zkServer.sh start
/appdata/zookeeper/bin/zkServer.sh status   # 查看角色(leader/follower)
```

### 4.2 Flink HA 配置

在 `flink-conf.yaml` 中配置 HA(见 §5.1 完整配置):

```yaml
# ====== JM HA ======
high-availability: org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices
high-availability.zookeeper.quorum: zk-1:2181,zk-2:2181,zk-3:2181
high-availability.zookeeper.path.root: /flink
high-availability.storageDir: hdfs:///flink/ha/
high-availability.cluster-id: /cluster-sz

# JM 故障后自动恢复
jobmanager.execution.failover-strategy: region
```

### 4.3 HA 验证

```bash
# 1. 启动集群
/appdata/flink/bin/start-cluster.sh

# 2. 查看 JM 状态(两个 JM,一个 Active 一个 Standby)
curl -s http://jm-1:8081/overview | python3 -m json.tool | grep runningJobs
curl -s http://jm-2:8081/overview | python3 -m json.tool | grep runningJobs

# 3. 模拟 JM 故障
ssh jm-1 "kill -9 \$(pidof StandaloneSessionClusterEntrypoint)"
sleep 5

# 4. 确认 Standby JM 接管
curl -s http://jm-2:8081/overview | python3 -m json.tool | grep runningJobs
# 期望: jm-2 变为 Active,作业继续运行

# 5. 恢复 jm-1(成为新 Standby)
ssh jm-1 "/appdata/flink/bin/jobmanager.sh start"
```

---

## 5. 集群配置详解

### 5.1 flink-conf.yaml

**`/appdata/flink/conf/flink-conf.yaml`**:

```yaml
# ====== 基础 ======
jobmanager.rpc.address: jm-1              # JM RPC 地址(集群模式用 masters 文件)
jobmanager.rpc.port: 6123
jobmanager.bind-host: 0.0.0.0
jobmanager.web.address: 0.0.0.0
jobmanager.web.port: 8081

# ====== JM 内存 ======
jobmanager.memory.process.size: 4096m
jobmanager.memory.flink.size: 3072m
jobmanager.memory.heap.size: 2048m
jobmanager.memory.off-heap.size: 1024m
jobmanager.memory.jvm-metaspace.size: 256m
jobmanager.memory.jvm-overhead.size: 512m

# ====== TM 内存 ======
taskmanager.memory.process.size: 32768m
taskmanager.memory.flink.size: 24576m
taskmanager.memory.framework.heap.size: 1024m
taskmanager.memory.framework.off-heap.size: 512m
taskmanager.memory.task.heap.size: 16384m
taskmanager.memory.managed.size: 6144m
taskmanager.memory.network.min: 1024m
taskmanager.memory.network.max: 2048m
taskmanager.memory.network.fraction: 0.1
taskmanager.memory.jvm-metaspace.size: 512m
taskmanager.memory.jvm-overhead.min: 384m
taskmanager.memory.jvm-overhead.max: 1024m
taskmanager.memory.jvm-overhead.fraction: 0.1

# ====== TM 配置 ======
taskmanager.numberOfTaskSlots: 4          # 每 TM 的 slot 数(= CPU 核数 / 4)
taskmanager.bind-host: 0.0.0.0

# ====== 并行度 ======
parallelism.default: 24                   # 默认并行度(= Kafka partition 数)

# ====== Checkpoint ======
execution.checkpointing.interval: 60000   # 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 300000   # 5min 超时
execution.checkpointing.min-pause: 30000  # 两次 checkpoint 最小间隔 30s
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.storage: filesystem
state.checkpoints.dir: hdfs:///flink/checkpoints/
state.savepoints.dir: hdfs:///flink/savepoints/

# ====== 状态后端 ======
state.backend: rocksdb
state.backend.rocksdb.localdir: /appdata/flink/rocksdb
state.backend.rocksdb.memory.managed: true

# ====== RocksDB 调优 ======
state.backend.rocksdb.writebuffer.size: 256mb
state.backend.rocksdb.writebuffer.count: 4
state.backend.rocksdb.writebuffer.number-to-merge: 2
state.backend.rocksdb.block.blocksize: 32kb
state.backend.rocksdb.block.cache-size: 256mb
state.backend.rocksdb.compaction.style: LEVEL

# ====== JM HA ======
high-availability: org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices
high-availability.zookeeper.quorum: zk-1:2181,zk-2:2181,zk-3:2181
high-availability.zookeeper.path.root: /flink
high-availability.zookeeper.client.acl: open
high-availability.storageDir: hdfs:///flink/ha/
high-availability.cluster-id: /cluster-sz

# ====== 故障恢复 ======
jobmanager.execution.failover-strategy: region
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 30s

# ====== 网络 ======
taskmanager.network.memory.fraction: 0.1
taskmanager.network.memory.max: 2048m
taskmanager.network.memory.buffers-per-channel: 8

# ====== Metrics ======
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9999
metrics.reporter.prom.interval: 15 SECONDS

# ====== Web UI ======
web.submit.enable: true
web.cancel.enable: true
rest.flamegraph.enabled: true

# ====== JDK ======
# Flink 1.19 官方支持 Java 11/17,不支持 JDK 21+。
# 系统默认 java 可能是 JDK 25(给 Kafka/StarRocks),必须显式指定 JDK 17。
env.java.home: /usr/lib/jvm/java-17-openjdk

# ====== JVM ======
# 注意:覆盖 env.java.opts.all 会丢失 Flink 默认携带的 --add-opens 参数。
# JDK 17+ 默认强封装,任何类型落到 Kryo 时,其 Chill 序列化器
# (ArraysAsListSerializer 等)反射访问 java.util 内部字段会抛
# InaccessibleObjectException(module java.base does not "opens java.util")。
# 本工程 Flink 作业已在代码层消除 Kryo(@TypeInfo 显式声明原生序列化器),
# 但仍保留 --add-opens 兜底,防止其他作业/字段意外落入 Kryo。
env.java.opts.all: >-
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.text=ALL-UNNAMED
  --add-opens=java.base/java.time=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=100
  -XX:+AlwaysPreTouch
```

### 5.2 关键参数说明

#### 5.2.1 内存配置

```
TM 总内存 (32G) = Flink 内存 (24G) + JVM Metaspace (512M) + JVM Overhead (384M-1024M)
                          |
                          ├── Framework Heap (1G)    — Flink 框架堆内存
                          ├── Framework Off-Heap (512M) — Flink 框架堆外内存
                          ├── Task Heap (16G)        — 用户算子堆内存
                          ├── Managed Memory (6G)    — RocksDB 状态
                          └── Network (1-2G)         — 网络缓冲区
```

| 参数 | 值 | 说明 |
|---|---|---|
| `task.heap.size` | 16G | 用户算子堆内存,存放窗口状态对象 |
| `managed.size` | 6G | RocksDB 使用的堆外内存 |
| `network.fraction` | 0.1 | 网络缓冲区占比(shuffle 数据传输) |
| `jvm-overhead` | 384M-1024M | GC、线程栈等 JVM 开销 |

> **调优建议**:如果 RocksDB state 过大(> 10G),增加 `managed.size` 到 12G+;如果 GC 频繁,减少 `task.heap.size`。

#### 5.2.2 Slot 配置

```
TM (16C/32G) → 4 slots → 每 slot 4C/8G
全局并行度 = TM 数 × slots/TM = 6 × 4 = 24
```

| TM 规格 | slots/TM | 全局并行度(6 TM) | 说明 |
|---|---|---|---|
| 8C/16G | 2 | 12 | 小规模 |
| 16C/32G | 4 | 24 | 推荐(生产默认) |
| 32C/64G | 8 | 48 | 大规模 |

#### 5.2.3 Checkpoint 配置

| 参数 | 值 | 说明 |
|---|---|---|
| `interval` | 60s | Checkpoint 间隔,太短会增加延迟 |
| `mode` | EXACTLY_ONCE | 精确一次语义 |
| `timeout` | 300s | 超时时间,state 大时需调大 |
| `min-pause` | 30s | 两次 checkpoint 最小间隔 |
| `max-concurrent` | 1 | 不允许并发 checkpoint |
| `externalized-retention` | RETAIN_ON_CANCELLATION | 作业取消时保留 checkpoint |

---

## 6. 作业部署

### 6.1 打包

```bash
cd examples/flink-agg5m-starrocks

# 编译打包
mvn clean package -Pprod

# 产物
ls -la target/flink-agg5m-starrocks-1.0.0.jar
# 期望: ~35MB fat jar
```

### 6.2 上传 JAR

```bash
# 上传到 JM 节点
scp target/flink-agg5m-starrocks-1.0.0.jar jm-1:/appdata/flink/jobs/

# 如果用 Web UI 提交,也可通过浏览器上传
```

### 6.3 提交作业

```bash
# 通过 CLI 提交
/appdata/flink/bin/flink run \
  -d \                                  # detached 模式(提交后不等待)
  -p 24 \                               # 全局并行度(= Kafka partition 数)
  -c com.example.promgw.Agg5mJob \
  /appdata/flink/jobs/flink-agg5m-starrocks-1.0.0.jar \
  --env prod \
  --city sz \
  --kafka-brokers kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094 \
  --topic prom.sz.routed.app_business \
  --group-id flink-agg5m-sz-app-business \
  --starrocks-host <beijing-fe-vip> \
  --starrocks-port 8030 \
  --starrocks-db prom \
  --starrocks-table sr_bj_metrics_5m \
  --starrocks-user root \
  --starrocks-password "" \
  --label-prefix sz_5m \
  --dlq-topic prom.sz.dlq.sr.5m \
  --dlq-enabled true \
  --sr-batch-size 500 \
  --sr-batch-interval-ms 10000 \
  --source-parallelism 24 \
  --agg-parallelism 24 \
  --window-minutes 5 \
  --checkpoint-path hdfs:///flink/checkpoints/agg5m-sz \
  --checkpoint-interval-ms 60000 \
  --allowed-lateness-ms 30000
```

### 6.4 参数模板(各城)

| 参数 | 深圳 | 合肥 | 北京 |
|---|---|---|---|
| `--city` | `sz` | `hf` | `bj` |
| `--kafka-brokers` | `kafka-1.sz:9094,...` | `kafka-1.hf:9094,...` | `kafka-1.bj:9094,...` |
| `--topic` | `prom.sz.routed.app_business` | `prom.hf.routed.app_business` | `prom.bj.routed.app_business` |
| `--group-id` | `flink-agg5m-sz-app-business` | `flink-agg5m-hf-app-business` | `flink-agg5m-bj-app-business` |
| `--starrocks-host` | `<beijing-fe-vip>` | `<beijing-fe-vip>` | `<beijing-fe-vip>` |
| `--label-prefix` | `sz_5m` | `hf_5m` | `bj_5m` |
| `--dlq-topic` | `prom.sz.dlq.sr.5m` | `prom.hf.dlq.sr.5m` | `prom.bj.dlq.sr.5m` |
| `-p` (并行度) | 24 | 8 | 24 |

### 6.5 通过 Web UI 提交

1. 访问 `http://jm-1:8081`
2. 左侧菜单 → "Submit new Job"
3. 上传 JAR 文件
4. 填入 Main Class: `com.example.promgw.Agg5mJob`
5. 填入 Program Arguments(同 §6.3 的参数)
6. 设置 Parallelism: 24
7. 点击 "Submit"

### 6.6 作业管理

```bash
# 查看运行中的作业
/appdata/flink/bin/flink list -r

# 查看所有作业(含已完成)
/appdata/flink/bin/flink list -a

# 取消作业(正常停止,触发 Savepoint)
/appdata/flink/bin/flink stop --savepointPath hdfs:///flink/savepoints/ <job-id>

# 取消作业(立即停止,不触发 Savepoint)
/appdata/flink/bin/flink cancel <job-id>

# 从 Savepoint 恢复
/appdata/flink/bin/flink run -s hdfs:///flink/savepoints/savepoint-xxxxx -d -p 24 \
  -c com.example.promgw.Agg5mJob \
  /appdata/flink/jobs/flink-agg5m-starrocks-1.0.0.jar \
  --env prod --city sz ...
```

---

## 7. Checkpoint 与 Savepoint

### 7.1 Checkpoint 管理

Checkpoint 是 Flink 自动触发的增量状态快照,用于故障恢复:

```bash
# 查看作业的 Checkpoint 统计
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints | python3 -m json.tool

# 查看 Checkpoint 详情
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints/details/<checkpoint-id> | python3 -m json.tool

# 列出 HDFS 上的 Checkpoint
hdfs dfs -ls /flink/checkpoints/agg5m-sz/

# 清理旧 Checkpoint(Flink 会自动清理,通常无需手动)
hdfs dfs -rm -r /flink/checkpoints/agg5m-sz/ckpt-000023
```

### 7.2 Savepoint 管理

Savepoint 是手动触发的全量状态快照,用于升级、迁移:

```bash
# 触发 Savepoint(作业继续运行)
/appdata/flink/bin/flink savepoint <job-id> hdfs:///flink/savepoints/

# 停止作业并触发 Savepoint
/appdata/flink/bin/flink stop --savepointPath hdfs:///flink/savepoints/ <job-id>

# 从 Savepoint 恢复
/appdata/flink/bin/flink run -s hdfs:///flink/savepoints/savepoint-xxxxx \
  -d -p 24 -c com.example.promgw.Agg5mJob \
  /appdata/flink/jobs/flink-agg5m-starrocks-1.0.0.jar --env prod --city sz ...

# 删除 Savepoint
/appdata/flink/bin/flink savepoint -d hdfs:///flink/savepoints/savepoint-xxxxx
```

### 7.3 Checkpoint vs Savepoint

| 维度 | Checkpoint | Savepoint |
|---|---|---|
| 触发方式 | 自动(定时) | 手动 |
| 格式 | 增量(RocksDB) | 全量 |
| 用途 | 故障恢复 | 升级、迁移、A/B 测试 |
| 保留 | 作业取消时可选保留 | 手动删除前永久保留 |
| 性能影响 | 小(增量) | 较大(全量) |
| 格式兼容 | 版本相关 | 跨版本兼容(向后兼容) |

### 7.4 滚动升级流程

```bash
# 1. 触发 Savepoint
SAVEPOINT_PATH=$(/appdata/flink/bin/flink stop \
  --savepointPath hdfs:///flink/savepoints/ \
  <job-id> | grep "Savepoint completed" | grep -oP 'at \K[^ ]+')
echo "Savepoint: ${SAVEPOINT_PATH}"

# 2. 替换 JAR
scp target/flink-agg5m-starrocks-1.1.0.jar jm-1:/appdata/flink/jobs/

# 3. 从 Savepoint 恢复
/appdata/flink/bin/flink run -s ${SAVEPOINT_PATH} \
  -d -p 24 -c com.example.promgw.Agg5mJob \
  /appdata/flink/jobs/flink-agg5m-starrocks-1.1.0.jar \
  --env prod --city sz ...

# 4. 验证作业正常运行
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool

# 5. 清理旧 Savepoint(可选)
/appdata/flink/bin/flink savepoint -d ${SAVEPOINT_PATH}
```

---

## 8. 监控部署

### 8.1 Prometheus 指标暴露

Flink 1.19 内置 Prometheus Reporter,在 `flink-conf.yaml` 中已配置:

```yaml
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9999
metrics.reporter.prom.interval: 15 SECONDS
```

```bash
# 验证
curl -s http://tm-1:9999/metrics | head -20
```

### 8.2 Prometheus 抓取配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: flink-jm
    static_configs:
      - targets:
        - jm-1:9999
        - jm-2:9999
    scrape_interval: 15s

  - job_name: flink-tm
    static_configs:
      - targets:
        - tm-1:9999
        - tm-2:9999
        - tm-3:9999
        - tm-4:9999
        - tm-5:9999
        - tm-6:9999
    scrape_interval: 15s
```

### 8.3 关键监控指标

| 指标 | Flink metric | 告警阈值 |
|---|---|---|
| 消费速率 | `flink_taskmanager_job_task_numRecordsInPerSecond` | 突降 50% |
| 输出速率 | `flink_taskmanager_job_task_numRecordsOutPerSecond` | 突降 50% |
| 事件时间延迟 | `flink_taskmanager_job_task_currentEventTimeLag` | > 60s |
| 处理延迟 | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | > 1000ms/s |
| Checkpoint 耗时 | `flink_taskmanager_job_lastCheckpointDuration` | > 60s |
| Checkpoint 失败 | `flink_taskmanager_job_numFailedCheckpoints` | > 0 |
| Checkpoint 大小 | `flink_taskmanager_job_lastCheckpointSize` | - |
| Kafka Consumer Lag | `flink_taskmanager_job_task_KafkaSource_records_lag_max` | > 10000 |
| TM 内存使用 | `flink_taskmanager_Status_JVM_Memory_Direct_MemoryUsed` | > 80% |
| GC 暂停 | `flink_taskmanager_Status_JVM_GarbageCollector_TotalTime` | > 500ms |
| SR 批量 flush 成功 | `flink_taskmanager_job_task_promgw_srFlushSuccess` | - |
| SR 批量 flush 失败 | `flink_taskmanager_job_task_promgw_srFlushFailure` | > 0 |
| SR DLQ 行数 | `flink_taskmanager_job_task_promgw_srDlqRows` | > 0 告警 |
| Decode 失败 | `flink_taskmanager_job_task_promgw_decodeFailures` | > 0 告警 |

### 8.4 告警规则

**`/etc/prometheus/rules/flink-alerts.yaml`**:

```yaml
groups:
  - name: flink
    rules:
      # 作业失败
      - alert: FlinkJobFailed
        expr: flink_jobmanager_numRunningJobs < 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink 作业已停止"

      # 消费滞后
      - alert: FlinkConsumerLagHigh
        expr: flink_taskmanager_job_task_KafkaSource_records_lag_max > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink Kafka 消费 lag > 10000"

      # 事件时间延迟
      - alert: FlinkEventTimeLagHigh
        expr: flink_taskmanager_job_task_currentEventTimeLag > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink 事件时间延迟 > 60s"

      # Checkpoint 失败
      - alert: FlinkCheckpointFailed
        expr: increase(flink_taskmanager_job_numFailedCheckpoints[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink Checkpoint 失败"

      # Checkpoint 耗时过长
      - alert: FlinkCheckpointSlow
        expr: flink_taskmanager_job_lastCheckpointDuration > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink Checkpoint 耗时 > 60s"

      # TM 宕机
      - alert: FlinkTaskManagerDown
        expr: count(up{job="flink-tm"} == 0) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink TaskManager 宕机"

      # StarRocks Stream Load flush 失败
      - alert: StarRocksFlushFailure
        expr: increase(flink_taskmanager_job_task_promgw_srFlushFailure[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks Stream Load 批量写入失败,数据已进入 DLQ"

      # DLQ 行数增长(StarRocks 持续不可用)
      - alert: StarRocksDlqRowsIncreasing
        expr: increase(flink_taskmanager_job_task_promgw_srDlqRows[10m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks DLQ 行数持续增长,检查 StarRocks 可用性"

      # Decode 失败(Kafka 消息格式异常)
      - alert: FlinkDecodeFailures
        expr: increase(flink_taskmanager_job_task_promgw_decodeFailures[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Flink Kafka 消息解码失败,检查 prom-gw 编码格式"
```

### 8.5 Grafana Dashboard

| Dashboard | ID | 说明 |
|---|---|---|
| Flink Dashboard | 11000 | 作业概览、消费速率、Checkpoint |
| Flink RocksDB | 14932 | RocksDB state 监控 |

---

## 9. 性能调优

### 9.1 并行度调优

| 阶段 | 并行度 | 说明 |
|---|---|---|
| Kafka Source | = partition 数 | 1:1 消费,确保吞吐 |
| Dedup + Decode | = source 并行度 | 同链路 |
| 窗口聚合 | = source 并行度 | 避免 shuffle |
| StarRocks Sink | = source 并行度 / 2 | 攒批 Stream Load(BufferingStarRocksSink),每 subtask 独立攒批 |

```bash
# 命令行指定各阶段并行度(在 Agg5mJob 代码中已支持)
--source-parallelism 24
--agg-parallelism 24
```

### 9.2 RocksDB 调优

```yaml
# flink-conf.yaml
state.backend.rocksdb.memory.managed: true        # 使用 managed memory
state.backend.rocksdb.writebuffer.size: 256mb      # 写缓冲,减少 flush 频率
state.backend.rocksdb.writebuffer.count: 4         # 最大写缓冲数
state.backend.rocksdb.writebuffer.number-to-merge: 2  # 合并时的写缓冲数
state.backend.rocksdb.block.blocksize: 32kb        # 块大小
state.backend.rocksdb.block.cache-size: 256mb      # 块缓存
state.backend.rocksdb.compaction.style: LEVEL      # 层级压缩
```

### 9.3 网络缓冲区调优

```yaml
# 适用于高吞吐场景
taskmanager.network.memory.fraction: 0.15          # 网络内存占比
taskmanager.network.memory.max: 4096m              # 最大网络内存
taskmanager.network.memory.buffers-per-channel: 4  # 每 channel 缓冲区数
```

### 9.4 作业级调优(Agg5mJob)

| 参数 | 默认值 | 调优建议 | 说明 |
|---|---|---|---|
| `--window-minutes` | 5 | 不要改 | 5min 是设计要求 |
| `--allowed-lateness-ms` | 30000 | 10000-60000 | 允许的乱序时间,影响窗口触发延迟 |
| `--checkpoint-interval-ms` | 60000 | 30000-120000 | 太短影响吞吐,太长影响恢复 |
| `--source-parallelism` | 4 | = partition 数 | 确保 1:1 消费 |
| `--agg-parallelism` | 4 | = source 并行度 | 避免 shuffle |
| `--sr-batch-size` | 500 | 200-2000 | 攒批行数上限,达到即 flush。越大 HTTP 请求数越少,但内存占用越高 |
| `--sr-batch-interval-ms` | 10000 | 5000-30000 | 攒批时间上限(ms),超时即 flush。确保 5min 窗口数据在下次窗口触发前写入 |

### 9.5 JVM 调优

```yaml
# flink-conf.yaml(必须保留 --add-opens,理由见 5.1 JVM 说明)
env.java.opts.all: >-
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=100
  -XX:+AlwaysPreTouch
  -XX:+ExplicitGCInvokesConcurrent
```

---

## 10. 运维操作

### 10.1 集群启停

```bash
# 启动整个集群(JM + TM)
/appdata/flink/bin/start-cluster.sh

# 停止整个集群
/appdata/flink/bin/stop-cluster.sh

# 单独启动 / 停止 JM
/appdata/flink/bin/jobmanager.sh start
/appdata/flink/bin/jobmanager.sh stop

# 单独启动 / 停止 TM
/appdata/flink/bin/taskmanager.sh start
/appdata/flink/bin/taskmanager.sh stop
```

### 10.2 滚动重启 TM

```bash
# 逐台重启 TM(不影响作业运行,前提:有足够 slot)
for tm in tm-1 tm-2 tm-3 tm-4 tm-5 tm-6; do
    echo "重启 ${tm}..."
    ssh ${tm} "/appdata/flink/bin/taskmanager.sh stop"
    sleep 5
    ssh ${tm} "/appdata/flink/bin/taskmanager.sh start"
    sleep 10
    # 等待 TM 注册到 JM
    curl -s http://jm-1:8081/taskmanagers | python3 -m json.tool | grep ${tm}
done
```

### 10.3 日志查看

```bash
# JM 日志
tail -f /applog/flink/flink-*-standalonesession-*.log

# TM 日志
tail -f /applog/flink/flink-*-taskexecutor-*.log

# 作业日志(在 TM 上)
ls /applog/flink/
# flink-flink-agg5m-sz-*_*.log

# 查看错误日志
grep -i "error\|exception\|failed" /applog/flink/flink-*.log | tail -50
```

### 10.4 作业状态排查

```bash
# 查看作业概览
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool

# 查看作业详情(含各算子指标)
curl -s http://jm-1:8081/jobs/<job-id> | python3 -m json.tool

# 查看作业异常
curl -s http://jm-1:8081/jobs/<job-id>/exceptions | python3 -m json.tool

# 查看 Backpressure
curl -s http://jm-1:8081/jobs/<job-id>/vertices/<vertex-id>/backpressure | python3 -m json.tool

# 查看 Watermark
curl -s http://jm-1:8081/jobs/<job-id>/vertices/<vertex-id>/watermarks | python3 -m json.tool
```

### 10.5 常见问题处理

| 现象 | 排查 | 解决 |
|---|---|---|
| 作业启动即失败 | 查看作业异常日志 | 检查 Kafka 连接、StarRocks 可达性、参数格式 |
| 消费 lag 持续增大 | 查看 TM 指标,检查 Backpressure | 扩 partition / 扩 TM / 调整并行度 |
| Checkpoint 超时 | 查看 state 大小,RocksDB IOPS | 增大 managed memory / 用 SSD / 增大 checkpoint timeout |
| Checkpoint 失败 | 查看异常日志 | 检查 HDFS 连通性 / 磁盘空间 |
| StarRocks 写入失败 | 查看 Sink 异常日志,检查 `srFlushFailure` / `srDlqRows` 指标 | 检查 FE VIP 可达性 / Label 冲突 / BE 压力 / DLQ Kafka 可达性 |
| 攒批延迟过高 | 查看 `srFlushSuccess` 速率与 buffer 大小 | 调小 `--sr-batch-size` 或 `--sr-batch-interval-ms` |
| JM 内存不足 | 查看 JM 日志,GC 情况 | 增大 JM 内存 / 减少作业数 |
| TM OOM | 查看 TM 日志 | 增大 task.heap / 减少 slot 数 / 优化 state |

### 10.6 资源调优速查表

| 场景 | TM 规格 | slots/TM | TM 数 | 并行度 | managed | 说明 |
|---|---|---|---|---|---|---|
| 本地开发 | 4C/8G | 2 | 1 | 4 | 1G | 单节点 |
| 小型生产 | 8C/16G | 2 | 2 | 4 | 2G | < 100K samples/s |
| 中型生产 | 16C/32G | 4 | 4 | 16 | 6G | 100K-1M samples/s |
| 大型生产 | 16C/32G | 4 | 6 | 24 | 8G | > 1M samples/s(推荐) |
| 超大规模 | 32C/64G | 8 | 8 | 64 | 16G | > 5M samples/s |

---

## 11. 附录

### 11.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `flink-conf.yaml` | `/appdata/flink/conf/flink-conf.yaml` | 主配置 |
| `masters` | `/appdata/flink/conf/masters` | JM 节点列表 |
| `workers` | `/appdata/flink/conf/workers` | TM 节点列表 |
| `log4j2.xml` | `/appdata/flink/conf/log4j2.xml` | 日志配置 |
| `flink-agg5m-starrocks-1.0.0.jar` | `/appdata/flink/jobs/` | 作业 JAR |
| `zoo.cfg` | `/appdata/zookeeper/conf/zoo.cfg` | ZooKeeper 配置 |

### 11.2 常用命令速查

```bash
# 集群管理
/appdata/flink/bin/start-cluster.sh
/appdata/flink/bin/stop-cluster.sh
/appdata/flink/bin/jobmanager.sh start|stop
/appdata/flink/bin/taskmanager.sh start|stop

# 作业管理
/appdata/flink/bin/flink list -r|-a
/appdata/flink/bin/flink run -d -p 24 -c <main-class> <jar> [args]
/appdata/flink/bin/flink cancel <job-id>
/appdata/flink/bin/flink stop --savepointPath <path> <job-id>

# Savepoint
/appdata/flink/bin/flink savepoint <job-id> <path>
/appdata/flink/bin/flink run -s <savepoint-path> ...
/appdata/flink/bin/flink savepoint -d <savepoint-path>

# 查看状态
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool
curl -s http://jm-1:8081/taskmanagers | python3 -m json.tool
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints | python3 -m json.tool
```

### 11.3 命令行参数完整列表

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--env` | `local` | 环境预设:`local` / `prod` |
| `--city` | - | 城市标识:`bj` / `sz` / `hf` |
| `--kafka-brokers` | `localhost:9092` | Kafka broker 地址列表 |
| `--topic` | `prom.local.routed.app_business` | 消费的 Kafka topic |
| `--group-id` | `flink-agg5m-local` | Kafka consumer group ID |
| `--starrocks-host` | `localhost` | StarRocks FE VIP |
| `--starrocks-port` | `8030` | StarRocks FE HTTP 端口(Stream Load 复用,无 8070) |
| `--starrocks-db` | `prom` | StarRocks 数据库 |
| `--starrocks-table` | `sr_bj_metrics_5m` | StarRocks 表名 |
| `--starrocks-user` | `root` | StarRocks 用户名 |
| `--starrocks-password` | (空) | StarRocks 密码 |
| `--label-prefix` | `local_5m` | Stream Load label 前缀(每城唯一) |
| `--dlq-topic` | `prom.local.dlq.sr.5m` | DLQ Kafka topic |
| `--dlq-enabled` | `true` | 是否启用 DLQ |
| `--sr-batch-size` | `500` | StarRocks 攒批行数上限,达到即 flush |
| `--sr-batch-interval-ms` | `10000` | StarRocks 攒批时间上限(ms),超时即 flush |
| `--source-parallelism` | `4` | Kafka Source 并行度 |
| `--agg-parallelism` | `4` | 窗口聚合并行度 |
| `--window-minutes` | `5` | 窗口大小(分钟) |
| `--checkpoint-path` | `file:///tmp/flink-checkpoints` | Checkpoint 存储路径 |
| `--checkpoint-interval-ms` | `60000` | Checkpoint 间隔(ms) |
| `--allowed-lateness-ms` | `30000` | 允许的乱序时间(ms) |



---

