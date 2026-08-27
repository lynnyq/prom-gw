# StarRocks 生产部署与配置详解
> 本文档覆盖 prom-gw 配套 MPP 数据仓库 **StarRocks v3.4.10** 的生产环境完整部署,包括存算一体(Shared-Nothing)集群架构、FE/BE 部署、JBOD 多盘存储配置、Nginx 反向代理、监控集成和运维操作。
>
> StarRocks 是高性能 MPP 数据库,用于 prom-gw 的实时数仓分析,支持亚秒级查询、实时更新、联邦查询等功能。
>
> 配套文档:**Kafka 生产部署**(见 §2)、**Flink 生产部署**(见 §5)、**高可用与负载均衡**(见 §7)、**生产部署指南**(见 §1)


---

## 1. 部署架构

### 1.1 存算一体拓扑

StarRocks 采用存算一体(Shared-Nothing)架构,3 FE + 3 BE 节点组成高可用集群。FE 管理元数据和查询调度,BE 负责数据存储和计算执行:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  ┌─────────────────┐    ┌──────────────────────────────────┐     │
│  │  Nginx + VIP    │    │  FE Cluster (元数据 + 查询调度)   │     │
│  │  VIP:10.0.10.100│───▶│  sr-fe-1 (Leader)  10.0.10.31   │     │
│  │  443 → 8030      │    │  sr-fe-2 (Follower) 10.0.10.32   │     │
│  └─────────────────┘    │  sr-fe-3 (Follower) 10.0.10.33   │     │
│                          └──────────┬───────────────────────┘     │
│  ┌──────────────────────────────────┐                              │
│  │  MySQL Client (10.0.10.50)      │  9030 (MySQL 协议)            │
│  └──────────────────────────────────┘                              │
│                                     │                              │
│          ┌──────────────────────────┘                              │
│          ▼                                                         │
│  ┌──────────────────────────────────────────────────────┐         │
│  │  BE Cluster (存储 + 计算)                             │         │
│  │  sr-be-1  10.0.1.31   22×16T JBOD (/data01~/data22)  │         │
│  │  sr-be-2  10.0.1.32   22×16T JBOD (/data01~/data22)  │         │
│  │  sr-be-3  10.0.1.33   22×16T JBOD (/data01~/data22)  │         │
│  └──────────────────────────────────────────────────────┘         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

> **存算一体说明**:BE 节点同时承担数据存储和 SQL 计算执行,数据按 Tablet(分片)分布到 3 个 BE 节点,默认 3 副本冗余。22 块盘通过 JBOD 挂载,BE 自动将 Tablet 均匀分布到各盘。

### 1.2 端口规划

**FE 端口**:

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| 8030 | FE HTTP(Web UI + REST API) | Nginx VIP / 运维网段 |
| 9020 | FE RPC(Thrift,FE 间通信) | FE 网段内部 |
| 9030 | FE MySQL 协议(客户端查询) | 运维网段 / Nginx |
| 9010 | FE Edit Log(Follower 同步) | FE 网段内部 |

**BE 端口**:

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| 8040 | BE HTTP(REST API + 文件传输) | FE → BE 网段 |
| 9060 | BE Thrift(FE → BE 通信) | FE → BE 网段 |
| 9050 | BE Heartbeat(心跳上报) | FE → BE 网段 |
| 8060 | BE bRPC(FE → BE / BE 间通信) | FE → BE / BE 网段 |
| 9070 | BE Starlet(存算分离用,存算一体可忽略) | — |

### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 | 网络 | 说明 |
|---|---|---|---|---|---|
| FE | 8C/16G | 3 | 100G SSD | 万兆 | 元数据管理,轻量级,可与 BE 共置 |
| BE | 64C/256G | 3 | 22×16T HDD(JBOD) | 万兆 | 数据存储 + 计算,CPU 需支持 AVX2 |
| Nginx + Keepalived | 2C/4G | 2 | 50G SSD | 千兆 | 复用 **HA 部署**(见 §7) |

> **AVX2 检查**:BE 依赖 AVX2 指令集加速向量化执行,部署前需验证:
> ```bash
> cat /proc/cpuinfo | grep avx2
> # 有输出 = 支持
> ```

---

## 2. 前置准备

### 2.1 操作系统

```bash
# CentOS / RHEL 8+
cat /etc/redhat-release

# Ubuntu / Debian 22+
cat /etc/os-release
```

### 2.2 OpenJDK 25 安装

StarRocks v3.4 要求 JDK 11+,统一使用 OpenJDK 25(与 Kafka / Kafka-UI 保持一致):

```bash
# CentOS / RHEL
sudo yum install -y java-25-openjdk java-25-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-25-jdk

java -version   # 期望: openjdk version "25.x.x"
```

> **注意**:StarRocks 不支持 JRE,必须安装 JDK(含 `java-devel`)。若实例存在多个 JDK,可在 `fe.conf` / `be.conf` 中指定 `JAVA_HOME`。

### 2.3 系统调优

所有 FE 和 BE 节点执行以下系统配置:

```bash
# ====== 1. 关闭 THP(Transparent Huge Pages) ======
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag

# 持久化(/etc/rc.local)
cat >> /etc/rc.local << 'EOF'
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
EOF
sudo chmod +x /etc/rc.local

# ====== 2. 关闭 Swap ======
sudo swapoff -a
sudo sed -i '/swap/s/^/#/' /etc/fstab

# ====== 3. 文件描述符限制 ======
sudo tee /etc/security/limits.d/starrocks.conf << 'EOF'
*   soft    nofile    655360
*   hard    nofile    655360
*   soft    nproc     655350
*   hard    nproc     655350
*   soft    memlock   unlimited
*   hard    memlock   unlimited
EOF

# 重新登录 shell 生效,验证:
ulimit -n   # 期望: 655360

# ====== 4. 内核参数(/etc/sysctl.conf) ======
sudo tee -a /etc/sysctl.conf << 'EOF'
# 文件描述符
fs.file-max = 6553500

# 内存 overcommit(BE 需要)
vm.overcommit_memory = 1
vm.overcommit_ratio = 90

# 网络
net.ipv4.tcp_max_tw_buckets = 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 10000 65535
net.core.somaxconn = 65535

# 禁用 THP
kernel.numa_balancing = 0
EOF

sudo sysctl -p
```

### 2.4 NTP 时钟同步

```bash
# 安装 chrony
sudo yum install -y chrony        # CentOS / RHEL
sudo apt install -y chrony        # Ubuntu / Debian

# 配置 NTP 服务器
sudo vi /etc/chrony.conf
# 添加: server ntp.aliyun.com iburst
# (或内网 NTP 服务器)

sudo systemctl enable --now chronyd
chronyc tracking   # 验证同步状态
```

> **强制**:StarRocks 集群所有节点时钟偏差必须 < 5 秒,否则 BE 心跳异常导致节点状态波动。

### 2.5 安装 MySQL 客户端

FE 节点(或运维节点)需要 MySQL 客户端连接 StarRocks:

```bash
# CentOS / RHEL
sudo yum install -y mysql
# Ubuntu / Debian
sudo apt install -y mysql-client

mysql --version   # 5.5.0+ 即可
```

### 2.6 创建用户与目录

**在所有节点(FE + BE)上执行**:

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
# 部署目录(STARROCKS_HOME)
sudo mkdir -p /appdata/starrocks

# ====== BE 节点:创建 22 个 JBOD 数据目录 ======
for i in $(seq -w 1 22); do
    sudo mkdir -p /data${i}/starrocks
done

# ====== 设置属主 ======
sudo chown -R bdops:bdops /appdata/starrocks
for i in $(seq -w 1 22); do
    sudo chown -R bdops:bdops /data${i}/starrocks
done
```

> **目录说明**:
> - 程序目录: `/appdata/starrocks`  日志目录: `/applog/starrocks`(FE 日志在 `/applog/starrocks/fe/`,BE 日志在 `/applog/starrocks/be/`)
> - FE 元数据 = `/appdata/starrocks/fe/meta`(默认,可改至独立磁盘)
> - BE 数据 = `/data01/starrocks` ~ `/data22/starrocks`(22 个 JBOD 挂载点)

### 2.7 磁盘挂载(BE 节点 JBOD)

每台 BE 物理机 22 × 16T 盘,JBOD 模式(不做 RAID):

```bash
# /etc/fstab(BE 节点)
/dev/sdb1 /data01  ext4 noatime,nodiratime 0 2
/dev/sdc1 /data02  ext4 noatime,nodiratime 0 2
/dev/sdd1 /data03  ext4 noatime,nodiratime 0 2
/dev/sde1 /data04  ext4 noatime,nodiratime 0 2
/dev/sdf1 /data05  ext4 noatime,nodiratime 0 2
/dev/sdg1 /data06  ext4 noatime,nodiratime 0 2
/dev/sdh1 /data07  ext4 noatime,nodiratime 0 2
/dev/sdi1 /data08  ext4 noatime,nodiratime 0 2
/dev/sdj1 /data09  ext4 noatime,nodiratime 0 2
/dev/sdk1 /data10  ext4 noatime,nodiratime 0 2
/dev/sdl1 /data11  ext4 noatime,nodiratime 0 2
/dev/sdm1 /data12  ext4 noatime,nodiratime 0 2
/dev/sdn1 /data13  ext4 noatime,nodiratime 0 2
/dev/sdo1 /data14  ext4 noatime,nodiratime 0 2
/dev/sdp1 /data15  ext4 noatime,nodiratime 0 2
/dev/sdq1 /data16  ext4 noatime,nodiratime 0 2
/dev/sdr1 /data17  ext4 noatime,nodiratime 0 2
/dev/sds1 /data18  ext4 noatime,nodiratime 0 2
/dev/sdt1 /data19  ext4 noatime,nodiratime 0 2
/dev/sdu1 /data20  ext4 noatime,nodiratime 0 2
/dev/sdv1 /data21  ext4 noatime,nodiratime 0 2
/dev/sdw1 /data22  ext4 noatime,nodiratime 0 2

sudo mount -a
```

### 2.8 /etc/hosts 配置

所有节点配置主机名解析(或使用内部 DNS):

```bash
# /etc/hosts 追加
# FE nodes
10.0.10.31  sr-fe-1
10.0.10.32  sr-fe-2
10.0.10.33  sr-fe-3
# BE nodes
10.0.1.31   sr-be-1
10.0.1.32   sr-be-2
10.0.1.33   sr-be-3
```

---

## 3. 下载与安装

### 3.1 下载 StarRocks 3.4.10

```bash
# 在运维节点下载(或任一 FE 节点)
cd /tmp
wget https://github.com/StarRocks/starrocks/releases/download/3.4.10/StarRocks-3.4.10.tar.gz

# 备用下载地址(如 GitHub 无法访问)
# wget https://releases.starrocks.com/StarRocks-3.4.10.tar.gz

# 校验文件大小(约 1-2 GB)
ls -lh StarRocks-3.4.10.tar.gz
```

> **版本固定**:生产环境必须使用固定版本 `3.4.10`,禁止使用 main 分支构建。

### 3.2 解压与分发

```bash
# 解压到 STARROCKS_HOME
sudo mkdir -p /appdata/starrocks
sudo tar -xzf /tmp/StarRocks-3.4.10.tar.gz -C /appdata/starrocks --strip-components=1

# 目录结构
ls /appdata/starrocks/
# 期望: fe/  be/  LICENSE  NOTICE  README.md

sudo chown -R bdops:bdops /appdata/starrocks
```

**分发到其他节点**:

```bash
# 分发到 FE-2, FE-3
for host in sr-fe-2 sr-fe-3; do
    sudo -u bdops scp -r /appdata/starrocks bdops@$host:/appdata/
done

# 分发到 BE-1, BE-2, BE-3
for host in sr-be-1 sr-be-2 sr-be-3; do
    sudo -u bdops scp -r /appdata/starrocks bdops@$host:/appdata/
done

# 设置属主(远程节点)
for host in sr-fe-2 sr-fe-3 sr-be-1 sr-be-2 sr-be-3; do
    ssh bdops@$host "sudo chown -R bdops:bdops /appdata/starrocks"
done
```

### 3.3 目录结构

```
/appdata/starrocks/          ← STARROCKS_HOME(安装 + 日志)
  ├── fe/
  │   ├── bin/
  │   │   ├── start_fe.sh
  │   │   └── stop_fe.sh
  │   ├── conf/
  │   │   └── fe.conf        ← FE 主配置
  │   ├── lib/
  │   ├── log/               ← FE 日志目录
  │   │   ├── fe.log
  │   │   ├── fe.warn.log
  │   │   └── fe.audit.log
  │   └── meta/              ← FE 元数据(运行时创建)
  └── be/
      ├── bin/
      │   ├── start_be.sh
      │   └── stop_be.sh
      ├── conf/
      │   └── be.conf        ← BE 主配置
      ├── lib/
      ├── log/               ← BE 日志目录
      │   ├── be.INFO
      │   └── be.WARNING
      └── storage/           ← 默认存储(生产用 JBOD,见 §5)
```

---

## 4. FE 部署

### 4.1 fe.conf 配置

**在所有 FE 节点上配置 `/appdata/starrocks/fe/conf/fe.conf`**:

```bash
# ======================================================
# StarRocks FE 配置 - sr-fe-1 (Leader)
# 其他 FE 节点(sr-fe-2, sr-fe-3)仅 priority_networks 不同
# ======================================================

# ====== 网络端口 ======
http_port = 8030         # FE HTTP(Web UI + REST API)
rpc_port = 9020          # FE Thrift RPC(FE 间通信)
query_port = 9030         # MySQL 协议(客户端查询)
edit_log_port = 9010     # Edit Log(Follower 同步)

# ====== 元数据目录 ======
meta_dir = /appdata/starrocks/fe/meta

# ====== 网络(IP 访问模式) ======
# sr-fe-1:
priority_networks = 10.0.10.31/24
# sr-fe-2:
# priority_networks = 10.0.10.32/24
# sr-fe-3:
# priority_networks = 10.0.10.33/24

# ====== JDK(若多版本 JDK,指定路径) ======
# JAVA_HOME = /usr/lib/jvm/java-25-openjdk

# ====== JVM 参数 ======
# FE 默认堆 4G,生产建议 8G(取决于元数据量)
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
# JDK 8 兼容(JAVA_OPTS_FOR_JDK_8 在 JDK 11+ 可忽略)
JAVA_OPTS_FOR_JDK_11 = "-Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# ====== 集群配置 ======
# 默认副本数(3 BE 节点 = 3 副本)
default_replication_num = 3
# 默认存储介质(HDD,如全 SSD 则改为 ssd)
default_storage_medium = hdd
# Tablet 单副本存储上限(GB),超出自动分裂
storage_max_storages_per_disk = 100

# ====== 审计日志 ======
enable_audit_log = true
audit_log_dir = /applog/starrocks/fe
audit_log_modules = slow_query,external

# ====== 慢查询阈值(秒) ======
qe_max_connection = 2000
```

> **多 FE 节点注意**:所有 FE 的 `http_port` 必须相同。`priority_networks` 按节点 IP 设置。

### 4.2 启动 Leader FE(sr-fe-1)

```bash
# 在 sr-fe-1 上执行
su - bdops
cd /appdata/starrocks
./fe/bin/start_fe.sh --daemon

# 检查启动日志
cat /applog/starrocks/fe/fe.log | grep thrift
# 期望输出: thrift server started with port 9020

# 检查进程
jps | grep StarRocksFE
# 期望: xxxxx StarRocksFE
```

### 4.3 添加 Follower FE(sr-fe-2, sr-fe-3)

**通过 MySQL 客户端添加 Follower**:

```bash
# 在运维节点或 sr-fe-1 上执行
mysql -h sr-fe-1 -P9030 -uroot
```

```sql
-- 添加 Follower FE 节点(逐个添加)
ALTER SYSTEM ADD FOLLOWER "sr-fe-2:9010";
ALTER SYSTEM ADD FOLLOWER "sr-fe-3:9010";

-- 查看当前 FE 列表
SHOW PROC '/frontends'\G
```

**在 sr-fe-2 上启动 Follower FE**:

```bash
su - bdops
cd /appdata/starrocks
# 首次启动需指定 helper(指向 Leader FE)
./fe/bin/start_fe.sh --helper sr-fe-1:9010 --daemon

# 检查日志
cat /applog/starrocks/fe/fe.log | grep thrift
```

**在 sr-fe-3 上启动 Follower FE**:

```bash
su - bdops
cd /appdata/starrocks
./fe/bin/start_fe.sh --helper sr-fe-1:9010 --daemon

cat /applog/starrocks/fe/fe.log | grep thrift
```

> **helper 说明**:新 Follower 首次启动时需通过 `--helper` 指定一个已有 Follower FE(通常为 Leader)来同步全量元数据。仅首次启动需指定,后续重启无需 `--helper`。

### 4.4 验证 FE 集群

```sql
-- 通过 MySQL 客户端连接 Leader FE
mysql -h sr-fe-1 -P9030 -uroot

-- 查看 FE 节点状态
SHOW PROC '/frontends'\G
```

期望输出:
```
*************************** 1. row ***************************
              Name: 10.0.10.31_9010_xxx
                IP: 10.0.10.31
      EditLogPort: 9010
          HttpPort: 8030
         QueryPort: 9030
           RpcPort: 9020
             Role: LEADER
         ClusterId: xxxxxxx
             Join: true
            Alive: true
 ReplayedJournalId: xxx
     LastHeartbeat: 2026-08-20 10:00:00
      IsHelper: true
          ErrMsg:
        StartTime: 2026-08-20 09:50:00
          Version: 3.4.10-xxxxx
*************************** 2. row ***************************
             Role: FOLLOWER
            Alive: true
...
*************************** 3. row ***************************
             Role: FOLLOWER
            Alive: true
...
```

> **验证要点**:
> - `Alive` = `true` → 节点正常
> - `Role` = `LEADER` / `FOLLOWER` → 角色正确
> - `Join` = `true` → 已加入集群
> - 3 个 FE 中 1 个 LEADER + 2 个 FOLLOWER(自动选举)

---

## 5. BE 部署

### 5.1 be.conf 配置

**在所有 BE 节点上配置 `/appdata/starrocks/be/conf/be.conf`**:

```bash
# ======================================================
# StarRocks BE 配置 - sr-be-1
# 其他 BE 节点(sr-be-2, sr-be-3)仅 priority_networks 不同
# ======================================================

# ====== 网络端口 ======
be_port = 9060               # BE Thrift(FE → BE 通信)
be_http_port = 8040           # BE HTTP(REST API + 文件传输)
heartbeat_service_port = 9050 # BE 心跳(FE → BE 探活)
brpc_port = 8060              # BE bRPC(FE → BE / BE 间通信)
# starlet_port = 9070        # 存算分离用,存算一体可忽略

# ====== 数据存储目录(JBOD 22 块盘,分号分隔) ======
storage_root_path = /data01/starrocks;/data02/starrocks;/data03/starrocks;/data04/starrocks;/data05/starrocks;/data06/starrocks;/data07/starrocks;/data08/starrocks;/data09/starrocks;/data10/starrocks;/data11/starrocks;/data12/starrocks;/data13/starrocks;/data14/starrocks;/data15/starrocks;/data16/starrocks;/data17/starrocks;/data18/starrocks;/data19/starrocks;/data20/starrocks;/data21/starrocks;/data22/starrocks

# ====== 网络(IP 访问模式) ======
# sr-be-1:
priority_networks = 10.0.1.31/24
# sr-be-2:
# priority_networks = 10.0.1.32/24
# sr-be-3:
# priority_networks = 10.0.1.33/24

# ====== JDK(若需 Java UDF) ======
# JAVA_HOME = /usr/lib/jvm/java-25-openjdk
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC"
JAVA_OPTS_FOR_JDK_11 = "-Xmx8192m -XX:+UseG1GC"

# ====== 内存配置 ======
# mem_limit = 0.90            # BE 使用内存上限(物理内存比例)
# tablet_wal_max_size = 1GB   # 单 Tablet WAL 大小上限

# ====== 存储配置 ======
storage_format = default       # 默认行列混存格式
# 默认存储介质(如全 SSD 可改为 ssd)
# default_storage_medium = hdd

# ====== 并发配置 ======
# num_threads = 64             # CPU 核心数(默认自动检测)

# ====== 诊断配置 ======
sys_log_dir = /applog/starrocks/be
sys_log_level = INFO
# audit_log_dir = /applog/starrocks/be
```

> **storage_root_path 说明**:
> - 多盘使用分号(`;`)分隔,不是逗号
> - 可指定介质类型:`/data01/starrocks,medium:ssd;/data02/starrocks,medium:hdd`
> - BE 自动将 Tablet 均匀分布到所有盘,单盘故障不影响其他盘

### 5.2 启动 BE 节点

**在所有 BE 节点上执行(sr-be-1, sr-be-2, sr-be-3)**:

```bash
# 在 sr-be-1 上执行
su - bdops
cd /appdata/starrocks
./be/bin/start_be.sh --daemon

# 检查启动日志
cat /applog/starrocks/be/be.INFO | grep heartbeat
# 期望输出: heartbeat has started listening port on 9050

# 在 sr-be-2 上执行
cd /appdata/starrocks
./be/bin/start_be.sh --daemon
cat /applog/starrocks/be/be.INFO | grep heartbeat

# 在 sr-be-3 上执行
cd /appdata/starrocks
./be/bin/start_be.sh --daemon
cat /applog/starrocks/be/be.INFO | grep heartbeat
```

### 5.3 添加 BE 到集群

通过 MySQL 客户端连接 Leader FE,添加 BE 节点:

```bash
mysql -h sr-fe-1 -P9030 -uroot
```

```sql
-- 添加 3 个 BE 节点(一条 SQL 添加多个)
ALTER SYSTEM ADD BACKEND "sr-be-1:9050", "sr-be-2:9050", "sr-be-3:9050";

-- 查看 BE 节点状态
SHOW PROC '/backends'\G
```

期望输出(每个 BE 一行):
```
*************************** 1. row ***************************
        BackendId: 10001
              IP: 10.0.1.31
    HeartbeatPort: 9050
         BePort: 9060
        HttpPort: 8040
        BrpcPort: 8060
   LastStartTime: 2026-08-20 10:00:00
  LastHeartbeat: 2026-08-20 10:01:00
           Alive: true
SystemDecommissioned: false
ClusterDecommissioned: false
       TabletNum: 0
DataUsedCapacity: 0.000
   AvailCapacity: 22.0 TB        ← 22 块盘总可用
   TotalCapacity: 352.0 TB       ← 22 × 16T
         UsedPct: 0.00 %
  MaxDiskUsedPct: 0.00 %
         ErrMsg:
        Version: 3.4.10-xxxxx
```

> **验证要点**:
> - `Alive` = `true` → BE 正常
> - `TotalCapacity` ≈ 22 × 16T → 确认 22 块盘全部识别
> - `TabletNum` 初始为 0,建表后自动增长

---

## 6. 集群初始化与验证

### 6.1 连接集群

```bash
# 通过 MySQL 协议连接(可连任意 FE)
mysql -h sr-fe-1 -P9030 -uroot

# 或通过 Nginx VIP(见 §7)
mysql -h 10.0.10.100 -P9030 -uroot
```

> **初始密码**:root 默认无密码,生产必须设置。

### 6.2 设置 root 密码

```sql
-- 设置 root 密码
SET PASSWORD FOR 'root' = PASSWORD('YourStrongPassword123');

-- 退出后使用密码重连
-- mysql -h sr-fe-1 -P9030 -uroot -p
```

### 6.3 创建数据库与用户

```sql
-- 创建 prom-gw 分析库
CREATE DATABASE observability;

-- 创建专用用户
CREATE USER 'prom_gw' IDENTIFIED BY 'PromGwPassword123';

-- 授权
GRANT SELECT, INSERT, UPDATE, DELETE ON observability.* TO 'prom_gw';

-- 刷新权限
FLUSH PRIVILEGES;

-- 验证
SHOW DATABASES;
-- 期望: information_schema / observability
```

### 6.4 prom-gw 指标表 DDL(三独立表)

完整 SQL 脚本见 `deploy/starrocks/init_tables.sql`,生产部署执行:

```bash
mysql -h sr-fe-1 -P9030 -uprom_gw -p'PromGwPassword123' < deploy/starrocks/init_tables.sql
```

#### 6.4.1 表设计概览

| 表名 | 粒度 | 写入方 | 保留 | BUCKETS | 说明 |
|---|---|---|---|---|---|
| `metrics_5m` | 5 分钟 | Flink Stream Load | 7 天 | 32 | 唯一实时写入点 |
| `metrics_1h` | 1 小时 | 周期任务(5m 级联) | 90 天 | 16 | 由 agg_5m_to_1h 生成 |
| `metrics_1d` | 1 天 | 周期任务(1h 级联) | 3 年 | 8 | 由 agg_1h_to_1d 生成 |

#### 6.4.2 5 分钟明细表(实时写入)

```sql
CREATE TABLE IF NOT EXISTS metrics_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(仅查询)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口求和',
  value_max     DOUBLE                COMMENT '窗口最大值',
  value_min     DOUBLE                COMMENT '窗口最小值',
  value_avg     DOUBLE                COMMENT '窗口均值',
  value_p50     DOUBLE                COMMENT '窗口 p50',
  value_p99     DOUBLE                COMMENT '窗口 p99'
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

#### 6.4.3 1 小时聚合表(级联生成)

```sql
CREATE TABLE IF NOT EXISTS metrics_1h (
  ts            DATETIME     NOT NULL,
  metric        VARCHAR(128) NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL,
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL,
  labels        MAP<VARCHAR(64), VARCHAR(256)>,
  sample_count  BIGINT       NOT NULL COMMENT '1h = 12 个 5min 样本',
  value_sum     DOUBLE       NOT NULL,
  value_max     DOUBLE,
  value_min     DOUBLE,
  value_avg     DOUBLE,
  value_p50     DOUBLE       COMMENT 'percentile_approx t-digest 合并',
  value_p99     DOUBLE
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, labels_hash)
  PARTITION BY RANGE(ts) ()
  DISTRIBUTED BY HASH(metric, business) BUCKETS 16
  PROPERTIES (
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "compression" = "LZ4",
    "replicated_storage" = "true"
  );
```

#### 6.4.4 1 天聚合表(级联生成)

```sql
CREATE TABLE IF NOT EXISTS metrics_1d (
  ts            DATETIME     NOT NULL,
  metric        VARCHAR(128) NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL,
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL,
  labels        MAP<VARCHAR(64), VARCHAR(256)>,
  sample_count  BIGINT       NOT NULL COMMENT '1d = 24 个 1h 样本',
  value_sum     DOUBLE       NOT NULL,
  value_max     DOUBLE,
  value_min     DOUBLE,
  value_avg     DOUBLE,
  value_p50     DOUBLE,
  value_p99     DOUBLE
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, labels_hash)
  PARTITION BY RANGE(ts) ()
  DISTRIBUTED BY HASH(metric, business) BUCKETS 8
  PROPERTIES (
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-36",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "compression" = "LZ4",
    "replicated_storage" = "true"
  );
```

#### 6.4.5 三表设计要点

- **PRIMARY KEY 模型**:三表均使用 PK 模型,`REPLACE INTO` 自动去重,保证 Flink at-least-once 和周期任务幂等
- **labels_hash**:Flink 端用 SHA-1 计算 labels 稳定 hash,作为 PK 键列(StarRocks PK 键列不支持 MAP 类型)
- **级联而非跳级**:1d 从 1h 聚合(非从 5m 直跳),扫描量最小(1h 表 ~66 GB/天 vs 5m 表 ~1 TB/天)
- **INSERT OVERWRITE**:周期任务用 `INSERT OVERWRITE` 覆盖目标时间窗口,重跑无重复
- **p50/p99 聚合**:用 `percentile_approx` t-digest 跨窗口合并,为近似分位(非精确),仅作趋势参考
- **BUCKETS 递减**:5m 表 32 桶(高并发写)、1h 表 16 桶、1d 表 8 桶(减少 tablet 开销)
- **独立 TTL**:各表 `dynamic_partition` 独立清理,互不影响

### 6.5 级联聚合 SQL(5m → 1h → 1d)

三表数据并非 Flink 一次性产出。Flink 只实时写入 5m 表,1h 和 1d 表通过周期 SQL 任务从下级表级联聚合而成。

#### 6.5.1 5m → 1h 聚合(每小时执行)

```sql
-- 每小时整点执行,聚合上一个小时的 5m 数据到 1h 表
-- INSERT OVERWRITE 保证幂等:重跑覆盖,不产生重复
INSERT OVERWRITE metrics_1h
SELECT
    date_trunc('hour', ts)                                             AS ts,
    metric,
    business,
    ingest_city,
    source_dc,
    labels_hash,
    labels,
    SUM(sample_count)                                                  AS sample_count,
    SUM(value_sum)                                                     AS value_sum,
    MAX(value_max)                                                     AS value_max,
    MIN(value_min)                                                     AS value_min,
    SUM(value_sum) / SUM(sample_count)                                 AS value_avg,
    percentile_approx(value_p50, 0.5)                                   AS value_p50,
    percentile_approx(value_p99, 0.99)                                  AS value_p99
FROM metrics_5m
WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR
  AND ts <  date_trunc('hour', NOW())
GROUP BY 1, 2, 3, 4, 5, 6, 7;
```

**聚合逻辑说明**:
- 5m 表 12 行 → 1h 表 1 行(每小时 12 个 5 min 窗口)
- `sample_count`:求和(Σsample_count)
- `value_sum`:求和(Σvalue_sum)
- `value_max`:取 MAX(跨窗口最大值)
- `value_min`:取 MIN(跨窗口最小值)
- `value_avg`:Σvalue_sum / Σsample_count(加权平均)
- `value_p50` / `value_p99`:`percentile_approx` t-digest 近似合并

#### 6.5.2 1h → 1d 聚合(每天执行)

```sql
-- 每天 0 点执行,聚合前一天的 1h 数据到 1d 表
INSERT OVERWRITE metrics_1d
SELECT
    date_trunc('day', ts)                                              AS ts,
    metric,
    business,
    ingest_city,
    source_dc,
    labels_hash,
    labels,
    SUM(sample_count)                                                  AS sample_count,
    SUM(value_sum)                                                     AS value_sum,
    MAX(value_max)                                                     AS value_max,
    MIN(value_min)                                                     AS value_min,
    SUM(value_sum) / SUM(sample_count)                                 AS value_avg,
    percentile_approx(value_p50, 0.5)                                   AS value_p50,
    percentile_approx(value_p99, 0.99)                                  AS value_p99
FROM metrics_1h
WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY
  AND ts <  date_trunc('day', NOW())
GROUP BY 1, 2, 3, 4, 5, 6, 7;
```

**聚合逻辑说明**:
- 1h 表 24 行 → 1d 表 1 行(每天 24 个 1 小时窗口)
- 聚合函数与 5m → 1h 完全一致
- 级联优势:1h 表扫描量(~66 GB/天)远小于 5m 表(~1 TB/天)

#### 6.5.3 手动补跑(历史数据)

```sql
-- 补跑指定小时的 5m → 1h(替换时间为目标小时)
-- INSERT OVERWRITE metrics_1h
-- SELECT ... (同上)
-- FROM metrics_5m
-- WHERE ts >= '2026-08-27 00:00:00' AND ts < '2026-08-27 01:00:00'
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 补跑指定天的 1h → 1d(替换日期为目标天)
-- INSERT OVERWRITE metrics_1d
-- SELECT ... (同上)
-- FROM metrics_1h
-- WHERE ts >= '2026-08-26 00:00:00' AND ts < '2026-08-27 00:00:00'
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 首次全量初始化:从 5m 表最近 7 天聚合出 1h 数据
-- INSERT OVERWRITE metrics_1h
-- SELECT date_trunc('hour', ts) AS ts, metric, business, ingest_city, source_dc,
--        labels_hash, labels, SUM(sample_count) AS sample_count,
--        SUM(value_sum) AS value_sum, MAX(value_max) AS value_max,
--        MIN(value_min) AS value_min,
--        SUM(value_sum) / SUM(sample_count) AS value_avg,
--        percentile_approx(value_p50, 0.5) AS value_p50,
--        percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_5m
-- WHERE ts >= date_trunc('day', NOW()) - INTERVAL 7 DAY
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;
```

### 6.6 定时任务调度

#### 方案 A:StarRocks 内置 CREATE JOB(推荐)

StarRocks 2.5+ 支持内置定时任务,无需外部调度系统:

```sql
-- 5m → 1h:每小时整点执行
CREATE JOB IF NOT EXISTS agg_5m_to_1h
ON SCHEDULE EVERY 1 HOUR STARTS (date_trunc('hour', NOW()) + INTERVAL 1 HOUR)
DO
  INSERT OVERWRITE metrics_1h
  SELECT
      date_trunc('hour', ts) AS ts,
      metric, business, ingest_city, source_dc, labels_hash, labels,
      SUM(sample_count) AS sample_count,
      SUM(value_sum) AS value_sum,
      MAX(value_max) AS value_max,
      MIN(value_min) AS value_min,
      SUM(value_sum) / SUM(sample_count) AS value_avg,
      percentile_approx(value_p50, 0.5) AS value_p50,
      percentile_approx(value_p99, 0.99) AS value_p99
  FROM metrics_5m
  WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR
    AND ts <  date_trunc('hour', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 1h → 1d:每天 0 点执行
CREATE JOB IF NOT EXISTS agg_1h_to_1d
ON SCHEDULE EVERY 1 DAY STARTS (date_trunc('day', NOW()) + INTERVAL 1 DAY)
DO
  INSERT OVERWRITE metrics_1d
  SELECT
      date_trunc('day', ts) AS ts,
      metric, business, ingest_city, source_dc, labels_hash, labels,
      SUM(sample_count) AS sample_count,
      SUM(value_sum) AS value_sum,
      MAX(value_max) AS value_max,
      MIN(value_min) AS value_min,
      SUM(value_sum) / SUM(sample_count) AS value_avg,
      percentile_approx(value_p50, 0.5) AS value_p50,
      percentile_approx(value_p99, 0.99) AS value_p99
  FROM metrics_1h
  WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY
    AND ts <  date_trunc('day', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7;
```

**管理命令**:

```sql
-- 查看所有 JOB
SHOW JOBS;

-- 查看运行中 JOB
SHOW RUNNING JOBS;

-- 查看 JOB 执行历史
SHOW HISTORY FOR JOB agg_5m_to_1h;
SHOW HISTORY FOR JOB agg_1h_to_1d;

-- 暂停 JOB
STOP JOB agg_5m_to_1h;
STOP JOB agg_1h_to_1d;

-- 恢复 JOB
RESUME JOB agg_5m_to_1h;
RESUME JOB agg_1h_to_1d;

-- 删除 JOB
DROP JOB agg_5m_to_1h;
DROP JOB agg_1h_to_1d;
```

#### 方案 B:外部 crontab 调度

若 StarRocks 版本 < 2.5 或需要接入企业调度平台,用 crontab 调用:

```bash
# /etc/cron.d/starrocks-agg
# 5m → 1h:每小时第 1 分钟执行(延迟 1 分钟确保 5m 数据已落盘)
* 1 * * * bdops mysql -h sr-fe-1 -P9030 -uprom_gw -p'PromGwPassword123' observability \
  -e "INSERT OVERWRITE metrics_1h SELECT date_trunc('hour', ts) AS ts, metric, business, ingest_city, source_dc, labels_hash, labels, SUM(sample_count) AS sample_count, SUM(value_sum) AS value_sum, MAX(value_max) AS value_max, MIN(value_min) AS value_min, SUM(value_sum) / SUM(sample_count) AS value_avg, percentile_approx(value_p50, 0.5) AS value_p50, percentile_approx(value_p99, 0.99) AS value_p99 FROM metrics_5m WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR AND ts < date_trunc('hour', NOW()) GROUP BY 1, 2, 3, 4, 5, 6, 7;" \
  >> /applog/starrocks/agg_5m_to_1h.log 2>&1

# 1h → 1d:每天 00:10 执行(延迟 10 分钟确保 1h 聚合已完成)
10 0 * * * bdops mysql -h sr-fe-1 -P9030 -uprom_gw -p'PromGwPassword123' observability \
  -e "INSERT OVERWRITE metrics_1d SELECT date_trunc('day', ts) AS ts, metric, business, ingest_city, source_dc, labels_hash, labels, SUM(sample_count) AS sample_count, SUM(value_sum) AS value_sum, MAX(value_max) AS value_max, MIN(value_min) AS value_min, SUM(value_sum) / SUM(sample_count) AS value_avg, percentile_approx(value_p50, 0.5) AS value_p50, percentile_approx(value_p99, 0.99) AS value_p99 FROM metrics_1h WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY AND ts < date_trunc('day', NOW()) GROUP BY 1, 2, 3, 4, 5, 6, 7;" \
  >> /applog/starrocks/agg_1h_to_1d.log 2>&1
```

> **注意**:crontab 方案需自行处理失败重试和告警,建议配套 Prometheus + Alertmanager 监控 JOB 执行状态。

### 6.7 集群健康检查

```sql
-- 查看 FE 状态
SHOW PROC '/frontends'\G

-- 查看 BE 状态
SHOW PROC '/backends'\G

-- 查看集群整体状态
SHOW BACKENDS;
SHOW FRONTENDS;

-- 查看数据库
SHOW DATABASES;

-- 查看 Tablet 分布(建表后)
SHOW TABLET FROM observability.kafka_consumer_metrics LIMIT 10;
```

---

## 7. Nginx 反向代理

### 7.1 Nginx 配置

复用现有 [HA 与负载均衡部署**(见 §7) 的 Nginx,为 StarRocks FE Web UI 和 MySQL 协议做反向代理:

**`/etc/nginx/conf.d/starrocks.conf`**:

```nginx
# FE Web UI (HTTP 8030)
upstream starrocks_fe_http {
    server 10.0.10.31:8030;
    server 10.0.10.32:8030 backup;
    server 10.0.10.33:8030 backup;
}

# FE MySQL 协议 (TCP 9030) - stream 模块
# 需要 nginx 编译 --with-stream 模块

server {
    listen 443 ssl http2;
    server_name starrocks.prom-gw.internal;

    ssl_certificate     /etc/nginx/ssl/prom-gw.crt;
    ssl_certificate_key /etc/nginx/ssl/prom-gw.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # FE Web UI 反向代理
    location / {
        proxy_pass http://starrocks_fe_http;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 支持(Web UI 实时查询)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # 访问控制
    allow 10.0.10.0/24;
    allow 10.0.1.0/24;
    deny all;
}
```

**MySQL 协议 TCP 代理**(`/etc/nginx/stream.d/starrocks.conf`):

```nginx
# FE MySQL 协议负载均衡(TCP)
stream {
    upstream starrocks_fe_mysql {
        server 10.0.10.31:9030;
        server 10.0.10.32:9030;
        server 10.0.10.33:9030;
    }

    server {
        listen 9030;
        proxy_pass starrocks_fe_mysql;
        proxy_connect_timeout 10s;
        proxy_timeout 300s;
    }
}
```

```bash
sudo nginx -t
sudo nginx -s reload
```

---

## 8. 监控集成

### 8.1 FE Prometheus 指标

StarRocks FE 通过 HTTP `8030` 端口暴露 Prometheus 指标:

```bash
# 验证 FE 指标端点
curl -s http://sr-fe-1:8030/metrics | head -20
```

**Prometheus 抓取配置**(`prometheus.yml` 追加):

```yaml
scrape_configs:
  # StarRocks FE
  - job_name: starrocks-fe
    static_configs:
      - targets:
          - sr-fe-1:8030
          - sr-fe-2:8030
          - sr-fe-3:8030
    metrics_path: /metrics
    scrape_interval: 15s

  # StarRocks BE
  - job_name: starrocks-be
    static_configs:
      - targets:
          - sr-be-1:8040
          - sr-be-2:8040
          - sr-be-3:8040
    metrics_path: /metrics
    scrape_interval: 15s
```

### 8.2 关键指标

| 组件 | 指标 | 说明 |
|---|---|---|
| FE | `starrocks_fe_query_total` | 查询总数 |
| FE | `starrocks_fe_query_err` | 查询错误数 |
| FE | `starrocks_fe_query_latency` | 查询延迟 |
| FE | `starrocks_fe_txn_status` | 事务状态 |
| BE | `starrocks_be_process_cpu_usage` | CPU 使用率 |
| BE | `starrocks_be_process_mem_resident` | 内存使用 |
| BE | `starrocks_be_disks_total_capacity` | 磁盘总量 |
| BE | `starrocks_be_disks_avail_capacity` | 磁盘可用 |
| BE | `starrocks_be_tablet_num` | Tablet 数量 |
| BE | `starrocks_be_row_count` | 行数 |

### 8.3 告警规则

**`prometheus-rules.yml` 追加**:

```yaml
groups:
  - name: starrocks
    rules:
      - alert: StarRocksFEDown
        expr: up{job="starrocks-fe"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks FE down on {{ $labels.instance }}"

      - alert: StarRocksBEDown
        expr: up{job="starrocks-be"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks BE down on {{ $labels.instance }}"

      - alert: StarRocksDiskUsageHigh
        expr: 1 - starrocks_be_disks_avail_capacity / starrocks_be_disks_total_capacity > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks disk usage > 85% on {{ $labels.instance }}"

      - alert: StarRocksQueryErrorRate
        expr: rate(starrocks_fe_query_err[5m]) / rate(starrocks_fe_query_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks query error rate > 5%"
```

---

## 9. 运维操作

### 9.1 启停服务

```bash
# ====== 启动(必须先 FE 后 BE) ======
# 启动 Leader FE
su - bdops
cd /appdata/starrocks && ./fe/bin/start_fe.sh --daemon

# 启动 Follower FE(非首次,无需 --helper)
./fe/bin/start_fe.sh --daemon

# 启动 BE
cd /appdata/starrocks && ./be/bin/start_be.sh --daemon

# ====== 停止(必须先 BE 后 FE) ======
# 停止 BE
./be/bin/stop_be.sh

# 停止 FE
./fe/bin/stop_fe.sh
```

> **启停顺序**:
> - **启动**:FE → BE(FE 就绪后 BE 才能注册心跳)
> - **停止**:BE → FE(BE 优雅下线后再停 FE)

### 9.2 扩容(新增 BE)

```bash
# 1. 新 BE 节点:安装软件、配置 be.conf(§2 + §3 + §5)
# 2. 启动新 BE
su - bdops
cd /appdata/starrocks && ./be/bin/start_be.sh --daemon

# 3. 通过 MySQL 客户端添加
mysql -h sr-fe-1 -P9030 -uroot -p
```

```sql
ALTER SYSTEM ADD BACKEND "sr-be-4:9050";

-- 查看是否加入
SHOW PROC '/backends'\G

-- 触发负载均衡(可选)
-- ADMIN SHOW REPLICA DISTRIBUTION FROM TABLE observability.kafka_consumer_metrics;
```

> **扩容后自动均衡**:BE 加入集群后,StarRocks 自动迁移部分 Tablet 到新节点,实现负载均衡。可通过 `SHOW PROC '/backends'` 的 `TabletNum` 观察均衡进度。

### 9.3 缩容(下线 BE)

```sql
-- 1. 标记 DECOMMISSION(安全下线,自动迁移数据)
ALTER SYSTEM DECOMMISSION BACKEND "sr-be-4:9050";

-- 2. 等待数据迁移完成(TabletNum 降为 0)
SHOW PROC '/backends'\G

-- 3. 确认 SystemDecommissioned = true 且 TabletNum = 0 后移除
ALTER SYSTEM DROP BACKEND "sr-be-4:9050";
```

> **禁止直接 DROP**:未 DECOMMISSION 直接 DROP 会导致数据丢失副本,需等待 DECOMMISSION 完成再 DROP。

### 9.4 版本升级

```bash
# 1. 备份配置
sudo cp /appdata/starrocks/fe/conf/fe.conf /appdata/starrocks/fe/conf/fe.conf.bak.$(date +%Y%m%d)
sudo cp /appdata/starrocks/be/conf/be.conf /appdata/starrocks/be/conf/be.conf.bak.$(date +%Y%m%d)

# 2. 下载新版本
cd /tmp
wget https://github.com/StarRocks/starrocks/releases/download/<新版本>/StarRocks-<新版本>.tar.gz

# 3. 逐节点滚动升级(先升级 Follower FE,再 Leader FE,最后 BE)
# 3a. 停止节点
su - bdops
cd /appdata/starrocks
./fe/bin/stop_fe.sh    # 或 ./be/bin/stop_be.sh

# 3b. 解压新版本(覆盖 lib 和 bin)
sudo tar -xzf /tmp/StarRocks-<新版本>.tar.gz -C /appdata/starrocks_new --strip-components=1

# 3c. 保留旧配置
sudo cp /appdata/starrocks/fe/conf/fe.conf /appdata/starrocks_new/fe/conf/
sudo cp /appdata/starrocks/be/conf/be.conf /appdata/starrocks_new/be/conf/

# 3d. 切换目录
sudo mv /appdata/starrocks /appdata/starrocks_old
sudo mv /appdata/starrocks_new /appdata/starrocks
sudo chown -R bdops:bdops /appdata/starrocks

# 3e. 启动
./fe/bin/start_fe.sh --daemon    # 或 ./be/bin/start_be.sh --daemon

# 4. 验证
SHOW PROC '/frontends'\G
SHOW PROC '/backends'\G
```

> **升级前必读**:查阅 [3.4.10 Release Notes](https://github.com/StarRocks/starrocks/releases/tag/3.4.10),确认 Breaking Changes。滚动升级时保持集群至少 1 个 FE + 2 个 BE 在线。

### 9.5 备份与恢复

```bash
# 备份配置
sudo tar -czf starrocks-config-backup-$(date +%Y%m%d).tar.gz \
    -C /appdata/starrocks fe/conf/ be/conf/

# 恢复配置
sudo tar -xzf starrocks-config-backup-20260820.tar.gz -C /appdata/starrocks/
```

### 9.6 常见排查

```bash
# FE 日志
tail -100f /applog/starrocks/fe/fe.log
tail -100f /applog/starrocks/fe/fe.warn.log    # 警告日志
tail -100f /applog/starrocks/fe/fe.audit.log   # 审计日志

# BE 日志
tail -100f /applog/starrocks/be/be.INFO
tail -100f /applog/starrocks/be/be.WARNING

# 检查端口监听
ss -tlnp | grep -E '8030|9020|9030|9010'   # FE
ss -tlnp | grep -E '8040|9060|9050|8060'  # BE

# 检查磁盘使用
df -h /data01 /data22

# 检查 Tablet 分布
mysql -h sr-fe-1 -P9030 -uroot -p -e "SHOW PROC '/backends'\G"
```

---

## 10. 附录

### 10.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `fe.conf` | `/appdata/starrocks/fe/conf/fe.conf` | FE 主配置(端口 / 元数据 / JVM) |
| `be.conf` | `/appdata/starrocks/be/conf/be.conf` | BE 主配置(端口 / JBOD 存储 / JVM) |
| `fe.log` | `/applog/starrocks/fe/fe.log` | FE 主日志 |
| `fe.warn.log` | `/applog/starrocks/fe/fe.warn.log` | FE 警告日志 |
| `fe.audit.log` | `/applog/starrocks/fe/fe.audit.log` | FE 审计日志 |
| `be.INFO` | `/applog/starrocks/be/be.INFO` | BE 主日志 |
| `be.WARNING` | `/applog/starrocks/be/be.WARNING` | BE 警告日志 |
| `starrocks.conf` | `/etc/nginx/conf.d/starrocks.conf` | Nginx 反向代理(HTTP) |
| `starrocks.conf` | `/etc/nginx/stream.d/starrocks.conf` | Nginx TCP 代理(MySQL) |
| `starrocks.conf` | `/etc/security/limits.d/starrocks.conf` | 文件描述符限制 |
| `/data01~22/starrocks/` | BE 节点各盘 | BE 数据存储(JBOD 22 块盘) |

### 10.2 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| FE 启动失败 | `cat fe/log/fe.warn.log`;检查端口占用 | 清理 `meta/` 后重新初始化 / 释放端口 |
| BE 启动失败 | `cat be/log/be.WARNING`;检查 `storage_root_path` 路径是否存在 | 创建目录 / 修正配置 / 清理 `storage/` |
| BE 不加入集群 | `SHOW PROC '/backends'`;检查 `Alive` 和 `ErrMsg` | 检查网络 / NTP 时钟同步 / 端口放行 |
| Tablet 不可用 | `SHOW TABLET FROM <db>.<table>`;检查 `State` | `ADMIN REPAIR TABLE <db>.<table>` 触发修复 |
| 查询超时 | 检查 `fe.audit.log`;确认 BE 负载 / 数据量 | 调优 SQL / 扩容 BE / 分区裁剪 |
| 磁盘满 | `SHOW PROC '/backends'`;检查 `UsedPct` | 扩容 / 清理过期分区 / DECOMMISSION |
| FE 脑裂 | `SHOW PROC '/frontends'`;多个 LEADER | 检查 NTP / 网络,停止异常 FE 重新加入 |
| BE OOM | `dmesg \| grep -i oom`;检查 `mem_limit` | 调小 `mem_limit` / 扩容内存 |
| 副本不足 | `SHOW TABLET FROM <db>.<table>`;`ReplicaCount` < 3 | 检查 BE 存活 / `ADMIN REPAIR TABLE` |

### 10.3 JVM 调优

**FE JVM**(fe.conf):

```bash
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -Xlog:gc*:file=/applog/starrocks/fe/fe.gc.log:time,uptime:filecount=10,filesize=100m"
```

| 参数 | 值 | 说明 |
|---|---|---|
| `-Xmx8192m` | 8G | FE 元数据堆内存(表多时调大) |
| `-XX:+UseG1GC` | G1 GC | 低延迟 |
| `-XX:MaxGCPauseMillis=200` | 200ms | GC 暂停目标 |

**BE JVM**(be.conf,主要用于 Java UDF):

```bash
JAVA_OPTS = "-Xmx8192m -XX:+UseG1GC"
```

> **说明**:BE 主体为 C++ 进程,JVM 仅用于 Java UDF / 外部 Catalog 等功能。BE 内存管理通过 `mem_limit` 配置,不依赖 JVM 堆。

### 10.4 BE 存储路径速查

22 块 JBOD 盘的 `storage_root_path` 配置(分号分隔):

```bash
# be.conf 中的 storage_root_path(单行):
storage_root_path = /data01/starrocks;/data02/starrocks;/data03/starrocks;/data04/starrocks;/data05/starrocks;/data06/starrocks;/data07/starrocks;/data08/starrocks;/data09/starrocks;/data10/starrocks;/data11/starrocks;/data12/starrocks;/data13/starrocks;/data14/starrocks;/data15/starrocks;/data16/starrocks;/data17/starrocks;/data18/starrocks;/data19/starrocks;/data20/starrocks;/data21/starrocks;/data22/starrocks
```

如需指定 SSD 介质(前 2 块为 SSD,后 20 块为 HDD):

```bash
storage_root_path = /data01/starrocks,medium:ssd;/data02/starrocks,medium:ssd;/data03/starrocks,medium:hdd;/data04/starrocks,medium:hdd;/data05/starrocks,medium:hdd;/data06/starrocks,medium:hdd;/data07/starrocks,medium:hdd;/data08/starrocks,medium:hdd;/data09/starrocks,medium:hdd;/data10/starrocks,medium:hdd;/data11/starrocks,medium:hdd;/data12/starrocks,medium:hdd;/data13/starrocks,medium:hdd;/data14/starrocks,medium:hdd;/data15/starrocks,medium:hdd;/data16/starrocks,medium:hdd;/data17/starrocks,medium:hdd;/data18/starrocks,medium:hdd;/data19/starrocks,medium:hdd;/data20/starrocks,medium:hdd;/data21/starrocks,medium:hdd;/data22/starrocks,medium:hdd
```

### 10.5 v3.4.10 主要变更

| 类别 | 说明 |
|---|---|
| Security | 修复 LZ4-java CVE 漏洞 (CVE-2025-12183, CVE-2025-66566) |
| Bug Fix | 多语句提交时 Profile 中 SQL 信息记录错误 |
| Bug Fix | Java UDF/UDAF 在全 NULL 列时 OOM |
| Bug Fix | 排名窗口函数(无 PARTITION BY)执行计划异常致 BE 崩溃 |
| Bug Fix | Object/JSON 列操作后悬垂指针致段错误 |
| Bug Fix | trim() 特殊 Unicode 空格致越界 |
| Bug Fix | BE 心跳在崩溃时短暂返回成功致 FE 误判 |
| Bug Fix | ExecutionGroup + JOIN + 窗口函数数据乱序 |
| Bug Fix | CASE-WHEN 深嵌套致 FE OOM |
| 说明 | v3.4.10 推荐作为升级到 v3.5 的前置版本 |

> **升级 v3.5 注意**:从 v3.4 升级 v3.5 需将 JDK 升级到 17+,并移除 `JAVA_OPTS` 中 CMS/CMS 相关参数。建议先升级到 3.4.10 再升级 v3.5。



---

