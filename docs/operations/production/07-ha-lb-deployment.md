# 高可用与负载均衡部署详解

## 6. LVS 负载均衡部署

### 6.1 LVS + Keepalived 配置

每机房 2 台 LVS 主备,DR 模式转发到 prom-gw 实例。

**`/etc/keepalived/keepalived.conf`(LVS-Master)**:

```
global_defs {
    router_id LVS_BJ
}

vrrp_instance VI_1 {
    state MASTER
    interface eth0
    virtual_router_id 51
    priority 100
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass promgw_lvs
    }
    virtual_ipaddress {
        10.0.1.100/24          # LVS VIP
    }
}

virtual_server 10.0.1.100 19201 {
    delay_loop 2
    lb_algo rr                  # 轮询
    lb_kind DR                  # 直接路由
    protocol TCP

    real_server 10.0.1.11 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.12 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.13 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.14 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
}
```

### 6.2 prom-gw 实例配置 VIP

DR 模式要求 real_server( prom-gw)配置 VIP 到 lo 接口:

```bash
# 在每台 prom-gw 机器上
sudo ip addr add 10.0.1.100/32 dev lo
echo 1 | sudo tee /proc/sys/net/ipv4/conf/lo/arp_ignore
echo 2 | sudo tee /proc/sys/net/ipv4/conf/lo/arp_announce
echo 1 | sudo tee /proc/sys/net/ipv4/conf/all/arp_ignore
echo 2 | sudo tee /proc/sys/net/ipv4/conf/all/arp_announce
```

---


---

# 7. 高可用与负载均衡 {#7-高可用与负载均衡}
> 本文档覆盖 prom-gw 生产环境的高可用架构设计、Nginx/HAProxy 负载均衡配置、Keepalived VIP 高可用、健康检查、SSL/TLS、多机房容灾、故障切换测试和运维操作。
>
> 配套文档:**生产部署指南**(见 §1)(含 LVS 方案)、**压力测试指南**(见 §8)、**SLO 指标**(见 §12)、**故障剧本**(见 §11)


---

## 1. 高可用架构设计

### 1.1 设计目标

| 目标 | 指标 | 说明 |
|---|---|---|
| 实例可用性 | 99.95% 月度 | 单实例故障不影响服务 |
| 端到端可用性 | 99.9% 月度 | 含 Kafka 链路 |
| 故障切换时间 | < 5s | LB 健康检查 + 自动摘流 |
| 数据零丢失 | 100% | Kafka 故障降级 WAL |
| 水平扩展 | 线性 | 单机房支持 2-10 实例 |

### 1.2 高可用分层

```
┌─────────────────────────────────────────────────────────┐
│                    客户端 (Prometheus)                     │
│                  remote_write → VIP:19201                │
└──────────────────────┬──────────────────────────────────┘
                       │
            ┌──────────▼──────────┐
            │  Keepalived VIP     │  ← 主备自动切换
            │  (10.0.1.100)       │
            └──────────┬──────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Nginx / HAProxy (LB)    │  ← 4 层负载均衡
         │   nginx-lb-1 / nginx-lb-2 │  ← 主备双机
         └─────────────┬─────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
   │prom-gw-1│   │prom-gw-2│   │prom-gw-N│  ← 无状态,水平扩展
   │ :19201  │   │ :19201  │   │ :19201  │
   └────┬────┘   └────┬────┘   └────┬────┘
        │              │              │
        └──────────────┼──────────────┘
                       │
              ┌────────▼────────┐
              │  Kafka 集群     │  ← 3 副本 KRaft
              │  (3 Broker)     │
              └─────────────────┘
```

### 1.3 无状态设计

prom-gw 设计为**无状态服务**,所有实例对等:

| 维度 | 说明 |
|---|---|
| 请求处理 | 每个实例独立处理 RemoteWrite 请求,无 session 粘性需求 |
| 配置同步 | 所有实例加载相同 ruleset/token(本地文件或 Nacos) |
| 数据投递 | 每个实例独立写 Kafka,producer 幂等 + acks=all 保证不重复 |
| WAL | 每个实例本地 WAL,故障切换时未 drain 的数据由本机启动时 replay |
| 唯一有状态部分 | downsample/deadvalue stage 的 series 状态,实例故障后重建(可接受) |

> **关键**:因 prom-gw 无状态,LB 可使用最简单的轮询策略,无需会话保持。

### 1.4 多节点时序保证机制

负载均衡模式下,多个 prom-gw 实例并发处理同一 Prometheus 的不同请求,可能引发时序问题。prom-gw 不做全局排序,而是通过**分层保证**让时序问题在下游可解。

#### 1.4.1 同 series 顺序保证(Kafka partition 亲和)

每条 sample 的 Kafka message key 使用 `SeriesKey()`(FNV-1a 64 位 hash,对 business + metric + sorted labels 计算):

```
SeriesKey = FNV-1a64(business + \x00 + metric + \x00 + label1=val1 + \x00 + label2=val2 + \x00 + ...)
```

pipeline 逐 sample 投递时把 SeriesKey 作为 Kafka key,使同一 series 的所有 sample 落到同一 partition,partition 内严格有序。即使不同 prom-gw 节点处理了同一 series 的不同请求,Kafka 的 partition 亲和性保证最终顺序。

相关代码:
- [internal/parser/sample.go](../../internal/parser/sample.go) `SeriesKey()` 方法
- [internal/ruleengine/pipeline.go](../../internal/ruleengine/pipeline.go) 逐 sample 设置 `m.Key = seriesKey`

#### 1.4.2 跨节点重复消除(幂等 producer)

Kafka producer 默认开启幂等写(`enable.idempotence=true`),配合 `acks=all` + `retries=10`,网络重试不产生重复消息。多节点并发写同一 Kafka 不会重复。

相关代码:[internal/kafkasink/producer.go](../../internal/kafkasink/producer.go) `Idempotent` 配置(默认 true)。

#### 1.4.3 WAL 降级时的顺序保证

Kafka 不可用时降级到本地 WAL,WAL 按 segment mtime 顺序重放,Kafka 恢复后按写入顺序 drain,不破坏时序。

相关代码:[internal/wal/wal.go](../../internal/wal/wal.go) `Replay()` 方法。

#### 1.4.4 下游消费侧去重(Flink)

同一 WriteRequest 可能被 LB 分发到不同 prom-gw 节点,各节点都写入 Kafka。下游 Flink 作业按 payload hash 去重,60s 窗口内同 hash 视为重复。

相关代码:[examples/flink-agg5m-starrocks/.../DedupFunction.java](../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/decoder/DedupFunction.java)。

#### 1.4.5 时序保证矩阵

| 场景 | 机制 | 保证级别 | 相关组件 |
|---|---|---|---|
| 同 series 的 sample 顺序 | SeriesKey → 同 partition | partition 内严格有序 | prom-gw + Kafka |
| 多节点写入重复 | Kafka 幂等 producer(默认开启) | 不重复 | prom-gw producer |
| Kafka 故障期间数据 | WAL 按 segment mtime 顺序重放 | 不丢不重 | prom-gw WAL |
| 跨 series 顺序 | 无需保证 | 各 series 独立时间戳 | - |
| 同请求多节点重复 | Flink DedupFunction(60s 窗口) | 下游去重 | Flink 消费侧 |
| 迟到数据 | Flink watermark + 窗口聚合 | 窗口内容忍 | Flink 消费侧 |

#### 1.4.6 关键结论

- **多节点 LB 部署不会引入时序问题**:每个 sample 自带 `Timestamp`(毫秒),时序由数据本身决定,不依赖到达顺序
- **不需要会话粘性**:即使同一 Prometheus 的两个请求被分到不同 prom-gw 节点,只要 SeriesKey 相同(同一 series),最终都落同一 partition,Kafka 保证顺序
- **不同 series 之间无顺序依赖**:各自独立时间线,无需全局排序
- **下游职责**:prom-gw 只保证"同 series 落同 partition 且不重复",全局排序和迟到数据处理交给 Flink(watermark + 窗口聚合)

### 1.5 故障切换矩阵

| 故障场景 | 影响 | 切换机制 | 恢复时间 |
|---|---|---|---|
| 单个 prom-gw 实例宕机 | 流量自动分摊到其他实例 | LB 健康检查摘流 | 5-10s |
| LB 主节点宕机 | VIP 漂移到备节点 | Keepalived VRRP | 1-3s |
| Kafka Broker 宕机 | 数据写入其他副本 | Kafka 自动 leader 选举 | 10-30s |
| Kafka 集群不可用 | prom-gw 降级 WAL | 自动降级 + 恢复后 drain | 即时降级 |
| 机房故障 | 切换到灾备机房 | DNS / 全局 LB | 5-15min |
| 磁盘满 | WAL 硬拒绝 503 | 告警 + 扩容 | 人工介入 |

---

## 2. 部署拓扑

### 2.1 单机房标准拓扑(推荐)

每机房部署:**2 台 LB(主备)+ 2-4 台 prom-gw**

```
机房 (BJ)
┌──────────────────────────────────────────────────┐
│                                                  │
│  Prometheus ──> VIP:19201 (Keepalived)           │
│                    │                             │
│          ┌─────────┴─────────┐                   │
│          │                   │                   │
│    Nginx-LB-1          Nginx-LB-2               │
│    (MASTER)           (BACKUP)                  │
│    10.0.1.101          10.0.1.102               │
│          │                   │                   │
│    ┌─────┼───────────────────┼─────┐            │
│    │     │                   │     │            │
│  prom-gw-1  prom-gw-2  prom-gw-3  prom-gw-4     │
│  10.0.1.11  10.0.1.12  10.0.1.13  10.0.1.14    │
│    │           │           │           │        │
│    └───────────┴───────────┴───────────┘        │
│                    │                             │
│              Kafka 集群 (3 Broker)               │
│              10.0.1.21/22/23                     │
└──────────────────────────────────────────────────┘
```

### 2.2 资源规划

| 角色 | 规格 | 数量 | 说明 |
|---|---|---|---|
| Nginx LB | 4C/8G/50G | 2 | 主备,Keepalived VIP |
| prom-gw | 8C/16G/100G SSD | 2-4 | 按流量扩展,WAL 独立盘 |
| Kafka | 64C/512G/12×16T | 3 | KRaft 模式,3 副本 |

### 2.3 端口规划

| 端口 | 组件 | 暴露范围 | LB 转发 |
|---|---|---|---|
| 19201 | prom-gw RemoteWrite | LB 后端 | VIP:19201 → 后端 :19201 |
| 8080 | prom-gw metrics | Prometheus 抓取 | 不经 LB(直连) |
| 8081 | prom-gw healthz | LB 健康检查 | LB 主动探测 |
| 8082 | prom-gw Admin | 运维网段 | 不经 LB(直连,白名单) |
| 8443 | Nginx 管理 UI(可选) | 运维网段 | - |

### 2.4 网络隔离

```
Prometheus 网段 (10.0.1.0/28)    → 只能访问 VIP:19201
LB 网段 (10.0.1.16/28)           → 能访问 prom-gw :19201/:8081
prom-gw 网段 (10.0.1.32/27)      → 能访问 Kafka :9094
Kafka 网段 (10.0.1.64/28)        → 仅 prom-gw + Flink 可访问
运维网段 (10.0.0.0/24)           → 能访问 Admin :8082、SSH
```

---

## 3. Nginx 负载均衡配置

### 3.1 Nginx 安装

```bash
# 安装 Nginx(需包含 stream 模块)
sudo yum install -y nginx              # CentOS/RHEL
# 或
sudo apt install -y nginx              # Ubuntu/Debian

# 验证 stream 模块
nginx -V 2>&1 | grep -o 'stream'       # 应输出 stream
```

> **编译安装**(官方源未含 stream 时):
> ```bash
> ./configure --with-stream --with-stream_ssl_module --with-http_ssl_module
> make && sudo make install
> ```

### 3.2 4 层负载均衡(RemoteWrite TCP 转发)

prom-gw 的 RemoteWrite 是 TCP 协议(protobuf + snappy over HTTP),使用 Nginx stream 模块做 4 层转发,性能最优。

**`/etc/nginx/nginx.conf`**:

```nginx
user nginx;
worker_processes auto;
worker_rlimit_nofile 65535;

events {
    worker_connections 16384;
    use epoll;
    multi_accept on;
}

# ====== 4 层负载均衡 (RemoteWrite) ======
stream {
    log_format remote_write '$remote_addr [$time_local] '
                           'protocol=$protocol status=$status '
                           'bytes_sent=$bytes_sent bytes_received=$bytes_received '
                           'session_time=$session_time '
                           'upstream_addr=$upstream_addr '
                           'upstream_connect_time=$upstream_connect_time';

    access_log /var/log/nginx/remote_write.access.log remote_write;
    error_log  /var/log/nginx/remote_write.error.log warn;

    # upstream: prom-gw 实例池
    upstream prom_gw_backend {
        # least_conn: 最少连接数调度(比 rr 更均衡)
        least_conn;
        # 超时与失败判定
        server 10.0.1.11:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.12:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.13:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.14:19201 max_fails=3 fail_timeout=10s;
    }

    # 健康检查(主动探测,需 nginx-plus 或 nginx_upstream_check_module)
    # 开源方案:用 max_fails + fail_timeout 被动检查 + 外部 consul-template
    # 如已安装 nginx_upstream_check_module:
    # check interval=3000 rise=2 fall=3 timeout=2000 type=tcp;
    # check_http_send "GET /healthz HTTP/1.0\r\n\r\n";
    # check_http_expect_alive http_2xx;

    server {
        listen 19201;                    # 监听 RemoteWrite 端口
        proxy_pass prom_gw_backend;
        proxy_connect_timeout 3s;        # 连接 prom-gw 超时
        proxy_timeout 60s;               # 单请求超时(RemoteWrite batch 可能较大)
        proxy_buffer_size 16k;
        proxy_next_upstream on;          # 连接失败时尝试下一台
        proxy_next_upstream_tries 2;     # 最多重试 2 次
        proxy_next_upstream_timeout 5s;  # 重试总超时
    }
}
```

**关键参数说明**:

| 参数 | 值 | 说明 |
|---|---|---|
| `worker_connections` | 16384 | 单 worker 最大连接数,1.5M samples/s 约需 8000 并发连接 |
| `least_conn` | - | 最少连接数调度,比 round-robin 更均衡 |
| `max_fails` | 3 | 10s 内失败 3 次标记为 down |
| `fail_timeout` | 10s | 标记 down 后 10s 再重试 |
| `proxy_connect_timeout` | 3s | 连接后端超时,快速失败切换 |
| `proxy_timeout` | 60s | 单请求超时,大 batch 需要足够时间 |
| `proxy_next_upstream` | on | 连接失败自动重试下一台 |

### 3.3 7 层负载均衡(Admin API / Metrics)

Admin API 和 metrics 不建议经 LB(直连更安全),但如需统一入口可用 http 模块:

```nginx
# 追加到 /etc/nginx/nginx.conf 的 http 块(与 stream 块同级)
http {
    # Admin API 负载均衡(仅运维网段访问)
    upstream prom_gw_admin {
        server 10.0.1.11:8082;
        server 10.0.1.12:8082;
        server 10.0.1.13:8082;
        server 10.0.1.14:8082;
    }

    server {
        listen 8082;
        # IP 白名单(仅运维网段)
        allow 10.0.0.0/24;
        deny all;

        location / {
            proxy_pass http://prom_gw_admin;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 3s;
            proxy_read_timeout 10s;
        }
    }

    # Metrics 负载均衡(Prometheus 抓取,通常直连不经 LB)
    # 如需经 LB,用以下配置(但建议直连,便于定位单实例问题)
    upstream prom_gw_metrics {
        server 10.0.1.11:8080;
        server 10.0.1.12:8080;
        server 10.0.1.13:8080;
        server 10.0.1.14:8080;
    }

    server {
        listen 8080;
        allow 10.0.0.0/24;   # 仅 Prometheus 抓取网段
        deny all;

        location /metrics {
            proxy_pass http://prom_gw_metrics/metrics;
        }
    }
}
```

### 3.4 Nginx 性能调优

**`/etc/sysctl.d/99-nginx.conf`**:

```ini
# 网络栈调优
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 10000 65535

# 连接追踪
net.netfilter.nf_conntrack_max = 262144
net.ipv4.tcp_max_tw_buckets = 262144

# 缓冲区
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# 文件句柄
fs.file-max = 1048576
```

```bash
sudo sysctl --system
```

**Nginx worker 亲和性**(可选,高负载场景):

```nginx
worker_processes auto;
worker_cpu_affinity auto;          # 自动绑定 CPU
```

**`/etc/security/limits.d/nginx.conf`**:

```
nginx  soft  nofile  65535
nginx  hard  nofile  65535
```

### 3.5 Nginx systemd 服务

**`/etc/systemd/system/nginx.service`**(或使用系统自带):

```ini
[Unit]
Description=Nginx Load Balancer for prom-gw
After=network.target

[Service]
Type=forking
PIDFile=/run/nginx.pid
ExecStartPre=/usr/sbin/nginx -t -c /etc/nginx/nginx.conf
ExecStart=/usr/sbin/nginx -c /etc/nginx/nginx.conf
ExecReload=/usr/sbin/nginx -s reload
ExecStop=/usr/sbin/nginx -s stop
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nginx
sudo nginx -t                    # 验证配置
```

### 3.6 验证

```bash
# 1. Nginx 配置语法检查
sudo nginx -t

# 2. 通过 VIP 写入(模拟 Prometheus)
curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://10.0.1.100:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_prod" \
  --data-binary @payload.bin
# 期望: 200

# 3. 查看 LB 转发日志
tail -f /var/log/nginx/remote_write.access.log

# 4. 查看后端连接分布
ss -tnp | grep 19201 | awk '{print $5}' | sort | uniq -c
# 期望: 4 个后端 IP 连接数大致相等
```

---

## 4. Keepalived 高可用配置

### 4.1 双机主备架构

```
          VIP: 10.0.1.100
            │
    ┌───────┴───────┐
    │               │
Nginx-LB-1      Nginx-LB-2
(MASTER)        (BACKUP)
priority=100    priority=90
10.0.1.101      10.0.1.102
```

- 正常时:VIP 在 LB-1,所有流量走 LB-1
- LB-1 故障:VIP 漂移到 LB-2,< 3s 切换
- LB-1 恢复:根据 `preempt` 配置决定是否抢占

### 4.2 安装 Keepalived

```bash
sudo yum install -y keepalived     # CentOS/RHEL
# 或
sudo apt install -y keepalived     # Ubuntu/Debian

keepalived -v                      # 验证版本
```

### 4.3 MASTER 节点配置

**`/etc/keepalived/keepalived.conf`(LB-1 / MASTER)**:

```nginx
global_defs {
    router_id NGINX_LB_BJ         # 路由标识,按机房修改
    enable_script_security         # 脚本需 root 权限
    script_user root
}

# Nginx 健康检查脚本
vrrp_script check_nginx {
    script "/etc/keepalived/check_nginx.sh"
    interval 2                     # 每 2s 检查一次
    timeout 2                      # 超时 2s
    fall 2                         # 连续失败 2 次标记 down
    rise 2                         # 连续成功 2 次标记 up
}

vrrp_instance VI_1 {
    state MASTER                   # 主节点
    interface eth0                 # 物理网卡
    virtual_router_id 51           # VRRP 组 ID(主备必须一致)
    priority 100                   # 优先级(主 > 备)
    advert_int 1                   # VRRP 通告间隔

    authentication {
        auth_type PASS
        auth_pass PromGw@2026     # 认证密码(主备一致,≤8 字符)
    }

    virtual_ipaddress {
        10.0.1.100/24              # VIP
    }

    track_script {
        check_nginx                # 关联健康检查脚本
    }

    # VIP 切换时触发通知
    notify_master "/etc/keepalived/notify.sh master"
    notify_backup "/etc/keepalived/notify.sh backup"
    notify_fault  "/etc/keepalived/notify.sh fault"
}
```

### 4.4 BACKUP 节点配置

**`/etc/keepalived/keepalived.conf`(LB-2 / BACKUP)**:

```nginx
global_defs {
    router_id NGINX_LB_BJ
    enable_script_security
    script_user root
}

vrrp_script check_nginx {
    script "/etc/keepalived/check_nginx.sh"
    interval 2
    timeout 2
    fall 2
    rise 2
}

vrrp_instance VI_1 {
    state BACKUP                   # 备节点
    interface eth0
    virtual_router_id 51           # 必须与 MASTER 一致
    priority 90                    # 低于 MASTER
    advert_int 1

    authentication {
        auth_type PASS
        auth_pass PromGw@2026
    }

    virtual_ipaddress {
        10.0.1.100/24
    }

    track_script {
        check_nginx
    }

    notify_master "/etc/keepalived/notify.sh master"
    notify_backup "/etc/keepalived/notify.sh backup"
    notify_fault  "/etc/keepalived/notify.sh fault"
}
```

### 4.5 健康检查脚本

**`/etc/keepalived/check_nginx.sh`**:

```bash
#!/bin/bash
# 检查 Nginx 进程是否存活
# Keepalived 通过 exit code 判断:0=健康,1=不健康

if pgrep -x nginx > /dev/null 2>&1; then
    exit 0
else
    # 尝试拉起 Nginx
    systemctl restart nginx
    sleep 2
    if pgrep -x nginx > /dev/null 2>&1; then
        exit 0
    fi
    exit 1
fi
```

```bash
sudo chmod +x /etc/keepalived/check_nginx.sh
```

### 4.6 通知脚本

**`/etc/keepalived/notify.sh`**:

```bash
#!/bin/bash
# VIP 切换通知脚本
# 在 MASTER/BACKUP 切换时触发,发送告警

STATE=$1
HOSTNAME=$(hostname)
VIP="10.0.1.100"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

MESSAGE="[${DATE}] ${HOSTNAME} VRRP state changed to ${STATE} (VIP=${VIP})"

# 写日志
echo "${MESSAGE}" >> /var/log/keepalived-notify.log

# 发送告警(示例:调用企业微信/钉钉 webhook)
# WEBHOOK_URL="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
# curl -s -X POST "${WEBHOOK_URL}" \
#   -H "Content-Type: application/json" \
#   -d "{\"msgtype\":\"text\",\"text\":{\"content\":\"${MESSAGE}\"}}"

# 发送邮件(可选)
# echo "${MESSAGE}" | mail -s "Keepalived VIP switch" ops@example.com

exit 0
```

```bash
sudo chmod +x /etc/keepalived/notify.sh
```

### 4.7 Keepalived systemd 服务

```bash
sudo systemctl enable --now keepalived
sudo systemctl status keepalived
```

### 4.8 验证 VIP

```bash
# 在 LB-1 (MASTER) 上查看 VIP
ip addr show eth0 | grep 10.0.1.100
# 期望: inet 10.0.1.100/24 scope global eth0

# 在 LB-2 (BACKUP) 上确认无 VIP
ip addr show eth0 | grep 10.0.1.100
# 期望: 无输出

# 通过 VIP 访问
curl -sS http://10.0.1.100:8081/healthz
# 期望: {"status":"ok"}

# 模拟 MASTER 故障(在 LB-1 上)
sudo systemctl stop nginx
sleep 5

# 在 LB-2 上确认 VIP 已漂移
ip addr show eth0 | grep 10.0.1.100
# 期望: inet 10.0.1.100/24 scope global eth0

# 恢复 LB-1
sudo systemctl start nginx
sleep 5
# VIP 回到 LB-1(如配置了 preempt,默认启用)
```

### 4.9 Keepalived 参数速查

| 参数 | MASTER | BACKUP | 说明 |
|---|---|---|---|
| `state` | MASTER | BACKUP | 初始角色 |
| `priority` | 100 | 90 | 优先级,高者获得 VIP |
| `virtual_router_id` | 51 | 51 | VRRP 组 ID,主备一致 |
| `advert_int` | 1 | 1 | 通告间隔(秒) |
| `auth_pass` | 一致 | 一致 | 认证密码 |
| `preempt` | 默认启用 | 默认启用 | MASTER 恢复后是否抢占 VIP |
| `preempt_delay` | 0 | 0 | 抢占延迟(秒),避免抖动 |

---

## 5. HAProxy 替代方案

### 5.1 适用场景

| 方案 | 优势 | 劣势 | 推荐场景 |
|---|---|---|---|
| Nginx | 生态成熟、stream+http 统一 | 4 层健康检查需第三方模块 | 通用场景(推荐) |
| HAProxy | 原生健康检查、统计面板、4 层最强 | 无 7 层静态文件能力 | 纯 4 层高并发场景 |
| LVS | 内核态、性能最高、DR 模式 | 配置复杂、需 ARP 抑制 | 超高吞吐(>10Gbps) |

### 5.2 HAProxy 配置

**`/etc/haproxy/haproxy.cfg`**:

```haproxy
global
    log /dev/log local0
    maxconn 65535
    user haproxy
    group haproxy
    daemon
    stats socket /run/haproxy/admin.sock mode 660 level admin

defaults
    log global
    mode tcp
    option tcplog
    option dontlognull
    retries 3
    timeout connect 3s
    timeout client  60s
    timeout server  60s
    timeout check   2s

# 4 层负载均衡: prom-gw RemoteWrite
frontend prom_gw_write
    bind *:19201
    default_backend prom_gw_backend

backend prom_gw_backend
    # 最少连接调度
    balance leastconn
    # 主动健康检查(每 2s 探测 8081/healthz)
    option httpchk GET /healthz
    http-check expect status 200
    server prom-gw-1 10.0.1.11:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-2 10.0.1.12:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-3 10.0.1.13:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-4 10.0.1.14:19201 check port 8081 inter 2s rise 2 fall 3

# 统计面板
listen stats
    bind *:8404
    mode http
    stats enable
    stats uri /
    stats auth admin:PromGw@2026
    stats refresh 5s
```

```bash
sudo systemctl enable --now haproxy

# 访问统计面板
# http://lb-ip:8404/  (用户名: admin, 密码: PromGw@2026)
```

### 5.3 HAProxy + Keepalived

HAProxy 与 Keepalived 配合方式与 Nginx 完全相同,只需修改 `check_nginx.sh` 为 `check_haproxy.sh`:

```bash
#!/bin/bash
if pgrep -x haproxy > /dev/null 2>&1; then
    exit 0
else
    systemctl restart haproxy
    sleep 2
    pgrep -x haproxy > /dev/null 2>&1 && exit 0 || exit 1
fi
```

---

## 6. 健康检查机制

### 6.1 三层健康检查

```
┌─────────────────────────────────────────────────────┐
│  Layer 1: Keepalived → Nginx 进程存活检查            │
│  (每 2s 执行 check_nginx.sh)                        │
├─────────────────────────────────────────────────────┤
│  Layer 2: Nginx → prom-gw 实例健康检查               │
│  (被动: max_fails=3 fail_timeout=10s)               │
│  (主动: nginx_upstream_check_module,可选)           │
├─────────────────────────────────────────────────────┤
│  Layer 3: prom-gw → Kafka 连通性检查                 │
│  (prom-gw 内部:Kafka 失败自动降级 WAL)              │
└─────────────────────────────────────────────────────┘
```

### 6.2 Nginx 被动健康检查(默认)

Nginx 开源版默认使用被动健康检查:

| 参数 | 作用 | 配置 |
|---|---|---|
| `max_fails=3` | 10s 内失败 3 次标记 down | `server 10.0.1.11:19201 max_fails=3` |
| `fail_timeout=10s` | 标记 down 后 10s 重试 | `fail_timeout=10s` |
| `proxy_next_upstream` | 连接失败时重试下一台 | `proxy_next_upstream on` |

**缺点**:只有有流量时才能检测到故障,低流量时检测延迟。

### 6.3 Nginx 主动健康检查(推荐)

安装 `nginx_upstream_check_module` 实现主动探测:

```nginx
upstream prom_gw_backend {
    least_conn;
    server 10.0.1.11:19201;
    server 10.0.1.12:19201;
    server 10.0.1.13:19201;
    server 10.0.1.14:19201;

    # 主动健康检查
    check interval=3000 rise=2 fall=3 timeout=2000 type=http;
    check_http_send "GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n";
    check_http_expect_alive http_2xx;

    # 健康检查面板(可选)
    # check_status;
}
```

安装方法:

```bash
# 下载模块
cd /tmp
wget https://github.com/yaoweibin/nginx_upstream_check_module/archive/refs/tags/v0.5.0.tar.gz
tar -xzf v0.5.0.tar.gz

# 重新编译 Nginx
cd /path/to/nginx-source
patch -p1 < /tmp/nginx_upstream_check_module-0.5.0/check_1.20.1+.patch
./configure --with-stream --add-module=/tmp/nginx_upstream_check_module-0.5.0
make && sudo make install
```

### 6.4 外部健康检查(Consul / Prometheus)

大型部署可用 Consul + consul-template 动态管理 upstream:

```bash
# consul-template 模板
cat > /etc/consul-template/nginx-upstream.ctmpl << 'EOF'
upstream prom_gw_backend {
    least_conn;
    {{ range service "prom-gw" }}
    server {{ .Address }}:{{ .Port }} max_fails=3 fail_timeout=10s;
    {{ end }}
}
EOF

# consul-template 守护进程
consul-template \
    -consul-addr 10.0.0.10:8500 \
    -template "/etc/consul-template/nginx-upstream.ctmpl:/etc/nginx/conf.d/upstream.conf:nginx -s reload"
```

prom-gw 注册到 Consul:

```json
{
  "service": {
    "name": "prom-gw",
    "address": "10.0.1.11",
    "port": 19201,
    "check": {
      "http": "http://10.0.1.11:8081/healthz",
      "interval": "5s",
      "timeout": "2s",
      "deregister_critical_service_after": "30s"
    }
  }
}
```

---

## 7. SSL/TLS 配置

### 7.1 mTLS 双向认证(高安全场景)

如需对 RemoteWrite 链路加密,使用 Nginx 终止 TLS:

```nginx
stream {
    upstream prom_gw_backend {
        least_conn;
        server 10.0.1.11:19201;
        server 10.0.1.12:19201;
        server 10.0.1.13:19201;
        server 10.0.1.14:19201;
    }

    server {
        listen 19201 ssl;                    # TLS 监听

        # 服务端证书
        ssl_certificate     /etc/nginx/ssl/server.crt;
        ssl_certificate_key /etc/nginx/ssl/server.key;
        ssl_protocols       TLSv1.2 TLSv1.3;
        ssl_ciphers         ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
        ssl_session_cache   shared:SSL:10m;
        ssl_session_timeout 10m;

        # 客户端证书验证(mTLS)
        ssl_client_certificate /etc/nginx/ssl/ca.crt;
        ssl_verify_client on;
        ssl_verify_depth 2;

        proxy_pass prom_gw_backend;
    }
}
```

### 7.2 证书生成

```bash
# 1. 生成 CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
    -subj "/CN=prom-gw-ca"

# 2. 生成服务端证书
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/CN=nginx-lb"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
    -set_serial 01 -out server.crt

# 3. 生成客户端证书(Prometheus 侧)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/CN=prometheus"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
    -set_serial 02 -out client.crt

# 4. 部署证书
sudo mkdir -p /etc/nginx/ssl
sudo cp ca.crt server.crt server.key /etc/nginx/ssl/
sudo chmod 600 /etc/nginx/ssl/server.key
```

### 7.3 Prometheus 侧 mTLS 配置

```yaml
# prometheus.yml
remote_write:
  - url: https://10.0.1.100:19201/api/v1/write    # 注意 https
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    tls_config:
      ca_file: /etc/prometheus/ssl/ca.crt
      cert_file: /etc/prometheus/ssl/client.crt
      key_file: /etc/prometheus/ssl/client.key
      server_name: nginx-lb
      insecure_skip_verify: false
```

### 7.4 性能影响

| 模式 | 吞吐影响 | 延迟增加 | CPU 开销 |
|---|---|---|---|
| 无 TLS(默认) | 基线 | 基线 | 基线 |
| TLS 终止(Nginx) | -10~15% | +2-5ms | Nginx CPU +20% |
| mTLS | -15~20% | +3-8ms | Nginx CPU +30% |

> **建议**:内网环境(同一 VPC / 专线)无需 TLS,性能优先。跨不可信网络时启用 mTLS。

---

## 8. 安全加固

### 8.1 Nginx 限流

防止恶意流量打垮 prom-gw:

```nginx
http {
    # 限制每 IP 请求速率(RemoteWrite 流量较大,设宽松值)
    limit_req_zone $binary_remote_addr zone=remote_write:10m rate=1000r/s;

    server {
        listen 8082;
        location / {
            limit_req zone=remote_write burst=2000 nodelay;
            proxy_pass http://prom_gw_admin;
        }
    }
}
```

### 8.2 IP 白名单

```nginx
# stream 模块 4 层 IP 白名单(需 nginx 1.19+)
stream {
    server {
        listen 19201;
        # 仅允许 Prometheus 和 LB 网段
        allow 10.0.1.0/28;      # Prometheus 网段
        allow 10.0.1.16/28;     # LB 网段
        deny all;
        proxy_pass prom_gw_backend;
    }
}
```

### 8.3 DDoS 防护

```nginx
http {
    # 限制并发连接数
    limit_conn_zone $binary_remote_addr zone=conn_limit:10m;

    server {
        listen 8082;
        location / {
            limit_conn conn_limit 100;       # 每 IP 最多 100 并发
            limit_req zone=remote_write burst=2000 nodelay;
            proxy_pass http://prom_gw_admin;
        }
    }
}
```

### 8.4 安全清单

| 项 | 配置 | 说明 |
|---|---|---|
| Nginx 版本隐藏 | `server_tokens off;` | 不暴露版本号 |
| 超时设置 | `proxy_connect_timeout 3s` | 快速失败 |
| 请求体限制 | `client_max_body_size 10m;` | 限制 payload 大小 |
| 日志脱敏 | 不记录 Authorization header | 避免泄露 token |
| 证书权限 | `chmod 600 *.key` | 私钥仅 root 可读 |
| 防火墙 | iptables / 安全组 | 仅开放必要端口 |

---

## 9. 多机房容灾

### 9.1 同城双活

同城两个可用区(AZ),每个 AZ 独立部署 prom-gw 集群:

```
同城 (BJ)
┌──────────────── AZ-1 ────────────────┐  ┌──────────────── AZ-2 ────────────────┐
│                                      │  │                                      │
│  Prometheus-AZ1                      │  │  Prometheus-AZ2                      │
│      │                               │  │      │                               │
│  Nginx-LB-AZ1 (VIP-AZ1)              │  │  Nginx-LB-AZ2 (VIP-AZ2)              │
│      │                               │  │      │                               │
│  prom-gw-1, prom-gw-2               │  │  prom-gw-3, prom-gw-4               │
│      │                               │  │      │                               │
│  Kafka-AZ1, Kafka-AZ2 (同城副本)     │  │  Kafka-AZ1, Kafka-AZ2 (同城副本)     │
└──────────────────────────────────────┘  └──────────────────────────────────────┘
                        │                                │
                        └────────── Kafka 跨 AZ 复制 ─────┘
```

Prometheus 配置双 remote_write(主备):

```yaml
remote_write:
  - url: http://vip-az1:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
  - url: http://vip-az2:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
```

> prom-gw 的 Kafka producer 开启幂等写,双写消息在 Kafka 端去重。

### 9.2 跨城灾备

三城部署,每城独立 prom-gw + Kafka,跨城专线同步:

```
北京 (主)              深圳 (灾备)            合肥 (灾备)
┌──────────┐          ┌──────────┐          ┌──────────┐
│ Prometheus│          │ Prometheus│          │ Prometheus│
│ prom-gw×4 │          │ prom-gw×4 │          │ prom-gw×2 │
│ Kafka×3   │ ←专线→   │ Kafka×3   │ ←专线→   │ Kafka×3   │
└──────────┘          └──────────┘          └──────────┘
      │                      │                      │
      └──────── Flink 跨城汇聚 → StarRocks (北京) ────┘
```

**DNS 全局负载均衡**(跨城切换):

```bash
# 正常:prom-gw.example.com → 北京 VIP
prom-gw.example.com.  60  IN  A  10.0.1.100    # 北京 VIP

# 灾备切换:DNS 指向深圳
prom-gw.example.com.  60  IN  A  10.2.1.100    # 深圳 VIP
```

### 9.3 容灾切换流程

| 步骤 | 操作 | 负责人 | 耗时 |
|---|---|---|---|
| 1 | 确认主机房故障(健康检查 + 人工确认) | on-call | 2min |
| 2 | DNS 切换到灾备机房 VIP | 运维 | 1min |
| 3 | 等待 DNS 生效(TTL 60s) | - | 1-5min |
| 4 | 验证灾备机房 prom-gw + Kafka 健康 | 运维 | 2min |
| 5 | 验证数据链路(Prometheus → prom-gw → Kafka) | 运维 | 2min |
| 6 | 通告相关团队 | on-call | 即时 |

**总计**:5-15min(取决于 DNS TTL 和人工确认速度)

---

## 10. 故障切换测试

### 10.1 测试矩阵

| 测试场景 | 操作 | 期望结果 | 恢复时间 |
|---|---|---|---|
| prom-gw 单实例宕机 | `systemctl stop prom-gw@bj` | LB 自动摘流,其他实例接管 | 5-10s |
| Nginx MASTER 宕机 | `systemctl stop nginx`(LB-1) | VIP 漂移到 LB-2 | 1-3s |
| Kafka 单 Broker 宕机 | `systemctl stop kafka`(Broker-1) | Kafka 自动 leader 选举 | 10-30s |
| Kafka 集群不可用 | 停所有 Kafka | prom-gw 降级 WAL,返回 200 | 即时 |
| 磁盘满 | 写满 /data/wal | prom-gw 返回 503,告警触发 | 即时 |

### 10.2 测试脚本

**测试 1:prom-gw 单实例宕机**

```bash
#!/bin/bash
# test_failover_single_instance.sh
set -e

VIP=10.0.1.100
TARGET=prom-gw-1  # 10.0.1.11

echo "=== 测试 1: prom-gw 单实例宕机 ==="

# 1. 持续压测(后台)
go run ./test/loadgen \
    --url=http://${VIP}:19201/api/v1/write \
    --token=tk_app_business_prod \
    --rate=500000 --duration=120s &
LOADGEN_PID=$!
echo "loadgen started (pid=$LOADGEN_PID)"

# 2. 30s 后停掉一个 prom-gw 实例
sleep 30
echo ">>> 停止 ${TARGET}"
ssh ${TARGET} "sudo systemctl stop prom-gw@bj"

# 3. 观察 30s(应有短暂错误,然后恢复)
sleep 30
echo ">>> 恢复 ${TARGET}"
ssh ${TARGET} "sudo systemctl start prom-gw@bj"

# 4. 等待压测结束
wait $LOADGEN_PID

echo "=== 测试完成 ==="
echo "检查 loadgen 输出:err_batches 应在故障期间有少量增长,恢复后归零"
```

**测试 2:Nginx MASTER 宕机**

```bash
#!/bin/bash
# test_failover_nginx_master.sh
set -e

VIP=10.0.1.100
LB1=nginx-lb-1

echo "=== 测试 2: Nginx MASTER 宕机 ==="

# 1. 确认 VIP 在 LB-1
echo ">>> VIP 位置:"
ssh ${LB1} "ip addr show eth0 | grep ${VIP}" || echo "VIP not on ${LB1}"

# 2. 持续写入
for i in $(seq 1 60); do
    CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
        -X POST http://${VIP}:19201/api/v1/write \
        -H "Content-Type: application/x-protobuf" \
        -H "Content-Encoding: snappy" \
        -H "Authorization: Bearer tk_app_business_prod" \
        --data-binary @payload.bin 2>/dev/null || echo "000")
    echo "$(date +%H:%M:%S) HTTP=${CODE}"
    sleep 1
done &
WRITER_PID=$!

# 3. 10s 后停 LB-1
sleep 10
echo ">>> 停止 ${LB1} 的 nginx"
ssh ${LB1} "sudo systemctl stop nginx"

# 4. 等 VIP 漂移
sleep 5
echo ">>> 检查 VIP"
ssh nginx-lb-2 "ip addr show eth0 | grep ${VIP}" && echo "VIP moved to nginx-lb-2"

# 5. 恢复 LB-1
sleep 10
echo ">>> 恢复 ${LB1}"
ssh ${LB1} "sudo systemctl start nginx"

wait $WRITER_PID
echo "=== 测试完成 ==="
echo "期望:VIP 漂移期间有 1-3s 的连接失败,之后恢复"
```

### 10.3 验收标准

| 测试项 | 通过标准 |
|---|---|
| 单实例宕机 | 错误率 < 1%,恢复时间 < 10s |
| LB 主备切换 | VIP 漂移 < 3s,丢连接 < 5 个 |
| Kafka 故障 | prom-gw 自动降级 WAL,无 5xx |
| 磁盘满 | 返回 503,告警触发 |
| 全链路恢复 | 5min 内所有指标恢复正常 |

---

## 11. 监控与告警

### 11.1 Nginx 监控

安装 `nginx-prometheus-exporter`:

```bash
# 部署 exporter
docker run -d --name nginx-exporter \
    -p 9113:9113 \
    nginx/nginx-prometheus-exporter:0.11 \
    -nginx.scrape-uri=http://nginx-lb-1:8080/stub_status
```

Nginx 开启 stub_status(需编译 `--with-http_stub_status_module`):

```nginx
http {
    server {
        listen 8080;
        location /stub_status {
            stub_status;
            allow 10.0.0.0/24;   # 仅 Prometheus 抓取
            deny all;
        }
    }
}
```

### 11.2 关键监控指标

| 指标 | PromQL | 告警阈值 |
|---|---|---|
| Nginx 活跃连接 | `nginx_connections_active` | > 10000 |
| Nginx 请求速率 | `rate(nginx_connections_total[1m])` | - |
| 后端响应时间 | `nginx_upstream_response_time_seconds` | p99 > 1s |
| 后端可用性 | `nginx_upstream_servers{state="up"}` | < 实例总数 |
| VIP 状态 | Keepalived 日志 / 自定义脚本 | VIP 不在任意节点 |
| prom-gw 错误率 | `rate(gateway_errors_total[1m])` | > 0.01% |
| prom-gw 背压 | `rate(gateway_backpressure_rejected_total[1m])` | > 0 |

### 11.3 告警规则

**`/etc/prometheus/rules/ha-lb-alerts.yaml`**:

```yaml
groups:
  - name: ha-lb
    rules:
      # Nginx 后端实例 down
      - alert: NginxBackendDown
        expr: nginx_upstream_servers{state="up"} < 2
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nginx 后端可用实例数 < 2"

      # Nginx 活跃连接过高
      - alert: NginxHighConnections
        expr: nginx_connections_active > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Nginx 活跃连接数 > 10000"

      # prom-gw 实例 healthz 不可达
      - alert: PromGwInstanceDown
        expr: up{job="prom-gw"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "prom-gw 实例 {{ $labels.instance }} 不可达"

      # VIP 不可达(所有 LB 均无法响应)
      - alert: VIPUnreachable
        expr: |
          count(up{job="nginx-lb"} == 0) == count(up{job="nginx-lb"})
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "所有 Nginx LB 均不可达,VIP 可能不可用"
```

### 11.4 Grafana 大盘

导入以下 dashboard:

| Dashboard | ID | 说明 |
|---|---|---|
| Nginx Overview | 1120 | Nginx 连接/请求/响应时间 |
| HAProxy Overview | 2428 | HAProxy 统计(如使用) |
| prom-gw | 仓库内 `deploy/grafana/dashboards/prom-gw.json` | prom-gw 全量指标 |

---

## 12. 运维操作

### 12.1 滚动升级 prom-gw

```bash
#!/bin/bash
# rolling_update_prom_gw.sh
# 逐台升级 prom-gw,LB 自动摘流

INSTANCES="prom-gw-1 prom-gw-2 prom-gw-3 prom-gw-4"
NEW_BIN=/tmp/prom-gw

for host in $INSTANCES; do
    echo "=== 升级 ${host} ==="

    # 1. LB 自动摘流(Nginx max_fails=3 fail_timeout=10s 会标记 down)
    # 2. 停实例
    ssh ${host} "sudo systemctl stop prom-gw@bj"

    # 3. 等待 in-flight 请求处理完(30s 优雅停机)
    sleep 35

    # 4. 替换二进制
    scp ${NEW_BIN} ${host}:/tmp/prom-gw
    ssh ${host} "sudo cp /tmp/prom-gw /appdata/prom-gw/bin/prom-gw"

    # 5. 启动
    ssh ${host} "sudo systemctl start prom-gw@bj"

    # 6. 等待健康检查通过
    for i in $(seq 1 30); do
        if ssh ${host} "curl -fsS http://127.0.0.1:8081/healthz" 2>/dev/null; then
            echo "  ✓ ${host} healthy"
            break
        fi
        sleep 1
    done

    # 7. 观察 30s
    echo "  观察 30s..."
    sleep 30
done

echo "=== 全部升级完成 ==="
```

### 12.2 Nginx 配置热加载

```bash
# 修改配置后(如增减后端实例)
sudo vim /etc/nginx/nginx.conf

# 语法检查
sudo nginx -t

# 热加载(不断连接)
sudo nginx -s reload
# 或
sudo systemctl reload nginx
```

### 12.3 添加/移除 prom-gw 实例

**添加实例**:

```bash
# 1. 部署新实例(见 production-guide.md §5)
sudo systemctl enable --now prom-gw@bj

# 2. Nginx upstream 添加 server
sudo vim /etc/nginx/nginx.conf
# 在 upstream prom_gw_backend 中添加:
#   server 10.0.1.15:19201 max_fails=3 fail_timeout=10s;

# 3. 热加载
sudo nginx -t && sudo nginx -s reload

# 4. 验证连接
curl -sS http://10.0.1.15:8081/healthz
```

**移除实例**:

```bash
# 1. Nginx upstream 标记为 down(先摘流)
sudo vim /etc/nginx/nginx.conf
#   server 10.0.1.11:19201 down;
sudo nginx -s reload

# 2. 等待连接排空(观察 Nginx 日志,确认无新连接)
sleep 60

# 3. 停实例
ssh prom-gw-1 "sudo systemctl stop prom-gw@bj"

# 4. 从 upstream 删除
sudo vim /etc/nginx/nginx.conf
# 删除 server 10.0.1.11:19201 down;
sudo nginx -s reload
```

### 12.4 VIP 手动切换

```bash
# 在 MASTER 上手动放弃 VIP(触发漂移到 BACKUP)
ssh nginx-lb-1 "sudo systemctl stop keepalived"

# 确认 VIP 已漂移
ssh nginx-lb-2 "ip addr show eth0 | grep 10.0.1.100"

# 恢复(如需切回)
ssh nginx-lb-1 "sudo systemctl start keepalived"
# 默认 preempt 模式下,VIP 会自动回到 LB-1
```

### 12.5 常用排查命令

```bash
# 查看 VIP 位置
for lb in nginx-lb-1 nginx-lb-2; do
    echo -n "${lb}: "
    ssh ${lb} "ip addr show eth0 | grep 10.0.1.100" || echo "no VIP"
done

# 查看 Nginx 后端连接分布
ssh nginx-lb-1 "ss -tnp | grep ':19201' | awk '{print \$5}' | sort | uniq -c"

# 查看 Nginx upstream 健康状态(需 check_module)
curl -s http://nginx-lb-1:8080/upstream_status | jq .

# 查看 Keepalived 状态
ssh nginx-lb-1 "systemctl status keepalived"
ssh nginx-lb-1 "journalctl -u keepalived --since '5min ago'"

# 查看所有 prom-gw 实例健康
for host in prom-gw-1 prom-gw-2 prom-gw-3 prom-gw-4; do
    echo -n "${host}: "
    curl -fsS -m 2 http://${host}:8081/healthz 2>/dev/null && echo " OK" || echo " FAIL"
done
```

---

## 13. 附录

### 13.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `nginx.conf` | `/etc/nginx/nginx.conf` | Nginx 主配置(stream + http) |
| `keepalived.conf` | `/etc/keepalived/keepalived.conf` | Keepalived VRRP 配置 |
| `check_nginx.sh` | `/etc/keepalived/check_nginx.sh` | Nginx 健康检查脚本 |
| `notify.sh` | `/etc/keepalived/notify.sh` | VIP 切换通知脚本 |
| `haproxy.cfg` | `/etc/haproxy/haproxy.cfg` | HAProxy 配置(替代方案) |
| `prom-gw@.service` | `/etc/systemd/system/prom-gw@.service` | prom-gw systemd template |
| `sysctl.conf` | `/etc/sysctl.d/99-nginx.conf` | 内核网络参数 |

### 13.2 Nginx vs HAProxy vs LVS 对比

| 维度 | Nginx | HAProxy | LVS |
|---|---|---|---|
| 工作层 | 4 层 + 7 层 | 4 层 + 7 层 | 4 层(内核态) |
| 性能 | 高(10Gbps) | 高(10Gbps) | 极高(40Gbps+) |
| 健康检查 | 被动(主动需模块) | 原生主动 + 面板 | 有限 |
| 配置复杂度 | 中 | 低 | 高(需 ARP 抑制) |
| 统计面板 | 需第三方 | 内置 | 需 ipvsadm |
| TLS 终止 | 支持 | 支持 | 不支持 |
| 会话保持 | cookie(7层) | cookie(7层) | source_hash |
| 适用规模 | 中大型 | 中大型 | 超大型 |
| 推荐度 | ★★★★★ | ★★★★ | ★★★(需超高吞吐) |

### 13.3 端口与防火墙速查

```bash
# iptables 规则示例(Nginx LB 节点)
sudo iptables -A INPUT -p tcp --dport 19201 -s 10.0.1.0/28 -j ACCEPT  # Prometheus
sudo iptables -A INPUT -p tcp --dport 19201 -s 10.0.0.0/24 -j ACCEPT  # 运维网段
sudo iptables -A INPUT -p tcp --dport 19201 -j DROP
sudo iptables -A INPUT -p tcp --dport 8082 -s 10.0.0.0/24 -j ACCEPT  # Admin
sudo iptables -A INPUT -p tcp --dport 8082 -j DROP

# 保存规则
sudo iptables-save > /etc/sysconfig/iptables
```



---

