# Kafbat UI 生产部署指南

> 本文档覆盖 prom-gw 配套 Kafka 集群的 Web 监控管理界面 **Kafbat UI v1.5.0** 的生产环境完整部署,包括 JAR 部署、systemd 服务管理、集群配置、Nginx 反向代理、认证授权、监控和运维操作。
>
> Kafbat UI 是 [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) 的开源 Web UI,用于监控和管理 Apache Kafka 集群,支持 Topic 管理、消息浏览、Consumer Group 监控、Schema Registry、多集群管理等功能。
>
> 配套文档:[Kafka 生产部署](kafka-production-deployment.md)、[生产部署指南](production-guide.md)、[高可用与负载均衡](ha-lb-deployment.md)、[故障剧本](runbook.md)

## 目录

1. [部署架构](#1-部署架构)
2. [前置准备](#2-前置准备)
3. [JAR 部署与 systemd 服务](#3-jar-部署与-systemd-服务)
4. [配置文件详解](#4-配置文件详解)
5. [Nginx 反向代理](#5-nginx-反向代理)
6. [认证与 RBAC](#6-认证与-rbac)
7. [监控集成](#7-监控集成)
8. [运维操作](#8-运维操作)
9. [附录](#9-附录)

---

## 1. 部署架构

### 1.1 标准拓扑

Kafbat UI 部署在独立运维节点(或与 Prometheus 共置),通过 PLAINTEXT 协议连接 Kafka 集群,通过 Prometheus 协议拉取 JMX Exporter 指标:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  ┌─────────────────┐         ┌─────────────────────┐    │
│  │  Kafbat UI      │         │  Nginx + Keepalived │    │
│  │  10.0.10.30:8080│ ←────── │  VIP: 10.0.10.100   │    │
│  │  (JAR/systemd) │  反向代理 │  443 → 8080        │    │
│  └────┬──────┬─────┘         └─────────────────────┘    │
│       │      │                                          │
│       │      │ PLAINTEXT                Prometheus       │
│       ▼      ▼                          ▼                │
│  ┌────────────────────┐         ┌──────────────────┐    │
│  │ Kafka Cluster      │         │ JMX Exporter     │    │
│  │ kafka-1:9092       │         │ kafka-1:9404     │    │
│  │ kafka-2:9092       │         │ kafka-2:9404     │    │
│  │ kafka-3:9092       │         │ kafka-3:9404     │    │
│  └────────────────────┘         └──────────────────┘    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **安全说明**:Kafbat UI 与 Kafka 集群在同一 VPC 内,通过 PLAINTEXT 协议(无 SSL/SASL)通信。对外暴露的 Web 界面通过 Nginx 反向代理 + Basic Auth / OAuth2 认证保护,详见 [§5](#5-nginx-反向代理) 和 [§6](#6-认证与-rbac)。

### 1.2 端口规划

| 端口 | 协议 | 用途 | 暴露范围 |
|---|---|---|---|
| 8080 | HTTP | Kafbat UI Web 界面(本地监听) | 仅本机 / Nginx |
| 443 | HTTPS | Nginx 对外暴露(VIP) | 运维网段 |
| 9092 | PLAINTEXT | 连接 Kafka Broker(客户端通信) | Kafbat UI 节点 → Kafka 网段 |
| 9404 | HTTP | 拉取 Kafka JMX Exporter 指标 | Kafbat UI 节点 → Kafka 网段 |

### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 | 说明 |
|---|---|---|---|---|
| Kafbat UI | 4C/8G | 1-2 | 50G SSD | JAR + systemd 部署,可挂载到 Nginx VIP 后做 HA |
| Nginx + Keepalived | 2C/4G | 2 | 50G SSD | 复用现有 [HA 部署](ha-lb-deployment.md) |

> **说明**:Kafbat UI 为无状态应用(配置通过 YAML 文件持久化),可通过 Nginx VIP 后部署多实例实现 HA。单实例也可满足 prom-gw 集群的监控需求。

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

Kafbat UI 基于 Spring Boot 3,需要 JDK 17+,统一使用 OpenJDK 25(与 Kafka 部署保持一致):

```bash
# CentOS / RHEL
sudo yum install -y java-25-openjdk java-25-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-25-jdk

java -version   # 期望: openjdk version "25.x.x"
```

### 2.3 创建用户与目录

```bash
# 创建专用用户
sudo useradd -r -m -d /appdata/kafka-ui -s /sbin/nologin kafbat-ui

# 部署目录(JAR 包 + 配置文件)
sudo mkdir -p /appdata/kafka-ui/config
# 日志目录
sudo mkdir -p /applog/kafka-ui

sudo chown -R kafbat-ui:kafbat-ui /appdata/kafka-ui /applog/kafka-ui
```

### 2.4 下载 JAR 包

```bash
# 下载 v1.5.0 版本(固定版本)
cd /appdata/kafka-ui
sudo wget -O api-v1.5.0.jar \
  https://github.com/kafbat/kafka-ui/releases/download/v1.5.0/api-v1.5.0.jar

# 校验 SHA256
echo "8bebff7b21ddb084b5b647e271136f7d97f46da6c7bc70f9cd47775dfbd3c10e  api-v1.5.0.jar" | sha256sum -c -
# 期望: api-v1.5.0.jar: OK

# 创建版本软链(方便升级)
sudo ln -s api-v1.5.0.jar kafka-ui.jar
sudo chown -R kafbat-ui:kafbat-ui /appdata/kafka-ui

# 验证
ls -lh /appdata/kafka-ui/kafka-ui.jar
# 期望: lrwxrwxrwx ... kafka-ui.jar -> api-v1.5.0.jar
```

> **版本固定**:生产环境必须使用固定版本 `1.5.0`,禁止直接使用 main 分支构建,避免引入不兼容变更。升级时替换 JAR 并更新软链即可。

---

## 3. JAR 部署与 systemd 服务

### 3.1 application.yml 主配置

**`/appdata/kafka-ui/config/application.yml`**:

```yaml
# ======================================================
# Kafbat UI 应用配置
# ======================================================
kafka:
  clusters:
    - name: prom-gw-sz               # 深圳集群
      bootstrapServers: kafka-1:9092,kafka-2:9092,kafka-3:9092
      # PLAINTEXT 协议,不设置 securityProtocol / ssl
      readOnly: false                # 生产建议 false(需要 Topic 管理操作时)
      metrics:
        type: PROMETHEUS             # 使用 Prometheus JMX Exporter
        port: 9404                   # Kafka JMX Exporter 端口(见 Kafka 部署 §5.1)
      # 消息浏览限制
      polling:
        throttleRate: 0              # 0 = 不限速(内网环境)

    # 多集群示例(如需监控北京/合肥集群)
    # - name: prom-gw-bj
    #   bootstrapServers: kafka-bj-1:9092,kafka-bj-2:9092,kafka-bj-3:9092
    #   metrics:
    #     type: PROMETHEUS
    #     port: 9404

# ======================================================
# 通用配置
# ======================================================
server:
  port: 8080

logging:
  level:
    root: info
    io.kafbat.ui: info
  file:
    name: /applog/kafka-ui/kafbat-ui.log

# 动态配置(允许 Web 界面修改运行时配置)
dynamic_config:
  enabled: true

# 内部 Topic 前缀
kafka_internalTopicPrefix: "_"

# 会话超时
server:
  reactive:
    session:
      timeout: 30m

# 关闭 GitHub 版本检查(内网无法访问)
github_release_info:
  enabled: false

# 生产关闭 Swagger UI(调试时可开)
swagger_ui:
  enabled: false

# Actuator 端点暴露
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

> **动态配置文件**:首次部署创建空文件,Web 界面修改的配置会写入此文件:
> ```bash
> sudo touch /appdata/kafka-ui/config/dynamic_config.yaml
> sudo chown kafbat-ui:kafbat-ui /appdata/kafka-ui/config/dynamic_config.yaml
> ```

### 3.2 systemd 服务

**`/etc/systemd/system/kafbat-ui.service`**:

```ini
[Unit]
Description=Kafbat UI v1.5.0 (JAR)
After=network.target
Wants=network.target

[Service]
Type=simple
User=kafbat-ui
Group=kafbat-ui

# JVM 参数
Environment="JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+AlwaysPreTouch -Djava.awt.headless=true"

# 启动命令:指定配置文件目录 + 动态配置
ExecStart=/usr/bin/java $JAVA_OPTS \
  -Dspring.config.additional-location=file:/appdata/kafka-ui/config/ \
  -Dspring.config.name=application \
  -jar /appdata/kafka-ui/kafka-ui.jar

# 优雅停机:SIGTERM 触发 Spring Boot 优雅关闭
ExecStop=/bin/kill -TERM $MAINPID

# 重启策略
Restart=always
RestartSec=10

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096

# 工作目录
WorkingDirectory=/appdata/kafka-ui

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/appdata/kafka-ui /applog/kafka-ui /tmp

# 超时
TimeoutStartSec=120
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

> **配置加载说明**:`-Dspring.config.additional-location` 指向自定义配置目录,Kafbat UI 会优先读取 `/appdata/kafka-ui/config/application.yml`(覆盖 JAR 包内默认配置)。动态配置 `dynamic_config.yaml` 也在此目录下自动加载。

### 3.3 启动与验证

```bash
# 1. 加载 systemd 配置
sudo systemctl daemon-reload
sudo systemctl enable kafbat-ui

# 2. 启动
sudo systemctl start kafbat-ui

# 3. 查看状态
sudo systemctl status kafbat-ui
# 期望: active (running)

# 4. 查看启动日志
sudo journalctl -u kafbat-ui -f --no-pager
# 或查看应用日志
tail -f /applog/kafka-ui/kafbat-ui.log

# 5. 验证健康检查
curl -s http://localhost:8080/actuator/health
# 期望: {"status":"UP"}

# 6. 验证 Web 界面(本地访问)
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/
# 期望: 200 或 401(启用 Basic Auth 时)
```

> **首次启动可能较慢**:Kafbat UI 首次启动需初始化 Kafka Admin Client 连接、加载集群元数据,通常 30-60 秒内就绪。systemd 的 `TimeoutStartSec=120` 已预留足够时间。

---

## 4. 配置文件详解

### 4.1 关键配置项说明

| 配置项(YAML 路径) | 环境变量 | 推荐值 | 说明 |
|---|---|---|---|
| `kafka.clusters[0].name` | `KAFKA_CLUSTERS_0_NAME` | `prom-gw-sz` | 唯一标识,Web 界面显示 |
| `kafka.clusters[0].bootstrapServers` | `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` | `kafka-1:9092,...` | Kafka Broker 地址列表 |
| 安全协议 | `KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL` | (不设) | PLAINTEXT 不设置,SSL 时设为 `SSL` |
| `kafka.clusters[0].metrics.type` | `KAFKA_CLUSTERS_0_METRICS_TYPE` | `PROMETHEUS` | 使用 Prometheus JMX Exporter(已部署) |
| `kafka.clusters[0].metrics.port` | `KAFKA_CLUSTERS_0_METRICS_PORT` | `9404` | 与 Kafka 部署 §5.1 一致 |
| `kafka.clusters[0].readOnly` | `KAFKA_CLUSTERS_0_READONLY` | `false` | `true` 时禁止 Topic 增删改 |
| `dynamic_config.enabled` | `DYNAMIC_CONFIG_ENABLED` | `true` | 允许 Web 界面修改配置 |
| `swagger_ui.enabled` | `SWAGGER_UI_ENABLED` | `false` | 生产关闭,调试时开 |
| `github_release_info.enabled` | `GITHUB_RELEASE_INFO_ENABLED` | `false` | 内网关闭 |
| `server.port` | `SERVER_PORT` | `8080` | Web 界面端口 |
| `server.servlet.context-path` | `SERVER_SERVLET_CONTEXT_PATH` | (不设) | 如需 `/kafka-ui` 前缀路径 |
| 管理超时 | `KAFKA_ADMIN-CLIENT-TIMEOUT` | `30000` | Kafka Admin API 超时(ms) |
| `kafka.clusters[0].polling.throttleRate` | `KAFKA_CLUSTERS_0_POLLING_THROTTLE_RATE` | `0` | 消息浏览限速(bytes/sec),0=不限 |
| `logging.level.root` | `LOGGING_LEVEL_ROOT` | `info` | 日志级别(trace/debug/info/warn/error) |

> **配置优先级**:命令行参数 > 环境变量 > `application.yml`(additional-location)> JAR 包内默认配置。推荐使用 YAML 文件管理,环境变量用于临时覆盖。

### 4.2 多集群配置

prom-gw 多城部署(深圳/北京/合肥)场景,可在一个 Kafbat UI 实例中管理所有集群:

```yaml
# /appdata/kafka-ui/config/application.yml
kafka:
  clusters:
    - name: prom-gw-sz
      bootstrapServers: kafka-sz-1:9092,kafka-sz-2:9092,kafka-sz-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404

    - name: prom-gw-bj
      bootstrapServers: kafka-bj-1:9092,kafka-bj-2:9092,kafka-bj-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404

    - name: prom-gw-hf
      bootstrapServers: kafka-hf-1:9092,kafka-hf-2:9092,kafka-hf-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404
```

对应环境变量方式(索引从 0 开始):

```bash
# /etc/systemd/system/kafbat-ui.service 的 [Service] 段
Environment="KAFKA_CLUSTERS_0_NAME=prom-gw-sz"
Environment="KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-sz-1:9092,..."
Environment="KAFKA_CLUSTERS_0_METRICS_TYPE=PROMETHEUS"
Environment="KAFKA_CLUSTERS_0_METRICS_PORT=9404"

Environment="KAFKA_CLUSTERS_1_NAME=prom-gw-bj"
Environment="KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS=kafka-bj-1:9092,..."
...
```

---

## 5. Nginx 反向代理

### 5.1 Nginx 配置

复用现有 [HA 与负载均衡部署](ha-lb-deployment.md) 的 Nginx,增加 Kafbat UI 的反向代理:

**`/etc/nginx/conf.d/kafka-ui.conf`**:

```nginx
upstream kafbat_ui {
    server 10.0.10.30:8080;
    # 如有多实例:
    # server 10.0.10.31:8080;
}

server {
    listen 443 ssl http2;
    server_name kafka-ui.prom-gw.internal;

    # SSL 证书(复用现有证书)
    ssl_certificate     /etc/nginx/ssl/prom-gw.crt;
    ssl_certificate_key /etc/nginx/ssl/prom-gw.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # 反向代理到 Kafbat UI
    location / {
        proxy_pass http://kafbat_ui;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 支持(实时消息浏览需要)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 超时设置(消息浏览可能耗时较长)
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # 健康检查端点(不走认证,供 Prometheus / Keepalived 探测)
    location /actuator/health {
        proxy_pass http://kafbat_ui;
        access_log off;
    }

    # 访问控制(仅运维网段)
    allow 10.0.10.0/24;       # 运维网段
    allow 10.0.1.0/24;        # Kafka 运维网段
    deny all;
}
```

```bash
# 测试配置
sudo nginx -t
# 重载
sudo nginx -s reload
```

### 5.2 Keepalived VIP

若复用现有 Keepalived VIP(见 [HA 部署](ha-lb-deployment.md)),VIP 已配置好 SSL 和浮动 IP,Kafbat UI 只需作为 Nginx upstream 接入即可。

---

## 6. 认证与 RBAC

### 6.1 Basic Auth(推荐内网使用)

Kafbat UI 支持内置 Basic Auth,适合内网运维场景。在 `application.yml` 中启用:

**`/appdata/kafka-ui/config/application.yml` 追加**:

```yaml
# ====== Basic Auth 认证 ======
auth:
  type: BASIC

# Spring Security 配置
spring:
  security:
    user:
      name: admin
      password: "{bcrypt}$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68deJU3uOAvXm"  # bcrypt 加密
```

> **多用户 RBAC**:Kafbat UI 的完整 RBAC 需要 OAuth2 / LDAP 后端支持。Basic Auth 模式下使用单一管理员账号,适合小型运维团队。如需多角色管理,见 [§6.3](#63-oauth2-认证可选)。

**密码生成**(使用 Spring Boot 内置 BCrypt):

```bash
# 使用 htpasswd 生成 bcrypt 密码
htpasswd -nbB admin "YourPassword123"
# 输出: admin:$2y$10$xxxxxxx...
# 将 $2y$ 替换为 {bcrypt}$2a$ 后填入配置

# 或使用 Java 生成(利用已安装的 JDK)
java -cp /appdata/kafka-ui/kafka-ui.jar \
  -Dloader.main=org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder \
  org.springframework.boot.loader.PropertiesApplication \
  "YourPassword123"
```

### 6.2 RBAC 角色说明

| 角色 | 权限 | 适用场景 |
|---|---|---|
| `VIEWER` | 只读:查看 Topic / Consumer Group / 消息浏览 | 运维查看 |
| `DEVELOPER` | Topic 管理:创建 / 删除 / 修改配置 / 生产消息 | 开发调试 |
| `ADMIN` | 全部权限 + 集群配置管理 | 集群管理员 |

### 6.3 OAuth2 认证(可选)

如需对接企业 GitLab / Google / Keycloak OAuth2 实现多用户 RBAC:

**`/appdata/kafka-ui/config/application.yml` 追加**:

```yaml
auth:
  type: OAUTH2

spring:
  security:
    oauth2:
      client:
        registration:
          keycloak:
            client-id: "kafbat-ui"
            client-secret: "xxx"
            scope: openid,profile,email
        provider:
          keycloak:
            issuer-uri: "https://keycloak.internal/realms/prom-gw"
```

> **详细 OAuth2 / LDAP 配置**见 [Kafbat UI 官方文档](https://ui.docs.kafbat.io/configuration/rbac-role-based-access-control)。

---

## 7. 监控集成

### 7.1 Kafbat UI 自身监控

Kafbat UI 暴露 Spring Boot Actuator 健康检查和 Prometheus 指标端点(已在 `application.yml` 的 `management` 段开启):

```bash
# 健康检查
curl -s http://localhost:8080/actuator/health
# {"status":"UP"}

# 应用信息
curl -s http://localhost:8080/actuator/info

# Prometheus 指标
curl -s http://localhost:8080/actuator/prometheus | head -20
```

**Prometheus 抓取配置**(`prometheus.yml` 追加):

```yaml
scrape_configs:
  - job_name: kafbat-ui
    static_configs:
      - targets: ['10.0.10.30:8080']
        # 如有多实例:
        # - '10.0.10.31:8080'
    metrics_path: /actuator/prometheus
    scrape_interval: 30s
```

### 7.2 Kafka 集群监控(已集成)

Kafbat UI 通过 Prometheus JMX Exporter(端口 9404)直接在 Web 界面展示 Kafka 指标,无需额外配置。Web 界面可查看:

| 功能 | 对应 Kafka 指标 |
|---|---|
| Broker Overview | 在线 Broker 列表、controller 状态 |
| Topic 详情 | partition 数、副本分布、ISR 状态 |
| Consumer Group | 各 partition 的 offset、lag |
| 消息浏览 | 实时查看 / 搜索 / 过滤 Topic 消息 |
| 指标图表 | BytesIn/Out、MessagesIn、RequestLatency 等 |

### 7.3 告警规则补充

Kafbat UI 自身告警规则(`prometheus-rules.yml` 追加):

```yaml
groups:
  - name: kafbat-ui
    rules:
      - alert: KafbatUIDown
        expr: up{job="kafbat-ui"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafbat UI is down on {{ $labels.instance }}"

      - alert: KafbatUIHighMemory
        expr: jvm_memory_used_bytes{job="kafbat-ui", area="heap"} > 2*1024*1024*1024
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafbat UI JVM heap > 2GB on {{ $labels.instance }}"
```

---

## 8. 运维操作

### 8.1 启停与重启

```bash
# 启动
sudo systemctl start kafbat-ui

# 停止(优雅停机,SIGTERM 触发 Spring Boot 优雅关闭)
sudo systemctl stop kafbat-ui

# 重启
sudo systemctl restart kafbat-ui

# 查看状态
sudo systemctl status kafbat-ui

# 查看实时日志(systemd journal)
sudo journalctl -u kafbat-ui -f --no-pager

# 查看应用日志
tail -f /applog/kafka-ui/kafbat-ui.log

# 查看最近 100 行日志
sudo journalctl -u kafbat-ui --no-pager -n 100
```

### 8.2 配置变更

```bash
# 1. 修改 application.yml
sudo vi /appdata/kafka-ui/config/application.yml

# 2. 重启生效
sudo systemctl restart kafbat-ui

# 3. 验证
sudo systemctl status kafbat-ui
curl -s http://localhost:8080/actuator/health
```

### 8.3 版本升级

```bash
cd /appdata/kafka-ui

# 1. 备份配置
sudo cp config/application.yml config/application.yml.bak.$(date +%Y%m%d)

# 2. 下载新版本 JAR(如升级到 1.6.0)
sudo wget -O api-1.6.0.jar \
  https://github.com/kafbat/kafka-ui/releases/download/v1.6.0/api-v1.6.0.jar

# 3. 校验 SHA256(从 Release 页面获取)
echo "<新版本sha256>  api-1.6.0.jar" | sha256sum -c -

# 4. 更新软链
sudo chown kafbat-ui:kafbat-ui api-1.6.0.jar
sudo ln -sfn api-1.6.0.jar kafka-ui.jar

# 5. 重启
sudo systemctl restart kafbat-ui

# 6. 验证
sudo systemctl status kafbat-ui
curl -s http://localhost:8080/actuator/health
curl -s http://localhost:8080/actuator/info
```

> **升级前必读**:升级前查阅 [Release Notes](https://github.com/kafbat/kafka-ui/releases),确认 Breaking Changes。建议在测试环境先验证。旧版本 JAR 可保留以便回滚:更新软链指回旧版本即可。

### 8.4 备份与恢复

```bash
# 备份配置
sudo tar -czf kafka-ui-config-backup-$(date +%Y%m%d).tar.gz \
    -C /appdata/kafka-ui config/ \
    -C /etc/systemd/system kafbat-ui.service

# 恢复配置
sudo tar -xzf kafka-ui-config-backup-20260820.tar.gz -C /
sudo systemctl daemon-reload
sudo systemctl restart kafbat-ui
```

### 8.5 常见排查操作

```bash
# 查看进程信息
ps -ef | grep kafka-ui.jar

# 查看 JVM 堆信息(排查 OOM)
# 获取 PID
PID=$(pgrep -f kafka-ui.jar)
sudo -u kafbat-ui jcmd $PID GC.heap_info

# 查看 JVM 线程栈(排查卡死)
sudo -u kafbat-ui jstack $PID | head -100

# 查看端口监听
ss -tlnp | grep 8080

# 查看磁盘使用
du -sh /appdata/kafka-ui /applog/kafka-ui

# 清理旧版本 JAR(保留当前版本)
cd /appdata/kafka-ui
ls -lh api-*.jar
# 确认 kafka-ui.jar 软链指向后,删除旧版本
# sudo rm api-1.4.0.jar
```

---

## 9. 附录

### 9.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `kafka-ui.jar` → `api-v1.5.0.jar` | `/appdata/kafka-ui/` | Kafbat UI JAR 包(软链) |
| `application.yml` | `/appdata/kafka-ui/config/application.yml` | 主配置(集群 / 通用 / Actuator) |
| `dynamic_config.yaml` | `/appdata/kafka-ui/config/dynamic_config.yaml` | 运行时动态配置(Web 修改) |
| `kafbat-ui.service` | `/etc/systemd/system/kafbat-ui.service` | systemd 服务 |
| `kafka-ui.conf` | `/etc/nginx/conf.d/kafka-ui.conf` | Nginx 反向代理 |
| `kafbat-ui.log` | `/applog/kafka-ui/kafbat-ui.log` | 应用日志 |

### 9.2 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| 服务无法启动 | `journalctl -u kafbat-ui` 查看启动日志;检查 JDK 是否安装 | 安装 OpenJDK 25 / 修正配置 |
| Web 界面无法访问 | `systemctl status kafbat-ui`;`curl localhost:8080` 测试 | 重启服务 / 检查端口监听 |
| 连接 Kafka 超时 | 检查 `bootstrapServers` 是否可达;检查安全组 9092 | 修正配置 / 开放安全组 |
| Broker 指标不显示 | 检查 `metrics.type=PROMETHEUS` 和 `port=9404`;`curl kafka-1:9404/metrics` | 确认 JMX Exporter 已部署(见 [Kafka 部署 §5.1](kafka-production-deployment.md#51-jmx-exporter)) |
| 消息浏览卡住 | 检查 Kafka Broker 状态;检查 `polling.throttleRate` | 调整限速 / 检查 Broker |
| OOM(进程被 kill) | `dmesg \| grep -i oom`;`jcmd PID GC.heap_info` | 调大 `-Xmx` / 检查内存泄漏 |
| Basic Auth 登录失败 | 检查 `application.yml` 密码格式(bcrypt);检查 `auth.type=BASIC` | 重新生成密码 |
| 动态配置丢失 | 检查 `dynamic_config.yaml` 文件权限 | 确认 kafbat-ui 用户有写权限 |
| Nginx 502 | 检查 Kafbat UI 服务是否运行;检查 upstream 地址 | 重启服务 / 修正 Nginx upstream |
| 端口被占用 | `ss -tlnp \| grep 8080` | 修改 `server.port` 或释放端口 |

### 9.3 JVM 调优

systemd 服务中 `JAVA_OPTS` 环境变量控制 JVM 参数:

```ini
# /etc/systemd/system/kafbat-ui.service
Environment="JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

| 参数 | 值 | 说明 |
|---|---|---|
| `-Xms2g -Xmx2g` | 2G 堆 | 单集群监控足够,多集群建议 4G |
| `-XX:+UseG1GC` | G1 GC | 低延迟,适合中等堆 |
| `-XX:MaxGCPauseMillis=100` | 100ms | GC 暂停目标 |
| `-XX:+AlwaysPreTouch` | - | 启动时预触达堆内存,减少运行时页错误 |
| `-Djava.awt.headless=true` | - | 无头模式,服务器环境必需 |

> **GC 日志**:如需排查 GC 问题,追加 `-Xlog:gc*:file=/applog/kafka-ui/gc.log:time,uptime:filecount=10,filesize=100m`。

### 9.4 日志轮转

Kafbat UI 日志通过 Logback 配置,已在 `application.yml` 中指定日志文件路径。如需日志轮转,在 `application.yml` 追加 Logback 配置,或使用 logrotate:

**`/etc/logrotate.d/kafbat-ui`**:

```
/applog/kafka-ui/*.log {
    daily
    rotate 10
    size 100M
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

```bash
sudo logrotate -d /etc/logrotate.d/kafbat-ui   # 测试(dry-run)
```

### 9.5 v1.5.0 主要特性

| 特性 | 说明 |
|---|---|
| MessagePack SerDe | 支持 MessagePack 序列化格式 |
| Swagger UI | 内置 API 文档(通过 `swagger_ui.enabled` 开启) |
| Consumer Lag 实时更新 | Consumer Group lag 实时刷新 |
| CSV 导出 | 表格数据导出为 CSV |
| Connector Consumer Group 集成 | Kafka Connect connector 关联 Consumer Group |
| OAuth2 增强 | Schema Registry 和代理的 OAuth2 改进 |
| ACL 增强 | ACL 管理功能增强 |

### 9.6 相关文档

- [Kafka 生产部署](kafka-production-deployment.md) — Kafka 集群部署(含 JMX Exporter §5.1)
- [生产部署指南](production-guide.md) — prom-gw 整体部署
- [高可用与负载均衡](ha-lb-deployment.md) — Nginx / Keepalived 部署
- [故障剧本](runbook.md) — Kafka 故障处理
- [Kafbat UI 官方文档](https://ui.docs.kafbat.io/) — 完整配置参考
- [Kafbat UI GitHub](https://github.com/kafbat/kafka-ui) — 源码与 Release
