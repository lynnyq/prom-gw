-- ============================================================================
-- prom-gw StarRocks 按 sysid 分表 DDL 模板
-- ============================================================================
-- 版本   : v1.0
-- 适用   : StarRocks 3.4.10
-- 数据库 : observability
--
-- 说明:
--   按 sysid 把不同 S 系统的指标数据写入不同 StarRocks 表,表名通过 sysid 区分。
--   每个 sysid 三张表:
--     metrics_<sysid>_5m (Flink 实时写入,留存 7 天)
--     metrics_<sysid>_1h (5m → 1h 级联聚合,留存 90 天)
--     metrics_<sysid>_1d (1h → 1d 级联聚合,留存 3 年)
--   例: sysid=s001 → metrics_s001_5m / metrics_s001_1h / metrics_s001_1d
--
-- 新增 S 系统步骤:
--   1. 在 prom-gw 规则配置(configs/rules/sysid-routing.yaml)的 route.rules 中
--      新增 { sysid: "<新sysid>" } → prom.<city>.routed.<新sysid>
--   2. 在 Kafka 中预创建 topic: prom.<city>.routed.<新sysid>
--   3. 在 Flink 作业启动参数 --sysid-table-mapping 中追加 <新sysid>=<表名>
--   4. 执行本文件中的建表模板(把 {SYSID} 替换为实际 sysid),创建 5m/1h/1d 表
--      及 agg_5m_to_1h_{SYSID}、agg_1h_to_1d_{SYSID} 级联任务
--
-- 本表 schema 与 metrics_5m 对齐,额外包含 sysid 列(便于在单表内追溯)。
-- ============================================================================

USE observability;

-- ============================================================================
-- 按 sysid 分表 DDL 模板(把 {SYSID} 替换为实际 sysid,如 s001)
-- ============================================================================
-- 复制以下 CREATE TABLE 语句,把 metrics_{SYSID}_5m 中的 {SYSID} 替换为实际 sysid。

CREATE TABLE IF NOT EXISTS metrics_{SYSID}_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名(如 go_goroutines)',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID(与表名后缀一致,便于追溯)',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和 Σvalue',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值 = value_sum / sample_count',
  value_p50     DOUBLE                COMMENT '窗口内 p50(中位数)',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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
    "replicated_storage" = "true",
    "enable_unique_key_merge_on_write" = "false"
  );


-- ============================================================================
-- 示例:为 sysid=s001 和 sysid=s002 创建表
-- ============================================================================

-- sysid=s001 的表
CREATE TABLE IF NOT EXISTS metrics_s001_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名(如 go_goroutines)',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和 Σvalue',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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
    "replicated_storage" = "true",
    "enable_unique_key_merge_on_write" = "false"
  );

-- sysid=s002 的表
CREATE TABLE IF NOT EXISTS metrics_s002_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名(如 go_goroutines)',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和 Σvalue',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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
    "replicated_storage" = "true",
    "enable_unique_key_merge_on_write" = "false"
  );


-- ============================================================================
-- 可选:未知 sysid 的兜底表(对应 Flink --default-table 参数)
-- ============================================================================
-- 若配置了 --default-table metrics_default_5m,则未匹配 sysid 的数据会写入此表。
CREATE TABLE IF NOT EXISTS metrics_default_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID(未知 sysid 的数据集中于此,便于排查)',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
  PARTITION BY RANGE(ts) ()
  DISTRIBUTED BY HASH(metric, business) BUCKETS 16
  PROPERTIES (
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "compression" = "LZ4",
    "replicated_storage" = "true"
  );


-- ============================================================================
-- 按 sysid 分表 DDL 模板 - 1h 表(5m → 1h 级联聚合,留存 90 天)
-- ============================================================================
-- 复制以下 CREATE TABLE,把 {SYSID} 替换为实际 sysid。
CREATE TABLE IF NOT EXISTS metrics_{SYSID}_1h (
  ts            DATETIME     NOT NULL COMMENT '1 h 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID(与表名后缀一致)',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 5 min 样本数(1 h = 12 个)',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT 'percentile_approx 跨窗口 t-digest 合并',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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


-- ============================================================================
-- 按 sysid 分表 DDL 模板 - 1d 表(1h → 1d 级联聚合,留存 3 年)
-- ============================================================================
CREATE TABLE IF NOT EXISTS metrics_{SYSID}_1d (
  ts            DATETIME     NOT NULL COMMENT '1 day 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 1 h 样本数(1 day = 24 个)',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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


-- ============================================================================
-- 级联聚合任务 - 按 sysid 分表(5m → 1h,每小时整点执行)
-- ============================================================================
-- 把 {SYSID} 替换为实际 sysid。每个 sysid 独立调度,互不影响。
CREATE JOB IF NOT EXISTS agg_5m_to_1h_{SYSID}
ON SCHEDULE EVERY 1 HOUR STARTS (date_trunc('hour', NOW()) + INTERVAL 1 HOUR)
DO
  INSERT OVERWRITE metrics_{SYSID}_1h
  SELECT
      date_trunc('hour', ts)                                            AS ts,
      metric,
      business,
      ingest_city,
      source_dc,
      sysid,
      labels_hash,
      labels,
      SUM(sample_count)                                                 AS sample_count,
      SUM(value_sum)                                                    AS value_sum,
      MAX(value_max)                                                    AS value_max,
      MIN(value_min)                                                    AS value_min,
      SUM(value_sum) / SUM(sample_count)                                AS value_avg,
      percentile_approx(value_p50, 0.5)                                 AS value_p50,
      percentile_approx(value_p99, 0.99)                                AS value_p99
  FROM metrics_{SYSID}_5m
  WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR
    AND ts <  date_trunc('hour', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


-- ============================================================================
-- 级联聚合任务 - 按 sysid 分表(1h → 1d,每天 0 点执行)
-- ============================================================================
CREATE JOB IF NOT EXISTS agg_1h_to_1d_{SYSID}
ON SCHEDULE EVERY 1 DAY STARTS (date_trunc('day', NOW()) + INTERVAL 1 DAY)
DO
  INSERT OVERWRITE metrics_{SYSID}_1d
  SELECT
      date_trunc('day', ts)                                             AS ts,
      metric,
      business,
      ingest_city,
      source_dc,
      sysid,
      labels_hash,
      labels,
      SUM(sample_count)                                                 AS sample_count,
      SUM(value_sum)                                                    AS value_sum,
      MAX(value_max)                                                    AS value_max,
      MIN(value_min)                                                    AS value_min,
      SUM(value_sum) / SUM(sample_count)                                AS value_avg,
      percentile_approx(value_p50, 0.5)                                 AS value_p50,
      percentile_approx(value_p99, 0.99)                                AS value_p99
  FROM metrics_{SYSID}_1h
  WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY
    AND ts <  date_trunc('day', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


-- ============================================================================
-- 示例:为 sysid=s001 创建 1h / 1d 表及级联任务
-- ============================================================================

-- 1h 表
CREATE TABLE IF NOT EXISTS metrics_s001_1h (
  ts            DATETIME     NOT NULL COMMENT '1 h 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 5 min 样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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

-- 1d 表
CREATE TABLE IF NOT EXISTS metrics_s001_1d (
  ts            DATETIME     NOT NULL COMMENT '1 day 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心',
  sysid         VARCHAR(64)  NOT NULL COMMENT 's 系统 ID',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 1 h 样本数',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值',
  value_p50     DOUBLE                COMMENT '窗口内 p50',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, business, ingest_city, source_dc, sysid, labels_hash)
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

-- 5m → 1h 级联任务
CREATE JOB IF NOT EXISTS agg_5m_to_1h_s001
ON SCHEDULE EVERY 1 HOUR STARTS (date_trunc('hour', NOW()) + INTERVAL 1 HOUR)
DO
  INSERT OVERWRITE metrics_s001_1h
  SELECT
      date_trunc('hour', ts) AS ts, metric, business, ingest_city, source_dc, sysid,
      labels_hash, labels,
      SUM(sample_count) AS sample_count,
      SUM(value_sum) AS value_sum,
      MAX(value_max) AS value_max,
      MIN(value_min) AS value_min,
      SUM(value_sum) / SUM(sample_count) AS value_avg,
      percentile_approx(value_p50, 0.5) AS value_p50,
      percentile_approx(value_p99, 0.99) AS value_p99
  FROM metrics_s001_5m
  WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR
    AND ts <  date_trunc('hour', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

-- 1h → 1d 级联任务
CREATE JOB IF NOT EXISTS agg_1h_to_1d_s001
ON SCHEDULE EVERY 1 DAY STARTS (date_trunc('day', NOW()) + INTERVAL 1 DAY)
DO
  INSERT OVERWRITE metrics_s001_1d
  SELECT
      date_trunc('day', ts) AS ts, metric, business, ingest_city, source_dc, sysid,
      labels_hash, labels,
      SUM(sample_count) AS sample_count,
      SUM(value_sum) AS value_sum,
      MAX(value_max) AS value_max,
      MIN(value_min) AS value_min,
      SUM(value_sum) / SUM(sample_count) AS value_avg,
      percentile_approx(value_p50, 0.5) AS value_p50,
      percentile_approx(value_p99, 0.99) AS value_p99
  FROM metrics_s001_1h
  WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY
    AND ts <  date_trunc('day', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


-- ============================================================================
-- 手动补跑(按 sysid 分表)
-- ============================================================================

-- 补跑指定小时的 5m → 1h(替换 {SYSID} 和 {YYYY-MM-DD HH:00:00})
-- INSERT OVERWRITE metrics_{SYSID}_1h
-- SELECT date_trunc('hour', ts) AS ts, metric, business, ingest_city, source_dc, sysid,
--        labels_hash, labels,
--        SUM(sample_count) AS sample_count, SUM(value_sum) AS value_sum,
--        MAX(value_max) AS value_max, MIN(value_min) AS value_min,
--        SUM(value_sum) / SUM(sample_count) AS value_avg,
--        percentile_approx(value_p50, 0.5) AS value_p50,
--        percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_{SYSID}_5m
-- WHERE ts >= '{YYYY-MM-DD HH:00:00}' AND ts < '{YYYY-MM-DD HH:00:00}' + INTERVAL 1 HOUR
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

-- 补跑指定天的 1h → 1d(替换 {SYSID} 和 {YYYY-MM-DD})
-- INSERT OVERWRITE metrics_{SYSID}_1d
-- SELECT date_trunc('day', ts) AS ts, metric, business, ingest_city, source_dc, sysid,
--        labels_hash, labels,
--        SUM(sample_count) AS sample_count, SUM(value_sum) AS value_sum,
--        MAX(value_max) AS value_max, MIN(value_min) AS value_min,
--        SUM(value_sum) / SUM(sample_count) AS value_avg,
--        percentile_approx(value_p50, 0.5) AS value_p50,
--        percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_{SYSID}_1h
-- WHERE ts >= '{YYYY-MM-DD}' AND ts < '{YYYY-MM-DD}' + INTERVAL 1 DAY
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


-- ============================================================================
-- 验证 SQL
-- ============================================================================

-- 查看所有 sysid 表的数据量(5m / 1h / 1d)
-- SELECT 's001' AS sysid, '5m' AS win, COUNT(*) AS cnt FROM metrics_s001_5m
-- UNION ALL SELECT 's001', '1h', COUNT(*) FROM metrics_s001_1h
-- UNION ALL SELECT 's001', '1d', COUNT(*) FROM metrics_s001_1d;

-- 按 sysid 查询某 S 系统的 1h 指标
-- SELECT ts, metric, business, ingest_city, value_avg, value_p99
-- FROM metrics_s001_1h
-- WHERE metric = 'go_goroutines'
--   AND ts >= '2026-10-27 00:00:00'
-- ORDER BY ts
-- LIMIT 20;
