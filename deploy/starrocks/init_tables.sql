-- ============================================================================
-- prom-gw StarRocks 三表 DDL + 级联聚合 SQL
-- ============================================================================
-- 版本   : v1.0
-- 适用   : StarRocks 3.4.10
-- 数据库 : observability
-- 说明   :
--   1. metrics_5m  — 5 分钟明细,Flink Stream Load 唯一写入点,保留 7 天
--   2. metrics_1h  — 1 小时聚合,由 5m 级联,保留 90 天
--   3. metrics_1d  — 1 天聚合,由 1h 级联,保留 3 年
--   4. 5m → 1h 定时聚合   — StarRocks CREATE JOB 每小时执行
--   5. 1h → 1d 定时聚合  — StarRocks CREATE JOB 每天执行
--
-- 执行顺序: 1 → 2 → 3 → 4 → 5
-- ============================================================================

USE observability;

-- ============================================================================
-- 1) 5 min 表:Flink 跨城 Stream Load 唯一写入点,留存 7 天
-- ============================================================================
-- PRIMARY KEY 模型:Flink at-least-once 写入用 REPLACE INTO 自动去重
-- labels_hash:labels 的 SHA-1 hash(由 Flink LabelsHasher 计算),作 PK 键列
--   (StarRocks PK 键列不支持 MAP 类型,用 VARCHAR 存 hash 替代)
CREATE TABLE IF NOT EXISTS metrics_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间',
  metric        VARCHAR(128) NOT NULL COMMENT '指标名(如 go_goroutines)',
  business      VARCHAR(64)  NOT NULL COMMENT '业务标识(来自 token,替代原 tenant)',
  ingest_city   VARCHAR(16)  NOT NULL COMMENT '采集机房城市:bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL COMMENT '源数据中心:东坝/马坡/南法信/五联/南湾/合肥',
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 SHA-1 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数(15s 步长 × 20 = 300)',
  value_sum     DOUBLE       NOT NULL COMMENT '窗口内求和 Σvalue',
  value_max     DOUBLE                COMMENT '窗口内最大值',
  value_min     DOUBLE                COMMENT '窗口内最小值',
  value_avg     DOUBLE                COMMENT '窗口内均值 = value_sum / sample_count',
  value_p50     DOUBLE                COMMENT '窗口内 p50(中位数)',
  value_p99     DOUBLE                COMMENT '窗口内 p99'
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
    "replicated_storage" = "true",
    "enable_unique_key_merge_on_write" = "false"
  );


-- ============================================================================
-- 2) 1 h 表:周期任务从 5m 表级联聚合,留存 90 天
-- ============================================================================
CREATE TABLE IF NOT EXISTS metrics_1h (
  ts            DATETIME     NOT NULL COMMENT '1 h 窗口起始时间',
  metric        VARCHAR(128) NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL,
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL,
  labels        MAP<VARCHAR(64), VARCHAR(256)>,
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 5 min 样本数(1 h = 12 个)',
  value_sum     DOUBLE       NOT NULL,
  value_max     DOUBLE,
  value_min     DOUBLE,
  value_avg     DOUBLE,
  value_p50     DOUBLE       COMMENT 'percentile_approx 跨窗口 t-digest 合并',
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


-- ============================================================================
-- 3) 1 d 表:周期任务从 1h 表级联聚合,留存 3 年
-- ============================================================================
CREATE TABLE IF NOT EXISTS metrics_1d (
  ts            DATETIME     NOT NULL COMMENT '1 day 窗口起始时间',
  metric        VARCHAR(128) NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL,
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL,
  labels        MAP<VARCHAR(64), VARCHAR(256)>,
  sample_count  BIGINT       NOT NULL COMMENT '窗口内 1 h 样本数(1 day = 24 个)',
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


-- ============================================================================
-- 4) 周期任务:5m → 1h 级联聚合(每小时整点执行)
-- ============================================================================
-- 用 INSERT OVERWRITE 覆盖目标小时分区,保证幂等(重跑不产生重复)
-- StarRocks 2.5+ 内置 CREATE JOB 调度;若版本不足,用外部 crontab 调用
CREATE JOB IF NOT EXISTS agg_5m_to_1h
ON SCHEDULE EVERY 1 HOUR STARTS (date_trunc('hour', NOW()) + INTERVAL 1 HOUR)
DO
  INSERT OVERWRITE metrics_1h
  SELECT
      date_trunc('hour', ts)                                            AS ts,
      metric,
      business,
      ingest_city,
      source_dc,
      labels_hash,
      labels,
      SUM(sample_count)                                                 AS sample_count,
      SUM(value_sum)                                                    AS value_sum,
      MAX(value_max)                                                    AS value_max,
      MIN(value_min)                                                    AS value_min,
      SUM(value_sum) / SUM(sample_count)                                AS value_avg,
      percentile_approx(value_p50, 0.5)                                 AS value_p50,
      percentile_approx(value_p99, 0.99)                                AS value_p99
  FROM metrics_5m
  WHERE ts >= date_trunc('hour', NOW()) - INTERVAL 1 HOUR
    AND ts <  date_trunc('hour', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- ============================================================================
-- 5) 周期任务:1h → 1d 级联聚合(每天 0 点执行)
-- ============================================================================
CREATE JOB IF NOT EXISTS agg_1h_to_1d
ON SCHEDULE EVERY 1 DAY STARTS (date_trunc('day', NOW()) + INTERVAL 1 DAY)
DO
  INSERT OVERWRITE metrics_1d
  SELECT
      date_trunc('day', ts)                                             AS ts,
      metric,
      business,
      ingest_city,
      source_dc,
      labels_hash,
      labels,
      SUM(sample_count)                                                 AS sample_count,
      SUM(value_sum)                                                    AS value_sum,
      MAX(value_max)                                                    AS value_max,
      MIN(value_min)                                                    AS value_min,
      SUM(value_sum) / SUM(sample_count)                                AS value_avg,
      percentile_approx(value_p50, 0.5)                                 AS value_p50,
      percentile_approx(value_p99, 0.99)                                AS value_p99
  FROM metrics_1h
  WHERE ts >= date_trunc('day', NOW()) - INTERVAL 1 DAY
    AND ts <  date_trunc('day', NOW())
  GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- ============================================================================
-- 6) 手动触发级联聚合(用于首次初始化或补跑历史数据)
-- ============================================================================

-- 6a) 补跑指定小时的 5m → 1h 聚合
-- 用法:替换 {YYYY-MM-DD HH:00:00} 为目标小时整点
-- INSERT OVERWRITE metrics_1h
-- SELECT
--     date_trunc('hour', ts) AS ts,
--     metric, business, ingest_city, source_dc, labels_hash, labels,
--     SUM(sample_count) AS sample_count,
--     SUM(value_sum) AS value_sum,
--     MAX(value_max) AS value_max,
--     MIN(value_min) AS value_min,
--     SUM(value_sum) / SUM(sample_count) AS value_avg,
--     percentile_approx(value_p50, 0.5) AS value_p50,
--     percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_5m
-- WHERE ts >= '{YYYY-MM-DD HH:00:00}'
--   AND ts <  '{YYYY-MM-DD HH:00:00}' + INTERVAL 1 HOUR
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 6b) 补跑指定天的 1h → 1d 聚合
-- 用法:替换 {YYYY-MM-DD} 为目标日期
-- INSERT OVERWRITE metrics_1d
-- SELECT
--     date_trunc('day', ts) AS ts,
--     metric, business, ingest_city, source_dc, labels_hash, labels,
--     SUM(sample_count) AS sample_count,
--     SUM(value_sum) AS value_sum,
--     MAX(value_max) AS value_max,
--     MIN(value_min) AS value_min,
--     SUM(value_sum) / SUM(sample_count) AS value_avg,
--     percentile_approx(value_p50, 0.5) AS value_p50,
--     percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_1h
-- WHERE ts >= '{YYYY-MM-DD}'
--   AND ts <  '{YYYY-MM-DD}' + INTERVAL 1 DAY
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 6c) 首次全量初始化 1h 表(从 5m 表历史数据聚合)
-- INSERT OVERWRITE metrics_1h
-- SELECT
--     date_trunc('hour', ts) AS ts,
--     metric, business, ingest_city, source_dc, labels_hash, labels,
--     SUM(sample_count) AS sample_count,
--     SUM(value_sum) AS value_sum,
--     MAX(value_max) AS value_max,
--     MIN(value_min) AS value_min,
--     SUM(value_sum) / SUM(sample_count) AS value_avg,
--     percentile_approx(value_p50, 0.5) AS value_p50,
--     percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_5m
-- WHERE ts >= date_trunc('day', NOW()) - INTERVAL 7 DAY
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 6d) 首次全量初始化 1d 表(从 1h 表全量聚合)
-- INSERT OVERWRITE metrics_1d
-- SELECT
--     date_trunc('day', ts) AS ts,
--     metric, business, ingest_city, source_dc, labels_hash, labels,
--     SUM(sample_count) AS sample_count,
--     SUM(value_sum) AS value_sum,
--     MAX(value_max) AS value_max,
--     MIN(value_min) AS value_min,
--     SUM(value_sum) / SUM(sample_count) AS value_avg,
--     percentile_approx(value_p50, 0.5) AS value_p50,
--     percentile_approx(value_p99, 0.99) AS value_p99
-- FROM metrics_1h
-- WHERE ts >= date_trunc('day', NOW()) - INTERVAL 90 DAY
-- GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- ============================================================================
-- 7) 验证 SQL
-- ============================================================================

-- 7a) 查看三表数据量(执行后确认)
-- SELECT '5m' AS tbl, COUNT(*) FROM metrics_5m
-- UNION ALL
-- SELECT '1h' AS tbl, COUNT(*) FROM metrics_1h
-- UNION ALL
-- SELECT '1d' AS tbl, COUNT(*) FROM metrics_1d;

-- 7b) 验证主键唯一性(应无重复)
-- SELECT COUNT(*) AS total_rows FROM metrics_5m;
-- SELECT COUNT(DISTINCT CONCAT(CAST(ts AS STRING), metric, business, ingest_city, source_dc, labels_hash)) AS unique_keys FROM metrics_5m;

-- 7c) 抽样查询(按业务 + 指标 + 时间范围)
-- SELECT ts, metric, business, ingest_city, source_dc, value_avg, value_p99
-- FROM metrics_1h
-- WHERE business = 'app-business'
--   AND metric = 'go_goroutines'
--   AND ts >= '2026-08-27 00:00:00'
--   AND ts <  '2026-08-28 00:00:00'
-- ORDER BY ts;

-- 7d) 动态分区验证(确认过期分区已自动清理)
-- SHOW PARTITIONS FROM metrics_5m;
-- SHOW PARTITIONS FROM metrics_1h;
-- SHOW PARTITIONS FROM metrics_1d;

-- 7e) JOB 调度状态验证
-- SHOW JOBS;
-- SHOW RUNNING JOBS;
-- SHOW HISTORY FOR JOB agg_5m_to_1h;
-- SHOW HISTORY FOR JOB agg_1h_to_1d;
