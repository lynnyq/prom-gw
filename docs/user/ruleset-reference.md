# Ruleset 字段参考

一个 ruleset 配置文件示例(`configs/rules/app-business.yaml`):

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business              # 没路由命中时的兜底 topic
    version: 1                                    # 必须严格递增
    stages:
      - type: relabel                             # 标签增删改
        config:
          ...
      - type: route                               # 路由到不同 topic
        config:
          ...
      - type: sample                              # 采样
        config:
          rate: 0.5
      - type: enrich                              # 静态/模板 label
        config:
          ...
      - type: downsample                          # 按时间桶聚合
        config:
          ...
      - type: deadvalue                           # 死值丢弃
        config:
          ...
```

## 全局字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `rulesets[].name` | string | ✓ | ruleset 名称,作为唯一 ID |
| `rulesets[].default_topic` | string | ✓ | 没路由命中时的兜底 Kafka topic |
| `rulesets[].version` | int | ✓ | 版本号,PUT/Admin API 必须严格递增 |
| `rulesets[].stages` | array | | 处理阶段(空 = 透传) |

## 通用 stage 配置

每个 stage 有:
- `type`:阶段类型
- `config`:阶段配置(每种 type 字段不同)

## 各 stage 字段

### relabel

标签增删改。

```yaml
- type: relabel
  config:
    drop_labels: ["env_internal", "tmp_a", "tmp_b"]   # 精确匹配(label name 完全相等)
    keep_labels: ["__name__", "instance", "job", "env", "region"]  # 白名单(优先级 > drop)
    label_map:                                # 重命名/改写 key
      "region": "datacenter"                  # 把 region 重写为 datacenter
      "env_name": "env"                       # 反向:把 env_name 重写回 env
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `drop_labels` | []string | 要删除的 label 名,**精确匹配**(`tmp_a` 仅匹配 `tmp_a`,不匹配 `tmp_*`) |
| `keep_labels` | []string | 白名单(其他全部删),优先级 > drop_labels |
| `label_map` | map | 改写 key(把已有 label 的 name 替换成 map 中 value),value 不支持模板 |
| `add_labels` | ❌ | **当前版本未实现**;新增静态 label 请使用 `enrich` 阶段代替 |

> 需要 glob 匹配(`tmp_*` 之类)请使用 `enrich` 阶段或 `route` 阶段的 fallback 逻辑。

### route

按 label 路由到不同 topic(match 全部采用**精确匹配**,不支持 glob)。

```yaml
- type: route
  config:
    rules:
      - match: { team: app }
        topic: prom.app
      - match: { team: infra, env: prod }     # 多 key 全部命中才算匹配
        topic: prom.infra_prod
      - match: { team: mobile-app }          # 字面精确值,无 glob
        topic: prom.mobile
    default_topic: prom.default              # 不命中时使用(也可省略,继承外层 default_topic)
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `rules[].match` | map | 精确匹配,所有 key=value 必须命中(字符串完全相等) |
| `rules[].topic` | string | 命中时投递到此 topic |
| `default_topic` | string | 不命中时的 topic(默认 = `rulesets[].default_topic`) |

注意:
- 同一个 sample 只能路由到 1 个 topic,按 `rules` 顺序匹配,第一个命中即生效
- 匹配到但 `topic` 为空 → 整条 sample 丢弃(见 `stage.go::RouteStage`)
- 若要按 metric 前缀分流,请使用 `RuleSet.match.metric_prefix` 在 ruleset 维度做

### sample

随机采样丢弃。

```yaml
- type: sample
  config:
    rate: 0.1   # 0.0 - 1.0,保留 10%
```

### enrich

静态 / 模板 label 注入(给 sample 增加 / 覆盖 label,字段值支持 `${labels.X}` 模板引用)。

```yaml
- type: enrich
  config:
    labels:
      environment: production
      cluster: "${labels.cluster_name}"      # 引用 sample 已有 label
```

模板语法:
- `${labels.X}`:取该 sample 的 label X;若 X 不存在,跳过该 enrich 条目
  并记 `gateway_errors_total{type="enrich_template_missing"}`
- 静态值:直接作为 label value

> 暂不支持 `${tenant}` / `${source_dc}` 引用;如需机房/租户标识,请在
> Prometheus 端用 `external_labels` 注入,prom-gw 会随 `WriteRequest` 一并透传,
> 经 `relabel.keep_labels` 保留即可。

### downsample

按时间桶聚合(状态型)。

```yaml
- type: downsample
  config:
    interval: 1m              # 桶大小,Go duration 格式:30s / 1m / 5m / 1h
    aggregations: [avg, max, min, sum, count, p50, p99]   # 至少 1 个
    max_series: 1000000       # 内存上限,超出按 LRU 驱逐
    p99_max_samples: 4096     # 单 series 单桶 p50/p99 采样上限(可选,默认 4096)
```

注意:
- 状态全内存,重启后丢失(由 Prometheus 短期重传补)
- 多个 downsample stage **不能串联**(会冲突),只允许 1 个
- p50/p99 使用**桶内排序切片精确计算**(非 P² 算法);超 `p99_max_samples` 时退化
  为"top-k reservoir sampling"以保上界,误差与桶大小相关

### deadvalue

死值丢弃(状态型)。

```yaml
- type: deadvalue
  config:
    window: 5m                # 时间窗,期间值不变则丢弃;Go duration 格式
    max_series: 1000000       # LRU 容量(可选,默认 1M)
```

行为:
- 同一 series 在 `window` 内值未变 → 丢弃
- 值变化或超过 window → 发出
- 重启后状态丢失,首条必发
- NaN/Inf 与 lastValue 比较时视为"变化",总是发出(避免静默丢弃 exporter 异常)

## Stage 顺序

- 阶段按 YAML 顺序串行执行
- 常见顺序:`relabel → route → sample → enrich → downsample → deadvalue`
- **状态型 stage 必须放在最后**(downsample / deadvalue 之后不能再有 stage)

## 热更新

修改 YAML 文件后,fsnotify 5s 内自动检测:
- 校验失败:旧版本保留,日志 WARN
- 校验成功:5s 内切换,日志 INFO

也可以通过 Admin API 强制重载:
```bash
curl -X POST http://prom-gw-1:8082/v1/rulesets/app-business:reload
```

## 版本管理

每次修改必须递增 `version`:
- YAML 文件:直接编辑
- Admin API:`PUT /v1/rulesets/{name}` body 里指定 `version`,必须严格大于当前

历史版本保留最近 10 版(可配置),可回滚:
```bash
curl -X POST 'http://prom-gw-1:8082/v1/rulesets/app-business:rollback?to_version=1'
```

## 完整示例

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      # 1. 删除内部调试 label
      - type: relabel
        config:
          drop_labels: ["_internal_*", "scrape_id"]
          keep_labels: ["__name__", "instance", "job", "env", "region", "team"]

      # 2. 路由:team 区分 topic
      - type: route
        config:
          rules:
            - match: { team: app }
              topic: prom.app
            - match: { team: infra }
              topic: prom.infra

      # 3. 采样:丢掉一半
      - type: sample
        config:
          rate: 0.5

      # 4. enrich 加 source
      - type: enrich
        config:
          labels:
            gateway_dc: "${source_dc}"
            gateway_tenant: "${tenant}"

      # 5. downsample 1 分钟桶
      - type: downsample
        config:
          interval: 1m
          aggregations: [avg, max, p99]
```
