# 用例:1 分钟下采样

高频指标(每 15s 一次)下采样到 1 分钟桶,只保留 avg / max / p99。

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      # 1. 先做 relabel(只留关注的 label)
      - type: relabel
        config:
          keep_labels: ["__name__", "instance", "job", "env", "region"]

      # 2. 下采样
      - type: downsample
        config:
          interval: 1m
          aggregations: [avg, max, p99]
          max_series: 500000     # 限制状态大小
```

效果:
- 原始每 15s 一次 → 1 分钟桶聚合(avg/max/p99)
- 同一个 series 的 1m 桶只有 3 个 sample → 数据量减少 ~75%
- 状态全内存,重启后从 Prometheus 短期重传补
