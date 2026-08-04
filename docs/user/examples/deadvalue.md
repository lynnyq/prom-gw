# 用例:死值丢弃

监控项一直不变(健康检查)时,只在值变化时上报,降低数据量。

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      # 1. 只关注健康检查类指标
      - type: relabel
        config:
          keep_labels: ["__name__", "instance", "job"]
          # 可以再叠一个 relabel 只保留 name 以 health/ping 开头
      - type: relabel
        config:
          drop_labels: ["__name__"]
          keep_labels: []  # 故意
      # 实际更简单的方式:用 sample + deadvalue

      - type: deadvalue
        config:
          window: 5m       # 5 分钟内值不变 → 丢
          max_series: 100000
```

实际场景常见组合:
```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      - type: relabel
        config:
          keep_labels: ["__name__", "instance", "job", "env"]
      - type: route
        config:
          rules:
            # 健康检查类 → topic-prom-health
            - match: { __name__: "up" }     # (实际需要在 Prometheus 端把 __name__ 转为 name)
              topic: prom.health
      # 对 prom.health 走 deadvalue;对其他走默认
      - type: deadvalue
        config:
          window: 5m
```

> ⚠️ 提示:deadvalue 是状态型 stage,重启后状态丢失,**前 5 分钟数据可能重复**。
> 如对重复敏感,先在 Prometheus 端用 `metric_relabel_configs` + `keep` 过滤掉。
