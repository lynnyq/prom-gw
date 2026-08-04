# 用例:敏感数据脱敏

部分指标含用户 ID、邮箱等 PII 信息,在 prom-gw 侧脱敏。

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      # 1. 删除含 PII 的 label(精确匹配)
      - type: relabel
        config:
          drop_labels: ["user_id", "email", "phone", "ip_addr"]
          keep_labels: ["__name__", "instance", "job", "env", "region", "team"]

      # 2. 路径里带 user_id 的 metric,整个丢掉
      - type: route
        config:
          rules:
            - match: { __name__: "user_session_count" }
              topic: prom.drop.pii

      # 3. 加标识(静态 label,便于审计)
      - type: enrich
        config:
          labels:
            pii_scrubbed: "true"
```

更严格场景:某些指标直接 drop(用 `sample` 阶段):
```yaml
stages:
  - type: relabel
    config:
      drop_labels: ["user_id", "email", "phone", "ip_addr"]
      # 注:当前 drop_labels 仅支持精确匹配,不支持 glob
```

**注意**:
- prom-gw 只对 label 脱敏;value 里的 PII 不处理(因为 value 是数值,不会含 PII)
- 强烈建议在 **Prometheus 端** 就用 `metric_relabel_configs` 做第一层脱敏,prom-gw 是第二道防线
