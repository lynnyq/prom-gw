# 用例:按团队路由

不同团队的数据落到不同 Kafka topic,方便独立消费。

```yaml
rulesets:
  - name: app-business
    default_topic: prom.app_business
    version: 1
    stages:
      - type: relabel
        config:
          keep_labels: ["__name__", "instance", "job", "team", "env", "region"]
      - type: route
        config:
          rules:
            - match: { team: app }
              topic: prom.team.app
            - match: { team: infra }
              topic: prom.team.infra
            - match: { team: data }
              topic: prom.team.data
            - match: { team: security }
              topic: prom.team.security
```

效果:
- 所有 sample 按 `team` label 路由到 4 个 topic 之一
- 没 team label 的走 `default_topic`
- 下游 4 个消费者独立,互不干扰
