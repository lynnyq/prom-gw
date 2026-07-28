// Package config 集中管理所有配置:
//   - tokens.yaml 启动加载 + SIGHUP 重载
//   - rules/*.yaml fsnotify 监听 + 编译 + 原子切换
//   - Nacos 长轮询(Phase 4 接入)
//   - 启动参数 /etc/prom-gw/config.yaml
package config
