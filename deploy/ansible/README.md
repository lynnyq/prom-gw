# prom-gw Ansible 部署骨架

> 范围:本目录提供最小可用的 Ansible 部署骨架,用于 VM/bare-metal 环境的批量安装/升级/回滚。
> 详细操作见 `docs/operations/deploy.md`。

## 目录结构

```
deploy/ansible/
├── README.md                 # 本文件
├── ansible.cfg               # ansible 运行时配置
├── inventory/
│   ├── production.ini         # 生产环境主机清单(示例)
│   └── group_vars/
│       └── all.yml           # 全部 group 共享变量
├── playbooks/
│   ├── site.yml              # 顶层入口(role 编排)
│   ├── deploy.yml            # 滚动部署
│   ├── rollback.yml          # 一键回滚到上一版本
│   └── healthcheck.yml       # 健康检查 + 就绪探针
└── roles/
    └── prom_gw/
        ├── tasks/main.yml    # 安装/启动/重启
        ├── handlers/main.yml # systemd reload/restart
        ├── templates/
        │   ├── prom-gw.service.j2
        │   ├── prom-gw.env.j2
        │   └── rules.default.yaml.j2
        └── defaults/main.yml # 角色默认变量
```

## 快速开始(占位,需要补齐 inventory 与角色)

```bash
# 1. 编辑 inventory/production.ini 填入目标主机
$EDITOR inventory/production.ini

# 2. 滚动部署
ansible-playbook -i inventory/production.ini playbooks/deploy.yml \
    -e prom_gw_version=v1.0.0

# 3. 健康检查
ansible-playbook -i inventory/production.ini playbooks/healthcheck.yml
```

## 设计约束

- **单机多实例**:本角色支持在同一台机器上起 1..N 个 prom-gw 实例(通过 `prom_gw_instances` 列表配置),端口自动从 `prom_gw_base_port` 递增。
- **零停机**:滚动升级时按 batch 串行,每批次摘流 → 升级 → 健康检查通过 → 重新挂回。
- **回滚**:`prom_gw_version` 变量可指向历史 tag;Ansible 会拉取对应 release 产物。
- **WAL 挂载**:角色会在每次部署前断言 `/data/wal` 独立挂载(防止 fsync 抖动)。
- **鉴权白名单**:默认从 inventory 的 `prom_gw_admin_allow_cidr` 注入 systemd env。

## 当前状态

- `ansible.cfg` / `inventory/` / `playbooks/*.yml` / `roles/prom_gw/` **仅为骨架**,
  实际生产部署仍以 `docs/operations/deploy.md` 中描述的手工脚本为主。
- 真实环境部署前需补齐:清单主机、SSH 凭证、版本号、admin CIDR。
