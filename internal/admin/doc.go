// Package admin 提供 REST API 供运维操作 ruleset / token / health。
// 监听 :8082,默认仅放行白名单(--admin-allow-cidr),未来切 IAM(mTLS + OIDC token)。
package admin
