// Package auth 定义鉴权抽象接口(预留 IAM 接入扩展点)。
// v1 只实现 LocalTokenAuthenticator(读 tokens.yaml);
// 未来 IAM 接入只需新增 OIDCAuthenticator,receiver.Auth 中间件不变。
package auth
