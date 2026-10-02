# HTTP/WebSocket 隧道协议（tunnelwire）

状态：已实现（v1）

本文描述跑在一条聚合流之上的应用层隧道协议。它不属于 `/netacc/agg/1.0.0` 聚合协议本身：聚合层仍只负责可靠有序字节流；tunnelwire 在字节流内部承载 HTTP 请求或 WebSocket 会话。

## 1. 目标

- `tunnelFetch('/api/x')`：把请求交给 tunnel server 本地 `http.Handler`；
- `tunnelFetch('http(s)://host/path')`：让 server 作为受限 HTTP 代理访问目标；
- `tunnelWs('/ws')`：建立到 server 本地 WS handler 的逻辑 WebSocket；
- `tunnelWs('ws(s)://host/path')`：让 server 作为受限 WS 代理访问目标；
- 一条 HTTP 请求或一条 WS 会话对应一条聚合流；
- 在聚合流没有 `CloseWrite` 的前提下，用显式 `END` 表达请求体/响应体结束。

## 2. 流前缀与帧格式

每条隧道聚合流由 client 先写 5 字节 magic：

```text
"NTUN" || 0x01
```

其中 `0x01` 是协议版本。server 可用该 magic 把隧道协议流与未升级的 raw TCP 聚合流分开。

magic 之后为自描述帧序列：

```text
frame := uvarint(frame_type) uvarint(payload_len) payload
```

`uvarint` 与 Go `binary.Uvarint` / `binary.AppendUvarint` 完全一致。payload 必须一次性完整落在流内；解码端在读取 payload 前校验类型和长度上限。

### 帧类型

| type | 名称 | 方向 | payload |
|---:|---|---|---|
| 1 | `OPEN` | client → server | JSON `OpenMsg` |
| 2 | `RESULT` | server → client | JSON `ResultMsg` |
| 3 | `DATA` | 双向 | HTTP body chunk |
| 4 | `END` | 双向 | 空 payload；本方向 HTTP body 结束 |
| 5 | `ABORT` | 双向 | JSON `AbortMsg` |
| 6 | `WS_MESSAGE` | 双向 | `1 byte opcode || message` |
| 7 | `WS_CLOSE` | 双向 | JSON `CloseMsg` |
| 8 | `WS_PING` | 双向 | ping payload |
| 9 | `WS_PONG` | 双向 | pong payload |

### 大小限制

| 对象 | 上限 |
|---|---:|
| `OPEN` / `RESULT` / `ABORT` / `WS_CLOSE` JSON | 64 KiB |
| `DATA` | 64 KiB |
| `WS_MESSAGE` / `WS_PING` / `WS_PONG` | 4 MiB |
| `OPEN.target` UTF-8 字节数 | 8 KiB |

首版 `WS_MESSAGE` 不分片；消息超过上限应直接失败。END 必须为空 payload。未知帧类型、超长 payload、方向不符或 JSON 非法均为协议违例，应发送 `ABORT` 后关闭流。

## 3. 控制消息

### 3.1 `OpenMsg`

```json
{
  "v": 1,
  "kind": "http",
  "target": "/api/users",
  "method": "POST",
  "host": "api.internal",
  "headers": [["Content-Type", "application/json"]],
  "protocols": []
}
```

字段：

- `v`：协议版本，必须为 `1`；
- `kind`：`"http"` 或 `"ws"`；
- `target`：HTTP 会话为 `/path` 或 `http(s)://...`；WS 会话为 `/path` 或 `ws(s)://...`；fragment（`#...`）不进入线上请求；
- `method`：仅 HTTP，默认 `GET`；
- `host`：逻辑 Host。绝对 URL 默认取 URL host；相对路径时可由调用方指定；
- `headers`：`[name, value]` 二元数组，保序并保留重复字段；`Host` 不重复进 headers；
- `protocols`：仅 WS，期望协商的子协议列表。

server 在分发前必须独立校验 `OPEN`：`target`/`host` 不得含控制字符，`method`、header 名和 WS 子协议必须是 RFC 9110 token，header value 不得含 NUL/CR/LF；client 侧校验不能代替这层防御。

### 3.2 `ResultMsg`

HTTP 响应头：

```json
{
  "kind": "http",
  "status": 200,
  "statusText": "OK",
  "headers": [["Content-Type", "application/json"]]
}
```

WS 握手成功：

```json
{
  "kind": "ws",
  "status": 101,
  "protocol": "chat",
  "headers": []
}
```

WS 或 HTTP 拒绝仍用 `RESULT` 表达，例如：

```json
{"kind":"http","status":403,"statusText":"Forbidden","error":"目标被拒绝"}
```

### 3.3 `AbortMsg` / `CloseMsg`

```json
{"code":"protocol_error","message":"WS_MESSAGE opcode 非法"}
```

```json
{"code":1000,"reason":"bye"}
```

`ABORT.code` 是机器可读短字符串；常见值为 `protocol_error`、`bad_open`、`internal_error`、`client_close`、`upstream_error`、`aborted`。实现应允许新增 code。

## 4. HTTP 会话

```text
client: magic OPEN(http) DATA* END
server:                  RESULT(http) DATA* END
```

- client 发送 OPEN 后开始发送请求体 `DATA` 帧，最后发送 `END`；
- server 可以先读请求体，也可以提前返回响应；`END` 是方向性结束标记，不要求聚合流半关闭；
- server 先回 `RESULT`，再发送响应体 `DATA` 帧，最后发送 `END`；
- `Content-Length` 只是 HTTP 元数据；协议层 body 边界仍以 `END` 为准；
- HTTP 4xx/5xx 是正常 `RESULT`，不是 transport 错误；
- 响应体未读完就放弃时，client 应发 `ABORT` 并关闭流。

## 5. WebSocket 会话

```text
client: magic OPEN(ws)
server:                  RESULT(ws,status=101)
both:                    WS_MESSAGE / WS_PING / WS_PONG / WS_CLOSE
```

- `RESULT.status=101` 表示握手成功；
- 非 101 的 `RESULT` 表示握手拒绝；
- `WS_MESSAGE` payload 首字节为 opcode：`1` text、`2` binary；
- `WS_PING`/`WS_PONG` 映射真实 WebSocket ping/pong；应用读循环应自动回 pong；
- 任一侧发送 `WS_CLOSE` 进入关闭流程；对端通常回显相同 code/reason，然后双方收尾；
- `WS_CLOSE.code` 须为 RFC6455 可线上出现的关闭码（1000-1014 中除保留码外，或 3000-4999）；本端主动发送 API 与浏览器一致，仅允许 `1000` 或 `3000-4999`；`reason` ≤123 UTF-8 字节；
- `ABORT` 表示非正常终止。

## 6. server 分流语义

server 接收到聚合流后先探测 magic：

- 有 magic：按 `OPEN.kind` 分发到 HTTP 或 WS；
- 无 magic：若配置了 `WithUpstream`，按旧行为桥接固定上游；
- 有 magic 但对应 handler/proxy 未配置：返回拒绝 `RESULT`；
- 无 magic 且未配置 upstream：关闭流。

HTTP target 分流：

| target | 处理 |
|---|---|
| `/api/x` | `WithTunnelHTTPHandler` 的 `http.Handler` |
| `http://host/path` / `https://host/path` | `WithTunnelHTTPProxy` 代理 |
| 其它形态 | `400` |

WS target 分流：

| target | 处理 |
|---|---|
| `/ws` | `WithTunnelWSHandler` 的 `TunnelWSHandler` |
| `ws://host/path` / `wss://host/path` | `WithTunnelWSProxy` 代理 |
| 其它形态 | 非 101 `RESULT` 拒绝 |

## 7. 代理安全边界

HTTP/WS 代理默认是**显式 allowlist**：

```go
type TunnelTargetFilter func(
    p peer.ID,
    kind TunnelKind,
    target *url.URL,
) bool
```

`Allow == nil` 或返回 `false` 一律拒绝。应至少校验：

- scheme（HTTP 只允许 `http`/`https`，WS 只允许 `ws`/`wss`）；
- host/port 或完整 origin；
- 发起方 `peer.ID`；
- 是否允许访问内网/loopback 地址。

转发真实 HTTP 上游时会剥除逐跳头：`Connection`、`Keep-Alive`、`Proxy-*`、`TE`、`Trailer`、`Transfer-Encoding`、`Upgrade` 以及 `Connection` 动态点名的字段。

## 8. 收尾语义

聚合流的 `Write`/`write` 只保证字节进入发送缓冲，不代表对端已收到；`Close` 可能丢弃尚未装帧下发的数据。因此协议实现发送最后一个 `END`/`WS_CLOSE`/拒绝 `RESULT` 后采用 graceful close：

```text
等待 pendingBytes == 0 且 ackedBytes >= sentBytes（默认最多 3s）
然后关闭聚合流
```

这保证最后控制帧在进入 FIN 前已被对端聚合层确认。异常路径仍可直接 `ABORT` 或 reset。

## 9. 公开 API 概览

### Go client

```go
resp, err := agg.TunnelFetch(ctx, serverPeer, req, opts...)
conn, err := agg.TunnelWS(ctx, serverPeer, "/ws", headers, protocols, opts...)
```

### Go server

```go
srv, err := tunnel.NewServer(agg,
    tunnel.WithTunnelHTTPHandler(apiHandler),
    tunnel.WithTunnelHTTPProxy(tunnel.HTTPProxyConfig{Allow: allow}),
    tunnel.WithTunnelWSHandler(wsHandler),
    tunnel.WithTunnelWSProxy(tunnel.WSProxyConfig{Allow: allow}),
    // 可选共存 raw TCP：
    tunnel.WithUpstream("127.0.0.1:80"),
)
```

### TypeScript client

```ts
const client = new NetaccClient(node, { tunnelTarget: serverAddr })
const resp = await client.tunnelFetch('/api/users')
const ws = client.tunnelWs('/ws')
```

也可用 `tunnelFetchTo(target, input, init)` / `tunnelWsTo(target, url, init)` 逐调用指定 server。
