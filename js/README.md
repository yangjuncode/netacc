# netacc（TypeScript / 浏览器客户端）

`netacc` 是 [yangjuncode/netacc](https://github.com/yangjuncode/netacc) 的 TypeScript 实现：基于 js-libp2p 的**流级多路径带宽聚合**客户端，面向浏览器环境，与 Go 端实现线格式互通。

一条 `AggregatedStream` 对应用呈现为可靠有序字节流（`read()`/`write()`/异步迭代/`close()`），数据面自动把字节流条带化到多条底层连接（路径）上，按各路径实测速率调度，并在路径失效时透明重传未确认数据。

本包**不实现任何传输层**：WebTransport / WebSocket / WebRTC / circuit-relay 都由调用方 js-libp2p node 的 transport 配置决定，`NetaccClient` 只通过 `dial`/`newStream`/`handle` 和 multiaddr 消费它们。

## 协议互通

与 Go 端互通的协议地址（multistream protocol id）：

| 协议 | 用途 |
|---|---|
| `/netacc/agg/1.0.0` | 聚合流握手（Hello/HelloAck，握手流升格为首条数据路径） |
| `/netacc/path/1.0.0` | 路径绑定子流（PATH_ATTACH 凭 16 字节 agg_stream_id 挂接） |

帧格式、ACK 语义（cum+SACK+window+seq+ts_echo/ts_path）、path_id 分侧命名空间（发起方偶数、接收方奇数）、防御上限（DATA ≤64KiB、ranges ≤32、控制体 ≤64KiB）与 Go 端 `frame.go`/`handshake.go`/`stream.go` 完全一致——本包的编解码金向量由 Go 端 `proto.Marshal` 实际输出固化在测试里。

## 安装

```bash
npm install netacc libp2p @libp2p/interface @multiformats/multiaddr
# 按需加传输（peerDependency，均为可选）：
npm install @libp2p/websockets        # WebSocket
npm install @libp2p/webtransport      # WebTransport
npm install @libp2p/webrtc            # WebRTC（浏览器直连）
npm install @libp2p/circuit-relay-v2  # 中继电路路径
```

`libp2p`、`@libp2p/interface`、`@multiformats/multiaddr` 是 peer 依赖（要求 `libp2p@^3` 的消息流式 Stream API）。

## 基本用法

```ts
import { createLibp2p } from 'libp2p'
import { websockets } from '@libp2p/websockets'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { NetaccClient } from 'netacc'

const node = await createLibp2p({
  transports: [websockets()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
})
const client = new NetaccClient(node)

// 出向：向 Go 端 multiaddr 开一条聚合流
const st = await client.openStream('/ip4/192.0.2.1/tcp/4001/ws/p2p/12D3KooW…')
await st.write(new TextEncoder().encode('hello'))
for await (const chunk of st) {
  // 保序字节流；对端 close 后迭代正常结束
}
await st.close()

// 入向：accept 逐条接收（或 onStream 回调式）
const incoming = await client.accept()
```

## HTTP/WebSocket 隧道

`NetaccClient` 内置 `tunnelwire` 应用层协议（见仓库 `docs/spec/tunnel-http-ws.md`），可把网页中的 HTTP/WS 请求经聚合流送到 Go `tunnel.Server`：

```ts
const client = new NetaccClient(node, {
  tunnelTarget: '/dns4/tunnel.example.com/tcp/443/wss/p2p/12D3KooW…',
})

// 相对路径：由 server 的 WithTunnelHTTPHandler 处理。
const res = await client.tunnelFetch('/api/users')

// 绝对 http/https URL：由 server 的 HTTP proxy 代访问（默认须命中 allowlist）。
const upstream = await client.tunnelFetch('https://api.internal/users')

// 相对路径：由 server 的 WithTunnelWSHandler 处理；ws/wss URL 走 WS proxy。
const ws = client.tunnelWs('/ws', { protocols: ['chat'] })
await ws.opened
ws.send('hello')
ws.onmessage = ev => console.log(ev.data)
ws.close(1000)
```

也可以逐请求指定 server：

```ts
await client.tunnelFetchTo(serverAddr, '/api/users')
const ws = client.tunnelWsTo(serverAddr, 'wss://api.internal/events')
```

`tunnelFetch` 支持 `method`/`headers`/`body`/`signal`/`openOptions`；body 可以是字符串、字节、Blob、URLSearchParams 或 `ReadableStream<Uint8Array>`。HTTP 4xx/5xx 返回正常 `Response`，协议错误和对端 ABORT 才 reject。

`TunnelWebSocket` 是 WebSocket-like `EventTarget`：`readyState`、`send()`、`close()`、`onopen`/`onmessage`/`onerror`/`onclose`、`binaryType`、`protocol`、`bufferedAmount`；`opened` Promise 用来等待握手成功。它不是浏览器原生 `WebSocket`，相对路径与 `ws(s)://` 代理由 tunnel server 解释。

## js-libp2p 传输配置

聚合路径的传输能力 = node 的 transport 配置。`addPath(addr)`/`openStream(addr)` 直接吃对应的 multiaddr 形态：

### WebSocket（`/tcp/…/ws` 或 `/wss`）

```ts
import { websockets } from '@libp2p/websockets'
const node = await createLibp2p({
  transports: [websockets()],
  // …加密器/流复用器同上
})
```

地址形如 `/dns4/host/tcp/443/wss/p2p/<peerID>`。生产上一般用 `wss`（TLS）；浏览器只能拨 `wss` 至有效公开证书。

### WebTransport（`/quic-v1/webtransport`）

```ts
import { webtransport } from '@libp2p/webtransport'
const node = await createLibp2p({ transports: [webtransport()], /* … */ })
```

地址形如 `/ip4/…/udp/…/quic-v1/webtransport/certhash/<…>/certhash/<…>/p2p/<peerID>`。

**证书哈希**：浏览器要求 WebTransport 服务端用「自签名 ≤14 天证书 + `/certhash/` 组件」或公开 CA 证书。Go 侧 listen 地址里要带上由 `libp2p` 生成的 certhash（`getEmbeddedCertHashes`/等价物），浏览器端拨的 multiaddr 必须包含两个 certhash 组件，否则 WebTransport 握手会被浏览器拒绝。

### WebRTC（`/webrtc` 浏览器直连）

```ts
import { webRTC } from '@libp2p/webrtc'
const node = await createLibp2p({
  transports: [webRTC()],
  services: { identify: /* … */ },   // WebRTC 信令依赖 identify
})
```

地址形如 `/…/webrtc/p2p/<peerID>`（或 `/webrtc-direct`）。注意 WebRTC 拨号需要私有的 SDP 信令通道（js-libp2p 通过 `/p2p/<peer>/webrtc` 协议向 Go 侧 listener 换取 offer/answer），Go 侧需监听 `/webrtc-direct` 地址。

### circuit-relay 中继路径

`addPath`/`addRelayPath` 接受 `/p2p-circuit` 电路地址（如 `/ip4/relay/tcp/4001/p2p/<relayPeer>/p2p-circuit/p2p/<destPeer>`）。要求：

- 本端 node 已配 `@libp2p/circuit-relay-v2` transport；
- 对端（Go 侧）在该 relay 上有有效 reservation。

`AggregatedStream.addRelayPath({peerId, addrs})` 实现了规格 §4.3 的按需协调：向对端发 `PATH_REQUEST` → 等对端 reservation 就绪回 `PATH_READY` → 自动拼电路地址拨 CONNECT。

**当前限制**：本端作为 PATH_REQUEST *响应方*做 reservation 时，js-libp2p 没有公开的手动 reservation API——`NetaccClient` 通过 `reserveRelay` 配置钩子把 `{peerId, addrs}` 交给应用侧完成（例如调用自建 relay 协调通道）；未配置时收到 PATH_REQUEST 会回错误应答。直拨电路地址不受影响。

## 聚合流 API

```ts
class AggregatedStream {
  read(maxBytes?): Promise<Uint8Array | null>   // null = 对端正常关闭（EOF）
  readAll(): Promise<Uint8Array>
  [Symbol.asyncIterator]()                       // for await 迭代
  write(data: Uint8Array): Promise<void>         // 进入发送缓冲即 resolve（非对端确认）
  close(): Promise<void>                         // 全路径发 FIN → 关闭底层
  addPath(addr): Promise<number>                 // 直拨补挂路径，返回 path_id
  addRelayPath(relay, opts?): Promise<number>    // PATH_REQUEST 协调 + 中继路径
  removePath(pathId): void                       // 摘除路径（拒绝最后一条）
  paths(): PathInfo[]                            // 存活路径观测快照
  stats(): StreamStats                           // 聚合指标快照
  subscribe(maxQueue?): { events, unsubscribe }  // path_added/removed/degraded 事件
}
```

```ts
class NetaccClient {
  constructor(node: Libp2p, options?: NetaccClientOptions)
  openStream(target, options?): Promise<AggregatedStream>
  accept(options?): Promise<AggregatedStream>
  onStream(handler): () => void
  tunnelFetch(input, init?): Promise<Response>
  tunnelFetchTo(target, input, init?): Promise<Response>
  tunnelWs(url, init?): TunnelWebSocket
  tunnelWsTo(target, url, init?): TunnelWebSocket
  close(): Promise<void>
}
```

`target` 支持 `PeerId`、`Multiaddr`、`Multiaddr[]`、字符串（`/…` 按 multiaddr 解析，否则按 peerID）。`write()` 返回的是「已进发送缓冲」语义；真正的对端确认进度看 `stats().ackedBytes`。

## 浏览器安全上下文与限制

- **安全上下文**：WebRTC/WebTransport 在浏览器里要求 `https:`/`localhost` 等安全上下文；普通 HTTP 页面只有 WebSocket 可用。
- **WebTransport 证书**：浏览器对自签名证书有 14 天有效期上限，且 multiaddr 必须带 `/certhash/`（见上）。Go 侧证书轮换时客户端拿到的旧 certhash 会失效。
- **浏览器不能监听 TCP/UDP**：入向连通依赖 WebSocket（wss）/WebRTC/中继；浏览器节点通常只能做发起方（openStream/addPath），对 Go 侧 addPath 反向补挂进浏览器不可行（除非经 circuit-relay）。
- **relay 带宽**：`/p2p-circuit` 是限额连接（js-libp2p 的 `runOnLimitedConnection`，本包默认开启以兼容中继路径），吞吐受 relay reservation 上限约束。

## 与 Go 端的差异 / 未实现项

- **传输层不内嵌**：Go 侧直接持有 TCP/QUIC socket；本包完全依赖 node 的 transport 配置，地址可达性以 js-libp2p 为准。
- **`addRelayPath` 响应端 reservation 需 `reserveRelay` 钩子**（js-libp2p 无公开 reservation API）。
- **无自动路径策略层**：Go 端有基于需求的自动加/减路径策略；本包首版只暴露手动 `addPath`/`removePath`，调度器内部固定为最短排空时间 + inflight 硬顶。
- **无 Go 侧 `Policy`/`CreatePath`-style 高阶描述符**：只认 multiaddr / `{peerId, addrs}`。
- **peerStore 地址发现**：`openStream({minPaths:n})` 依赖调用方 node 的 peerStore 里有对端可达地址（identify/手动喂入），否则补挂失败。
- **`setDeadline`/`ReadDeadline` 等 net.Conn 式超时**未暴露：浏览器侧请用 `AbortSignal`（`openStream`/`accept`/`addPath` 的 `signal` 选项）或上层超时包裹。

## 开发

```bash
npm install
npm run typecheck   # tsc --noEmit（含 test/）
npm run build       # 产出 dist/
npm test            # vitest：编解码金向量 + 重排 + 内存管道数据面 + 真实 libp2p TCP 双节点集成
```

测试里 `test/client.test.ts` 起两个真实 js-libp2p 节点（TCP loopback + Noise + Yamux）跑完整握手/echo/路径增删/断连终态；`test/frame.test.ts` 与 `test/proto.test.ts` 的输入字节全部由 Go 端实际编码输出固化。`test/tunnel-interop.test.ts` 会构建并启动 `js/e2e/tunnel-server`，验证 `tunnelFetch`/`tunnelWs` 与真实 Go tunnel server 的端到端互通。
