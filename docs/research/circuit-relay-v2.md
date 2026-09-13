# 调研：circuit-relay v2 限额可配置性与改造挂点

关联 issue：#4。调研基于 go-libp2p **v0.49.0**（commit `5c7e6ec`，撰写时最新 release）源码与
[libp2p circuit-relay v2 规范](https://github.com/libp2p/specs/blob/master/relay/circuit-v2.md)（r3, 2023-02-28）。

## 结论速览

| 需求 | 能否纯配置达成 | 依据 |
|---|---|---|
| 解除/调大流量、时长限额 | ✅ 能 | `relay.WithInfiniteLimits()` / `WithLimit` / `WithResources` |
| peerID 白名单 | ✅ 能 | `relay.WithACL(ACLFilter)`，`AllowReserve` + `AllowConnect` |
| 承载持续大流量转发 | ✅ 能（建议调大 `BufferSize`） | `Limit=nil` 后走无界 byte-copy；缓冲默认仅 2048 字节 |
| 逐流公平限速（max-min fairness） | ❌ 不能 | 无限速/整形挂点，须 fork relay 包或自写 hop 协议服务 |
| A→C 段独立选择传输（如 WebSocket） | ✅ 能 | hop/stop 流跑在任意已建安全连接上，两段传输互相独立 |
| 端到端加密 | ✅ 是 | 被中继流由两端再升级安全+多路复用，relay 只见密文 |

**最小改造方案**：中继节点直接 `relay.New(h, relay.WithInfiniteLimits(), relay.WithACL(白名单))`
（不经 `EnableRelayService`，可跳过公网可达性门槛）；公平限速通过 **vendor/fork
`p2p/protocol/circuitv2/relay` 包**（约 750 行、自包含），在转发 copy 循环写路径插入共享限速器，
wire protocol 与客户端均零改动。详见文末「最小改造方案」。

## 限额的可配置性

### 限额是什么、在哪执行

- `Resources.Limit *RelayLimit`：`Duration`（默认 2min）与 `Data`（默认 `1<<17` = 128KiB，**每方向**）
  —— `p2p/protocol/circuitv2/relay/resources.go:8-43, 63-68`。
- 执行完全在 **relay 侧**：握手成功后 `handleConnect` 按 `r.rc.Limit` 分流：
  `Limit != nil` 时给两条流设 `SetDeadline(now+Duration)` 并跑 `relayLimited`
  （`io.LimitReader` 截断 + 达标后 `CloseRead`/`Reset`）；`Limit == nil` 时跑 `relayUnlimited`
  —— `relay.go:473-482, 507-551`。
- relay 把 limit 通过 `HopMessage.limit` / `StopMessage.limit` 告知两端
  （`makeLimitMsg`，relay.go:684-696）；**客户端不执行限额**，只把连接标记为
  `ConnStats.Limited`（dial.go:183-188、handlers.go:71-76）。标记的后果是 swarm 默认
  不在 limited 连接上开新流，除非调用方用 `network.WithAllowLimitedConn(ctx, reason)`
  （`core/network/context.go:97-109`）。**解除限额后 relay 不发送 limit 字段，
  被中继连接在两端表现为普通非 limited 连接**——对本库是必要前提。

### 解除/调大的公开选项

`p2p/protocol/circuitv2/relay/options.go`：

- `WithInfiniteLimits()`（:38）— `rc.Limit = nil`，彻底解除；
- `WithLimit(*RelayLimit)`（:18）— 只调 Duration/Data；
- `WithResources(Resources)`（:10）— 整体替换资源参数：
  `MaxReservations`(128)、`MaxCircuits`(16/端)、`BufferSize`(2048)、`ReservationTTL`(1h)、
  `MaxReservationsPerIP`(8)/`PerASN`(32) 均可调（resources.go:8-34、constraints.go）。
  注意 `BufferSize` 是每方向 copy 缓冲，默认 2KiB 对大流量偏小，建议调到 16–64KiB。

### 启用方式的一个坑

`libp2p.EnableRelayService(opts...)`（options.go:294）并不直接起服务，而是交给
`relaysvc.RelayManager` 监听 `EvtLocalReachabilityChanged`，**仅当判定为公网可达才创建
relay**（`p2p/host/basic/basic_host.go:116-119, 282-290`、`p2p/host/relaysvc/relay.go:65-90`）。
本库的中继节点由应用自有渠道接入、不依赖 AutoNAT 判定，建议**绕过该 manager，直接
`relay.New(h, opts...)`**（relay.go:69，无条件注册 `/libp2p/circuit/relay/0.2.0/hop` handler）。

## relay 侧暴露的扩展点

| 挂点 | 能力边界 |
|---|---|
| `WithACL(ACLFilter)` | 接口仅两方法：`AllowReserve(p, addr)`、`AllowConnect(src, srcAddr, dest)`（acl.go:10-17）。peerID 白名单可实现到「源/目的」粒度；**无带宽、流量维度**。 |
| `WithResources` / `WithLimit` / `WithInfiniteLimits` | 上述资源与限额参数。 |
| `WithReservationAddressFilter` | 过滤写入 reservation voucher 的地址（默认 `manet.IsPublicAddr`，options.go:30）。私网/自建中继场景可能要放开。 |
| `WithMetricsTracer` | `BytesTransferred(n)` 等观测回调（metrics.go），**只读、不能整形**。 |
| stream handler | `New` 内部 `h.SetStreamHandler(proto.ProtoIDv2Hop, r.handleStream)`（relay.go:105）；handler 是私有方法，无注入点。 |
| 转发循环 | `copyWithBuffer`/`relayUnlimited`/`relayLimited` 均为私有（relay.go:507-592）。握手完成后**没有任何拦截两条流或注入限速 writer 的钩子**。 |

go-libp2p 的 ResourceManager 只做资源记账/拒绝（内存、连接数、流数），**不做带宽整形**，
不能拿来实现公平限速。

## 转发模型：逐流裸字节拷贝

- 每条被中继电路 = A→C 的一条 hop 流 `s` + C→B 的一条 stop 流 `bs`，握手完成后
  **每方向一个 goroutine 做 `io.CopyBuffer` 式字节拷贝**（`copyWithBuffer`，relay.go:560-592），
  无包结构、无帧格式、无 per-message 开销。半关闭语义正确传播（`CloseWrite`）。
- 推论 1：公平限速只能改这个 copy 循环（或包一层 writer）。
- 推论 2：多条电路常复用**同一条** A↔C / C↔B 物理连接（流多路复用），OS 层 tc/fq_codel
  只能按连接限速、区分不出单条电路——**逐流公平必须在 relay 进程内做**，外部整形不可行。

## 两段传输各自如何建立

- **A→C 段（源→中继）**：A 侧 `client.dialPeer` 用 `host.NewStream(relay.ID, ProtoIDv2Hop)`
  （dial.go:128），走 A 与中继间任何已建/可拨的连接——TCP、QUIC、WebSocket、WebTransport、
  WebRTC 皆可，**由 A 独立选择**（用什么地址拨 C 就是什么传输）。唯一限制：该连接本身不能是
  `/p2p-circuit` 中继连接（`isRelayAddr` 检查，relay.go:283, 188；v1 式中继套中继被禁）。
- **C→B 段（中继→目的）**：B 先在自己选定的传输上连 C 并 RESERVE（`client.Reserve`，
  reservation.go:63，同样是传输无关的 hop 流）；CONNECT 到来时 relay 在**已有**的 C↔B 连接上开
  `ProtoIDv2Stop` 流，`network.WithNoDial` 明确禁止拨新连接（relay.go:362-364）。
  即此段传输 = B 预留时连 C 用的传输，由 B 选。
- **两段传输互相独立**：A 用 WebSocket 连 C、B 用 QUIC 连 C 是完全合法的组合。
  对聚合层意味着一条中继路径的两段可以分别挑传输，A 侧拥有完全的 A→C 传输选择权。

## 端到端加密

- 规范原文：relay 桥接两条流后，"*B* and *A* upgrade the relayed connection with a security
  protocol and a multiplexer, just like they would e.g. upgrade a TCP connection"（circuit-v2.md）。
- 代码：源端 `dialAndUpgrade` → `upgrader.Upgrade`（client/transport.go:86）产出
  `transport.CapableConn`（Noise/TLS + yamux）；目的端 `Listen` → `UpgradeGatedMaListener`
  （transport.go:104）同样完整升级。relay 拷贝的只是密文。
- **对聚合层呈现为普通端到端加密连接**，可在其上正常开多路复用流，与 CONTEXT.md 的
  「中继路径」定义一致。

## 最小改造方案（建议）

目标能力拆解：

| 能力 | 做法 | 改动量 |
|---|---|---|
| 解除限额 | `relay.WithInfiniteLimits()` | 0 行上游改动 |
| peerID 白名单 | 实现 `ACLFilter`（AllowReserve/AllowConnect 查白名单集合），`relay.WithACL` | ~30 行自有代码 |
| 大流量承载 | `WithResources` 调大 `BufferSize`(≥16KiB)、`MaxCircuits`、`ReservationTTL`；直连 `relay.New` 跳过可达性门槛 | 0 行上游改动 |
| 逐流公平限速 | **fork `p2p/protocol/circuitv2/relay`**，在 `handleConnect` 末尾把 `relayUnlimited` 换成经共享限速器的 copy | fork 内 ~50-100 行 |

具体建议：

1. **不自写协议服务**。relay 包已覆盖 reservation 簿记、voucher 签发、ACL、配额等全部
   协议义务（~750 行），自写 hop handler 需重写这些却换不到额外控制力。
2. **fork 方式用 vendor 复制**（拷贝 `relay/` 目录进本仓库或维护轻 fork）而非 patch 上游：
   包内改动集中在 `relayUnlimited`/`copyWithBuffer` 的写路径——把 `dst.Write` 前经过一个
   按电路维度做 max-min 公平分配的 token bucket（空闲带宽独占、新电路加入时占用大者降速）。
   wire protocol（pbv2）与 client 包完全不动，可继续随上游升级。
3. 客户端（A、B 两侧）**零改动**：client 包对无 limit 的连接走正常 upgrade 路径。
4. 待办风险项：fork 需跟上游 relay 包的安全修复同步；公平限速器本身需要原型验证
   （建议单独立项做原型 ticket）。

## 附：关键源码位置（go-libp2p v0.49.0）

| 主题 | 位置 |
|---|---|
| 限额默认值 | `p2p/protocol/circuitv2/relay/resources.go:63-68` |
| 限额执行/无限转发 | `p2p/protocol/circuitv2/relay/relay.go:473-551` |
| byte-copy 循环 | `p2p/protocol/circuitv2/relay/relay.go:560-592` |
| ACL 接口 | `p2p/protocol/circuitv2/relay/acl.go:10-17` |
| relay 选项 | `p2p/protocol/circuitv2/relay/options.go:10-58` |
| CONNECT 处理（ACL/预留/配额检查） | `p2p/protocol/circuitv2/relay/relay.go:258-341` |
| C→B 禁止拨号 | `p2p/protocol/circuitv2/relay/relay.go:362` |
| A→C 拨号（NewStream，传输无关） | `p2p/protocol/circuitv2/client/dial.go:117-133` |
| 端到端升级 | `p2p/protocol/circuitv2/client/transport.go:77-91, 98-105` |
| 客户端 limit 标记 | `p2p/protocol/circuitv2/client/dial.go:182-188`、`handlers.go:70-76` |
| 可达性门控 | `p2p/host/relaysvc/relay.go:65-90` |
| 协议 ID | `p2p/protocol/circuitv2/proto/protocol.go:4-5` |
