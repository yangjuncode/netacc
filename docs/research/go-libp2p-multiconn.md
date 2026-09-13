# 调研：go-libp2p 同一对等点上的多连接与指定连接开流能力

> 对应 issue：#2
> 调研版本：**go-libp2p v0.49.0**（tag `v0.49.0`，commit `5c7e6ec`，`go 1.25.7`）。
> 下文所有 `file:line` 均指该版本源码树（<https://github.com/libp2p/go-libp2p/tree/v0.49.0>）。

## TL;DR（结论先行）

- **swarm 允许同一 peer 并存多条连接**：`conns.m[peer]` 是连接切片，`addConn` 不做任何去重（`p2p/net/swarm/swarm.go:420`）。不同传输各一条、同一传输多条都天然支持。
- **但公开拨号 API 不能「按需」建立第二条连接**：`dialPeer`/`dialWorker` 在拨号前都会先调 `bestAcceptableConnToPeer`，已有可接受连接时直接返回旧连接，不再发起新拨号（`swarm_dial.go:245-249`、`dial_worker.go:174-178`）。`WithForceDirectDial` 只在「现有连接是 relayed/proxied」时强制重拨，对「已有直连、再要一条直连」无效（`swarm.go:648-663`）。
- **可以在指定连接上开流**：`network.Conn.NewStream(ctx)` 是公开 API（`core/network/conn.go:81-82`，实现于 `p2p/net/swarm/swarm_conn.go:212`）。配合 `Network.ConnsToPeer(p)`（`core/network/network.go:200`）枚举该 peer 的全部连接、逐条开流，**「每条连接 = 一条路径」成立**。
- **不 fork 上游、仅用公开 API 实现多路径直连聚合：可行**，两条路线：
  1. **swarm 托管连接（干净但连接数受限）**：靠对端配合（双方互拨、对端暴露多传输/多监听地址）自然形成多条 `network.Conn`，全部进 `ConnsToPeer`，`conn.NewStream` 逐条开流。
  2. **传输层直拨（确定性、任意 N 条）**：`host.Network().(*swarm.Swarm).TransportForDialing(addr).Dial(ctx, addr, peerID)` 得到 `transport.CapableConn`，`OpenStream` + `multistream.SelectProtoOrFail` 协商协议。连接不进 swarm 连接表（无 identify/notifiee/Conn 包装），但对端按正常 inbound 流程收流并走 `SetStreamHandler` 分发。
  - 推荐混合：swarm 连接 + 额外 transport 直拨连接，全部抽象为「路径」。

---

## 1. 同一 peer 能否保持多条并行连接？

**能。** swarm 的连接表就是 `map[peer.ID][]*Conn`：

- `s.conns.m[p] = append(s.conns.m[p], c)` — `swarm.go:420`，`addConn`（`swarm.go:367-450`）**无去重**，每完成一次传输层握手（无论出向还是入向）就追加一条。
- `ConnsToPeer(p)` 返回该 peer 的全部存活连接（`swarm.go:581-593`）。
- 官方测试佐证：`TestSimultOpen`（`p2p/net/swarm/dial_test.go:120-166`）让两个 swarm 互相拨号 10 次，最终每端「最多 2 条连接」（一条出向 + 一条入向）——多连接并存是被测试保证的行为。

**自然产生多连接的场景**：

| 场景 | 说明 |
|---|---|
| 同时拨号（simultaneous open） | 双方都 `DialPeer` 对方 → 各得一条出向 + 一条入向，共 2 条 |
| 对端用多个传输/地址拨入 | 每个入向连接单独 `addConn` |
| 单次拨号中多个地址竞速完成 | dialWorker 会给 peer 的每个唯一地址各拨一次（`trackedDials`，`dial_worker.go:80-82`）；首个连接成功后剩余拨号是否继续取决于取消时序（见 §3），**不可靠** |
| relayed → direct 升级 | `WithForceDirectDial` 场景：relay 连接保留 + 新直连连接 |

## 2. 拨号去重：为什么不能简单地「再 Connect 一次」

`Swarm.DialPeer → dialPeer`（`swarm_dial.go:221-285`）：

```go
// swarm_dial.go:245-249
conn := s.bestAcceptableConnToPeer(ctx, p)
if conn != nil {
    return conn, nil   // 已有可接受连接 → 直接返回，不拨号
}
...
conn, err = s.dsync.Dial(ctx, p)
```

即使绕过这层（比如并发调用），`dialWorker.loop` 收到每个拨号请求时**再查一次**（`dial_worker.go:174-178`），有可接受连接同样直接响应旧连接。`dialSync` 还保证同一 peer 同时只有一个拨号流程（`dial_sync.go:26-32`）。

`bestAcceptableConnToPeer`（`swarm.go:648-663`）只在 `WithForceDirectDial` 且现有连接是 **proxied**（`conn.Transport().Proxy() == true`，即 relay）时才视为「不可接受」从而重拨。**已有直连连接时，没有任何公开 ctx 选项能逼它再拨一条。**

另外注意 `dialSync.Dial` 的收尾（`dial_sync.go:100-112`）：最后一个 `Dial` 调用返回时 `cancelCause(errConcurrentDialSuccessful)` 取消拨号上下文并关闭 `reqch`，**会中止仍在排队的其它地址拨号**——所以不能指望「给 peerstore 塞多个地址、一次 Connect 就必然拿到多条连接」；那是竞态，不是契约。

## 3. swarm 建流的连接选择策略

`Swarm.NewStream`（`swarm.go:475-528`）每次建流都调 `bestConnToPeer`（`swarm.go:627-646`）+ `isBetterConn`（`swarm.go:595-625`），优先级：

1. **非 limited 优先**（relayed 连接排最后）；
2. **直连优先**（`!conn.Transport().Proxy()`，relay/proxy 连接次之）；
3. **已开流数多者优先**（往「热」连接上堆流）；
4. 其余取最新一条连接。

即 `host.NewStream` / `swarm.NewStream` **永远只选一条「最佳」连接**，没有公开钩子改变选路（`WithNoDial`、`WithAllowLimitedConn`、`WithForceDirectDial` 只控制「是否拨号/等直连」）。要做条带化必须绕过它。

## 4. 在指定连接上开流：`network.Conn.NewStream()`

**可行，这是本库的核心挂点。**

```go
for _, c := range host.Network().ConnsToPeer(peerID) {   // core/network/network.go:200
    s, err := c.NewStream(ctx)                            // core/network/conn.go:81-82
    // s.Conn() == c  (swarm_stream.go:52)，接收端同样可用 stream.Conn() 反查归属路径
    s.SetProtocol(protoID)                                // network.Stream 支持 SetProtocol
}
```

- 实现：`(*swarm.Conn).NewStream` → `c.conn.OpenStream`（`swarm_conn.go:212-247`），走资源管理器计账（`ResourceManager().OpenStream`）。
- 唯一限制：`Stat().Limited == true`（circuit-relay 连接）时需 `network.WithAllowLimitedConn(ctx, ...)`（`swarm_conn.go:213-217`）。
- `network.Conn` 上可用于「路径」管理的公开属性：`ID()`（本进程内唯一，`swarm_conn.go:51-54`）、`Local/RemoteMultiaddr()`、`ConnState().Transport`（`"tcp"`/`"quic-v1"` 等，`conn.go:106-116`）、`Stat().NumStreams`、`GetStreams()`、`IsClosed()`。
- 接收端：对端 `SetStreamHandler` 收到的 `network.Stream.Conn()` 可区分流来自哪条连接 → 收端按路径重组。

## 5. 拨号时如何强制走某个传输

| 手段 | 位置 | 说明 |
|---|---|---|
| **peerstore 地址过滤** | `peerstore.ClearAddrs(p)` + `AddAddr(p, quicAddr, ttl)` | 最直接：`addrsForDial`（`swarm_dial.go:293-315`）只从 peerstore 取地址。只留 `/quic-v1` 地址 → 只会走 QUIC |
| **ConnectionGater** | `InterceptAddrDial(p, addr)`（`swarm_dial.go:549-555`） | `libp2p.ConnectionGater(...)` 选项，按地址放行/拒绝 → 可程序化地禁掉某传输 |
| **`WithForceDirectDial`** | `network.WithForceDirectDial`（`core/network/context.go:28`） | 只是**过滤掉 proxy/relay 地址**（`swarm_dial.go:304-306`），不能在 TCP/QUIC 之间选择 |
| **自定义 DialRanker** | `swarm.WithDialRanker`（`swarm.go:107-113`）/ `libp2p.SwarmOpts(...)` | 控制地址拨号顺序与延迟（默认 `DefaultDialRanker`，`dial_ranker.go:81`：QUIC > WebTransport > TCP > WebRTC，公网 TCP 比 QUIC 晚 250ms）。可用于让多地址同时开拨 |
| **传输层直拨** | `(*swarm.Swarm).TransportForDialing(addr)`（`swarm_transport.go:15`） | 拿到 `transport.Transport` 后 `Dial(ctx, addr, pid)` —— 最精确的「指定传输」，见 §6 |

`host.Network()` 的底层就是 `*swarm.Swarm`（`config/config.go:219` `swarm.NewSwarm` 是唯一实现，注释「TODO: Make the swarm implementation configurable」），类型断言 `host.Network().(*swarm.Swarm)` 对默认构造的 host 成立。

## 6. 如何（在公开 API 内）拿到 N 条连接

### 6a. swarm 托管连接 —— 受「去重」限制

- 一次 `Connect` 最多产出一条「按需」连接；想有第二条只能靠：
  - **双方互拨**：A、B 各自 `DialPeer` 对方 → 每人看到 2 条 `network.Conn`（出向+入向），可以是不同传输/地址（见 `TestSimultOpen`）。✅ 确定性，需对端也是本库节点。
  - 对端监听多个地址 + 竞态多拨：❌ 不可靠（§2 的取消逻辑）。
- 这些连接进 `ConnsToPeer`、有 identify、有 notifiee、走资源管理器——**最干净，推荐优先消耗**。

### 6b. 传输层直拨 —— 确定性 N 条（推荐兜底）

```go
sw := host.Network().(*swarm.Swarm)
tpt := sw.TransportForDialing(addr)                  // 由 multiaddr 选定传输（tcp/quic-v1/ws/...）
cc, err := tpt.Dial(ctx, addr, peerID)               // transport.CapableConn：已完成安全+多路复用握手
ms, err := cc.OpenStream(ctx)                        // network.MuxedConn.OpenStream，core/network/mux.go:136
err = msmux.SelectProtoOrFail(protoID, ms)           // github.com/multiformats/go-multistream，与 identify 同款
                                                   // （p2p/protocol/identify/id.go:440 附近 newStreamAndNegotiate）
```

- 同一 peer、同一传输可重复 `Dial` 出多条（TCP 多个 socket / QUIC 多条 connection），**数量只受本库策略与资源限制约束**。
- 代价与注意：
  - 连接**不进** `s.conns.m`：`ConnsToPeer`/`Conns` 查不到、无 `Connected`/`Disconnected` 通知、无 identify、不受 connmgr 管理；要自建生命周期与健康检查。
  - 资源管理仍部分生效：TCP 传输的 `Dial` 内部 `rcmgr.OpenConnection`（`p2p/transport/tcp/tcp.go:255`）。
  - 流是 `MuxedStream`（无 `SetProtocol`/`Stat`/`Scope`），协议协商用 multistream-select 手动做（`msmux` 是独立公开库）。对端无感知：它的 listener 照常 `addConn`、accept stream、`SetStreamHandler` 分发。
  - 对端看到的 inbound 连接数会增加——relay/NAT 场景的对端资源限制（rcmgr peer 连接数上限）要留意。

### 6c. 不推荐的方向

- fork/patch swarm 去掉 `bestAcceptableConnToPeer` 短路：**不必要**（本调研的明确答案）。
- `WithSimultaneousConnect`：是给打洞用的（`dial_worker.go:192` 换 `NoDelayDialRanker`），不能绕过已连接短路。
- 另起第二个 `swarm.NewSwarm`：listener/identify/peerstore 全部重复，过重且无必要。

## 7. identify 协议与 multiaddr 宣告在多连接下的行为

- **每条新连接各跑一次 identify**：`netNotifiee.Connected` → `addConnWithLock(c)` + `IdentifyWait(c)`（`p2p/protocol/identify/id.go:1031-1040`）。即每条连接建立后触发一轮 `/ipfs/id/1.0.0`；可手动 `ids.IdentifyConn(c)`（`id.go:374`）。
- **ObservedAddr 是逐连接的**：identify 响应里的 observed addr 由「该连接的对端地址」算出（`id.go:649` 起），多条连接会得到各自的 observed 地址（NAT 下可能不同）。
- **宣告地址过滤按连接生效**：`filterAddrs`（`id.go:1074+`）根据该连接的 remote addr 类型过滤我们宣告的监听地址（对端 loopback → 全发；private → 滤掉 loopback；public → 只发 public）。多条不同传输/网段的连接上，对端收到的宣告集合可能不同，但都会并入同一个 peerstore 条目（last-writer-wins 于签名记录，地址按 TTL 合并）。
- **断开清理也是逐连接的**：`Disconnected`（`id.go:1043+`）只在「该 peer 完全断开」（`Connectedness` 不再是 Connected/Limited）时才降级地址 TTL——**多条连接中只断一条不会污染 peerstore**，对我们有利。
- 结论：多连接下 identify 行为安全，唯一注意是 N 条连接带来 N 次 identify 握手开销（每次建连接一次性成本），以及 6b 的裸 transport 连接在我们这一侧没有 identify（对端仍会向我们发 identify 流——由它自己的 accept 循环处理，无妨）。

## 8. 其它工程注意点

- **connmgr 修剪**：连接管理器会按 peer 评分/tag 关连接（`core/connmgr`）。聚合用的 peer 建议 `host.ConnManager().Protect(p, "netacc")` 防止顺手被剪。
- **`conn.NewStream` 与 relayed 连接**：路径若是 circuit-relay 连接，开流必须带 `network.WithAllowLimitedConn`（否则 `ErrLimitedConn`）。
- **流归属**：接收端用 `stream.Conn().ID()`/`RemoteMultiaddr()` 给每条「路径」打标；`Conn.ID()` 仅进程内有效，不能当握手标识用（路径 ID 需在聚合协议里自己协商）。
- **连接断开通知**：`network.NotifyBundle` 的 `Disconnected` 按连接触发——一条路径断了其余路径不受影响，`Connectedness` 仍 Connected，正好符合「路径可动态增删」的模型。

## 9. 最终结论

| 问题 | 结论 |
|---|---|
| 同一 peer 多条并行连接？ | ✅ 支持且常见（swarm 不去重；互拨/多传输入向都会产生） |
| swarm 建流选路策略？ | `bestConnToPeer`：非 limited > 直连 > 流多 > 最新，**恒选一条**；无公开选路钩子 |
| `conn.NewStream()` 指定连接开流？ | ✅ 公开 API，`ConnsToPeer` + `conn.NewStream` = 每条连接一条路径 |
| 拨号强制走某传输？ | peerstore 过滤 / ConnectionGater / 自定义 DialRanker / `TransportForDialing` 直拨 |
| 「不 fork 上游能否实现多路径直连聚合」？ | **能**。swarm 连接（互拨+多传输，最多受自然形成数限制）+ `Transport.Dial` 直拨补充任意数量路径，`conn.NewStream`/`CapableConn.OpenStream` 逐路径开流，全程公开 API |

## 附：参考位置速查（go-libp2p v0.49.0）

| 主题 | 文件：行 |
|---|---|
| `network.Conn.NewStream` 接口 | `core/network/conn.go:81-82` |
| `(*swarm.Conn).NewStream` 实现 | `p2p/net/swarm/swarm_conn.go:212-247` |
| `Network.ConnsToPeer` | `core/network/network.go:200`；实现 `swarm.go:581-593` |
| `Swarm.NewStream` 选路 | `p2p/net/swarm/swarm.go:475-528` |
| `bestConnToPeer` / `isBetterConn` | `swarm.go:595-646` |
| `bestAcceptableConnToPeer` | `swarm.go:648-663` |
| `addConn`（无去重） | `swarm.go:367-450`（append 在 420） |
| `dialPeer` 已连接短路 | `swarm_dial.go:245-249` |
| `dialWorker` 已连接短路 | `dial_worker.go:174-178` |
| 拨号收尾取消剩余地址 | `dial_sync.go:100-112` |
| `addrsForDial` / forceDirect 过滤 | `swarm_dial.go:293-315` |
| `DefaultDialRanker` 排序规则 | `dial_ranker.go:36-106`（QUIC 优先，TCP +250ms 公网） |
| `TransportForDialing` | `swarm_transport.go:15` |
| `transport.Transport.Dial` / `CapableConn` | `core/transport/transport.go`（`Dial` 在 ~50 行） |
| `MuxedConn.OpenStream` | `core/network/mux.go:136` |
| ctx 选项 | `core/network/context.go`（`WithNoDial`/`WithForceDirectDial`/`WithAllowLimitedConn`/`WithSimultaneousConnect`） |
| identify 每连接触发 | `p2p/protocol/identify/id.go:1031-1040`、`374-448` |
| `stream.Conn()` | `p2p/net/swarm/swarm_stream.go:52` |
| 多连接行为测试 | `p2p/net/swarm/dial_test.go` `TestSimultOpen` |
