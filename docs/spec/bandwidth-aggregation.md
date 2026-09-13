# 设计规格：libp2p 流级带宽聚合库

> 状态：v0 草案（wayfinder 地图 #1 的交付物，汇总决策票 #8–#12 与原型票 #13、#15）
> 术语：遵循根目录 `CONTEXT.md`（聚合流 / 路径 / 直连路径 / 中继路径 / 调度器 / 重排缓冲 / 公平带宽分配 / 路径描述符）
> 基线：go-libp2p **v0.49.0**（quic-go v0.60.0、go-yamux v5.1.0、circuit-relay v2 规范 r3）

## 1. 目标与非目标

**目标**：把一个 libp2p 对等点之间的多条网络路径叠加成一条高带宽逻辑流。两类路径来源：

- **直连路径**：同一对端间的多条并行连接，跨 TCP / QUIC / WebSocket / WebTransport / WebRTC 多传输叠加。动机：运营商常对 UDP 整类 QoS 限速，TCP 系（尤其 WSS/443）可绕开；UDP 未受限时 QUIC 又提供高带宽机会型路径。
- **中继路径**：经中继节点转发的路径（A→C→B、A→D→B 等），直连带宽不足时借道中继分摊流量。

**非目标**：中继发现机制（应用通过自己的渠道获知中继信息，库只接受输入）；swarm 级透明聚合（本版为显式 API）；IP/tun 层隧道；与原生 libp2p 流互通（聚合流为私协议，只要求两端都跑本库）。

**既定前提**：路径可动态增删；双方向对称聚合；同一传输开多条连接也算多条路径；NAT 穿透交给 libp2p 现有能力（AutoNAT/DCUtR），聚合层只消费已建立的连接。

## 2. 总体架构

```
应用
  │  net.Conn 语义
  ▼
Aggregator（本库公开面：OpenStream / Accept / Stats / 事件）
  ├── 聚合流 = { 控制面（握手流，兼首条数据路径）, 数据路径集合 }
  │       ├── 调度器：帧 → 选路径（最短排空时间优先）
  │       ├── 重排缓冲 + 连接级接收窗口（收端）
  │       └── 路径管理器：自动策略（可插拔）+ 手动 AddPath/RemovePath
  └── 路径 = (底层连接, 其子流)
        ├── 直连路径：swarm 托管连接 / transport 层直拨连接
        └── 中继路径：circuit-relay v2 端到端加密电路

中继节点（独立部署，运行本库中继组件）
  └── 原生 relay.New + transport 装饰器（按 peer 公平限速）+ peerID 白名单
```

关键边界：**聚合层对路径来源无感知**。无论路径底层是 TCP 直连还是三跳中继，对调度器都是「一条有 est_rate/srtt/inflight 指标的已认证子流」。

## 3. 路径模型与连接建立（直连）

每条路径 = 一条底层连接 + 其上一条子流。

### 3.1 连接来源（调研 #2 结论）

| 来源 | 机制 | 说明 |
|---|---|---|
| swarm 托管 | `Connect` + `ConnsToPeer` | 互拨/多传输自然形成；有 identify、notifiee、connmgr 管理；受拨号去重限制（已有可接受连接时公开 API 不再拨新连接） |
| transport 直拨 | `sw.TransportForDialing(addr).Dial(ctx, addr, peerID)` → `CapableConn` | 确定性建任意数量路径；不进 swarm 连接表，生命周期由本库自理 |

推荐混合：优先消耗 swarm 托管连接，不足部分用 transport 直拨补齐。

### 3.2 指定连接开流

- swarm 连接：`conn.NewStream(ctx)`；直拨连接：`cc.OpenStream(ctx)`。
- **两种都必须手动跑 `msmux.SelectProtoOrFail(protoID, stream)`**——`conn.NewStream` 不做 multistream 协商（原型 #13 实测：不协商则对端 reset 0x1001）。
- relayed（limited）连接开流需 `network.WithAllowLimitedConn(ctx)`；本库中继解除限额后连接不标记 limited，无此需求。
- 接收端用 `stream.Conn()` 反查路径归属；`Conn.ID()` 仅进程内有效，协议层标识用 path_id。
- 聚合用 peer 应 `ConnManager().Protect(p, "netacc")` 防 connmgr 修剪。

### 3.3 传输优先级（调研 #3 结论）

| 优先级 | 传输 | 定位 |
|---|---|---|
| P0 | TCP | UDP QoS 免疫基线 |
| P0 | WebSocket（wss/443） | HTTPS 外观，穿透/伪装最强 |
| P1 | QUIC | 未受限时性能最优的机会型路径 |
| P2 | WebTransport | 仅浏览器无 CA 证书场景（与 QUIC 同走 UDP，go↔go 无增量） |
| P3 | WebRTC-direct | 仅浏览器场景；go-libp2p 未实现 `/webrtc`（browser↔browser） |

QUIC/WebTransport/WebRTC 同为 UDP，UDP 整类限速下同生共死——路径降权靠调度器按实测带宽自动完成，不做协议级规避切换的假设。

## 4. 聚合流协议（决策 #8、#11）

### 4.1 建立与控制面

- 发起方在任意已有连接上开握手流，protocol ID `/netacc/agg/1.0.0`，protobuf 消息协商聚合参数，生成 **128bit 随机 `agg_stream_id`**。
- **分离+复用混合**：握手流专职控制面，可同时兼作第一条数据路径。
- 建立成功 = 至少一条数据路径 attached。
- 控制消息（PATH_ADD/PATH_DROP/PATH_REQUEST/PATH_READY/遥测/心跳）为帧类型之一，**带内复用任意存活路径**，不绑定特定连接。
- 可达地址不在握手内自带，依赖 identify/peerstore。

### 4.2 路径认证

连接级认证 + ID 绑定：每条底层连接已完成 Noise/TLS 握手认证到 peerID（中继路径同样端到端加密，中继只见密文）；新路径流的 `PATH_ATTACH` 携带 `agg_stream_id` 即完成绑定，无逐路径签名。帧格式预留 MAC 字段位作未来加固扩展点。

### 4.3 路径管理

- **双向对称加路径**：任一侧可提议并建立（覆盖 NAT 后只能出向的一端）。
- `path_id` 由加路径方自带，分侧命名空间保证唯一；死亡路径重加用新 path_id。
- **中继路径按需协调**：`PATH_REQUEST(经中继C)` → 对端向 C 做 reservation → `PATH_READY` → 发起端 CONNECT。平时不占中继配额；常驻 reservation 清单为扩展项。
- 路径描述符（新术语，见 CONTEXT.md）：直连路径含传输偏好与目标 multiaddr；中继路径含中继 peerID 与逐跳传输偏好（A→C 段可选 WebSocket 绕 QoS；C→B 段为对端 reservation 所用传输）。

### 4.4 数据面帧结构

- **字节级全局序号**：帧头 = 载荷的流内字节偏移 + 长度；重组/重传/跨路径重分片统一为字节区间操作。
- **数据帧头自定义二进制**（varint 字段），控制消息 protobuf。
- path_id 不进数据帧头（子流自带归属）。
- 帧类型（明细进实现）：DATA / ACK / PATH_ATTACH / PATH_DROP / PATH_REQUEST / PATH_READY / PING / TELEMETRY / FIN / RST。
- **ACK**：累积字节偏移 + 乱序区间（SACK 式 ranges）+ 时间戳回显 + 接收窗口通告；优先走低延迟路径回送。

## 5. 调度器（决策 #9）

### 5.1 算法：最短排空时间优先

- 每帧选 `(queued_bytes + len) / est_rate` 最小的路径——与 Linux MPTCP 现行默认（`mptcp_subflow_get_send`）同构，等价于按实测带宽加权条带，自带 BLEST 式 HoL 规避。
- **每路径在途量上限**：`inflight_cap_i ≈ k · est_rate_i · srtt_i`（k 略大于 1），慢路径堆积有硬顶。
- **机会重发 + 惩罚**兜底：快路径有空但收端窗口被慢路径在途数据顶住时，复制队头未确认帧到快路径重发，并下调慢路径权重/cap。
- ECF 式等待判定（等快路径更早完成则不发慢路径）为后续增强项，不进首版。

### 5.2 逐路径指标

- RTT：帧携带发送时间戳、ACK 回显，RFC 6298 EWMA 维护 `srtt`/`rttvar`，另维护 `min_rtt`。
- 带宽：BBR 式 delivery-rate 采样（`delivered_bytes/Δt`，每 srtt 一样本，窗口最大值/EWMA）。
- 主动探测仅用于冷启动、est_rate 过期、疑似降级（小 PING 帧），不常态化。
- 对端遥测帧周期回报收端观测的各路径到达速率，校准发送端 est_rate。

### 5.3 失效与重传

- 全局序号在帧头，任何帧可原样换路径重发、收端按序号去重；帧允许按 `offset+len` 重新切分。
- 硬失效（写错误/流重置/路径 RTO）→ 未确认帧重注入其余路径。
- 软失效：`k·srtt_path` 未 ACK 视为可疑，先机会重发；路径 RTO 到期才摘除。
- 冗余发送仅用于失效过渡期加速，不提供常开低时延模式。
- 发送缓冲保留至连接级 ACK。

## 6. 收端：重排缓冲与流控（决策 #11）

- `reorder_buf = clamp(Σ est_rate_i · max srtt_i, min_buf, max_buf)`；min_buf = 最大单路径 BDP；max_buf 可配置硬顶（默认 32–64MB 量级）。
- 缓冲即连接级接收窗口：满 → 通告窗口收 0 → 发送端总量背压。
- 单一聚合窗口 + 底层路径自带流控（yamux/QUIC stream 窗口），不设逐路径配额窗口。

## 7. 中继组件（决策 #10，原型 #15 已验证）

### 7.1 组成

- 原生 `relay.New(h, WithInfiniteLimits(), WithACL(白名单), WithResources(...))`——绕过 `EnableRelayService` 的公网可达性门控；`BufferSize ≥ 16KiB`、按需调 `MaxCircuits`/`ReservationTTL`。
- wire protocol（pbv2）与 client 包零改动，随上游升级。

### 7.2 公平带宽分配：transport 装饰器

- 中继 host 注册自定义 `transport.Transport` 装饰器包装底层传输；其 `CapableConn` 的 `AcceptStream`/`OpenStream` 返回套限速的 `MuxedStream`。
- 限速桶按**对端 peerID** 共享（同一用户全部电路/流共配额）。
- 集中式配额重算器每 100–500ms 用 progressive filling 算 max-min 配额并 `SetLimit` 热更；叠加 cap=R 全局桶做总量保险。
- **需求测量陷阱（原型 #15 实测）**：被限速的流测不出真实需求，用观测速率作需求会产生配额缩水死亡螺旋。正确做法：本周期发生过令牌等待 → 需求视为不封顶进入均分；未等待才用观测速率 EWMA。
- 限速全靠回压（慢读 → rwnd 收缩 → 对端降速）；burst 设 1–2 个拷贝块；秒级收敛。
- 粒度取舍：按 peer 不按电路；C→B 段上不同源用户数据不可区分。若未来确需逐电路公平，退路为 vendor `p2p/protocol/circuitv2/relay`（~750 行）在 copy 写路径插限速。

### 7.3 访问控制与交付

- peerID 白名单经 `ACLFilter`（`AllowReserve`/`AllowConnect`，源/目的粒度）；令牌授权留作扩展点。
- 仅库组件交付（构造好包装传输 + relay.New 的 helper），不附 CLI。
- 中继对聚合无感知：每条中继路径在它看来就是一条普通电路。

## 8. 公开 API（决策 #12）

```go
agg := netacc.New(host, netacc.WithTransports(...), netacc.WithRelays(...), ...)
s, err := agg.OpenStream(ctx, peerID, opts...)   // 逐调用覆盖构造默认
in, err := agg.Accept(ctx)                       // 接收侧
```

- 聚合流实现 `net.Conn`（Read/Write/Close/SetDeadline），另挂扩展接口取 `Stats()`/`Paths()`。
- `Stats()` 快照：逐路径 RTT/est_rate/inflight/重传计数 + 聚合吞吐；事件订阅报路径增删/降级；不内置 prometheus 依赖。
- 路径策略：内建默认自动策略（实测带宽掉阈值自动加/换路径）+ 可插拔策略钩子 + 手动 `AddPath`/`RemovePath`。
- 调度器为内部接口，首版不公开。
- 错误语义：OpenStream 成功 = ≥1 条数据路径 attached（`WithMinPaths(n)` 提门槛）；全失败才返回 error。

## 9. 安全与信任模型

- 聚合流为私协议（自定义 protocol ID），原生节点无感知，不互通。
- 所有路径端到端加密到 peerID；中继只见密文。
- 中继侧白名单 + 公平限速防资源滥用；连接级认证 + agg_stream_id 绑定防路径误关联。

## 10. 未决项（地图 fog 的剩余）

- ECF 式等待判定增强：若实测异构路径下 HoL 仍明显再加。
- 受限链路叠加效果与基准测试方案：需 netem 等限速环境复测（原型数字均为 loopback）。
- 令牌授权、常驻中继 reservation 清单、调度器接口公开化：均为预留扩展点。

## 11. 参考资料

- 调研文档：`research/multiconn-streams`、`research/transports`、`research/circuit-relay`、`research/schedulers`、`research/fair-sharing` 各分支的 `docs/research/*.md`
- 原型：`prototype/striping`（双连接条带化）、`prototype/shaping-transport`（装饰器限速）分支
- 决策票：issues #8（握手/路径管理）、#9（调度器）、#10（中继改造）、#11（帧结构/重排/流控）、#12（API）
