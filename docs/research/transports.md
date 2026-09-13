# 调研：go-libp2p 各传输作聚合路径的可行性

> 对应 issue: yangjuncode/netacc#3
> 调研对象版本： **go-libp2p v0.49.0**（2026-07-28 发布的最新稳定版，Go 1.25.7）
> 关键依赖版本： quic-go v0.60.0、webtransport-go v0.11.1、pion/webrtc v4.1.2、gorilla/websocket v1.5.3、go-yamux v5.1.0
> 一手来源： go-libp2p 源码（v0.49.0 tag）、libp2p specs 仓库（webrtc-direct r2 2026-06-20、webrtc r0 2023-04-12、webtransport r0 2022-10-12）

## 结论速览

| 传输 | 默认启用 | 拨号前提 | 底层协议 | 受 UDP QoS 影响 | 浏览器可达 | 聚合路径优先级 |
|---|---|---|---|---|---|---|
| TCP | 是（含 PSK 私网） | 无，独立 dial | TCP | **否** | 否 | **P0 主力** |
| WebSocket | 是（含 PSK 私网） | 无；`/wss` 需对端 CA 证书+域名 | TCP | **否** | 是（需 wss+CA 证书） | **P0 规避主力** |
| QUIC | 是 | 无，独立 dial | UDP | 是 | 否 | P1 机会型 |
| WebTransport | 是 | multiaddr 必须带 `/certhash` | UDP（HTTP/3 over QUIC） | 是 | 是（自签证书亦可） | P2 浏览器场景 |
| WebRTC | 是（仅 webrtc-direct） | multiaddr 必须带 `/certhash`；无需信令 | UDP（ICE/DTLS/SCTP） | 是 | 是（浏览器→公网服务器） | P3 浏览器场景 |

核心结论： **TCP 与 WebSocket 是绕开 UDP 限速的两大支柱；QUIC、WebTransport、WebRTC-direct 三者同走 UDP，在运营商按 UDP 整类 QoS 的场景下同生共死，只能作机会型增益路径。** 项目语境（go 节点间聚合）下应优先保证 TCP/WS 路径的带宽叠加能力。

## 默认启用状态（源码依据）

`defaults.go`（v0.49.0）：

- `DefaultTransports`（L45–51）：`tcp` + `quic` + `ws` + `webtransport` + `webrtc` **五个全部默认启用**。
- `DefaultPrivateTransports`（L57–60）：配置 PSK 私网时**只有 TCP + WS**——QUIC、WebTransport、WebRTC 均不支持 PSK（构造时直接报错）。
- `DefaultListenAddrs`（L82–90）：默认监听 `/tcp/0`、`/udp/0/quic-v1`、`/udp/0/quic-v1/webtransport`、`/udp/0/webrtc-direct`（v4+v6 各一组）。

各传输进入默认集的时间线：

- TCP、WebSocket：自始即默认（v0.14 及更早已在内）。
- QUIC：v0.16.0 起默认（v0.15.0 的 `DefaultTransports` 尚无 QUIC）。
- WebTransport：v0.28.0 起默认（PR libp2p/go-libp2p#1915）。
- WebRTC-direct：v0.36.x 起默认（PR #2887「remove experimental tag, enable by default」，v0.36.0 被撤回，实际生效于 v0.36.1+，2024-08）。

## 逐传输分析

### 1. TCP — `p2p/transport/tcp`

- **multiaddr**: `/ip4/<ip>/tcp/<port>`（v6 同理）。
- **安全与复用**: TCP 裸字节流上先 multistream-select 协商安全层（默认 TLS 1.3→Noise，TLS 用带 `libp2p-tls-handshake:` 扩展的自签证书携带 host 公钥，`p2p/security/tls/crypto.go`），再协商流复用器（默认 yamux，`DefaultMuxers`）。
- **拨号前提**: 无任何额外前提，独立 dial。NAT 后场景可借 `p2p/transport/tcpreuse`（SO_REUSEPORT + simultaneous open）做 TCP 打洞（DCUtR 流程，需中继连接协调），或走中继路径。
- **UDP QoS 表现**: 完全免疫。
- **防火墙特征**: 裸 TCP+multistream 指纹明显；TLS security 时对外呈现 TLS 1.3 流量但证书非常规（libp2p 扩展自签证书），DPI 可识别为"非浏览器 TLS"。可通过 `libp2p.ShareTCPListener()`（options.go:662）与 WebSocket 共享同一 TCP 监听端口（`tcpreuse` 按首字节分流，listener.go:88–92）。
- **聚合适配**: 最简单可靠的路径来源；同 peer 可开多条 TCP 连接=多条路径；连接建立快、流开销最小。

### 2. QUIC — `p2p/transport/quic`（quic-go v0.60.0，经 `p2p/transport/quicreuse` 管理 UDP socket）

- **multiaddr**: `/ip4/<ip>/udp/<port>/quic-v1`（`dialMatcher` = IP+UDP+QUIC_V1，transport.go:282）。
- **安全与复用**: QUIC TLS 1.3 握手内嵌 libp2p 身份证书扩展（同 `libp2p-tls-handshake:`），无需单独安全协商；流复用为 QUIC 原生流，**不需要 yamux**。
- **拨号前提**: 独立 dial；内置 UDP 打洞支持（transport.go:33 `ErrHolePunching`、L195 `holePunch`，同样走 DCUtR 中继协调）。
- **UDP QoS 表现**: **直接命中**。与 WebTransport、WebRTC 同为 UDP，按类限速时同死。
- **防火墙特征**: QUIC 长头特征明显，UDP 443 上近似 HTTP/3 流量；但与真实 HTTP/3 的 TLS ClientHello/Cert 细节不同，可被专门 DPI 区分。
- **端口复用**: `quicreuse.ConnManager` 使 QUIC、WebTransport、WebRTC 可共享同一 UDP 端口（config.go:313–334，`SharedNonQUICPacketConn`）。
- **聚合适配**: 非限速环境下性能最优、单连接原生多流；swarm 默认拨号排序也最优先 QUIC（dial_ranker.go：公网 TCP 相对 QUIC 延迟 250ms、其他传输延迟 1s）。作为"UDP 未限速时的高性能机会路径"价值最大。

### 3. WebSocket — `p2p/transport/websocket`（gorilla/websocket v1.5.3）

- **multiaddr**: `.../tcp/<port>/ws`；加密形态 `.../tcp/<port>/tls/ws`（新式）或 `/wss`（旧式别名），域名场景 `/dns4/<host>/tcp/443/tls/sni/<host>/ws`（websocket.go:24–39 的 `WsFmt`/`WssFmt`）。
- **安全与复用**: WS 消息当作字节管，上面照旧跑 TLS/Noise + yamux；`/wss` 时实际为"底层 TLS（CA 证书）+ 上层 libp2p 安全握手"双层加密。
- **拨号前提**: 独立 dial，无信令。`/wss` 拨号要求对端证书在系统 CA 信任链内（浏览器同样要求）；go-libp2p 作为服务端监听 `/wss` 需显式 `WithTLSConfig` 提供证书（websocket.go:69；listener.go:74–75 无 tls.Config 即报错），常见做法是 `/ws` 明文监听 + 前置 TLS 终结反代。
- **UDP QoS 表现**: 免疫（TCP）。
- **防火墙特征**: **规避能力最强**。WS 建立走标准 HTTP Upgrade；`/wss` 在 443 端口上对外就是正常 HTTPS+TLS 流量，可挂在 CDN、Nginx、Cloudflare 之后，与一般网页流量难以区分。这是混淆运营商深度限速的首选传输。
- **聚合适配**: 性能略低于裸 TCP（每条 yamux 帧套一层 WS 消息头），但换来最强的穿透/伪装面；同时是浏览器节点参与聚合的唯一 TCP 系入口（wss）。

### 4. WebTransport — `p2p/transport/webtransport`（webtransport-go v0.11.1 + quic-go）

- **multiaddr**: `/ip4/<ip>/udp/<port>/quic-v1/webtransport/certhash/<hash>`（可多个 certhash）。
- **拨号前提**: **go-libp2p 强制要求 multiaddr 携带 `/certhash`**——无 certhash 直接报错 `can't dial webtransport without certhashes`（transport.go:159–161）。服务器证书由 host key 确定性生成、自签、**有效期≤14 天**（W3C `serverCertificateHashes` 限制，transport.go:41 `certValidity = 14*24h`），按 bucket 轮换、新旧证书 certhash 同时广播。含义：**学到的 WebTransport multiaddr 会随证书轮换过期**（地址簿需持续刷新）；持有 CA 签名证书的节点可用 `WithTLSConfig` 静态证书规避轮换（spec 允许此时不广播 certhash，但 go-libp2p 拨号侧仍强制要求 certhash）。
- **安全与复用**: 端点固定 `/.well-known/libp2p-webtransport?type=noise`；首条 WT 流上跑 Noise 握手，early data 携带服务器全部 certhash 供校验子集（transport.go:251–271）；流复用为 WT 原生流。
- **UDP QoS 表现**: **与 QUIC 同命**——它本身就是 HTTP/3 over QUIC over UDP，运营商按 UDP 限速时必死；且在线路上与普通 QUIC 几乎不可区分。
- **聚合适配**: 对 go↔go 聚合**与 QUIC 功能重叠且多一层 HTTP/3+Noise 握手**，无独立增益；独特价值仅在于浏览器可在无 CA 证书情况下拨入。低优先级。

### 5. WebRTC — `p2p/transport/webrtc`（pion/webrtc v4.1.2）

**重要澄清：go-libp2p 只实现了 WebRTC-Direct，未实现 `/webrtc`（浏览器↔浏览器）。**

- spec 中的完整 WebRTC（`specs/webrtc/webrtc.md`）：A、B 两个私有节点经中继 R 建立**中继连接**，在其上跑 `/webrtc-signaling/0.0.1` 协议交换 SDP offer/answer 与 ICE candidates（trickle ICE），并依赖 STUN 发现公网映射——即 issue 所说的"SDP-over-relay 信令"。**go-libp2p 无此实现**（全仓库对 `ma.P_WEBRTC` 仅 mdns.go 一处过滤引用），该传输只存在于 js-libp2p 等浏览器侧实现。
- go-libp2p 实现的是 WebRTC-Direct（`specs/webrtc/webrtc-direct.md`，浏览器→公网服务器）：
  - **multiaddr**: `/ip4/<ip>/udp/<port>/webrtc-direct/certhash/<hash>`，`CanDial` 要求至少一个 certhash（transport.go:202–205）。
  - **拨号前提**: **无信令通道**。拨号方用 multiaddr 中的 IP/端口/certhash **本地合成对端 SDP answer**（sdp.go `createServerSDP`）；服务端从入向 STUN binding request 的 USERNAME 字段恢复 ufrag/pwd、反推对端 offer（transport.go:320–330 注释：「The server has no signaling channel」）。握手有两个版本：v1（SDP munging，默认）与 v2（`WithDialerVersion(2)`，客户端 pwd 编码进服务端 ufrag；Chromium 的 `WebRTC-NoSdpMangleUfrag` 落地后浏览器侧将只能用 v2）。
  - **限制**: 服务端必须是公网可达的 ICE Lite 端点（不能打洞、不能 NAT 后监听）；拨号方可在 NAT 后；不支持 PSK；DTLS 证书由 host key 确定性派生，certhash 跨重启不变。
- **安全与复用**: datachannel 0（handshake channel）上跑 Noise 握手认证 peer ID；libp2p 流 = SCTP datachannel + protobuf 分帧，`max-message-size` 16384 字节。
- **UDP QoS 表现**: **同死**（STUN/DTLS/SCTP 全在 UDP 上，未配置 TURN/ICE servers，无 TCP 回退）。报文特征（STUN/DTLS）与 QUIC 不同，若 QoS 引擎按协议特征而非"UDP 整类"限速可能幸免，但不可依赖。
- **聚合适配**: 握手最重（ICE→DTLS→SCTP→Noise）、消息级 16KB 分帧开销大；对 go↔go 聚合边际价值最低，仅在需要浏览器直接参与时启用。

## UDP 限速场景的共同命运分析

- QUIC、WebTransport、WebRTC-direct **三个传输物理上全部是 UDP**，且默认可共享同一 UDP 端口（quicreuse + udpmux）。运营商"UDP 整类限速"时**三者同生共死**——不存在"QUIC 被限就换 WebTransport/WebRTC"的规避空间。
- 唯一可能的差异点是 QoS 按协议特征限速（如只限 QUIC/HTTP3）：此时 WebRTC 的 STUN/DTLS 报文可能漏网，但这依赖具体运营商策略，不能作为设计前提。
- go-libp2p 自带 `UDPBlackHoleSuccessCounter`（defaults.go:135–139，100 次拨号中成功<5 次判定 UDP 黑洞并停拨 UDP），**但它只识别硬阻断；QoS 限速下拨号依然成功、只是慢**——检测不出限速。因此限速场景的路径降权必须由本库调度器按实测带宽自行完成，这恰好是"按实测带宽加权条带"策略天然覆盖的行为。
- 结论：对抗 UDP QoS 的有效多样性只有 TCP 系（TCP、WS）。WS（wss/443）额外提供 HTTPS 伪装面。

## 与聚合架构的相互作用（源码事实）

- `swarm.NewStream`（swarm.go:475–528）永远只选 `bestConnToPeer` 一条"最优"连接开流——**聚合库不能用 `Host.NewStream`，必须自己按传输/multiaddr 分别拨号、持有多条 conn，并在指定 conn 上开路径流**。libp2p 允许同 peer 多条连接并存（`ConnsToPeer` 返回切片）。
- `DefaultDialRanker`（dial_ranker.go）：拨号偏好 QUIC > TCP（公网延迟 250ms）> relay（500ms）> 其他/webrtc-direct（1s）。这是默认智能拨号策略，聚合库自行按 multiaddr 拨号即不受影响。
- 同一传输向同一对端开多条连接可形成多条同构路径（TCP/QUIC 均支持），即"同一传输多条连接"也是合法路径来源。
- PSK 私网场景自动退化为只有 TCP+WS 两个传输可用。

## 聚合路径优先级建议

| 优先级 | 传输 | 理由 |
|---|---|---|
| **P0** | TCP | UDP QoS 免疫主力；无额外依赖、建连快、开销最小、支持打洞；多条 TCP 连接天然多路径 |
| **P0** | WebSocket（wss/443） | TCP 系第二条腿；HTTPS 外观+CDN/反代兼容，穿透与伪装最强；兼顾浏览器节点 |
| **P1** | QUIC | 未限速环境下性能最优（原生多流、0-RTT、连接迁移）；作机会型路径，由调度器按实测带宽自动降权 |
| **P2** | WebTransport | 与 QUIC 同为 UDP 且多一层封装，go↔go 无增量价值；仅当需要浏览器节点（无 CA 证书）时启用 |
| **P3** | WebRTC-direct | UDP + 最重握手 + 16KB 消息分帧；go↔go 价值最低；仅浏览器场景考虑 |
| — | `/webrtc`（browser↔browser） | go-libp2p **未实现**；需要中继+`/webrtc-signaling/0.0.1`+STUN，本库范围内不可作直连路径 |

## 参考来源

- go-libp2p v0.49.0 源码：`defaults.go`、`config/config.go`、`p2p/net/swarm/swarm.go`、`p2p/net/swarm/dial_ranker.go`、`p2p/transport/{tcp,quic,quicreuse,websocket,webtransport,webrtc}`
- https://github.com/libp2p/specs/blob/master/webrtc/webrtc.md （/webrtc，SDP-over-relay 信令，go-libp2p 未实现）
- https://github.com/libp2p/specs/blob/master/webrtc/webrtc-direct.md （r2, 2026-06-20；v1/v2 无信令握手）
- https://github.com/libp2p/specs/blob/master/webtransport/README.md （certhash、14 天证书、`/.well-known/libp2p-webtransport?type=noise`）
- go-libp2p CHANGELOG / releases：PR #1915（WT 默认化）、PR #2887（WebRTC 默认化）、PR #2889（WebRTC 复用 QUIC UDP socket）
