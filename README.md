# netacc

基于 go-libp2p 的**流级带宽聚合库**：把一条应用层字节流切片分发到多条并存的网络路径上聚合传输——对应用呈现一个普通的 `net.Conn`，底层自动做多路径调度、乱序重排、失效恢复。

## 它能解决什么

- **带宽叠加**：两条 100Mbps 链路聚成 ~200Mbps 的单流吞吐（移动网络 + Wi-Fi、双上行宽带等场景）
- **传输容错**：一条路径断流/被限速时数据自动换路重发，流不中断、字节不丢
- **QoS 绕行**：运营商整类限速 UDP 时自动补 TCP/WebSocket 路径（内建策略），或经中继走第三路径
- **NAT 穿透后的通路利用**：直连不通时把 libp2p circuit-relay 电路也当一条普通路径纳入聚合

## 工作原理

```
应用 ── Write/Read ──▶ Stream（net.Conn）
                          │
        字节级序号 + 重排缓冲 + 聚合窗口流控 + ACK/SACK
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
        路径 0 (TCP)   路径 1 (WS)   路径 2 (中继电路)
        /netacc/agg/1.0.0    /netacc/path/1.0.0 子流
```

- **分帧与保序**：数据按字节偏移切 DATA 帧下发；收端重排缓冲保序后 Read 取走，按偏移去重使重发/乱序到达安全
- **调度器**：最短排空时间优先（`(inflight+len)/est_rate`，与 Linux MPTCP 同构），按各路径实测带宽比例分流；逐路径 `k·est_rate·min_rtt` 在途硬顶防慢路径堆积
- **逐路径指标**：ACK 回显驱动 srtt/rttvar/min_rtt（RFC 6298 EWMA），BBR 式 delivery-rate 估 est_rate；收端 TELEMETRY 帧回报到达速率校准；PING 仅冷启动/指标失联探测
- **失效恢复**：在途段超 `2·srtt` 未被 ACK/SACK 覆盖 → 路径标可疑降权 + 段换路机会重发；持续超 RTO（`srtt+4·rttvar`）→ 摘除路径；硬失效（写错误/流重置）即刻摘除重注入
- **路径管理**：两侧对称加路径（`PATH_ATTACH`，path_id 分侧命名空间），中继路径按需协调（`PATH_REQUEST` → 对端向中继做 reservation → `PATH_READY` → 发起端 CONNECT 电路）
- **自动策略**：内建默认策略在发送积压持续时自动补路径、保守摘除垫底冗余路径；可经 `WithPolicy`/`WithStreamPolicy` 整体替换

## 安装

```bash
go get github.com/yangjuncode/netacc
```

要求 Go ≥ 1.27。

## 快速开始

```go
package main

import (
	"context"
	"io"
	"log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
)

func main() {
	// 两侧各一个 libp2p host（真实应用复用现有 host）。
	ha, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	defer ha.Close()
	hb, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	defer hb.Close()

	agga := netacc.New(ha, netacc.WithMinPaths(2)) // 构造默认：每条流至少 2 条路径
	defer agga.Close()
	aggb := netacc.New(hb)
	defer aggb.Close()

	// a 需要知道 b 的地址（真实部署经 identify/地址簿获取）。
	ha.Peerstore().AddAddrs(hb.ID(), hb.Addrs(), 0)
	_ = ha.Connect(context.Background(), peer.AddrInfo{ID: hb.ID()})

	// 接收侧按 net.Listener 模型 Accept。
	go func() {
		s, err := aggb.Accept(context.Background())
		if err != nil {
			return
		}
		defer s.Close()
		_, _ = io.Copy(s, s) // echo
	}()

	// 发起侧 OpenStream 返回的就是 net.Conn。
	s, err := agga.OpenStream(context.Background(), hb.ID())
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	s.Write([]byte("hello"))
	buf := make([]byte, 5)
	io.ReadFull(s, buf)
	log.Printf("echo: %s", buf)
}
```

`WithMinPaths(2)` 时 `OpenStream` 在握手后自动用 peerstore 中的对端地址直拨补挂；建流返回即保证 ≥2 条路径已 attached，否则报 `ErrMinPaths`。

## API

### Aggregator — 入口

```go
agg := netacc.New(h, opts...)                    // 注册协议处理器；Close() 注销
s, err := agg.OpenStream(ctx, peerID, oopts...)  // 主动建聚合流
s, err := agg.Accept(ctx)                        // 接收对端聚合流
agg.Close()                                      // 已建立的流不受影响
```

| Option | 作用域 | 说明 |
|---|---|---|
| `WithAcceptBacklog(n)` | `New` | 待 Accept 的入向流排队长度（默认 16） |
| `WithHandshakeTimeout(d)` | 两用 | 握手超时（默认 1min；ctx 自带 deadline 时不生效；传导到补挂握手） |
| `WithReorderBuffer(min, max)` | 两用 | 收端重排缓冲上下界（默认 1MiB/32MiB），容量随 `Σ速率·maxRTT` 联动 |
| `WithMinPaths(n)` | 两用 | 建流成功门槛：至少 n 条路径 attached（默认 1，不足返回 `ErrMinPaths`） |
| `WithPolicy(p)` | `New` | 替换默认路径策略；`nil` 关闭自动化 |
| `WithStreamPolicy(p)` | `OpenStream` | 该条流的策略覆盖；`nil` 关闭 |

「两用」= `SharedOption`：同一个 `WithXxx` 既可传 `New` 作构造默认，也可传 `OpenStream` 逐调用覆盖。

### Stream — net.Conn + 路径管理 + 观测

`Stream` 完整实现 `net.Conn`（Read/Write/Close/LocalAddr/RemoteAddr/三个 SetDeadline）。之外：

```go
s.ID()      [16]byte    // 聚合流标识
s.Peer()    peer.ID     // 对端

// ---- 手动路径管理（与自动策略并存，两侧对称可调用）----
id, err := s.AddPath(ctx, addr)         // 直拨 addr 建物理连接并 PATH_ATTACH
id, err := s.AddRelayPath(ctx, relayInfo) // 经中继 C 协调建电路路径（见下）
err := s.RemovePath(pathID)             // 摘除（最后一条会被拒：ErrLastPath）
infos := s.Paths()                      // 存活路径观测快照 []PathInfo

// ---- 观测 ----
st := s.Stats()                         // StreamStats 快照
events, unsub := s.Subscribe(bufLen)    // 事件订阅；unsub()/流终结关闭通道
```

`AddPath` 支持的地址形态（全传输矩阵）：

| 传输 | multiaddr 形态 |
|---|---|
| TCP | `/ip4\|dns4/.../tcp/<port>` |
| WebSocket | `.../tcp/<port>/ws`；`.../tls/sni/<host>/ws` 或 `/wss` |
| QUIC | `.../udp/<port>/quic-v1` |
| WebTransport | `.../udp/<port>/quic-v1/webtransport/certhash/<hash>` |
| WebRTC-direct | `.../udp/<port>/webrtc-direct/certhash/<hash>` |

`AddRelayPath` 的 `relay peer.AddrInfo` 即中继路径描述符：`Addrs` 是 C 的可达地址集，**地址顺序表达 A→C 段传输偏好**（要 WebSocket 就把 `/ws` 地址放最前）；C→B 段用对端 reservation 所用传输。

### Stats 快照

```go
type StreamStats struct {
	ID, Peer
	Paths        []PathStats // 逐路径：PathInfo + Delivered/RxBytes/RxRateBps/ResentSegs/ResentBytes
	TxRateBps    float64     // Σ 各路径有效 est_rate（调度器视角的上行容量）
	RxRateBps    float64     // Σ 各路径收端实测到达速率
	SentBytes, AckedBytes, Inflight, PendingBytes, SendBufCap
	LostSegs, ResentSegs, ResentBytes           // 重传口径（改判数 vs 实发数）
	RecvCum, RecvBufCap, RecvBuffered, RecvReady, RecvDropped // 收端缓冲
}
```

`PathInfo`：`ID / Dialed / ConnID / Transport / Local / Remote / AttachedAt / AutoAdded / SRTT / MinRTT / EstRate / Inflight / Suspect`。

### 事件订阅

```go
events, unsub := s.Subscribe(16)
defer unsub()
for ev := range events {
	// ev.Type ∈ {EventPathAdded, EventPathRemoved, EventPathDegraded}
	// ev.PathID / ev.Info / ev.Cause(仅 Removed) / ev.Dropped(漏报计数)
}
```

非阻塞派发：订阅者慢、通道满即丢该事件，不阻塞数据面；`Dropped` 在下一条成功投递的事件里携带漏报数。要完整指标用 `Stats()` 补快照。

### 错误语义（均 `errors.Is` 可判定）

| 错误 | 含义 |
|---|---|
| `ErrClosed` | Aggregator 已关闭（流自身关闭用 `net.ErrClosed`） |
| `ErrHandshake` | 握手失败：对端未跑本协议/拒绝/回显不符 |
| `ErrMinPaths` | `WithMinPaths` 门槛未满足（含 peerstore 无地址） |
| `ErrLastPath` | 不能摘除最后一条数据路径 |
| `ErrUnknownPath` | `RemovePath` 的 path_id 不在存活集 |
| `ErrNoAggregator` | 流不经 Aggregator 创建（测试构造等），无法拨号加路径 |
| `ErrReset` | 流被对端 RST |
| `ErrNoPaths` | 存活路径归零后的流终态 |

### Policy — 可插拔路径策略

```go
type Policy interface{ Decide(PolicySnapshot) PolicyDecision }

netacc.WithPolicy(myPolicy)        // 构造默认
netacc.WithStreamPolicy(myPolicy)  // 逐调用覆盖
netacc.WithPolicy(nil)             // 关闭自动化
```

快照 `PolicySnapshot` 携带逐路径视图（PathInfo + LowShareFor）、需求侧信号（PendingBytes/BacklogFor/UnackedBytes）、候选地址集（peerstore 经传输偏好排序、剔除中继形态）与护栏状态（NextAutoAddAt/AutoAddFails）。`PolicyDecision{Add, Remove}` 由框架在护栏内执行：单笔在途拨号、冷却/指数退避、路径总数硬顶 4、摘除热身期。默认策略 `DefaultPolicy()`：积压 ≥500ms 补路径、仅摘自动补挂且份额 <5% 持续 ≥3s 的路径。

## 中继组件（relay 子包）

`github.com/yangjuncode/netacc/relay` 是可独立嵌入的公平带宽中继——你的节点既消费别人的中继，也可以跑中继服务别人：

```go
import netaccrelay "github.com/yangjuncode/netacc/relay"

// 分配器决定总容量与逐 peer 公平份额；挂传输装饰器和 relay.New 必须用同一个。
alloc := netaccrelay.NewAllocator(64 << 20) // 64MiB/s 总容量

// host 的传输必须用装饰过的版本注册，限速才生效：
h, _ := libp2p.New(
	libp2p.NoTransports,
	netaccrelay.TCPTransport(alloc), // 等价 libp2p.Transport(tcp.New...) 多套一层
	libp2p.DefaultSecurity, libp2p.DefaultMuxers,
	libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
)

r, err := netaccrelay.New(h,
	netaccrelay.WithAllocator(alloc),
	netaccrelay.WithWhitelist(peerA, peerB), // 必须配 ACL：白名单/WithACLFilter/WithAllowAll
)
```

- **ACL 默认关闭**：不给 `WithWhitelist`/`WithACLFilter`/`WithAllowAll` 返回 `ErrNoACL`
- **公平限速**：token-bucket 传输装饰器，逐 peer 桶按需激活；分配器按「本拍等待需求」做 max-min 重分配（不惩罚沉默 peer，不保分低需求者）
- `WithBandwidth`/`WithAllocatorOptions`（`WithRecomputeInterval`/`WithBurst`/`WithMinShare`）/`WithResources`/`WithRelayOptions` 细调

## 已知限制

- **A→C 传输偏好尽力而为**：上游 circuitv2 client 复用既有连接时偏好可能退化；无既有连接时确定性命中
- **WSS 需真 CA 证书**：go-libp2p 自签证书过不了系统校验；生产建议反代终结 TLS 或签正式证书
- **certhash 过期**：WebTransport/WebRTC 证书轮换后旧 multiaddr 失效，需经 identify/地址簿刷新重拨（不内建地址发现）
- **ACK 线格式**：`ts_path` 字段为向后不兼容变更，双端须同版本
- **自动策略边界**：默认开但可关；「带宽不足」以发送积压持续为代理信号（无法区分对端不读与链路受限）；中继路径不入自动候选

## 开发与测试

```bash
go build ./... && go vet ./...
go test -race -count=1 ./...
```

设计规格在 `docs/spec/bandwidth-aggregation.md`；issue 用 GitHub issues 跟踪（`docs/agents/`）。欢迎 issue 与 PR。
