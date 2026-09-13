# 原型：双连接手工条带化（issue #13）

**PROTOTYPE，随时可删。** 验证「同一 peer 两条不同传输连接 + 指定连接开流 + 跨路径条带化」在 go-libp2p v0.49.0 上的可行性。

## 跑法

```bash
cd proto-striping && go build -o striping . && ./striping
```

## 测了什么

单进程内两个 libp2p host（loopback）：

1. **QUIC 连接走 swarm**：peerstore 只放 B 的 quic 地址，`Connect` 建立 swarm 托管连接；`conn.NewStream` + `msmux.SelectProtoOrFail` 开流。
2. **TCP 连接走 transport 层直拨**：`hostA.Network().(*swarm.Swarm).TransportForDialing(tcpAddr)` → `tpt.Dial` → `CapableConn.OpenStream` + `SelectProtoOrFail`。这条连接不进 `ConnsToPeer`。
3. **基线**：单条 TCP 直拨连接发 256MiB。
4. **条带化**：256MiB 按 64KiB 块在两条流上交替发送，B 侧按 `stream.Conn()` 归属统计。

## 实测（loopback，2026-09-13，go-libp2p v0.49.0）

| 阶段 | 发送耗时 | 吞吐 |
|---|---|---|
| 基线 TCP 单路径 | 338ms | ~758 MiB/s |
| 条带 QUIC+TCP | 289ms | ~885 MiB/s（QUIC 438 + TCP 442，各背 128MiB） |

加速比 **1.17x**。loopback 无带宽上限、瓶颈在加密 CPU，此数字只证明机制可行；真实受限链路下的叠加效果需带限速环境复测。

## 结论

- 同一 peer 两条不同传输连接可并存，`conn.NewStream`/`CapableConn.OpenStream` 都能在指定连接上开流；
- **注意**：`conn.NewStream` 返回的流不会自动跑 multistream 协商，必须手动 `msmux.SelectProtoOrFail(protoID, stream)`，否则对端 reset（code 0x1001）；
- 直拨连接需自行管理生命周期（不进 swarm 连接表、无 identify/notifiee）。
