# 原型：transport 装饰器按 peer 公平限速（issue #15）

**PROTOTYPE，随时可删。** 验证 #10 定案的方案：transport 装饰器包装底层传输，给每个对端 peerID 的连接流套共享 token bucket，中继本体用原生 `relay.New` 零改动。

## 跑法

```bash
cd proto-shaping && go build -o shaping . && ./shaping
```

## 结构

- `shapingTpt`：装饰 `transport.Transport`，Listen/Dial 返回包装后的 `CapableConn`；其 `AcceptStream`/`OpenStream` 吐出套限速的 `MuxedStream`。
- `limStream.Read`：`ReserveN` 取令牌，有延迟则 sleep 并标记 `waited`（=本周期被限速）。
- `allocator`：每 200ms 重算配额。**关键**：需求估计不能用观测速率——被限速的流测不出真实需求（会产生配额缩水死亡螺旋）；本原型用「发生过令牌等待 → 需求不封顶，否则观测速率 EWMA」，再做 progressive filling 均分。
- 拓扑：R（装饰 TCP + `relay.New(WithInfiniteLimits)`）、B（reservation 到 R）、A1/A2 经 `/p2p-circuit` 地址打流到 B。

## 实测（loopback，R 总额 200 MiB/s，go-libp2p v0.49.0）

| 阶段 | 结果 |
|---|---|
| 仅 A1 打流 | ~110–150 MiB/s 波动（单用户拿走绝大部分） |
| A1+A2 同打 | ~1s 内收敛到各 ~70–76 MiB/s，双方基本相等 |

**结论：方案可行。**（a）装饰器对 relay/identify/协商完全透明；（b）单用户高占用、第二用户加入后旧用户自动降速到公平份额——max-min 语义成立；（c）限速全靠读侧回压传导，无丢包式限速。

## 已知取舍 / 注意事项

- 实测吞吐上限（~150MiB/s）低于设定配额（200MiB/s）：瓶颈在「每读一次 ReserveN+sleep」的调度粒度与中继双跳拷贝开销，非机制问题；生产实现可换更精确的 pacing 或更大块。
- 粒度是**按 peer**：同一 peer 的所有电路/流共享配额；C→B 段上不同源用户的数据在同一连接上无法区分（逐电路公平才需要 vendor relay 包）。
- 同 peer 的控制流（hop/stop/identify）也计入配额，流量极小可忽略。
- 装饰器只包 Read 侧（入口限速即够）；如需出口整形可对称包 Write。
