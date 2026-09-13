# 调研：转发路径上的公平带宽分配算法（max-min / DRR / SFQ）

> 对应 issue：#6（Part of #1）
> 目标：中继节点（Relay）的「公平带宽分配」——总带宽空闲时单用户可用满，新用户加入时占用大者自动降速，最大化利用且防独占（max-min fairness 语义）。
> 范围：中继转发循环（逐连接 `io.Copy` 式的字节流转发）上的机制选型，不涉及发送端调度器。

## TL;DR

**推荐：主动限速路线** —— 每条转发流（每个方向）挂一个动态速率的 token bucket，套在 `io.Copy` 的读侧（或写侧），由一个集中式配额重算器按周期（100ms~1s）用 progressive filling 算法算 max-min 配额并 `SetLimit` 下发。限速效果完全靠 TCP 回压传导（中继不读 → 接收窗口收缩 → 对端 TCP 自动降速），用户态**不需要丢包**。

**队列调度（DRR/SFQ）作为备选**：只有当中继转发本身是逐帧转发、或需要在亚 RTT 时间尺度上隔离突发时才值得做；它要打破 `io.Copy` 模型引入中央队列，CPU/内存开销更高，且对字节流转发收益有限。SFQ 不记账、WFQ 在用户态字节流上成本大于收益，均不推荐作为主机制。

---

## 1. 问题定义

中继节点的转发形态是：每条中继路径（Relayed Path）在中继上表现为一对（或多对）已建立连接之间的双向字节流拷贝，即两个方向的 `io.Copy(dst, src)`。共享资源是中继节点的总出口/入口带宽 R（可配置或实测）。

要求的语义是 max-min fairness（Bertsekas & Gallager《Data Networks》§6 的经典定义；最早由 Jaffe 1981 的 bottleneck flow control 提出）：

- 任何流的配额不小于其它"更受限制"的流；
- 等价操作定义：所有活跃流从 0 开始等速抬升配额，先到需求或瓶颈者冻结（progressive filling），剩余带宽继续分给未冻结者，直到分完；
- 推论出的"弹性"：只有 1 条活跃流时它可以拿走全部 R；第 2 条到达时两者各分 R/2（除非某条需求更小），新流加入时大流降速而不是被硬顶在某个固定 cap 上。

要回答的工程问题：

1. 在 Go 的逐连接 `io.Copy` 转发循环里，用**主动限速**（动态 token bucket）还是**队列调度**（转发点按流排队发包）实现逐流公平？
2. 活跃流数量变化时配额怎么重算、收敛多快可行？
3. 与底层 TCP 拥塞控制/回压怎么互动（靠不发 ACK 的回压，还是主动丢/缓）？
4. CPU 与内存开销的量级？

## 2. 经典算法综述（一手来源）

### 2.1 Max-min fairness 本身

- Jaffe, "Bottleneck Flow Control", IEEE Trans. Commun., 1981：最早形式化"同一瓶颈上的所有流分相同速率"的最优操作点，并给出分布式收敛算法。
- Bertsekas & Gallager, *Data Networks*（2nd ed., 1992, §6）：教科书定义与 progressive filling（注水/渐进填充）算法——这正是"配额重算器"可以直接照搬的算法，集中式版本 O(n log n)。

### 2.2 包级公平排队（队列调度家族）

- Nagle, RFC 970 (1985)：最早提出按流（conversation）分队列轮转发包，防单流霸占路由器。
- Demers, Keshav & Shenker, "Analysis and Simulation of a Fair Queueing Algorithm", SIGCOMM 1989：FQ/WFQ 奠基作，按包虚拟完成时间排序逼近 bit-by-bit GPS，吞吐公平且给"用得少"的流更低延迟；代价是每包 O(log n) 的堆操作 + 需要维护虚拟时钟。
- McKenney, "Stochastic Fairness Queueing", INFOCOM 1990：SFQ——不为每流建精确队列，而是用哈希把流打到固定数目的队列上轮转服务（Linux `sch_sfq` 即此实现，见 `net/sched/sch_sfq.c` 头注释；现代内核里的 sfq 实际是赤字轮转而非时间戳 FQ）。用概率性公平换 O(1) 分类开销，适合高速软件实现，但**不提供记账语义**——配额是"机会均等"而非"速率可指定"。
- Shreedhar & Varghese, "Efficient Fair Queueing Using Deficit Round-Robin", SIGCOMM 1995（期刊版 IEEE/ACM ToN 1996）：DRR——每流一个 FIFO 队列 + 赤字计数器，每轮发 quantum（≈MTU）字节，O(1)/包，吞吐近似完美公平，且"不可分包也能用"。这是用户态做出口队列调度时的标准选择。

### 2.3 Token bucket 家族与分层借用

- Token bucket / TBF：速率 r、深度 b；Linux `tc` 生态的基础原语。
- Devera, HTB（`net/sched/sch_htb.c`，文档 `luxik.cdi.cz/~devik/qos/htb/`）：分层 token bucket，每类有 `rate`（保证速率）和 `ceil`（可借用上限），空闲带宽沿层级向有需求的子孙"借用"分配。HTB 的借用语义**本质上就是 max-min 的加权版**（先满足保证速率，剩余按需求分配）——这证明"每流一个可变速率桶 + 上级一个总量桶"的结构足以表达目标语义。本调研推荐方案的配额重算器相当于把 HTB 的借用决策从逐包改成了周期性集中计算。

### 2.4 参考实现：Linux `sch_fq`

`sch_fq`（Eric Dumazet, 2013–2015，`net/sched/sch_fq.c`，`man tc-fq`）是最贴近我们场景的工业实现：每流一个 FIFO + **每流 pacing 速率**（`SO_MAX_PACING_RATE`/EDT），出队按 round-robin；即"每流速率上限 + 轮转调度"的混合体，单 qdisc 可扩展到百万级并发流。它印证了两点：(a) 公平性主要靠"每流速率上限"表达，队列调度负责执行；(b) 用时间戳延迟发包（pacing）而不是丢包来整形，与 TCP 友好共处。

### 2.5 Go 生态

- `golang.org/x/time/rate`：`Limiter` 即 token bucket，支持 `SetLimit`/`SetBurst` 热改速率、`WaitN/ReserveN` 按量取令牌；内部一把 mutex + 按需定时器。可直接用，缺点是每次 `WaitN` 都过锁（可接受，见 §6）。
- `github.com/mxk/go-flowrate`：实现了**分层 token bucket**（子桶从父桶借令牌），若想保留"全局硬顶 R + 每流弹性"的两级结构可参考；代码老旧但思路正确。
- 另可参考 Kubernetes APF（`k8s.io/apiserver` 的 flowcontrol）：Go 里最完整的 FQ/shuffle-sharding 落地，但面向请求计数而非字节带宽，借鉴价值在设计而非代码。

## 3. 两条实现路线

### 路线 A：主动限速（每流动态 token bucket）

结构：

```
每条转发流（每个方向）:
    srcConn ──> [限流 Reader: 读前/读后向 per-flow bucket 取令牌] ──> dstConn
集中式配额重算器（单 goroutine，周期 T≈100ms~1s）:
    采样每流需求 → progressive filling 算配额 → SetLimit 到各桶
（可选）全局桶 cap=R：保证即使配额计算滞后，总量也不超物理带宽
```

- 限流点放读侧还是写侧等价；放**读侧**最自然：`io.Copy` 循环里"不读"本身就是回压信号。
- 弹性语义落在配额重算上：单流时配额=R（或 Inf）；n 流时分 R 的 max-min 份额。

优点：不改 `io.Copy` 数据面结构、无中央队列、无额外数据拷贝；与 TCP 回压天然协作；每流状态 ~100B。

缺点：公平粒度是"速率"而非"逐包交织"，亚 RTT 尺度上各流突发仍可能瞬时打满出口（靠 burst 参数和全局桶约束）；限速精度受 Go 定时器粒度影响（ms 级，对吞吐公平足够）。

### 路线 B：出口队列调度（per-flow FIFO + DRR/SFQ）

结构：

```
每条转发流的读循环把数据帧推入自己的有界队列；
单一 egress worker 按 DRR（quantum≈MTU~帧大小）轮转各非空队列，写 dstConn。
```

- 把"谁先发"从 N 个独立写 goroutine 收拢到 1 个调度点，得到字节级精确的 max-min 和强突发隔离。
- 当中继转发本来就把字节流**切成帧**（例如按 16KB 帧转发）时，DRR 是自然形态，多路复用到少数几条 TCP 连接时还顺带解决了 HoL 内的公平。

代价（对纯 `io.Copy` 转发而言）：

- 数据多一次拷贝/入队，丧失 `io.Copy` 的优化路径（Linux 上 TCP→TCP `io.Copy` 可走 splice 零拷贝；套了 Reader/队列后即退化）；
- 中央队列内存 = 活跃流数 × 每流缓冲上限，且必须做"队列满→停止读"的反向回压，复杂度明显上升；
- 单 worker 出队成为热点，多核下要么锁要么分片。

结论：B 是"更精确但更贵"的方案；对字节流电路式转发，其精度优势在吞吐公平目标下用不上。

### 路线 C（混合，实际推荐形态）

A 为主体 + 一个可选的全局桶（HTB 式两层）保证总量硬顶。配额重算器本身就是 max-min 的集中式执行者，调度层不再需要包级 FQ。这正是 HTB/sch_fq 在更高抽象层的同构。

## 4. 活跃流变化时的配额重算与收敛

### 4.1 配额算法

每周期 T，配额重算器：

1. 采样每流"需求" dᵢ：用上一周期实际转发字节数的 EWMA（如 α=0.25）作为需求估计；或用更简单鲁棒的指示——本周期内流是否"受限"（取令牌时发生等待/令牌耗尽视为需求≥当前配额，否则需求=实际速率）。
2. progressive filling（Bertsekas & Gallager）：
   - 各流配额 rᵢ 从 0 起同步抬升；
   - 流在 rᵢ = dᵢ（需求饱和）或触及单流上限时冻结；
   - 剩余带宽继续在未冻结流间均分，直到 Σrᵢ = R。
   - 加权变体：把"同步抬升"换成"按权重 wᵢ 比例抬升"即得 weighted max-min（白名单内再分级时可用）。
   - 复杂度 O(n log n)（按需求排序），n 在数百量级时每次重算为微秒级。
3. `SetLimit` 下发新速率（x/time/rate 的 `SetLimit` 不清空已有令牌，调速平滑）。

### 4.2 收敛速度

- 新流加入：注册即获得"乐观份额" R/n（n 为当前活跃数），不必等周期边界；存量大流在下个周期被压到新配额。
- 收敛时间 ≈ 几个重算周期 + TCP 自身一个 RTT 的窗口调整：T=200ms 时，新分配在 ~0.5s 内生效，1–2s 内完全稳定。对"防独占"目标（吞吐公平）绰绰有余；本机制不承诺毫秒级延迟公平，那需要 B 路线的逐包调度。
- 防抖：配额按"实际值向目标值靠拢"下发即可，token bucket 调速本身无振荡源；要避免的是需求估计的噪声，EWMA + 每周期一次重算已足够，无需更快。

### 4.3 需求不可知下的退化

若不想测需求，最简实现是"平均分配 R/n"，但违背"用得少的流不该被压"的语义（小需求流拿到用不掉的配额、大需求流饿死残余带宽）。需求感知只需几十字节的每流计数器，值得做。

## 5. 与 TCP 拥塞控制 / 回压的互动

- **限速的传导机制是回压，不是丢包，也不是"不发 ACK"**。用户态要做的只是"少读/慢读"：中继停止从 srcConn 读取 → 中继侧 TCP 接收缓冲填满 → 通告窗口（rwnd）收缩至 0 → 源端 TCP 发送速率被窗口钉在配额附近。窗口管理由内核协议栈自动完成，应用无需（也不应）干预 ACK。
- **千万不要用"丢已读数据"或"读了不转发"实现限速**：那等价于在 TCP 之上再造一条有损链路，触发对端重传、浪费带宽且可能把对端拥塞窗口打崩。
- **拥塞控制分层**：对端 TCP 的 cwnd 仍按它看到的路径 RTT/丢包自由增长，但最终发送速率 = min(cwnd 允许值, rwnd 允许值)；中继回压通过 rwnd 起作用，两层互不干扰。这与 sch_fq 用 pacing 延迟而非丢包整形的思路一致。
- **突发控制**：token bucket 的 burst 决定瞬时突发放行量。burst 过大会让多流突发在中继出口叠加（瞬时超限、出口缓冲膨胀→RTT 抖动/bufferbloat）；建议 burst ≈ 1–2 个拷贝块（32–128KB）或配额 × T 的小倍数，不要设成 BDP 级。
- **双向独立**：两个转发方向各挂各的桶、各自回压；若中继上下行不对称，可为两方向设不同 R。
- **非 TCP 传输**（如中继段本身跑 QUIC/自定义协议）：机制不变，回压改由该协议的流控窗口传导；限速点抽象不变。
- **可选的总量保险**：全局桶（路线 C）保证 N 个独立回压回路在任何时刻的总速率不超过 R，弥补配额滞后与 burst 叠加——低成本高价值，建议默认带上。

## 6. CPU 与内存开销量级

### 路线 A（推荐）

- 每流状态：一个 `rate.Limiter`（mutex + 几个字段，~100B）+ 需求统计计数器（~50B）。万级流也只有 MB 级。
- 数据面：每个拷贝块一次 `WaitN`/`ReserveN`（一次锁 + 时钟读，约 50–150ns；用 `ReserveN` 提前取令牌可无睡眠路径）。块大小取 32–128KB 摊薄；10Gbps 单流约每 ms 一次调用，开销可忽略。
- 控制面：O(n log n) progressive filling 每 T 秒一次；n=1000、T=200ms 时每次 <100µs。
- 代价注意：包装 Reader 后 `io.Copy` 退化为通用拷贝路径，失去 Linux TCP→TCP splice 零拷贝机会——这是接受范围内的一次 memcpy（~32KB 拷贝在 ns/µs 量级）。

### 路线 B

- 每流队列缓冲（有界，如 64–256KB/流）→ 内存随流数线性增长，百流即数十 MB；
- 每帧一次 enqueue + 出队锁/atomic，且数据额外过一次中央缓冲；
- 单 egress worker 在高吞吐下需要无锁队列或分片才能不成为瓶颈。

## 7. 候选方案对比

| 方案 | 公平语义 | 收敛 | 数据面改动 | CPU | 内存 | 与 TCP 协作 | 结论 |
|---|---|---|---|---|---|---|---|
| A. 每流动态 token bucket + 周期 max-min 重算 | max-min（周期粒度） | ~0.5–2s | 无（包一层 Reader） | 极低（每块一次取令牌） | ~150B/流 + io.Copy buf | 靠回压，天然 | **推荐** |
| A'. A + 全局桶（HTB 两层） | max-min + 总量硬顶 | 同上 | 无 | 极低 +1 次取令牌 | 同上 | 同上 | 推荐的加固项 |
| B. per-flow FIFO + DRR 出口调度 | max-min（逐包精确） | 即时（队列层面） | 改模型：中央队列+worker | 中（锁/拷贝/调度热点） | 每流 64–256KB 缓冲 | 需自建队列级回压 | 仅在逐帧转发或需突发隔离时采用 |
| B'. SFQ 式哈希分桶 | 概率性公平 | 即时 | 同 B | 低 | 固定桶数 | 同 B | 无记账语义，不能表达配额，不推荐 |
| WFQ/PGPS | 近似完美 | — | 同 B | O(log n)/包 | 同 B | 同 B | 用户态字节流上成本>收益，不推荐 |
| 固定每流 cap（反模式） | 非弹性 | — | 无 | 极低 | 低 | 好 | 不满足"空闲可用满"，仅作安全上限 |

## 8. 推荐结论

1. **主机制：路线 A'。** 每转发流每方向一个 token bucket（`x/time/rate` 起步，热路径上如需可换无锁自实现），速率由集中式配额重算器每 100–500ms 用需求感知的 progressive filling 重算；叠加一个 cap=R 的全局桶做总量保险。
2. **限速全靠回压**：在读侧（或写侧）`WaitN` 减速即可，让用户态不碰丢包、不碰 ACK；burst 设小（1–2 拷贝块）抑制突发叠加。
3. **收敛目标**：新流即时获乐观份额、一个重算周期内归位，秒级内整体稳定——吞吐公平目标下足够。
4. **接口预留**：把"配额重算器 → 每流限速器"抽象成接口，未来若中继改逐帧转发或需要亚 RTT 突发隔离，可在同接口下换 DRR 出口调度（路线 B），不必推翻。
5. **明确不做**：SFQ/WFQ 不作主机制；不做用户态丢包限速；不引入中央队列（除非走路线 B）。

## 9. 参考文献（一手来源）

1. J. M. Jaffe, "Bottleneck Flow Control," *IEEE Transactions on Communications*, 29(7):954–962, 1981. doi:10.1109/TCOM.1981.1095081
2. D. Bertsekas, R. Gallager, *Data Networks*, 2nd ed., Prentice-Hall, 1992（max-min fairness 与 progressive filling，§6）.
3. J. Nagle, "On Packet Switches with Infinite Storage," RFC 970, 1985.
4. A. Demers, S. Keshav, S. Shenker, "Analysis and Simulation of a Fair Queueing Algorithm," *Proc. ACM SIGCOMM*, 1989. http://education.sigcomm.org/papers/FQ1989.pdf
5. P. E. McKenney, "Stochastic Fairness Queueing," *Proc. IEEE INFOCOM '90*, 1990. doi:10.1109/INFCOM.1990.91316
6. M. Shreedhar, G. Varghese, "Efficient Fair Queueing Using Deficit Round Robin," *Proc. ACM SIGCOMM*, 1995, pp. 231–242. doi:10.1145/217391.217453
7. M. Devera, Linux HTB：`net/sched/sch_htb.c` 与 HTB manual, http://luxik.cdi.cz/~devik/qos/htb/ （`man tc-htb`）.
8. E. Dumazet, Linux `sch_fq`：`net/sched/sch_fq.c`（per-flow pacing + RR dequeue）；`man tc-fq` / `man tc-sfq`（man7.org）.
9. `golang.org/x/time/rate`：token bucket `Limiter`，https://pkg.go.dev/golang.org/x/time/rate
10. `github.com/mxk/go-flowrate`：Go 分层 token bucket 实现，https://github.com/mxk/go-flowrate
