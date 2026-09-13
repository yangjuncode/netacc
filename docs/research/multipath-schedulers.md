# 调研：多径调度算法（MPTCP/MPQUIC）与带宽估计、重传策略

> 对应 GitHub issue #5。术语沿用根目录 CONTEXT.md：聚合流、路径、调度器、重排缓冲。
> 本项目的语境与 MPTCP/MPQUIC 有一个重要差别：我们的"路径"是 libp2p 子流，底下是
> **已经可靠有序**的 TCP/QUIC 流，而不是裸 IP 路径。因此调度器拿不到底层拥塞窗口（cwnd），
> 所有路径指标必须在应用层自行采样。下文每节都会落到这一点。

## 1. 调度算法全景

文献与实现中出现过的主要包级调度算法（packet scheduler）如下。IETF 有一份专门
综述草案 draft-bonaventure-iccrg-schedulers-02，把各算法写成了伪代码，是设计时的
最佳速查表 [SRC-6]。

| 算法 | 规则 | 出处/实现 | 评价 |
|---|---|---|---|
| Round-Robin | 在可用路径间轮转 | CSWS'14 实验表明性能差 [SRC-2]；草案 §4.1 明确不推荐 | 仅作基线 |
| Weighted Round-Robin | 按固定分布表轮转，可加 deficit 计数器 | 草案 §4.2 | 静态权重无法适应带宽变化 |
| Strict Priority | 按路径优先级排序取第一个可用的 | 草案 §4.3 | 适合"计费路径兜底"，不适合聚合吞吐 |
| RTT Threshold | 只用 SRTT 低于阈值的路径 | 草案 §4.4 | 可作其他算法的"护栏"组件 |
| Lowest-RTT-First（minRTT） | 在 cwnd 未堵、不在 recovery 的路径中取 SRTT 最小者 | MPTCP 传统默认；草案 §4.5；NSDI'12 [SRC-3] | 短流低延迟、长流可打满；异构路径下靠"机会重发+惩罚"补救 HoL |
| 冗余发送（ReMP / redundant） | 同一数据在所有（或选定）路径上各发一份 | DEMS 论文提到的 ReMP [SRC-9]；MPQUIC 冗余模式 | 换可靠性/延迟牺牲带宽；适合失效过渡期与超低时延控制消息 |
| BLEST | 发送窗口阻塞估计：若发在慢路径上的数据会导致快路径因收端窗口被占而无法发送，则不发慢路径 | Ferlin et al., IFIP Networking 2016 [SRC-7] | 异构路径下 goodput +12%、伪重传 −80%（论文实测） |
| ECF | Earliest Completion First：比较"现在发慢路径"与"等快路径腾出来"哪个完成更早，必要时主动等待 | Lim et al., SIGMETRICS 2017 [SRC-8] | 流媒体/Web 负载下显著优于 minRTT；带迟滞防抖 |
| DEMS | chunk 边界感知，解耦两条路径分别送一段数据、保证收端同时完成，允许少量冗余首字节 | Guo et al., MobiCom 2017 [SRC-9] | 面向"有明确 chunk 边界的下载"，对纯字节流收益有限 |
| 最短排空时间优先 | `linger_time = 排队字节数 / 平均发送速率`，取最小者；无活动路径时才用 backup 路径 | **Linux 内核现行默认**，net/mptcp/protocol.c `mptcp_subflow_get_send` [SRC-10] | 等价于按实测带宽加权的条带化；内核注释明确说它实现了 BLEST 的 HoL 规避目标 |

要点：

- **MPQUIC 不规定调度算法**。draft-ietf-quic-multipath（截至 -21+）只定义路径管理
  （Path ID、每路径独立 packet number space、PATH_ABANDON 等），§5.5 Packet Scheduling
  只给实现指导，§5.6 Retransmissions 列出三种重传策略但都不强制 [SRC-5]。
  CoNEXT'17 论文实现的是 minRTT 与 round-robin，并用 PATHS 帧向对端交换每路径
  RTT/cwnd 等状态辅助调度 [SRC-4]。
- **Linux MPTCP 的默认调度器已经从"纯 minRTT"演进为"最短排空时间"**：对每个子流计算
  `sk_wmem_queued / avg_pacing_rate`（排队字节按该子流实测 pacing rate 排空所需时间），
  取最小者；代码注释明确写明这是按 BLEST 思路规避快路径 HoL 阻塞 [SRC-10]。
  这实际上就是"带宽比例条带化"的生产级实现：分到的数据量自然正比于各路径实测速率。
- picoquic 是目前唯一主线合入 multipath 的 QUIC 实现（draft-ietf-quic-multipath）；
  其调度入口是 `picoquic_select_next_path_tuple` + `picoquic_prepare_stream_and_datagrams`，
  默认按每路径 cwnd 可用空间分配，并会把 ACK 尽量安排在低延迟路径上回送 [SRC-11]。
  quiche 的 multipath PR #1310、quic-go 的 multipath（issue #3343）截至本文仍未合入主线
  [SRC-12][SRC-13]。
- SCTP 的并发多径（CMT，draft-tuexen-tsvwg-sctp-multipath）历史经验主要贡献是暴露了
  **收端缓冲被慢路径占位**的问题：乱序数据填满接收窗口后快路径也被迫停摆，催生了
  CMT-PF（potentially-failed 状态）等重传/路径判定机制 [SRC-14]。

## 2. 加权条带如何避免慢路径拖累（HoL blocking）

按带宽比例给各路径分帧，若不做任何保护，慢路径上堆积的在途数据会在收端堵住
重排缓冲头部，快路径先到的大量数据只能排队 —— 这就是经典的 HoL blocking。
一手来源里有四条被验证过的对策，建议全部吸收：

1. **权重来自实测排空速率，而非静态配置**。Linux 默认调度器用 `wmem/pace` 最小者优先，
   效果上等价于按各路径 `avg_pacing_rate` 比例分配字节 [SRC-10]。对我们来说，
   `est_rate_i`（见 §3）就是权重；给路径 i 分的字节份额 ∝ est_rate_i。
2. **每路径在途量上限（应用层软 cwnd）**。给路径 i 设在途帧上限
   `inflight_cap_i ≈ k · est_rate_i · srtt_i`（k 略大于 1）。慢路径 srtt 大、est_rate 小，
   cap 天然小，堆积有硬上限；这等价于把 MPTCP 的"cwnd 堵了就换下一条"移植到应用层。
3. **BLEST/ECF 式完成时间预测**：分帧时估算"此帧现在发慢路径的到达时刻"vs
   "等快路径排空后再发的到达时刻"，若后者更早则不发慢路径（ECF）；或估计此帧是否会
   成为阻塞收端窗口的队头（BLEST）[SRC-7][SRC-8]。Linux 的 linger_time 判据是该思想的
   简化落地。
4. **机会重发 + 惩罚（opportunistic reinjection + penalization）**：快路径有空但收端
   窗口被慢路径在途数据顶住时，把队头未确认帧**复制**到快路径重发；触发该机制即说明
   慢路径拖后腿，同时下调其权重/cap（MPTCP 是 cwnd/2）[SRC-3][SRC-6 §4.5]。
   这是最便宜、最鲁棒的兜底，强烈建议实现。

## 3. 逐路径带宽与 RTT 估计

**被动采样为主，主动探测为辅。**

- **RTT**：每帧携带全局序号 + 发送时间戳；对端在数据 ACK 里回显。按 RFC 6298 的
  EWMA 维护 `srtt = 7/8·srtt + 1/8·r` 与 rttvar [SRC-15]。注意我们的"RTT"包含底层
  传输的排队延迟——这不是缺陷，它正是把拥塞折算进调度的信号（等价于 TCP 把排队
  延迟计入 RTT）。建议同时维护 `min_rtt`（路径传播延迟基线）与 `srtt`（含排队）。
- **带宽**：BBR 式 delivery-rate 采样——用 ACK 计算 `delivered_bytes / Δt`，取窗口
  最大值或 EWMA 得 `est_rate` [SRC-16]。Linux MPTCP 直接复用每子流 `sk_pacing_rate`
  的 EWMA（`avg_pacing_rate`）[SRC-10]；我们位于应用层，应自行在 ACK 回执上算
  goodput。采样周期建议以 srtt 为单位（每 srtt 一个样本），并用 ACK 聚合
  （一条 ACK 确认一段序号区间）摊薄开销。
- **主动探测**只用于两种情形：a) 路径空闲但有数据待调度、est_rate 已过期（冷启动或
  恢复）；b) 路径疑似降级时发小 PING 帧验证 RTT 与存活。类比自己 QUIC 的
  PATH_CHALLENGE 与 picoquic 的路径验证 [SRC-5][SRC-11]。不建议常态化发包探测——
  数据流本身已经提供了足够采样密度，常探测会偷带宽且污染估计。
- **跨端信息共享**可选：MPQUIC 的 PATHS/UNIFLOWS 帧向对端通报每路径 RTT、cwnd、
  字节与丢包统计 [SRC-4]；我们可用控制帧周期性通报各路径的收端观测速率，帮助发送端
  校准 est_rate（对端视角的到达速率比发端自测更准）。

## 4. 路径失效时在途帧的重传策略

**序号空间映射在本设计中几乎是免费的**：全局序号写在帧头（载荷层），不在底层
传输序号里，因此任何一帧都可以原样换路径重发，收端按全局序号去重即可。对比之下，
MPTCP 需要 DSS 选项把 64 位 DSN 映射到各子流的 32 位 SSN，且 RFC 8684 §3.3.6 要求
**为兼容中间盒仍须在原子流上重传原数据**——我们没有 middlebox 约束，这条可以省掉
[SRC-1]。QUIC 的做法同样是解耦：重传的 STREAM 帧换新包号、可换路径、甚至可重新
分片（§5.6 明确说 STREAM 帧边界在重传时不必保留）[SRC-5]。

draft-ietf-quic-multipath §5.6 列出的三种重传策略直接对应我们的选择 [SRC-5]：

- a) 同路径重发——仅用于路径未失效时的超时重发；
- b) **换路径重发（推荐默认）**：路径写失败/流重置/连续 N 次未收到 ACK 时，把该路径上
  全部未确认帧按原全局序号重注入其余路径。这正是 Linux `__mptcp_retransmit_pending_data`
  的做法：路径 failover 时把整条 MPTCP 级 rtx 队列清掉 `already_sent` 标记重发 [SRC-10]；
  也是 draft 对 PATH_ABANDON 的规定（lost frames SHOULD be retransmitted on a different
  path）[SRC-5]。
- c) 多路径复制——草案明确不推荐常规使用（带宽浪费）；只保留为可选的低时延模式
  （等价 ReMP/冗余调度）。

工程细节：

- **失效判定**：写错误/stream reset 立即失效；软失效用定时器——帧发出后
  `k·srtt_path` 未 ACK 即视为可疑，先机会重发到快路径（§2 第 4 条），路径 RTO 到期才
  正式摘除。MPTCP 在"子流重传超上限或 ICMP 错误"时才宣告子流失败 [SRC-1 §3.3.6]。
- **帧允许重新切分**：若目标路径发送缓冲/MTU 更小，允许把一个大帧拆成几个同序号的
  子帧（序号空间用 `seq + offset + len` 表达即可），与 QUIC 重传 STREAM 帧可重新
  分片同理 [SRC-5]。
- **发送缓冲保留**：数据必须在"连接级 ACK 且其走过的所有路径都已确认"之前保留在
  发送缓冲（RFC 8684 §3.3.6 的同款要求），否则无法换路径重发。

## 5. 收端重排缓冲的界定

RFC 8684 §3.3.4 给出了至今仍是标准的界定 [SRC-1]：

- **下界**：`max_i(bw_i · rtt_i)`——至少容纳单条最快路径的 BDP，否则无法跑满；
- **紧上界**：`max_i(rtt_i) · Σ_j bw_j`——即使最慢路径上发生丢包需要一次快速重传，
  其余路径仍可全速推进；路径 RTO 的场景可能还不够，RFC 明确说"重传策略与缓冲
  大小的关系留作未来研究"。

实现侧的参考：

- MPTCP 是**连接级单一接收窗口**，所有子流共享；收端用 DATA_ACK + window 决定连接级
  是否收包 [SRC-1 §3.3.4]。我们同样是单一重排缓冲 + 全局窗口背压，结构一致。
- quic-go 默认值可作量级参考：流级初始窗口 512KB / 上限 6MB，连接级上限 15MB，
  且内置接收窗口自动调大算法（一个 RTT 内吃掉窗口的大部分即扩容）[SRC-13]。
- picoquic 的实现经验：ACK 尽量走低延迟路径回送，能显著压缩缓冲被占用的时间
  [SRC-11]。

**建议**：`reorder_buf = clamp(Σ est_rate_i · max srtt_i, min_buf, max_buf)`，配合
连接级通告窗口对发送端施加背压；min_buf 取最大单路径 BDP 下界，max_buf 作为内存
保护硬顶（如 16–64MB 可配置）。注意：§2 的 BLEST/ECF 式调度与机会重发会直接降低
所需的乱序驻留量，缓冲给得越大只是越耐突发，不能替代调度。

## 6. 候选调度方案对比与推荐

| | 候选 A：minRTT + 机会重发 + 惩罚 | 候选 B：最短排空时间优先（加权条带）【推荐】 | 候选 C：B + ECF 式等待判定 |
|---|---|---|---|
| 规则 | 可用路径中取 srtt 最小；窗口被顶时复制队头帧到快路径并惩罚慢路径 | 选 `(queued+len)/est_rate` 最小的路径，配合每路径 inflight cap | 同 B，且慢路径帧若"等快路径更早完成"则主动不发 |
| 需要估计的量 | srtt | est_rate（间接含 RTT） | est_rate + srtt + 队列占用 |
| HoL 防护 | 事后补救（重发） | 事中避免（cap+权重） | 事前预测，最强 |
| 异构路径表现 | 中（NSDI'12 已验证，但靠补救） | 好（Linux 生产默认，BLEST 思路） | 最好（ECF 实测优于 minRTT） |
| 实现复杂度 | 低 | 低-中 | 中 |
| 适用 | 兜底/首版最简实现 | 吞吐优先默认策略 | 时延敏感或高异构场景的可选增强 |

**推荐**：默认实现候选 B——它恰好就是 CONTEXT.md 里"吞吐优先加权条带（RTT 感知）"
的标准化实现形式，与 Linux 现行默认调度器同构，代码量小、无静态权重、权重随实测
速率自适应；同时把候选 A 的"机会重发+惩罚"作为 HoL 兜底机制并入 B（这是正交组件，
NSDI'12 与内核代码都这么做）；候选 C 作为后续优化项，在实测异构路径下仍有 HoL 时
再加。冗余发送仅保留为：a) 路径失效过渡期加速重传；b) 可配置的低时延模式。

## 参考来源

- [SRC-1] RFC 8684, "TCP Extensions for Multipath Operation with Multiple Addresses (MPTCPv1)",
  2020. §3.3.1 DSN/SSN 映射；§3.3.4 收端窗口与缓冲上下界；§3.3.6 重传与跨子流重注入。
  https://www.rfc-editor.org/rfc/rfc8684.html
- [SRC-2] C. Paasch, S. Ferlin, Ö. Alay, O. Bonaventure, "Experimental Evaluation of
  Multipath TCP Schedulers", ACM SIGCOMM CSWS 2014.（RR 性能差的实验依据）
- [SRC-3] C. Raiciu et al., "How Hard Can It Be? Designing and Implementing a Deployable
  Multipath TCP", USENIX NSDI 2012.（minRTT + 机会重发 + cwnd 惩罚）
- [SRC-4] Q. De Coninck, O. Bonaventure, "Multipath QUIC: Design and Evaluation",
  ACM CoNEXT 2017. https://multipath-quic.org/conext17-deconinck.pdf
  （minRTT/RR 调度器、PATHS 帧交换每路径指标）
- [SRC-5] draft-ietf-quic-multipath（最新版，-21+），"Multipath Extension for QUIC".
  §2.3 每路径独立 packet number space；§5.5 Packet Scheduling（不规定算法）；
  §5.6 Retransmissions（同路径/异路径/复制三策略、STREAM 帧重传可重分片、
  PATH_ABANDON 的 lost frames SHOULD 换路径重发）。
  https://datatracker.ietf.org/doc/html/draft-ietf-quic-multipath
- [SRC-6] draft-bonaventure-iccrg-schedulers-02, "Multipath schedulers", 2021.
  RR/WRR/StrictPriority/RTT-Threshold/LowestRTT-First/组合调度器的伪代码与取舍。
  https://datatracker.ietf.org/doc/html/draft-bonaventure-iccrg-schedulers-02
- [SRC-7] S. Ferlin, Ö. Alay, O. Mehani, R. Boreli, "BLEST: Blocking Estimation-based
  MPTCP Scheduler for Heterogeneous Networks", IFIP Networking 2016.
  （goodput +12%，伪重传 −80%）
- [SRC-8] Y.-S. Lim, E. M. Nahum, D. Towsley, R. J. Gibbens, "ECF: An MPTCP Path
  Scheduler to Manage Heterogeneous Paths", ACM SIGMETRICS 2017.
- [SRC-9] Y. Guo, A. Nikravesh, Z. M. Mao, F. Qian, S. Sen, "Accelerating Multipath
  Transport Through Balanced Subflow Completion" (DEMS), ACM MobiCom 2017.
- [SRC-10] Linux 内核 net/mptcp/protocol.c：`mptcp_subflow_get_send`（linger_time =
  sk_wmem_queued/avg_pacing_rate 最小者优先，注释声明按 BLEST 思路避免 HoL）、
  `mptcp_subflow_get_retrans`（重注入选无积压子流）、
  `__mptcp_retransmit_pending_data`（failover 时整条 rtx 队列清 already_sent 重发）；
  net/mptcp/sched.c（调度器注册/BPF 可替换框架）。torvalds/linux。
- [SRC-11] picoquic（private-octopus/picoquic）multipath 实现与 C. Huitema 的实现笔记
  "Implementing Multipath in QUIC", 2021-01-26.
  https://www.privateoctopus.com/2021/01/26/implementing-multipath-in-quic/
  （按每路径 cwnd 调度、ACK 走低延迟路径回送）
- [SRC-12] cloudflare/quiche PR #1310 "Multipath support with non-zero length
  Connection IDs"（截至本文仍为 open，未合入主线）。
- [SRC-13] quic-go：流控默认值与自动调窗（internal/protocol/params.go：初始流窗口
  512KB、流上限 6MB、连接上限 15MB；docs/quic/flowcontrol）；multipath 支持跟踪于
  quic-go/quic-go issue #3343（未合入）。
- [SRC-14] draft-tuexen-tsvwg-sctp-multipath, "Load Sharing for the Stream Control
  Transmission Protocol"（SCTP CMT 负载共享扩展；收端缓冲阻塞与 PF 状态经验）。
- [SRC-15] RFC 6298, "Computing TCP's Retransmission Timer"（SRTT/RTTVAR EWMA 公式）。
- [SRC-16] N. Cardwell et al., "BBR: Congestion-Based Congestion Control", ACM Queue
  14(5) / CACM 60(2), 2017（delivery-rate 采样估计可用带宽）。
