// 路径策略层（规格书 §8「内建默认自动策略 + 可插拔策略钩子」，
// issue #24）。
//
// 定位：策略是「流」级机制——每条聚合流按周期把自身观测快照喂给
// Policy.Decide，输出动作（加路径地址集 / 摘路径 / 不动）经框架
// 护栏执行。挂接点选在 Stream 的周期 tick（发送泵 onTick）而非
// Aggregator 独立 ticker，理由：
//   - 触发源正是逐路径指标（est_rate/srtt/inflight，#20 已就位）与
//     发送积压，两者都活在 Stream.mu 下——就近取材免一层聚合器路由；
//   - 节拍复用 telemetryInterval，不另起计时器；
//   - Decide 是应用代码、决策执行含拨号 I/O——一律挪出发送泵：
//     评估在独立 goroutine 进行（policyEvalActive 去抖，同流串行），
//     拨号在又一 goroutine（policyDialActive 串行化）。
//
// 默认自动策略（defaultPolicy，无状态、可多流共享）：
//   - 补路径：发送积压持续 ≥policyAddMinBacklog 视为「需求>供给」
//     （本层无法区分「对端应用不读」与「链路受限」，两者都表现为
//     窗口顶满+待发积压；代价受上限/冷却约束可控）→ 按 §3.3 偏好序
//     补直连路径。UDP 系受限场景下候选排序天然先出 TCP/WS（P0），
//     即决策票 #12 的「自动降级受限传输补替代传输」。
//   - 摘路径（保守）：仅摘本策略自动补上的、速率份额长期垫底
//     （<policyLowSharePct 且持续 ≥policyRemoveMinLowFor）的路径，
//     且摘除后至少保留 policyRemoveKeepPaths 条、当前无积压——
//     手动加的路径与活跃路径一律不动。硬失效路径由数据面
//     dropPath 即刻处理，不属策略职责。
//   - 中继路径不在自动策略范围：候选地址剔除 /p2p-circuit 形态
//     （reservation 占中继配额，不该被自动策略静默消耗），中继路径
//     须显式 AddRelayPath（规格 §4.3 按需协调）。
//
// 框架护栏（对一切 Policy 生效，防指标抖动造成加/摘振荡）：
//   - 加路径：单次决策至多尝试 policyAddTryN 个候选、同一时刻只许
//     一笔在途拨号、成功后冷却 policyCooldown、连续失败指数退避
//     （policyAddMaxShift 封顶）、路径总数硬顶 policyMaxPaths；
//   - 摘路径：跳过热身期（policyPathWarmup）内的新路径；最后一
//     条存活路径由 RemovePath 本身拒绝。
package netacc

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// 策略层常量（默认策略阈值 + 框架护栏参数）。
const (
	// policyMaxPaths 是自动加路径的路径总数硬顶（含手动/首路径）。
	// 取 4：规格 §3.3 有意义的路径形态就 TCP/WS/QUIC 三类上下，
	// 更多路径徒增连接开销而边际收益递减。
	policyMaxPaths = 4

	// policyAddMinBacklog 是「需求>供给」积压须持续的时长才触发
	// 补路径——短于它的瞬时写入突发不算带宽缺口（防抖第一级）。
	policyAddMinBacklog = 500 * time.Millisecond

	// defaultPolicyCooldown 是两次自动拨号的最小间隔基数：成功后
	// 亦生效（让新路径先跑出实测指标再评下一拍）；连续失败按
	// 2^fails 指数退避、policyAddMaxShift 封顶（~32s）。
	defaultPolicyCooldown = time.Second
	policyAddMaxShift     = 5

	// policyDialTimeout 是单笔自动拨号的兜底超时；流终态亦即时取消。
	policyDialTimeout = 15 * time.Second

	// policyPathWarmup 是新路径热身期：期内速率天然偏低，不参与
	// 垫底摘除评估、也不被摘除（防即挂即摘振荡）。
	policyPathWarmup = 2 * time.Second

	// policyLowSharePct 是「垫底」判据：路径有效速率份额低于聚合
	// 速率此比例（须持续 policyRemoveMinLowFor）才可被默认策略摘除。
	policyLowSharePct = 0.05

	// policyRemoveMinLowFor 是垫底须持续的时长；摘除是慢操作，
	// 阈值取大防指标抖动误摘活跃路径。
	policyRemoveMinLowFor = 3 * time.Second

	// policyRemoveKeepPaths 是默认策略摘除后至少保留的路径数——
	// 首路径之外仍留一条冗余，不为省一条连接冒单点风险。
	policyRemoveKeepPaths = 2

	// policyAddTryN 是单次决策最多尝试的候选地址数（按序拨至首个
	// 成功）；每拍至多补挂一条，要更密集由后续各拍冷却后继续。
	policyAddTryN = 3
)

// Policy 是流级路径策略的公开钩子（规格书 §8 可插拔策略）：输入流
// 观测快照，输出本拍动作。
//
// 同一 Policy 实例可被多条聚合流共享：框架保证同一流内串行调用
// Decide，不同流之间可能并发——实现应保持无状态（快照已携带
// 冷却/积压/垫底等防抖所需的全部持续时长），或自行按
// PolicySnapshot.StreamID 分键并内部同步。
//
// Decide 在发送泵周期 tick 触发的独立 goroutine 内执行，仍应快速
// 返回：它阻塞的只是一拍评估，不是数据面。
type Policy interface {
	Decide(snap PolicySnapshot) PolicyDecision
}

// PolicyFunc 是 Policy 的函数适配器，便于测试与轻量自定义。
type PolicyFunc func(PolicySnapshot) PolicyDecision

// Decide 实现 Policy。
func (f PolicyFunc) Decide(snap PolicySnapshot) PolicyDecision { return f(snap) }

// PolicyPath 是一条存活路径的策略视图：观测快照 + 框架维护的垫底时长。
type PolicyPath struct {
	PathInfo
	// LowShareFor 是该路径速率份额持续低于 policyLowSharePct 的时长
	// （0 = 当前不垫底）；聚合速率为 0 或路径在热身期内恒不计。
	// 框架按时间戳而非拍数维护，评估跳拍不影响准确性。
	LowShareFor time.Duration
}

// PolicySnapshot 是喂给 Policy.Decide 的一次流观测快照。
type PolicySnapshot struct {
	StreamID [16]byte  // 聚合流标识（agg_stream_id）
	Peer     peer.ID   // 对端
	Now      time.Time // 快照时刻

	// Paths 为全部存活路径的观测视图（含指标与垫底时长）。
	Paths []PolicyPath

	// ---- 需求侧信号 ----
	// PendingBytes 是已 Write 但尚未装帧下发的字节数。收到过窗口通告
	// 后持续 >0 即「需求>供给」——泵不是被窗口就是被在途硬顶顶住。
	PendingBytes int
	// BacklogFor 是 PendingBytes>0 的持续时长（0 = 当前无积压），
	// 默认策略用它作补路径主触发源。
	BacklogFor time.Duration
	// UnackedBytes 是已发未被对端累积确认的字节数。
	UnackedBytes uint64

	// ---- 供给侧候选 ----
	// CandidateAddrs 是 peerstore 中对端地址经 §3.3 偏好排序并剔除
	// 中继形态后的候选集；未被在途路径占用的地址排在前面（同地址
	// 再拨建的是同传输第二条连接，UDP 类整类受限时收益低，仅作兜底）。
	CandidateAddrs []ma.Multiaddr

	// ---- 护栏状态（框架维护，供决策参考；执行端仍兜底）----
	// NextAutoAddAt 之前框架不执行自动拨号（冷却/退避）。
	NextAutoAddAt time.Time
	// AutoAddFails 是连续自动挂接失败计数（退避指数输入）。
	AutoAddFails int
}

// PolicyDecision 是 Decide 的输出动作集；全零值 = 本拍不动。
type PolicyDecision struct {
	// Add 为按序尝试的拨号地址集：框架在护栏允许时按序拨至首个
	// 成功（每拍至多补挂一条）。
	Add []ma.Multiaddr
	// Remove 为建议摘除的 path_id 集：框架逐个校验存在性、热身期
	// 与「至少保留一条」后执行。
	Remove []uint64
}

// DefaultPolicy 返回内建默认自动策略（本文件头注释详述语义）：
// 无状态实现，可安全地被多条聚合流共享。
func DefaultPolicy() Policy { return defaultPolicy{} }

// defaultPolicy 即规格 §8/决策票 #12 的内建默认自动策略：无状态——
// 持续时长类判据（积压/垫底/冷却）全部由框架在快照中给出。
type defaultPolicy struct{}

// Decide 实现默认策略：积压持续超阈值 → 补路径（偏好序前 N 个候选）；
// 无积压且路径富余 → 摘除自动补挂且长期垫底者（每拍至多一条——
// 加/摘互斥由 BacklogFor 条件天然保证）。
func (defaultPolicy) Decide(snap PolicySnapshot) PolicyDecision {
	var dec PolicyDecision
	live := len(snap.Paths)
	if snap.BacklogFor >= policyAddMinBacklog &&
		live < policyMaxPaths &&
		!snap.Now.Before(snap.NextAutoAddAt) &&
		len(snap.CandidateAddrs) > 0 {
		n := min(policyAddTryN, len(snap.CandidateAddrs))
		dec.Add = append(dec.Add, snap.CandidateAddrs[:n]...)
	}
	if snap.BacklogFor == 0 && live > policyRemoveKeepPaths {
		for _, pp := range snap.Paths {
			if live-len(dec.Remove) <= policyRemoveKeepPaths {
				break
			}
			if !pp.AutoAdded || pp.LowShareFor < policyRemoveMinLowFor {
				continue
			}
			dec.Remove = append(dec.Remove, pp.ID)
		}
	}
	return dec
}

// ---------- Aggregator / OpenStream 选项 ----------

// WithPolicy 替换本 Aggregator 全部聚合流的默认路径策略（规格书 §8
// 可插拔钩子）。传 nil 关闭自动化——手动 AddPath/RemovePath 不受
// 影响。缺省为 DefaultPolicy()：决策票 #12 resolution 把自动策略
// 列为内建能力但未定默认开关，本库取「默认开但可关」。
func WithPolicy(p Policy) Option {
	return func(o *options) { o.policy = p }
}

// WithStreamPolicy 是 OpenStream 的逐调用策略覆盖（规格书 §8「opts
// 可逐调用覆盖构造默认」）：传 nil 对该条流关闭自动化。
func WithStreamPolicy(p Policy) OpenOption {
	return func(o *openOptions) { o.policy = p }
}

// ---------- Stream 侧执行层 ----------

// runPolicy 执行一拍策略评估：构快照 → Decide → 执行动作。
// 由 onTick 末尾异步触发；policyEvalActive 去抖保证同流串行——
// 上一拍未结（自定义 Decide 慢或快照构建被 mu 排队）则跳过本拍，
// 不堆积评估 goroutine。
func (s *Stream) runPolicy() {
	if s.policy == nil || !s.policyEvalActive.CompareAndSwap(false, true) {
		return
	}
	defer s.policyEvalActive.Store(false)
	now := time.Now()
	snap, ok := s.policySnapshot(now)
	if !ok {
		return
	}
	dec := s.policy.Decide(snap)
	if len(dec.Remove) > 0 {
		s.execPolicyRemove(dec.Remove, now)
	}
	if len(dec.Add) > 0 {
		s.execPolicyAdd(dec.Add, now)
	}
}

// policySnapshot 在 mu 下构出一次观测快照，顺带维护两项防抖计时：
// 积压起始时刻与逐路径垫底起始时刻（都以时间戳记录，跳拍无碍）。
// 流已关返回 ok=false。
func (s *Stream) policySnapshot(now time.Time) (PolicySnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return PolicySnapshot{}, false
	}
	// 积压判据：已拿到对端首个窗口通告且待发队列有未装帧数据——
	// gotWindow 门排除建流初期「还没见到首个窗口」的瞬时积压。
	if s.gotWindow && s.pendingBytes > 0 {
		if s.backlogSince.IsZero() {
			s.backlogSince = now
		}
	} else {
		s.backlogSince = time.Time{}
	}
	// 份额按双向贡献取大：effRate 是本端发送方向的实测，
	// rxRate 是本端收方向的到达速率——一条主要承载入向流量的
	// 路径同样有贡献，误记垫底会被保守摘除规则错杀。
	rates := make([]float64, len(s.paths))
	var aggRate float64
	for i, p := range s.paths {
		rates[i] = max(p.effRate(now), p.rxRate)
		aggRate += rates[i]
	}
	paths := make([]PolicyPath, 0, len(s.paths))
	for i, p := range s.paths {
		// 垫底计时：份额 < policyLowSharePct 且已过热身期才计入；
		// 聚合速率为 0（全员冷启动/失联）时没有垫底基线，一律不计。
		low := aggRate > 0 &&
			rates[i] < aggRate*policyLowSharePct &&
			now.Sub(p.attachedAt) >= policyPathWarmup
		if low {
			if p.lowSince.IsZero() {
				p.lowSince = now
			}
		} else {
			p.lowSince = time.Time{}
		}
		paths = append(paths, PolicyPath{
			PathInfo:    s.pathInfoLocked(p, now),
			LowShareFor: sinceOrZero(p.lowSince, now),
		})
	}
	return PolicySnapshot{
		StreamID:       s.id,
		Peer:           s.peer,
		Now:            now,
		Paths:          paths,
		PendingBytes:   s.pendingBytes,
		BacklogFor:     sinceOrZero(s.backlogSince, now),
		UnackedBytes:   s.sentOff - s.cumAcked,
		CandidateAddrs: s.policyCandidatesLocked(),
		NextAutoAddAt:  s.nextAutoAddAt,
		AutoAddFails:   s.autoAddFails,
	}, true
}

// sinceOrZero 返回 now-t；t 为零值（未开始计时）时返回 0。
func sinceOrZero(t, now time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	return now.Sub(t)
}

// policyCandidatesLocked 生成候选拨号地址集（调用方须持有 mu）：
// peerstore 的对端地址 → 剥 /p2p 后缀、剔除中继形态（见文件头注释）、
// 按 §3.3 偏好排序 → 把「未被在途路径占用」的地址稳定提前。
func (s *Stream) policyCandidatesLocked() []ma.Multiaddr {
	if s.policyAddrsFn != nil {
		return s.policyAddrsFn() // 测试 seam：注入候选地址
	}
	if s.agg == nil {
		return nil
	}
	var addrs []ma.Multiaddr
	for _, a := range s.agg.host.Peerstore().Addrs(s.peer) {
		a = stripP2PSuffix(a)
		if a == nil || PathTransportOf(a) == TransportRelay {
			continue
		}
		addrs = append(addrs, a)
	}
	SortAddrsByPreference(addrs)
	if len(addrs) > 1 {
		used := make(map[string]bool, len(s.paths))
		for _, p := range s.paths {
			if ra, ok := p.remoteAddr().(maAddr); ok {
				used[ra.ma.String()] = true
			}
		}
		var fresh, reused []ma.Multiaddr
		for _, a := range addrs {
			if used[a.String()] {
				reused = append(reused, a)
			} else {
				fresh = append(fresh, a)
			}
		}
		addrs = append(fresh, reused...)
	}
	return addrs
}

// execPolicyAdd 校验护栏后派生一笔自动拨号：按序尝试候选至首个成功。
// 失败经 policyCooldown 的 2^fails 指数退避（policyAddMaxShift 封顶）
// 防「坏地址+持续积压」下的无限重试。
func (s *Stream) execPolicyAdd(addrs []ma.Multiaddr, now time.Time) {
	s.mu.Lock()
	if s.closed || s.policyDialActive ||
		now.Before(s.nextAutoAddAt) || len(s.paths) >= policyMaxPaths {
		s.mu.Unlock()
		return
	}
	s.policyDialActive = true
	s.mu.Unlock()
	go s.policyDial(addrs)
}

// policyDial 是自动拨号执行体（独立 goroutine）：逐候选拨号至首个
// 成功或流终态，随后记账冷却/退避并释放 policyDialActive。
// 成功挂上的路径被标记 auto=true——默认策略只摘自动补挂的路径，
// 不与手动增删打架。
func (s *Stream) policyDial(addrs []ma.Multiaddr) {
	if len(addrs) == 0 {
		// 空决策不记账（不该计入失败退避），仅复位在途标记
		s.mu.Lock()
		s.policyDialActive = false
		s.mu.Unlock()
		return
	}
	var newID uint64
	dialed, aborted := false, false
	for _, addr := range addrs {
		select {
		case <-s.done:
			aborted = true
		default:
		}
		if aborted {
			break
		}
		ctx, cancel := s.policyCtx()
		id, err := s.policyDialFn(ctx, addr)
		cancel()
		if err != nil {
			continue
		}
		newID, dialed = id, true
		break
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policyDialActive = false
	now := time.Now()
	switch {
	case dialed:
		if p := s.pathsByID[newID]; p != nil {
			p.auto = true
		}
		s.autoAddFails = 0
		s.nextAutoAddAt = now.Add(s.policyCooldownDur())
	case aborted:
		// 流终态打断：成败记账已无意义，仅复位在途标记
	default:
		s.autoAddFails++
		shift := min(s.autoAddFails-1, policyAddMaxShift)
		s.nextAutoAddAt = now.Add(s.policyCooldownDur() << shift)
	}
}

// policyCtx 派生单笔自动拨号的 ctx：policyDialTimeout 兜底超时 +
// 随流终态即时取消（防已死流的拨号 goroutine 悬挂）。
func (s *Stream) policyCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), policyDialTimeout)
	go func() {
		select {
		case <-s.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// policyCooldownDur 返回自动拨号冷却基数：cfg 覆盖值优先（测试用），
// 否则取默认 defaultPolicyCooldown。
func (s *Stream) policyCooldownDur() time.Duration {
	if s.cfg.policyCooldown > 0 {
		return s.cfg.policyCooldown
	}
	return defaultPolicyCooldown
}

// execPolicyRemove 执行策略摘除：逐 id 校验存在性、热身期与
// 「至少保留一条存活路径」后调 RemovePath（其内部再兜底校验）。
func (s *Stream) execPolicyRemove(ids []uint64, now time.Time) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var targets []*path
	for _, id := range ids {
		if len(s.paths)-len(targets) <= 1 {
			break
		}
		p := s.pathsByID[id]
		if p == nil || now.Sub(p.attachedAt) < policyPathWarmup {
			continue
		}
		targets = append(targets, p)
	}
	s.mu.Unlock()
	for _, p := range targets {
		_ = s.RemovePath(p.id)
	}
}
