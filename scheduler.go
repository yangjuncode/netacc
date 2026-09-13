// 调度器与逐路径指标（规格书 §5，issue #20）。
//
// 本文件实现规格 §5.1 的「最短排空时间优先」调度与 §5.2 的逐路径
// 指标采样，并把调度器收敛为一条内部边界：
//
//	输入 = 各存活路径的实时指标（path 上的 srtt/est_rate/inflight 等，
//	       由 stream 的采样/记账代码在 mu 下更新）；
//	输出 = 分帧决策（本帧走哪条路径，或本轮不发）。
//
// 指标来源（§5.2）：
//   - RTT：DATA 帧携带 send_ts，ACK 回显 ts_echo + ts_path（被回显
//     DATA 走过的路径，见 frame.go），发送端按 RFC 6298 EWMA 维护
//     srtt/rttvar 并另记 min_rtt。备选方案「PING/TELEMETRY 帧做逐
//     路径探测」未被选为主机制：数据期 ACK 密度远高于任何探测
//     周期，且无需额外套餐；PING 只留作冷启动/指标过期探测。
//   - est_rate：BBR 式 delivery-rate 采样——ACK 推进 cum 时按
//     sendSeg.path 把新确认字节记到对应路径，每 ≥max(srtt, 下限)
//     出一个 delivered/Δt 样本；收端 TELEMETRY 帧周期回报其观测
//     的各路径到达速率作独立校准源。
package netacc

import (
	"math"
	"time"
)

// 调度与采样常量。
const (
	// inflightCapK 是每路径在途硬顶系数：cap = k·est_rate·srtt
	// （规格书 §5.1「k 略大于 1」）。取 1.5：给速率/RTT 估计抖动留
	// 余量，又不至于让慢路径堆积超过约 1.5 个 BDP。
	inflightCapK = 1.5

	// minInflightCap 是在途硬顶下界：est_rate 或 srtt 尚未采出/极小
	// 时，仍保证至少两个满帧的流水线空间——cap=0 会让该路径永远
	// 被调度跳过，既不能积累样本又形成隐性死锁。
	minInflightCap = 2 * maxFramePayload

	// coldRateBps 是 est_rate 未知/过期时的名义速率（冷启动策略）。
	// 所有 cold 路径按同一名义速率比较排空时间 → inflight 少者
	// 优先 → 自然均等分摊；实测样本到达后自动过渡到按带宽加权。
	// 取值只影响冷启动期，宜保守偏小（慢的冷路径别被过度灌包）。
	coldRateBps = 256 << 10 // 256KiB/s

	// rateSampleFloor 是 delivery-rate 采样间隔下限（§5.2「每 srtt
	// 一样本」）：loopback 等微秒级 RTT 路径上逐 ACK 采样会把
	// 单帧交付放大成速率尖峰，聚合至少一个下限窗口再出样本。
	rateSampleFloor = 2 * time.Millisecond

	// estRateStaleFloor 是 est_rate 保鲜期下界；实际保鲜期取
	// max(4·srtt, 本值)。过期样本不再参与调度（回退冷启动名义
	// 速率），并有发送需求时触发 PING 探测（§5.2 主动探测用途）。
	estRateStaleFloor = 500 * time.Millisecond

	// defaultTelemetryInterval 是收端 TELEMETRY 回报与窗口重算的
	// 默认周期（§5.2 周期回报 + §6 窗口联动）。
	defaultTelemetryInterval = 200 * time.Millisecond

	// peerRateTTL 是对端 TELEMETRY 观测速率的有效期：超时未再收
	// 到回报即作废，防陈旧观测永久抬高 est_rate。
	peerRateTTL = 3 * defaultTelemetryInterval

	// rttStaleAfter 是 RTT 样本保鲜期：超过则视路径指标失联，
	// 有发送需求时用 PING 重新探测（§5.2「疑似降级」的保守判据）。
	rttStaleAfter = 2 * time.Second

	// pingMinInterval 是同一路径两次主动 PING 的最小间隔（节流：
	// PING 只兜底采样缺口，不常态化——数据流本身已提供采样密度，
	// 常探测会偷带宽且污染估计，调研 §3）。
	pingMinInterval = 500 * time.Millisecond

	// rxRate 采样（收端到达速率）的窗口门槛：DATA 帧是离散到达的，
	// 采样窗太小会把「每窗一帧」误测成 frameLen/interval 的虚高速率
	// ——攒够 rxSampleMinBytes 或 rxSampleMaxWindow 才出一个样本，
	// 不足时字节滚入下一窗。
	rxSampleMinBytes  = 2 * maxFramePayload
	rxSampleMaxWindow = time.Second
)

// scheduler 是发送泵的分帧决策边界（规格书 §8：内部接口不公开）。
// 实现只做无副作用的纯决策——路径指标只能由 stream 的采样/记账
// 代码更新，调度器不写任何状态。调用方须持有 s.mu。
type scheduler interface {
	// pickData 为长度 frameLen 的 DATA 帧选路；返回 nil 表示本轮
	// 没有路径可用（全部在途顶满），等 ACK 释放 inflight 后再唤醒。
	pickData(paths []*path, frameLen int, now time.Time) *path
	// pickAck 为 ACK/PING/TELEMETRY/PATH_DROP 等控制帧选低延迟
	// 路径（规格书 §4.4：ACK 优先走低延迟路径回送）。
	pickAck(paths []*path, now time.Time) *path
}

// minDrainScheduler 最短排空时间优先（规格书 §5.1，与 Linux MPTCP
// mptcp_subflow_get_send 同构）：本帧若走路径 i，其队列排空耗时
//
//	drain_i = (inflight_i + len) / est_rate_i
//
// 取最小者——效果上等价于按各路径实测带宽比例分流，自带 BLEST 式
// HoL 规避；叠加每路径在途硬顶 inflight_cap_i = k·est_rate_i·srtt_i，
// 顶满的路径本轮跳过，慢路径堆积有硬上限。
//
// 与内核版的差异：我们没有 per-path 发送队列——数据在选定路径后
// 才从全局待发队列装帧，因此「排队量」用该路径在途未确认字节
// inflight_i 表达（排干它≈其末尾字节到对端的剩余时间）。机会重发
// + 惩罚、ECF 式等待判定分别为 #23 与后续增强项。
type minDrainScheduler struct{}

// pickData 实现最短排空时间优先。同分确定性打破（避免抖动）：
// inflight 少者优先，再并列取 path_id 小者。
func (minDrainScheduler) pickData(paths []*path, frameLen int, now time.Time) *path {
	var best *path
	bestDrain := math.Inf(1)
	for _, p := range paths {
		if int64(p.inflight) >= p.inflightCap(now) {
			continue // 在途硬顶：本轮跳过该路径
		}
		rate := p.effRate(now)
		if rate <= 0 {
			rate = coldRateBps // 冷启动：名义速率参与 → 均等分摊
		}
		drain := float64(p.inflight+frameLen) / rate
		if drain < bestDrain || (drain == bestDrain &&
			(p.inflight < best.inflight ||
				(p.inflight == best.inflight && p.id < best.id))) {
			best, bestDrain = p, drain
		}
	}
	return best
}

// pickAck 选 RTT 最小的路径回送控制帧；无 RTT 样本的路径排在
// 有样本者之后，全部无样本时退化为取首条（控制帧必须发得出）。
func (minDrainScheduler) pickAck(paths []*path, now time.Time) *path {
	var best *path
	for _, p := range paths {
		switch {
		case best == nil:
			best = p
		case p.hasRTT && (!best.hasRTT || p.srtt < best.srtt):
			best = p
		}
	}
	return best
}

// ---------- 逐路径指标采样（path 的方法，调用方均须持有 s.mu） ----------

// noteRTT 喂入一个本路径 RTT 样本，按 RFC 6298 维护 srtt/rttvar
// 并另记 min_rtt（§5.2）。样本来源有二：
//   - ACK 回显（ts_echo + ts_path 归属）：测得「DATA 正向走该路径 +
//     ACK 走当前最快路径回」的组合时延——与 MPTCP「ACK 可跨子流
//     回送」同构，正是调度关心的「发到该路径多久有回响」；我们的
//     RTT 含底层传输排队延迟，这正是把拥塞折算进调度的信号（调研 §3）；
//   - PING 应答：同路径往返回显，是干净的单路径双向样本，用于
//     无数据期的冷启动/过期探测。
//
// 跨路径重发不引入 Karn 歧义：ts_echo/ts_path 归属的是对端实际
// 收到那份拷贝的发送时刻与路径，样本天然落在先到路径上。
func (p *path) noteRTT(r time.Duration, now time.Time) {
	if r < 0 {
		return // 时钟回拨或伪造回显：丢弃（r=0 是合法的极速样本）
	}
	if !p.hasRTT {
		p.srtt, p.rttvar, p.minRTT = r, r/2, r
		p.hasRTT = true
	} else {
		d := p.srtt - r
		if d < 0 {
			d = -d
		}
		p.rttvar = (3*p.rttvar + d) / 4
		p.srtt = (7*p.srtt + r) / 8
		if r < p.minRTT {
			p.minRTT = r
		}
	}
	p.lastRTTAt = now
}

// onDelivered 记账 n 字节在本路径上被对端累积确认：扣在途量，
// 并按 BBR 式 delivery-rate（delivered/Δt）更新 est_rate。
// 采样间隔取 max(srtt, rateSampleFloor)（§5.2 每 srtt 一样本 +
// 防微秒级 Δt 噪声）；窗口未满时交付量滚入下一窗。
func (p *path) onDelivered(n int, now time.Time) {
	p.delivered += uint64(n)
	p.inflight -= n
	if p.lastRateAt.IsZero() {
		// 首个采样窗从路径挂接起算
		p.lastRateAt = p.attachedAt
		if p.lastRateAt.IsZero() {
			p.lastRateAt = now
		}
	}
	iv := p.srtt
	if iv < rateSampleFloor {
		iv = rateSampleFloor
	}
	dt := now.Sub(p.lastRateAt)
	if dt < iv {
		return
	}
	d := p.delivered - p.rateMark
	if d == 0 {
		// 窗口内零交付：不算样本（不拉低 est_rate），只推进窗口——
		// 路径真停摆由 est_rate 保鲜期过期兜底。
		p.lastRateAt = now
		return
	}
	sample := float64(d) / dt.Seconds()
	p.estRate = filterRate(p.estRate, sample)
	p.hasRate = true
	p.rateMark = p.delivered
	p.lastRateAt = now
}

// filterRate 是「升即采、降 1/8 EWMA」的速率滤波——BBR windowed-max
// 精神的轻量近似：实测交付/到达速率是路径容量的下界证据，新高立即
// 采纳避免低估；回落用 EWMA 平滑，防单次抖动误降。
func filterRate(cur, sample float64) float64 {
	if sample >= cur {
		return sample
	}
	return cur + (sample-cur)/8
}

// staleAfter 返回本路径 est_rate 的保鲜期：4 个 srtt，下界
// estRateStaleFloor（无 RTT 样本时 srtt=0 → 恒取下界）。
func (p *path) staleAfter() time.Duration {
	if d := 4 * p.srtt; d > estRateStaleFloor {
		return d
	}
	return estRateStaleFloor
}

// effRate 返回本路径当前有效速率估计（字节/秒，0 = 未知/过期）。
// 两个来源（§5.2）：对端 TELEMETRY 回报的收端观测速率（TTL 内）
// 优先于本地 delivery-rate 采样（保鲜期内）——
//
// 为何不是 max：本地样本按 seg.path 在 cum 推进时记账，而 cum 受
// 全局重排耦合——慢路径的洞会压住快路径段的释放时刻，堵点一旦
// 解开，大批字节落在同一采样窗内被记成尖峰。收端观测的到达速率
// 无此失真，是该路径实际吞吐的更准证据（调研 §3「对端视角更准」）。
// 「少发→观测低→更低发」的低估螺旋由冷启动回退兜底：观测过期/
// 为零即落回本地采样或名义速率，且 inflight 下界保证任何路径
// 始终保有可被观测的在途量。
func (p *path) effRate(now time.Time) float64 {
	if now.Sub(p.peerRateAt) <= peerRateTTL && p.peerRate > 0 {
		return p.peerRate
	}
	if p.hasRate && now.Sub(p.lastRateAt) <= p.staleAfter() {
		return p.estRate
	}
	return 0
}

// inflightCap 返回本路径在途未确认字节硬顶（§5.1：
// cap ≈ k·est_rate·rtt，k 略大于 1）。对规格公式的一处有据偏离：
// 用 min_rtt（传播延迟基线）而非 srtt（含排队延迟）——srtt 会被
// 在途积压本身抬高，cap=k·rate·srtt 形成正反馈（积压→ACK 变慢→
// srtt 涨→cap 涨→更能积压），慢路径堆积的「硬顶」名存实亡；
// cap ≈ k·BDP 才是在途量上限的本意，BDP 用带宽×传播基线。
// 指标缺失时取下界 minInflightCap，保证冷启动/慢路径至少能流水线
// 两个满帧（防 cap=0 死锁，也给慢路径留可被观测的在途量）。
func (p *path) inflightCap(now time.Time) int64 {
	cap := int64(inflightCapK * p.effRate(now) * p.minRTT.Seconds())
	if cap < minInflightCap {
		return minInflightCap
	}
	return cap
}
