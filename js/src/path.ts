/**
 * 一条聚合数据路径 + 逐路径调度指标（path.go / scheduler.go 的移植）。
 *
 * JS 单线程事件循环下不需要锁：所有指标字段由所属 AggregatedStream
 * 的同步代码段更新（等价于 Go 的 Stream.mu 保护段）。
 */

import type { ByteStream } from './bytestream.js'
import type { PathInfo } from './types.js'
import { transportOf } from './multiaddr.js'

// ---------- 调度与采样常量（与 Go 端 scheduler.go 一致，时长单位 ms） ----------

/** 每路径在途硬顶系数：cap = k·est_rate·srtt（k 略大于 1）。 */
export const INFLIGHT_CAP_K = 1.5
/** DATA 单帧载荷上限（frame.go maxFramePayload）。 */
const MAX_FRAME_PAYLOAD = 64 << 10
/**
 * 在途硬顶下界：est_rate/srtt 未采出时仍保证至少两个满帧的流水线空间，
 * 防 cap=0 死锁（冷路径永远被跳过、无法积累样本）。
 */
export const MIN_INFLIGHT_CAP = 2 * MAX_FRAME_PAYLOAD
/** est_rate 未知/过期时的冷启动名义速率（字节/秒，宜保守偏小）。 */
export const COLD_RATE_BPS = 256 << 10 // 256KiB/s
/** delivery-rate 采样间隔下限：微秒级 RTT 路径上防单帧交付放大成尖峰。 */
export const RATE_SAMPLE_FLOOR_MS = 2
/** est_rate 保鲜期下界；实际保鲜期取 max(4·srtt, 本值)。 */
export const EST_RATE_STALE_FLOOR_MS = 500
/** 收端 TELEMETRY 回报与窗口容量重算的默认周期。 */
export const DEFAULT_TELEMETRY_INTERVAL_MS = 200
/** 对端 TELEMETRY 观测速率的有效期（超时作废）。 */
export const PEER_RATE_TTL_MS = 3 * DEFAULT_TELEMETRY_INTERVAL_MS
/** RTT 样本保鲜期：超过则视路径指标失联。 */
export const RTT_STALE_AFTER_MS = 2000
/** 同一路径两次主动 PING 的最小间隔（节流：PING 只兜底采样缺口）。 */
export const PING_MIN_INTERVAL_MS = 500
/** 收端到达速率采样门槛：攒够最小字节窗或最长时窗才出样本。 */
export const RX_SAMPLE_MIN_BYTES = 2 * MAX_FRAME_PAYLOAD
export const RX_SAMPLE_MAX_WINDOW_MS = 1000

// ---------- 软失效判定与降权常量（规格 §5.3） ----------

/** 可疑判据系数：在途段超过 k·srtt 未被覆盖即标可疑。 */
export const SUSPECT_K = 2
/** 可疑判据下界：loopback 级 srtt 时防调度抖动误标健康路径。 */
export const SUSPECT_FLOOR_MS = 200
/**
 * 路径 RTO 下界：可疑态持续超过 max(srtt+4·rttvar, 本值) 仍无活性证据
 * 即按硬失效摘除（RFC 6298 RTOmin=1s 同值）。
 */
export const PATH_RTO_FLOOR_MS = 1000
/** 可疑路径的调度降权系数：effRate 与 inflightCap 均按 1/penalty 折算。 */
export const SUSPECT_PENALTY = 4

/**
 * filterRate 是「升即采、降 1/8 EWMA」的速率滤波——BBR windowed-max
 * 精神的轻量近似：新高立即采纳避免低估；回落用 EWMA 平滑防抖动误降。
 */
export function filterRate(cur: number, sample: number): number {
	if (sample >= cur) {
		return sample
	}
	return cur + (sample - cur) / 8
}

/**
 * Path 是聚合流内的一条数据路径：一条底层连接上的一条子流。
 * 帧归属天然由子流承载，数据帧头不带 path_id。
 */
export class Path {
	readonly id: number
	readonly conn: ByteStream
	/** 本路径独占的底层连接（addPath 直拨出的连接）；swarm 托管连接为空。 */
	own?: { close(): Promise<unknown>; abort?(err: Error): void }
	/** 本侧是否为该路径的发起方（拨号+attach 一侧）。 */
	readonly dialed: boolean
	dead = false
	/** 写串行化链：该路径上的一切帧写出经它排队，互不交错。 */
	writeChain: Promise<unknown> = Promise.resolve()

	attachedAtMs = 0 // 挂接时刻（epoch ms，观测展示）
	attachedAt = 0 // 挂接时刻（perf ms，采样窗起点）

	// ---- RTT 采样（RFC 6298 EWMA + min_rtt；单位 ms） ----
	srtt = 0
	rttvar = 0
	minRTT = 0
	hasRTT = false
	lastRTTAt = 0

	// ---- BBR 式 delivery-rate 采样 ----
	inflight = 0 // 在途未确认字节数（调度器账本）
	delivered = 0 // 累计被累积确认字节
	estRate = 0 // 速率估计（字节/秒）
	hasRate = false
	rateMark = 0 // 上次采样窗结束时 delivered 快照
	lastRateAt = 0 // 上次速率样本时刻（perf ms）

	// ---- 对端 TELEMETRY 回报的收端观测速率（校准 est_rate） ----
	peerRate = 0 // 字节/秒
	peerRateAt = -Infinity // 最近一次回报时刻（perf ms）

	// ---- 收端方向记账（TELEMETRY 回报 + 窗口容量联动输入） ----
	rxBytes = 0 // 累计到达字节
	rxMark = 0 // 上个回报周期结束时 rxBytes 快照
	rxMarkT = 0 // 上个回报周期结束时刻（perf ms；0 = 未初始化）
	rxRate = 0 // 平滑后的到达速率估计（字节/秒）

	// ---- 软失效状态（规格 §5.3） ----
	// suspect：名下在途段超过 k·srtt 仍未被 ACK/SACK 覆盖即置位；
	// 可疑态自 suspectSince 起持续超过 rto() 仍无活性证据 → 硬失效摘除。
	suspect = false
	suspectSince = 0 // perf ms
	/**
	 * suspectSinceTs 是 suspectSince 的发送时间戳（µs）。活性证据须晚于它
	 * 才算数：被回显的 DATA/PING 必须是标记之后发出的——旧拷贝的在途
	 * ACK 回声不能复活正在掉队的路径。
	 */
	suspectSinceTs = 0

	// ---- 重传与降级记账（Stats/事件数据源） ----
	resentSegs = 0
	resentBytes = 0
	degraded = false // 「疑似降级」沿状态（恢复后复位，每次失联只报一次）

	lastPingAt = -Infinity // 上次主动 PING 时刻（perf ms，节流）

	constructor(init: { id: number; conn: ByteStream; dialed: boolean; own?: Path['own'] }) {
		this.id = init.id
		this.conn = init.conn
		this.dialed = init.dialed
		this.own = init.own
	}

	/** suspectAfter 返回在途段的软失效判据：k·srtt，下界 suspectFloor（ms）。 */
	suspectAfter(): number {
		const d = SUSPECT_K * this.srtt
		return d > SUSPECT_FLOOR_MS ? d : SUSPECT_FLOOR_MS
	}

	/** rto 返回路径 RTO：srtt+4·rttvar，下界 pathRTOFloor（ms）。 */
	rto(): number {
		const d = this.srtt + 4 * this.rttvar
		return d > PATH_RTO_FLOOR_MS ? d : PATH_RTO_FLOOR_MS
	}

	/** clearSuspect 清除可疑标记与计时基线。 */
	clearSuspect(): void {
		this.suspect = false
		this.suspectSince = 0
	}

	/**
	 * noteRTT 喂入一个本路径 RTT 样本（ms），按 RFC 6298 维护
	 * srtt/rttvar 并另记 min_rtt。
	 *
	 * 只采信「标记之后发出」的帧产生的回显：echoTs 是被回显 DATA/PING
	 * 的原始发送时刻（µs）——晚于 suspectSinceTs 才证明路径在可疑之后
	 * 仍完成了一次往返；早于它的样本只是旧拷贝的在途回声。
	 */
	noteRTT(rMs: number, now: number, echoTs: number): void {
		if (rMs < 0 || !Number.isFinite(rMs)) {
			return // 时钟回拨或伪造回显：丢弃
		}
		if (!this.hasRTT) {
			this.srtt = rMs
			this.rttvar = rMs / 2
			this.minRTT = rMs
			this.hasRTT = true
		} else {
			const d = Math.abs(this.srtt - rMs)
			this.rttvar = (3 * this.rttvar + d) / 4
			this.srtt = (7 * this.srtt + rMs) / 8
			if (rMs < this.minRTT) {
				this.minRTT = rMs
			}
		}
		this.lastRTTAt = now
		if (echoTs > this.suspectSinceTs) {
			this.clearSuspect()
		}
	}

	/**
	 * onDelivered 记账 n 字节在本路径上被对端累积确认：扣在途量，
	 * 并按 BBR 式 delivery-rate（delivered/Δt）更新 est_rate。
	 */
	onDelivered(n: number, now: number): void {
		this.inflight -= n
		this.creditDelivered(n, now)
	}

	/**
	 * creditDelivered 只记交付量与速率样本，不动 inflight——SACK 覆盖的
	 * 段在被标 sacked 时已释放 inflight（字节已躺在对端重排缓冲、不在
	 * 路上；若等 cum 才释放会把在途量钉死在硬顶——环形死锁），cum
	 * 推进释放时只补记交付量。不清 suspect：cum 覆盖不含路径新鲜度证据。
	 */
	creditDelivered(n: number, now: number): void {
		this.delivered += n
		if (this.lastRateAt === 0) {
			// 首个采样窗从路径挂接起算
			this.lastRateAt = this.attachedAt !== 0 ? this.attachedAt : now
		}
		const iv = Math.max(this.srtt, RATE_SAMPLE_FLOOR_MS)
		const dt = now - this.lastRateAt
		if (dt < iv) {
			return
		}
		const d = this.delivered - this.rateMark
		if (d === 0) {
			// 窗口内零交付：不算样本（不拉低 est_rate），只推进窗口
			this.lastRateAt = now
			return
		}
		const sample = d / (dt / 1000)
		this.estRate = filterRate(this.estRate, sample)
		this.hasRate = true
		this.rateMark = this.delivered
		this.lastRateAt = now
	}

	/** staleAfter 返回本路径 est_rate 的保鲜期：4 个 srtt，下界 estRateStaleFloor。 */
	staleAfter(): number {
		const d = 4 * this.srtt
		return d > EST_RATE_STALE_FLOOR_MS ? d : EST_RATE_STALE_FLOOR_MS
	}

	/**
	 * effRate 返回本路径当前有效速率估计（字节/秒，0 = 未知/过期）：
	 * 对端 TELEMETRY 回报的收端观测速率（TTL 内）优先于本地
	 * delivery-rate 采样——收端观测无 cum 耦合失真。「少发→观测低→
	 * 更低发」的低估螺旋由冷启动回退兜底。
	 */
	effRate(now: number): number {
		if (now - this.peerRateAt <= PEER_RATE_TTL_MS && this.peerRate > 0) {
			return this.peerRate
		}
		if (this.hasRate && now - this.lastRateAt <= this.staleAfter()) {
			return this.estRate
		}
		return 0
	}

	/**
	 * inflightCap 返回本路径在途未确认字节硬顶：cap ≈ k·est_rate·min_rtt
	 * （用 min_rtt 传播基线而非 srtt——srtt 会被在途积压本身抬高形成
	 * 正反馈）。指标缺失时取下界 minInflightCap。可疑路径速率按
	 * 1/suspectPenalty 折算。
	 */
	inflightCap(now: number): number {
		let rate = this.effRate(now)
		if (this.suspect) {
			rate /= SUSPECT_PENALTY
		}
		const cap = INFLIGHT_CAP_K * rate * (this.minRTT / 1000)
		return cap < MIN_INFLIGHT_CAP ? MIN_INFLIGHT_CAP : cap
	}

	/** info 组装本路径的 PathInfo 快照。 */
	info(now: number): PathInfo {
		// 传输类型优先看本端 multiaddr（WebRTC-direct 等监听侧地址
		// 才带传输标记），判不出再退到对端地址。
		const local = this.conn.localAddr ?? ''
		const remote = this.conn.remoteAddr ?? ''
		const transport = transportOf(local) !== 'unknown' ? transportOf(local) : transportOf(remote)
		return {
			id: this.id,
			dialed: this.dialed,
			connId: this.conn.connId ?? '',
			transport,
			local,
			remote,
			attachedAt: this.attachedAtMs,
			srttMs: this.srtt,
			minRttMs: this.minRTT,
			estRateBps: this.effRate(now),
			inflight: this.inflight,
			suspect: this.suspect,
		}
	}
}
