/**
 * AggregatedStream：一条聚合流，对应用呈现可靠有序字节流语义
 * （stream.go 的 TS 移植；JS 单线程下以同步代码段取代 Stream.mu 临界区，
 * 以微任务/定时器取代 goroutine 泵）。
 *
 * 数据面（规格 §4.4/§5/§6）：
 *   - 写方向：write → 待发队列 → 按对端通告窗口切成 DATA 帧，经调度器
 *     （最短排空时间优先 + 每路径 inflight 硬顶）选路下发；
 *   - 读方向：每条路径一个接收循环解帧 → 共用同一重排缓冲保序 → read
 *     取走；每收一帧 DATA 回一条 ACK（累积偏移 + SACK ranges + 时间戳
 *     回显 + 路径归属 + 窗口通告）；
 *   - 流控：单一聚合窗口——发送端 unacked ≥ 对端通告窗口时停发，进而
 *     write 在发送缓冲占满后挂起，直到对端读走释放窗口；
 *   - 逐路径指标：ACK ts_echo/ts_path 驱动 srtt/rttvar/min_rtt，cum
 *     推进驱动 delivery-rate 估计；收端周期 TELEMETRY 回报校准对端
 *     est_rate；PING 仅冷启动/指标失联探测；
 *   - 路径集合：PATH_ATTACH 绑定新路径（path_id 分侧命名空间），路径
 *     死亡即摘除并把在途未确认段重注入剩余路径；PATH_DROP 带内告知；
 *   - 失效恢复：在途段 k·srtt 无 ACK/SACK 覆盖 → 标可疑搬回重发池；
 *     可疑超路径 RTO → 硬失效摘除。
 */

import { ReorderBuf } from './reorder.js'
import { pickAckPath, pickData } from './scheduler.js'
import {
	Path,
	DEFAULT_TELEMETRY_INTERVAL_MS,
	PING_MIN_INTERVAL_MS,
	RTT_STALE_AFTER_MS,
	RX_SAMPLE_MAX_WINDOW_MS,
	RX_SAMPLE_MIN_BYTES,
	filterRate,
} from './path.js'
import {
	FrameDecoder,
	FrameType,
	MAX_ACK_RANGES,
	MAX_ACK_SEQ_ADVANCE,
	MAX_FRAME_PAYLOAD,
	encodeAckFrame,
	encodeCtrlFrame,
	encodeDataFrame,
	type ByteRange,
	type DecodedFrame,
} from './frame.js'
import {
	decodePathAttach,
	decodePathDrop,
	decodePathReady,
	decodePathRequest,
	decodePing,
	decodeTelemetry,
	encodePathAttach,
	encodePathDrop,
	encodePathReady,
	encodePathRequest,
	encodePing,
	encodeTelemetry,
	type PathRateMsg,
} from './proto.js'
import { StreamReader, type ByteStream } from './bytestream.js'
import { isCircuitAddr } from './multiaddr.js'
import {
	netaccErr,
	StreamEOFError,
	type PathInfo,
	type PathStats,
	type StreamEvent,
	type StreamEventType,
	type StreamStats,
} from './types.js'
import { multiaddr, type Multiaddr } from '@multiformats/multiaddr'
import type { PeerId } from '@libp2p/interface'
import { peerIdFromCID, peerIdFromMultihash } from '@libp2p/peer-id'
import { CID } from 'multiformats/cid'
import * as MultihashDigest from 'multiformats/hashes/digest'
import { AGG_STREAM_ID_LEN } from './handshake.js'

export { AGG_STREAM_ID_LEN }

/** 重排缓冲默认下界/硬顶（规格 §6）。 */
export const DEFAULT_MIN_REORDER_BUF = 1 << 20 // 1MiB
export const DEFAULT_MAX_REORDER_BUF = 32 << 20 // 32MiB

/** 对端超发防御：占用超过 4×cap 视为协议违例，直接 teardown。 */
const OVERFLOW_FACTOR = 4

/** EOF 哨兵：读侧终态「对端正常关闭」的内部标记（read() 译成 null）。 */
const EOF = Symbol('netacc.eof')
type ReadErr = Error | typeof EOF

/** sendSeg 是一段已发未确认数据：保留至连接级 ACK 释放。 */
interface SendSeg {
	off: number
	data: Uint8Array
	/** 当前在哪条路径在途；null = 待（换路）重发。 */
	path: Path | null
	/** 当前这份拷贝的发出时刻（perf ms，重发会刷新）。 */
	sentAt: number
	/**
	 * sacked 表示本段区间已被对端 SACK ranges 覆盖（乱序暂存在收端）：
	 * 不再参与任何重发，原地等 cum 推进释放。
	 */
	sacked: boolean
}

/**
 * dialPath 的返回：拨通的新物理连接上开好的子流（ByteStream 形态）+
 * 归本路径所有的底层连接句柄（随路径摘除关闭）。
 */
export interface DialedPath {
	stream: ByteStream
	/** 本路径独占的底层连接；swarm 托管连接为空。 */
	conn?: { close(): Promise<unknown>; abort?(err: Error): void }
}

/** PathDialer 拨出一条新底层连接并开好 /netacc/path/1.0.0 子流。 */
export type PathDialer = (addr: Multiaddr, options?: { signal?: AbortSignal }) => Promise<DialedPath>

/** RelayReservation 处理对端 PATH_REQUEST 的钩子：向中继做 reservation。 */
export type RelayReserver = (relay: { peerId: PeerId; addrs: Multiaddr[] }) => Promise<void>

export interface AggregatedStreamOptions {
	/** 16 字节 agg_stream_id（握手协商结果）。 */
	id: Uint8Array
	/** 首条数据路径（握手流升格）。 */
	firstPath: ByteStream
	/** 本侧是否为聚合流发起方（决定 path_id 奇偶命名空间）。 */
	initiator: boolean
	/** 对端 peer（观测/拨号校验用）。 */
	peer?: PeerId
	/** 重排缓冲下界（字节，默认 1MiB）。 */
	minBuf?: number
	/** 重排缓冲硬顶（默认 32MiB）。 */
	maxBuf?: number
	/** 发送缓冲上限（默认 2×窗口下界）。 */
	sendBufCap?: number
	/** TELEMETRY 回报/窗口重算/软失效扫描周期（默认 200ms）。 */
	telemetryIntervalMs?: number
	/** addPath 拨号器（NetaccClient 注入；不经 client 创建的流为 undefined）。 */
	dialer?: PathDialer
	/** 对端 PATH_REQUEST 的 reservation 钩子（无 → 直接回错误应答）。 */
	reserveRelay?: RelayReserver
	/** 流终态时回调（NetaccClient 用来反注册 PATH_ATTACH 路由）。 */
	onClosed?: (s: AggregatedStream) => void
}

interface PendingRelay {
	resolve(err?: Error): void
}

function toErr(e: unknown): Error {
	return e instanceof Error ? e : new Error(String(e))
}

/** 微任务后执行（状态变更后的泵/ACK 触发去抖）。 */
function defer(fn: () => void): void {
	queueMicrotask(fn)
}

export class AggregatedStream implements AsyncIterable<Uint8Array> {
	readonly id: Uint8Array // [16]byte
	readonly peer?: PeerId

	private cfg: Required<Pick<AggregatedStreamOptions, 'minBuf' | 'maxBuf'>> & {
		sendBufCap: number
		telemetryIntervalMs: number
	}
	private readonly dialer?: PathDialer
	private readonly reserveRelayFn?: RelayReserver
	private readonly onClosed?: (s: AggregatedStream) => void

	private readonly base = performance.now() // 发送时间戳基准（µs 相对它）

	private readonly rbuf: ReorderBuf
	private livePaths: Path[] = []
	private pathsByID = new Map<number, Path>()

	// path_id 分侧命名空间：最低位标识分配侧（0=发起方，1=接收方），
	// 本侧计数器步长 2 保证两端互不冲突；首条握手路径恒为发起方 id 0。
	private nextPathID: number
	private readonly remoteParity: number
	private readonly seenRemote = new Set<number>()
	private finRecv = false

	// 中继路径按需协调：本侧 PATH_REQUEST 等 PATH_READY
	private nextRelayReqID = 0
	private readonly pendingRelay = new Map<number, PendingRelay>()

	// ---- 发送侧 ----
	private sendQ: Uint8Array[] = []
	private qHead = 0
	private pendingBytes = 0
	private sentOff = 0 // 下一待分配流内偏移
	private cumAcked = 0 // 对端累积确认偏移
	private unackedSegs: SendSeg[] = []
	private advWindow = 0 // 对端最新通告的接收窗口
	private gotWindow = false
	private ackSeq = 0 // 本侧 ACK 序号（逐帧递增）
	private peerAckSeq = 0 // 对端最新 ACK 序号
	private gotPeerAck = false
	/** peerRanges 是「最新一条已应用 ACK」携带的对端乱序已收区间（SACK 快照）。 */
	private peerRanges: ByteRange[] = []

	// ---- 接收侧 ----
	private lastDataTs = 0 // 最近收到的 DATA 发送时间戳（µs，ACK 回显）
	private lastDataPath: Path | null = null // 最近 DATA 所在路径（ts_path 归属）
	private freedSinceAck = 0 // 自上次 ACK 以来 read 释放的字节数
	private rerr: ReadErr | null = null // 读侧终态：EOF=对端正常关闭
	private werr: Error | null = null // 写侧终态
	private closed = false

	// 重传统计
	private lostSegs = 0
	private resentSegs = 0
	private resentBytes = 0

	// 事件订阅
	private subs = new Set<SubQueue>()

	// 触发去抖
	private ackPending = false
	private pumpPending = false
	private tickTimer: ReturnType<typeof setInterval> | null = null

	// 读写等待者广播（chan 替换式等价物）
	private readonly rWaiters = new Set<() => void>()
	private readonly wWaiters = new Set<() => void>()

	/**
	 * 构造聚合流并以 firstPath 为首条路径启动数据面。
	 * 调用方必须先完成握手再构造（接收方须先发完 HelloAck 再 new，
	 * 否则本流发出的帧会抢在 HelloAck 前污染握手字节序）。
	 */
	constructor(opts: AggregatedStreamOptions) {
		const minBuf = opts.minBuf && opts.minBuf > 0 ? opts.minBuf : DEFAULT_MIN_REORDER_BUF
		const maxBuf = opts.maxBuf && opts.maxBuf > 0 ? Math.max(opts.maxBuf, minBuf) : DEFAULT_MAX_REORDER_BUF
		this.cfg = {
			minBuf,
			maxBuf,
			sendBufCap: opts.sendBufCap && opts.sendBufCap > 0 ? opts.sendBufCap : 2 * minBuf,
			telemetryIntervalMs:
				opts.telemetryIntervalMs && opts.telemetryIntervalMs > 0
					? opts.telemetryIntervalMs
					: DEFAULT_TELEMETRY_INTERVAL_MS,
		}
		if (opts.id.length !== AGG_STREAM_ID_LEN) {
			throw netaccErr('internal', `agg_stream_id 应为 ${AGG_STREAM_ID_LEN} 字节`)
		}
		this.id = opts.id.slice()
		this.peer = opts.peer
		this.dialer = opts.dialer
		this.reserveRelayFn = opts.reserveRelay
		this.onClosed = opts.onClosed
		this.rbuf = new ReorderBuf(minBuf)

		const first = new Path({ id: 0, conn: opts.firstPath, dialed: opts.initiator })
		first.attachedAt = this.base
		first.attachedAtMs = Date.now()
		this.livePaths = [first]
		this.pathsByID.set(0, first)
		if (opts.initiator) {
			this.nextPathID = 2 // 偶数命名空间，0 已占
			this.remoteParity = 1 // 对端（接收方）分配奇数 id
		} else {
			this.nextPathID = 1 // 奇数命名空间
			this.remoteParity = 0
			this.seenRemote.add(0)
		}
	}

	/** start 启动数据面：首条路径的接收循环 + 初始窗口通告 + 周期 tick。 */
	start(): void {
		this.recvLoop(this.livePaths[0]!, new FrameDecoder(new StreamReader(this.livePaths[0]!.conn)))
		// 建流即通告初始窗口（对端收到前不发 DATA，防超发被重排缓冲丢弃）
		this.sendAck()
		this.tickTimer = setInterval(() => this.onTick(), this.cfg.telemetryIntervalMs)
		if (typeof this.tickTimer === 'object' && this.tickTimer !== null && 'unref' in this.tickTimer) {
			// Node 环境：别让 tick 定时器阻止进程退出
			;(this.tickTimer as { unref(): void }).unref()
		}
	}

	/** sendTs 返回当前发送时间戳（相对 base 的微秒数）。 */
	private sendTs(): number {
		const d = performance.now() - this.base
		return d < 0 ? 0 : Math.floor(d * 1000)
	}

	// ---------- 读 ----------

	/**
	 * read 读取最多 maxBytes 字节（默认取走全部已保序数据）。
	 * 对端正常关闭且无残余数据时返回 null（EOF）；出错 reject。
	 * 与 TCP/net.Conn 同构：返回只表示有数据可读，不保证填满。
	 */
	async read(maxBytes?: number): Promise<Uint8Array | null> {
		if (maxBytes === 0) {
			return new Uint8Array(0)
		}
		for (;;) {
			if (this.rbuf.avail() > 0) {
				const out = this.rbuf.read(maxBytes)
				// 窗口释放通告：原窗口为 0（发送端可能已停摆）时立即补 ACK；
				// 否则累计释放 ≥ 容量 1/4 再发，给慢读应用做去抖。
				const prevWindow = this.rbuf.window() - out.length
				this.freedSinceAck += out.length
				const needAck = prevWindow <= 0 || this.freedSinceAck >= this.rbuf.cap / 4
				if (needAck) {
					this.triggerAck()
				}
				return out
			}
			if (this.rerr !== null) {
				if (this.rerr === EOF) {
					return null
				}
				throw this.rerr
			}
			if (this.closed) {
				throw netaccErr('closed', 'netacc: 聚合流已关闭')
			}
			await this.wait(this.rWaiters)
		}
	}

	/** 异步迭代读取：持续产出数据块直到 EOF 或出错。 */
	async *[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
		for (;;) {
			const chunk = await this.read()
			if (chunk === null) {
				return
			}
			yield chunk
		}
	}

	/** readAll 读至 EOF 并返回拼接结果；出错 reject。 */
	async readAll(): Promise<Uint8Array> {
		const parts: Uint8Array[] = []
		let total = 0
		for await (const c of this) {
			parts.push(c)
			total += c.length
		}
		const out = new Uint8Array(total)
		let off = 0
		for (const p of parts) {
			out.set(p, off)
			off += p.length
		}
		return out
	}

	// ---------- 写 ----------

	/**
	 * write 把数据拷入发送缓冲后 resolve——不表示对端已收
	 * （与 TCP send buffer 语义同构）。发送缓冲（待发+在途未确认）
	 * 占满时挂起，直至窗口推进或出错。
	 */
	async write(data: Uint8Array): Promise<void> {
		let b = data
		while (b.length > 0) {
			if (this.werr !== null) {
				throw this.werr
			}
			if (this.closed) {
				throw netaccErr('closed', 'netacc: 聚合流已关闭')
			}
			const outstanding = this.pendingBytes + (this.sentOff - this.cumAcked)
			const room = this.cfg.sendBufCap - outstanding
			if (room > 0) {
				const n = Math.min(room, b.length)
				this.sendQ.push(b.slice(0, n))
				this.pendingBytes += n
				b = b.subarray(n)
				this.pump()
				continue
			}
			await this.wait(this.wWaiters)
		}
	}

	// ---------- 发送泵 ----------

	/** pump 尽量下发 DATA 帧；等价于 Go 的 sendLoop 消费 sendWake。 */
	private pump(): void {
		while (this.trySend()) {
			// trySend 返回 false 表示暂无可发
		}
	}

	/** wakeSend 与 Go 的 cap1 通道等价：这里直接同步泵（JS 单线程）。 */
	private wakeSend(): void {
		if (this.pumpPending) {
			return
		}
		this.pumpPending = true
		defer(() => {
			this.pumpPending = false
			this.pump()
		})
	}

	/**
	 * trySend 发一帧 DATA；返回 false 表示暂无可发。
	 * 优先级：重发池补洞 > 新数据。重发豁免窗口检查（这些字节区间
	 * 本就计入 unacked——窗口账本之内）；新数据须 unacked < 对端通告窗口。
	 */
	private trySend(): boolean {
		if (this.closed || this.werr !== null || !this.gotWindow) {
			return false
		}
		const now = performance.now()
		let off: number
		let payload: Uint8Array
		let resendIdx = -1
		let resendEnd = 0
		const res = this.nextResend()
		if (res !== null) {
			// 重发池补洞优先且豁免窗口检查：洞宽超单帧上限按
			// MAX_FRAME_PAYLOAD 重新切分（规格 §5.3）。
			resendIdx = res
			const rs = this.unackedSegs[res]!
			resendEnd = Math.min(this.resendHoleEnd, rs.off + rs.data.length)
			off = this.resendHoleStart
			const d0 = off - rs.off
			payload = rs.data.subarray(d0, d0 + (resendEnd - off))
			if (resendEnd - off > MAX_FRAME_PAYLOAD) {
				resendEnd = off + MAX_FRAME_PAYLOAD
				payload = rs.data.subarray(d0, d0 + MAX_FRAME_PAYLOAD)
			}
		} else {
			// 新数据受窗口约束（unacked ≥ 对端通告窗口即停摆）
			const unacked = this.sentOff - this.cumAcked
			if (this.pendingBytes === 0 || unacked >= this.advWindow) {
				return false
			}
			const room = this.advWindow - unacked
			const n = Math.min(this.sendQ[0]!.length - this.qHead, room, MAX_FRAME_PAYLOAD)
			off = this.sentOff
			payload = this.sendQ[0]!.subarray(this.qHead, this.qHead + n)
		}
		// 调度器分帧决策；null = 全部路径在途顶满，等 ACK 释放 inflight
		const p = pickData(this.livePaths, payload.length, now)
		if (p === null) {
			return false
		}
		if (resendIdx >= 0) {
			// 把本帧覆盖的洞段切出记到新路径名下
			this.splitResend(resendIdx, off, payload.length, p, now)
			p.inflight += payload.length
			p.resentSegs++
			p.resentBytes += payload.length
			this.resentSegs++
			this.resentBytes += payload.length
		} else {
			const n = payload.length
			this.qHead += n
			if (this.qHead === this.sendQ[0]!.length) {
				this.sendQ.shift()
				this.qHead = 0
			}
			this.sentOff += n
			this.pendingBytes -= n
			this.unackedSegs.push({ off, data: payload.slice(), path: p, sentAt: now, sacked: false })
			p.inflight += n
		}
		const ts = this.sendTs()
		const buf = encodeDataFrame(off, ts, payload)
		this.enqueueWrite(p, buf)
		return true
	}

	/** 重发池洞区间，由 nextResend 同步返回（保持 Go 的多返回值形态）。 */
	private resendHoleStart = 0
	private resendHoleEnd = 0

	/**
	 * nextResend 在重发池（path==null 的 unacked 段）里选出下一段真正
	 * 缺失的区间：先按最新 SACK（peerRanges）挖掉对端已收部分——只补洞
	 * 不重发已收字节。返回段下标并把洞区间写入 resendHole{Start,End}；
	 * 无可发返回 null。
	 */
	private nextResend(): number | null {
		for (let i = 0; i < this.unackedSegs.length; i++) {
			const rs = this.unackedSegs[i]!
			if (rs.path !== null || rs.sacked) {
				continue
			}
			const [a, b] = firstHole(rs.off, rs.off + rs.data.length, this.peerRanges)
			if (a >= b) {
				rs.sacked = true // 整段已在收端：等 cum 释放即可
				continue
			}
			this.resendHoleStart = a
			this.resendHoleEnd = b
			return i
		}
		return null
	}

	/**
	 * splitResend 把 unackedSegs[i] 中 [hs, hs+n) 切出记到路径 p 名下：
	 * 前缀（已被 SACK 覆盖）保留记账到 cum 释放；后缀留在重发池等下一轮。
	 * 保持 unackedSegs 按 off 升序、互不重叠的平铺不变量。
	 */
	private splitResend(i: number, hs: number, n: number, p: Path, now: number): void {
		const rs = this.unackedSegs[i]!
		const d0 = hs - rs.off
		const parts: SendSeg[] = []
		if (d0 > 0) {
			parts.push({ off: rs.off, data: rs.data.subarray(0, d0), path: null, sentAt: 0, sacked: true })
		}
		parts.push({ off: hs, data: rs.data.subarray(d0, d0 + n), path: p, sentAt: now, sacked: false })
		const rem = d0 + n
		if (rem < rs.data.length) {
			parts.push({ off: hs + n, data: rs.data.subarray(rem), path: null, sentAt: 0, sacked: false })
		}
		this.unackedSegs.splice(i, 1, ...parts)
	}

	/**
	 * sendAck 编码并经存活路径发出一条 ACK：累积偏移 + SACK ranges +
	 * 时间戳回显（含路径归属 ts_path）+ 当前接收窗口。回送路径由调度器
	 * 选最低延迟者。写失败由 dropPath 兜底（摘除后在存活路径上补通告）。
	 */
	private sendAck(): void {
		if (this.closed) {
			return
		}
		const cum = this.rbuf.cum()
		const tsEcho = this.lastDataTs
		let tsPath = 0 // 回显的路径归属：path_id+1（0=无有效回显）
		if (this.lastDataPath !== null) {
			tsPath = this.lastDataPath.id + 1
		}
		const window = Math.max(this.rbuf.window(), 0)
		const ranges = this.rbuf.ranges(MAX_ACK_RANGES)
		this.freedSinceAck = 0
		const seq = this.ackSeq++
		const p = pickAckPath(this.livePaths, performance.now())
		if (p === null) {
			return // 无存活路径：放弃本次 ACK，后续触发再试
		}
		this.enqueueWrite(p, encodeAckFrame(cum, tsEcho, tsPath, window, seq, ranges))
	}

	/** triggerAck 请求补一条 ACK（去抖，编码时刻取最新快照）。 */
	private triggerAck(): void {
		if (this.ackPending) {
			return
		}
		this.ackPending = true
		defer(() => {
			this.ackPending = false
			this.sendAck()
		})
	}

	/** enqueueWrite 把一帧挂到路径写链上（串行化）；写失败摘除路径。 */
	private enqueueWrite(p: Path, bytes: Uint8Array): void {
		p.writeChain = p.writeChain
			.then(async () => {
				if (p.dead || this.closed) {
					return
				}
				await p.conn.write(bytes)
			})
			.catch((err: unknown) => {
				this.dropPath(p, toErr(err), true)
			})
	}

	/** sendPing 在指定路径上写一条 PING 帧（尽力而为）。 */
	private sendPing(p: Path, reply: boolean, sendTs: number): void {
		if (p.dead || this.closed) {
			return
		}
		this.enqueueWrite(p, encodeCtrlFrame(FrameType.Ping, encodePing(sendTs, reply)))
	}

	/** sendTelemetry 把收端观测的各路径到达速率回报对端。 */
	private sendTelemetry(rates: PathRateMsg[]): void {
		if (this.closed) {
			return
		}
		const p = pickAckPath(this.livePaths, performance.now())
		if (p === null) {
			return
		}
		this.enqueueWrite(p, encodeCtrlFrame(FrameType.Telemetry, encodeTelemetry(rates)))
	}

	/** sendPathDrop 在任意存活路径上尽力发 PATH_DROP。 */
	private sendPathDrop(id: number): void {
		this.writeCtrl(FrameType.PathDrop, encodePathDrop(id))
	}

	/**
	 * writeCtrl 在存活路径上写一条控制帧；写失败由 dropPath 级联兜底。
	 * 无存活路径或流已关时返回 false。
	 */
	private writeCtrl(ft: FrameType, body: Uint8Array): boolean {
		if (this.closed) {
			return false
		}
		const p = pickAckPath(this.livePaths, performance.now())
		if (p === null) {
			return false
		}
		this.enqueueWrite(p, encodeCtrlFrame(ft, body))
		return true
	}

	// ---------- 周期 tick ----------

	/**
	 * onTick 周期任务（telemetryInterval 一拍）：
	 *  1. 汇总各路径收端到达速率 → TELEMETRY 回报对端；
	 *  2. 按 Σest_rate·max_srtt 重算收端窗口容量（规格 §6）；
	 *  3. 对「指标失联且有发送需求」/冷启动/可疑路径发 PING 探测；
	 *  4. 软失效扫描：逾期未覆盖段标可疑 + 搬回重发池，可疑超 RTO 摘除。
	 */
	private onTick(): void {
		if (this.closed) {
			return
		}
		const now = performance.now()
		const rates: PathRateMsg[] = []
		const needSend = this.pendingBytes > 0 || this.unackedSegs.length > 0
		for (const p of this.livePaths) {
			if (p.rxMarkT === 0) {
				p.rxMarkT = p.attachedAt
			}
			const d = p.rxBytes - p.rxMark
			const dt = now - p.rxMarkT
			// 收端到达速率采样：攒够最小字节窗或最长时窗才出样本；
			// 只报有新到达的路径（静默路径靠对端 TTL 过期作废）。
			if (dt > 0 && (d >= RX_SAMPLE_MIN_BYTES || dt >= RX_SAMPLE_MAX_WINDOW_MS)) {
				p.rxRate = filterRate(p.rxRate, d / (dt / 1000))
				if (d > 0) {
					rates.push({ pathId: p.id, rateBps: Math.floor(p.rxRate) })
				}
				p.rxMark = p.rxBytes
				p.rxMarkT = now
			}
			// 探测触发（规格 §5.2：仅冷启动/过期/疑似降级/可疑自证）：
			//  a) probeCold：正在收数据却无 RTT 样本——窗口容量联动需要 srtt；
			//  b) probeStale：指标曾有效但失联且有发送需求——疑似降级；
			//  c) suspect：可疑路径需要一条 PING 应答来自证活性。
			const probeCold = p.rxBytes > 0 && !p.hasRTT
			const probeStale =
				needSend &&
				((p.hasRate && now - p.lastRateAt > p.staleAfter()) ||
					(p.hasRTT && now - p.lastRTTAt > RTT_STALE_AFTER_MS))
			if (probeStale && !p.degraded) {
				p.degraded = true
				this.emitEvent('path_degraded', p.info(now))
			} else if (!probeStale && p.degraded) {
				p.degraded = false
			}
			if ((probeCold || probeStale || p.suspect) && now - p.lastPingAt > PING_MIN_INTERVAL_MS) {
				p.lastPingAt = now
				this.sendPing(p, false, this.sendTs())
			}
		}
		this.updateWindowCap()
		const { drops, moved } = this.checkSoftFail(now)
		if (rates.length > 0) {
			this.sendTelemetry(rates)
		}
		for (const p of drops) {
			this.dropPath(p, netaccErr('timeout', 'netacc: 路径 RTO 到期无 ACK 进展'), true)
		}
		if (moved) {
			this.wakeSend()
		}
	}

	/**
	 * updateWindowCap 按规格 §6 重算重排缓冲容量：
	 * reorder_buf = clamp(Σ rxRate_i · max_srtt_i, minBuf, maxBuf)。
	 * 带滞回：与现容量差不足 minBuf/8 时不调整。窗口由 0 重开立即补通告。
	 */
	private updateWindowCap(): void {
		let sumRate = 0
		let maxSRTT = 0
		for (const p of this.livePaths) {
			sumRate += p.rxRate
			if (p.srtt > maxSRTT) {
				maxSRTT = p.srtt
			}
		}
		let newCap = Math.floor(sumRate * (maxSRTT / 1000))
		if (newCap < this.cfg.minBuf) {
			newCap = this.cfg.minBuf
		}
		if (newCap > this.cfg.maxBuf) {
			newCap = this.cfg.maxBuf
		}
		if (Math.abs(newCap - this.rbuf.cap) < this.cfg.minBuf / 8) {
			return
		}
		const prevWindow = this.rbuf.window()
		this.rbuf.setCap(newCap)
		// 发送缓冲随窗口容量同步伸缩（sendBufCap = 2×窗口不变量）
		this.cfg.sendBufCap = 2 * newCap
		if (prevWindow <= 0 && this.rbuf.window() > 0) {
			this.triggerAck()
		}
	}

	/**
	 * checkSoftFail 软失效判定（规格 §5.3）：路径名下在途段超过
	 * suspectAfter（k·srtt，下界 suspectFloor）仍未被 cum/SACK 覆盖 →
	 * 路径标可疑并把段搬回重发池（path=null，保持原字节偏移）；
	 * 可疑态持续超过路径 RTO 仍无活性证据 → 返回待摘除路径。
	 */
	private checkSoftFail(now: number): { drops: Path[]; moved: boolean } {
		let moved = false
		for (const seg of this.unackedSegs) {
			if (seg.sacked) {
				continue
			}
			const end = seg.off + seg.data.length
			if (coveredBy(this.peerRanges, seg.off, end)) {
				seg.sacked = true
				if (seg.path !== null) {
					// 字节已到对端：当场释放 inflight 账本——若等 cum 才
					// 释放，已送达字节会把路径在途量钉死在硬顶（环形死锁）。
					seg.path.inflight -= seg.data.length
				}
				continue
			}
			if (seg.path === null) {
				continue // 已在重发池等下一轮
			}
			if (now - seg.sentAt <= seg.path.suspectAfter()) {
				continue // 还在该路径正常往返预算内
			}
			// 逾期未覆盖 → 标可疑并搬回重发池；inflight 账本同步释放
			const p = seg.path
			if (!p.suspect) {
				p.suspect = true
				p.suspectSince = now
				p.suspectSinceTs = this.sendTs()
			}
			p.inflight -= seg.data.length
			seg.path = null
			moved = true
		}
		const drops: Path[] = []
		for (const p of this.livePaths) {
			if (p.suspect && now - p.suspectSince > p.rto()) {
				drops.push(p)
			}
		}
		return { drops, moved }
	}

	// ---------- 路径集合管理 ----------

	/**
	 * attachPath 把一条已完成绑定握手（或首条握手路径/测试管道）的
	 * 子流挂入路径集并启动其接收循环。decoder 为该子流上已消费完
	 * 绑定帧的帧解码器——必须复用同一缓冲继续解帧，否则握手阶段
	 * 预读到的数据帧会丢失。
	 */
	attachPath(p: Path, decoder?: FrameDecoder): void {
		if (this.closed) {
			throw netaccErr('closed', 'netacc: 聚合流已关闭')
		}
		if (p.attachedAt === 0) {
			p.attachedAt = performance.now()
			p.attachedAtMs = Date.now()
		}
		this.livePaths.push(p)
		this.pathsByID.set(p.id, p)
		this.emitEvent('path_added', p.info(performance.now()))
		const fr = decoder ?? new FrameDecoder(new StreamReader(p.conn))
		this.recvLoop(p, fr)
		// 冷启动探测（规格 §5.2）：新路径尚无样本，一条 PING 让 RTT 先跑起来
		this.sendPing(p, false, this.sendTs())
		this.wakeSend()
	}

	/**
	 * dropPath 摘除一条路径：标记死亡（幂等）、移出路径集、把名下在途
	 * 未确认段改标「待重发」、关闭底层子流与自有连接。notify 为真时在
	 * 存活路径上尽力发 PATH_DROP 告知对端。无存活路径时流转终态。
	 */
	private dropPath(p: Path, cause: Error | undefined, notify: boolean): void {
		if (p.dead) {
			return
		}
		p.dead = true
		this.livePaths = this.livePaths.filter(q => q !== p)
		this.pathsByID.delete(p.id)
		for (const seg of this.unackedSegs) {
			if (seg.path === p) {
				if (!seg.sacked) {
					p.inflight -= seg.data.length
				}
				seg.path = null
				this.lostSegs++
			}
		}
		const empty = this.livePaths.length === 0
		const fin = this.finRecv

		void p.conn.close().catch(() => {})
		if (p.own !== undefined) {
			void p.own.close().catch(() => {})
		}
		this.emitEvent('path_removed', p.info(performance.now()), cause)
		if (notify) {
			this.sendPathDrop(p.id)
		}
		if (empty) {
			if (fin) {
				// 对端发过 FIN：读侧保持干净 EOF 语义
				if (this.rerr === null) {
					this.rerr = EOF
				}
			}
			this.shutdown(cause ?? netaccErr('no_paths', 'netacc: 聚合流已无可用数据路径'))
			return
		}
		// 路径死亡可能带走了在途 ACK：在存活路径上补一条最新通告
		this.triggerAck()
		this.wakeSend()
	}

	/** handlePathDrop 处理对端发来的 PATH_DROP（幂等）。 */
	private handlePathDrop(body: Uint8Array): void {
		let pathId: number
		try {
			;({ pathId } = decodePathDrop(body))
		} catch {
			return // 帧体损坏：忽略（该路径仍可用，不致 teardown）
		}
		const q = this.pathsByID.get(pathId)
		if (q !== undefined) {
			this.dropPath(q, undefined, false) // 对端已知，无需再通知
		}
	}

	/**
	 * reserveRemotePathID 校验并预定一个对端命名空间的 path_id：
	 * 奇偶位须落在对端命名空间，且该 id 不曾被使用（含已死路径——
	 * 规格 §4.3：死亡路径重加须换新 id）。
	 */
	reserveRemotePathID(id: number): boolean {
		if (id % 2 !== this.remoteParity) {
			return false
		}
		if (this.closed) {
			return false
		}
		if (this.seenRemote.has(id)) {
			return false
		}
		this.seenRemote.add(id)
		return true
	}

	/**
	 * addPath 主动为聚合流补挂一条数据路径（规格书 §8 手动路径口，
	 * 两侧对称可调用）：
	 * 经 dialer 直拨 addr 建一条新物理连接，在其上开 /netacc/path/1.0.0
	 * 子流并完成 PATH_ATTACH 绑定握手。返回分配到的 path_id。
	 *
	 * addr 可带可不带 /p2p/<peerID> 后缀；含 /p2p-circuit 的中继电路
	 * 地址同样接受（要求 node 已配 circuit-relay transport 且对端在
	 * 中继上有可用 reservation）。
	 */
	async addPath(addr: Multiaddr | string, opts?: { signal?: AbortSignal }): Promise<number> {
		if (this.dialer === undefined) {
			throw netaccErr('internal', 'netacc: 该聚合流不经 NetaccClient 创建，无法拨号加路径')
		}
		// 预定本侧命名空间的 path_id（先取号再拨号：即便拨号失败，
		// 作废一个 id 也无碍——已用 id 永不复用）
		if (this.closed) {
			throw netaccErr('closed', 'netacc: 聚合流已关闭')
		}
		const pathID = this.nextPathID
		this.nextPathID += 2

		const ma = typeof addr === 'string' ? multiaddr(addr) : addr
		const dialed = await this.dialer(ma, opts)
		try {
			// 绑定握手：PATH_ATTACH 为该子流首帧（规格 §4.2），
			// 等对端回显同构帧即接受。
			const bs = dialed.stream
			await bs.write(encodeCtrlFrame(FrameType.PathAttach, encodePathAttach(this.id, pathID)))
			const reader = new StreamReader(bs)
			const fr = new FrameDecoder(reader)
			const resp = await fr.next()
			if (resp === null || resp.type !== FrameType.PathAttach) {
				throw netaccErr('handshake', `netacc: 期望 PATH_ATTACH 回显，实收帧类型 ${resp?.type ?? 'EOF'}`)
			}
			const echo = decodePathAttach(resp.frame.body)
			if (!bytesEqual(echo.aggStreamId, this.id) || echo.pathId !== pathID) {
				throw netaccErr('handshake', 'netacc: PATH_ATTACH 回显不一致')
			}
			this.attachPath(new Path({ id: pathID, conn: bs, dialed: true, own: dialed.conn }), fr)
			return pathID
		} catch (e) {
			void dialed.stream.close().catch(() => {})
			if (dialed.conn !== undefined) {
				void dialed.conn.close().catch(() => {})
			}
			throw e
		}
	}

	/**
	 * addRelayPath 经中继 relay 为聚合流补挂一条中继路径（规格 §4.3
	 * 按需协调）：PATH_REQUEST → 等对端向中继做完 reservation 回
	 * PATH_READY → 经 <relay-addr>/p2p/<relay>/p2p-circuit/p2p/<对端>
	 * 电路地址拨 CONNECT。
	 *
	 * relay.peerId 是中继节点 peerID；relay.addrs 是中继可达地址集
	 * （顺序即 A→C 段传输偏好，首个可拨地址进电路地址）。
	 */
	async addRelayPath(
		relay: { peerId: PeerId; addrs: Multiaddr[] },
		opts?: { signal?: AbortSignal },
	): Promise<number> {
		if (this.dialer === undefined || this.peer === undefined) {
			throw netaccErr('internal', 'netacc: 该聚合流不经 NetaccClient 创建或对端未知，无法拨号加路径')
		}
		const peer = this.peer // 收窄到局部：await 后 this.peer 的类型窄化会失效
		if (this.closed) {
			throw netaccErr('closed', 'netacc: 聚合流已关闭')
		}
		if (relay.addrs.length === 0) {
			throw netaccErr('internal', 'netacc: 中继路径描述符缺可达地址集')
		}
		// 先定 A→C 段地址（进电路地址）：首个非 /p2p-circuit 形态地址
		const hopAddr = relay.addrs.find(a => !isCircuitAddr(a))
		if (hopAddr === undefined) {
			throw netaccErr('internal', 'netacc: 中继地址集全是 /p2p-circuit 形态，无可用 A→C 段载体')
		}
		const pathID = this.nextPathID
		this.nextPathID += 2
		const reqID = this.nextRelayReqID++
		const ready = new Promise<Error | undefined>((resolve) => {
			this.pendingRelay.set(reqID, { resolve: e => resolve(e) })
		})
		try {
			if (!this.writeCtrl(FrameType.PathRequest, encodePathRequest({
				requestId: reqID,
				relayPeer: relay.peerId.toMultihash().bytes,
				relayAddrs: relay.addrs.map(a => a.bytes as Uint8Array),
				acAddr: hopAddr.bytes as Uint8Array,
			}))) {
				throw netaccErr('no_paths', 'netacc: 无存活路径可发 PATH_REQUEST')
			}
			const rerr = await Promise.race([ready, abortPromise(opts?.signal)])
			if (rerr !== undefined) {
				throw netaccErr('handshake', `netacc: 对端向中继 ${relay.peerId.toString()} 做 reservation 失败: ${rerr.message}`)
			}
			if (this.closed) {
				throw netaccErr('closed', 'netacc: 聚合流已关闭')
			}
			// 对端 reservation 就绪 → 经电路地址 CONNECT
			const circuitAddr = hopAddr.encapsulate(
				`/p2p/${relay.peerId.toString()}/p2p-circuit/p2p/${peer.toString()}`,
			)
			const dialed = await this.dialer(circuitAddr, opts)
			try {
				const bs = dialed.stream
				await bs.write(encodeCtrlFrame(FrameType.PathAttach, encodePathAttach(this.id, pathID)))
				const fr = new FrameDecoder(new StreamReader(bs))
				const resp = await fr.next()
				if (resp === null || resp.type !== FrameType.PathAttach) {
					throw netaccErr('handshake', 'netacc: 中继电路上未收到 PATH_ATTACH 回显')
				}
				const echo = decodePathAttach(resp.frame.body)
				if (!bytesEqual(echo.aggStreamId, this.id) || echo.pathId !== pathID) {
					throw netaccErr('handshake', 'netacc: PATH_ATTACH 回显不一致')
				}
				this.attachPath(new Path({ id: pathID, conn: bs, dialed: true, own: dialed.conn }), fr)
				return pathID
			} catch (e) {
				void dialed.stream.close().catch(() => {})
				if (dialed.conn !== undefined) {
					void dialed.conn.close().catch(() => {})
				}
				throw e
			}
		} finally {
			this.pendingRelay.delete(reqID)
		}
	}

	/**
	 * removePath 摘除一条本流的数据路径：关闭底层子流（自有连接随之
	 * 关闭），并在存活路径上尽力发 PATH_DROP 告知对端；该路径上的在途
	 * 未确认数据自动重注入剩余路径。拒绝摘除最后一条存活路径。
	 */
	removePath(pathID: number): void {
		if (this.closed) {
			throw netaccErr('closed', 'netacc: 聚合流已关闭')
		}
		const p = this.pathsByID.get(pathID)
		if (p === undefined) {
			throw netaccErr('unknown_path', `netacc: 未知 path_id ${pathID}`)
		}
		if (this.livePaths.length <= 1) {
			throw netaccErr('last_path', 'netacc: 不能摘除最后一条数据路径')
		}
		this.dropPath(p, undefined, true)
	}

	/** paths 返回当前存活数据路径的观测快照（规格书 §8 Paths()）。 */
	paths(): PathInfo[] {
		const now = performance.now()
		return this.livePaths.map(p => p.info(now))
	}

	// ---------- 接收侧 ----------

	/**
	 * recvLoop 是单条路径的接收循环：逐帧解码分发，多条路径共用同一
	 * 重排缓冲。出错/EOF → 摘除本路径；是否终态由剩余路径数决定。
	 */
	private recvLoop(p: Path, fr: FrameDecoder): void {
		void (async () => {
			for (;;) {
				const r = await fr.next()
				if (r === null) {
					// 传输层 EOF = 对端关断该路径
					this.recvErr(p, new Error('netacc: 路径 EOF'))
					return
				}
				if (this.handleFrame(p, r.type, r.frame)) {
					return // handleFrame 返回 true 表示本路径接收循环应结束
				}
			}
		})().catch((err: unknown) => {
			this.recvErr(p, toErr(err))
		})
	}

	private recvErr(p: Path, err: Error): void {
		if (this.closed) {
			return
		}
		this.dropPath(p, err, true)
	}

	/** handleFrame 分发一帧；返回 true 表示 recvLoop 应结束。 */
	private handleFrame(p: Path, ft: FrameType, f: DecodedFrame): boolean {
		switch (ft) {
			case FrameType.Data: {
				this.rbuf.add(f.off, f.payload)
				this.lastDataTs = Number(f.sendTs)
				this.lastDataPath = p // ACK 回显的路径归属（ts_path）
				p.rxBytes += f.payload.length
				this.broadcast(this.rWaiters)
				// 纵深防御：诚实对端受窗口背压约束，size 恒 ≤ cap；补洞段
				// 豁免容量检查意味着恶意对端可无视窗口灌有序数据——占用
				// 超过 4×cap 即视为协议违例，直接 teardown 保内存。
				if (this.rbuf.sizeBytes() > OVERFLOW_FACTOR * this.rbuf.cap) {
					this.shutdown(netaccErr('internal', `netacc: 对端超发，重排缓冲 ${this.rbuf.sizeBytes()} 超过 ${OVERFLOW_FACTOR} 倍容量上限`))
					return true
				}
				// ACK 触发策略：每帧即回（编码时刻取最新快照，微任务去抖合并）。
				this.triggerAck()
				return false
			}
			case FrameType.Ack: {
				// 多路径下 ACK 跨路径超车：只应用 seq 递增的最新快照，
				// 否则晚到的旧 ACK 会把通告窗口覆盖回过期值造成死锁。
				if (this.gotPeerAck && f.seq <= this.peerAckSeq) {
					return false
				}
				const err = validateAck(f, this.sentOff, this.cumAcked, this.peerAckSeq, this.gotPeerAck)
				if (err !== null) {
					this.dropPath(p, netaccErr('protocol', `netacc: ACK 协议违例: ${err.message}`), true)
					return true
				}
				this.peerAckSeq = f.seq
				this.gotPeerAck = true
				const now = performance.now()
				// 逐路径 RTT 采样：ts_path 给出回显的路径归属
				if (f.tsPath !== 0 && f.tsEcho !== 0n) {
					const q = this.pathsByID.get(f.tsPath - 1)
					if (q !== undefined) {
						const rMs = (this.sendTs() - Number(f.tsEcho)) / 1000
						q.noteRTT(rMs, now, Number(f.tsEcho))
					}
				}
				const cum = f.cum
				if (cum > this.cumAcked) {
					this.cumAcked = cum
					this.freeAcked(now)
				}
				this.advWindow = f.window
				this.gotWindow = true
				// 消费 SACK ranges：软失效扫描据此区分「在路上/被丢弃」
				// 与「已到对端等 cum」，重发只补真正缺失的洞。
				this.peerRanges = normalizeRanges(f.ranges)
				this.broadcast(this.wWaiters)
				this.wakeSend()
				return false
			}
			case FrameType.PathDrop: {
				this.handlePathDrop(f.body)
				return false
			}
			case FrameType.Ping: {
				let pg: { sendTs: bigint; reply: boolean }
				try {
					pg = decodePing(f.body)
				} catch {
					return false // 帧体损坏：忽略，不致 teardown
				}
				if (pg.reply) {
					// 应答经同一路径返回：样本是本路径干净的双向 RTT
					const rMs = (this.sendTs() - Number(pg.sendTs)) / 1000
					p.noteRTT(rMs, performance.now(), Number(pg.sendTs))
					return false
				}
				// 探测请求：在同路径上原样回显（调度之外，归属明确）
				this.sendPing(p, true, Number(pg.sendTs))
				return false
			}
			case FrameType.Telemetry: {
				let tm: { rates: PathRateMsg[] }
				try {
					tm = decodeTelemetry(f.body)
				} catch {
					return false
				}
				// 收端观测速率是独立校准源：对端视角的到达速率不受本端
				// ACK 时序失真影响。
				const now = performance.now()
				for (const r of tm.rates) {
					const q = this.pathsByID.get(r.pathId)
					if (q !== undefined) {
						q.peerRate = r.rateBps
						q.peerRateAt = now
					}
				}
				this.wakeSend() // est_rate 可能被校准，重估调度
				return false
			}
			case FrameType.PathRequest: {
				this.handlePathRequest(f.body)
				return false
			}
			case FrameType.PathReady: {
				this.handlePathReady(f.body)
				return false
			}
			case FrameType.Fin: {
				// FIN 属全流语义：只记「对端已正常关闭」，EOF 待全部路径
				// 收场后统一落地——多路径下其它路径上可能还有在途数据。
				this.finRecv = true
				this.broadcast(this.rWaiters)
				this.dropPath(p, new StreamEOFError(), false)
				return true
			}
			case FrameType.Rst: {
				this.shutdown(netaccErr('reset', 'netacc: 聚合流被对端重置'))
				return true
			}
			default:
				// PATH_ATTACH 只应出现在路径绑定子流首帧（聚合流上收到
				// 即忽略）；其余帧类型均已实现。
				return false
		}
	}

	/**
	 * freeAcked 释放 cum 之前的发送缓冲（unackedSegs），并把新确认字节
	 * 按段记账到其在途路径：inflight 核销 + delivery-rate 采样。
	 * path=null 的段（死路径遗留待重发）无路径可归，不产样本。
	 */
	private freeAcked(now: number): void {
		while (this.unackedSegs.length > 0) {
			const f = this.unackedSegs[0]!
			const end = f.off + f.data.length
			if (end <= this.cumAcked) {
				if (f.path !== null) {
					// sacked 段的 inflight 在被 SACK 覆盖时已释放，
					// 此处只补记交付量，否则二次核销会把账本扣成负数。
					if (f.sacked) {
						f.path.creditDelivered(f.data.length, now)
					} else {
						f.path.onDelivered(f.data.length, now)
					}
				}
				this.unackedSegs.shift()
				continue
			}
			if (f.off < this.cumAcked) {
				const trim = this.cumAcked - f.off
				if (f.path !== null) {
					if (f.sacked) {
						f.path.creditDelivered(trim, now)
					} else {
						f.path.onDelivered(trim, now)
					}
				}
				f.data = f.data.subarray(trim)
				f.off = this.cumAcked
			}
			break
		}
	}

	// ---------- PATH_REQUEST / PATH_READY ----------

	/**
	 * handlePathRequest 处理对端发来的 PATH_REQUEST：本侧向指定中继
	 * 做 reservation（经 NetaccClient 注入的 reserveRelay 钩子），
	 * 成败都回 PATH_READY。
	 */
	private handlePathRequest(body: Uint8Array): void {
		let req: ReturnType<typeof decodePathRequest>
		try {
			req = decodePathRequest(body)
		} catch {
			return // 帧体损坏：连 request_id 都取不出，无法应答——丢弃
		}
		const reqID = req.requestId
		const reserve = this.reserveRelayFn
		if (reserve === undefined) {
			this.sendPathReady(reqID, new Error('netacc: 本端未配置中继 reservation 能力（reserveRelay 未设置）'))
			return
		}
		void (async () => {
			try {
				await reserve(decodeRelayInfo(req))
				this.sendPathReady(reqID, undefined)
			} catch (e) {
				this.sendPathReady(reqID, toErr(e))
			}
		})()
	}

	private sendPathReady(reqID: number, err?: Error): void {
		this.writeCtrl(FrameType.PathReady, encodePathReady(reqID, err?.message ?? ''))
	}

	/** handlePathReady 处理对端回送的 PATH_READY：按 request_id 投递给等待中的 addRelayPath。 */
	private handlePathReady(body: Uint8Array): void {
		let pr: { requestId: number; error: string }
		try {
			pr = decodePathReady(body)
		} catch {
			return
		}
		const ch = this.pendingRelay.get(pr.requestId)
		if (ch === undefined) {
			return // 迟到/重复应答（调用方已放弃）：丢弃
		}
		ch.resolve(pr.error === '' ? undefined : new Error(pr.error))
	}

	// ---------- 关闭 ----------

	/**
	 * close 在全部存活路径上尽力发 FIN 告知对端（对端读尽残余数据后
	 * 得到 EOF），随后关闭全部底层路径并唤醒全部阻塞中的 read/write。
	 */
	async close(): Promise<void> {
		if (this.closed) {
			return
		}
		this.closed = true
		const paths = this.livePaths.slice()
		this.livePaths = []

		// 给每条路径写 FIN：尽力而为，带短超时兜底（对端不再读时
		// 底层写可能滞留）。
		const fin = encodeCtrlFrame(FrameType.Fin)
		await Promise.allSettled(
			paths.map(p => {
				p.dead = true
				const w = p.writeChain.then(() => p.conn.write(fin)).then(() => p.conn.close())
				return Promise.race([w, delay(500)])
			}),
		)
		for (const p of paths) {
			p.conn.abort(new Error('netacc: 流已关闭'))
			if (p.own !== undefined) {
				void p.own.close().catch(() => {})
			}
		}
		this.finishTeardown()
	}

	/** shutdown 异常终态（全部路径失活/RST/帧格式违例）；幂等。 */
	private shutdown(err: Error): void {
		if (this.closed) {
			return
		}
		this.closed = true
		if (this.rerr === null) {
			this.rerr = err
		}
		if (this.werr === null) {
			this.werr = err
		}
		const paths = this.livePaths.slice()
		this.livePaths = []
		for (const p of paths) {
			p.dead = true
			p.conn.abort(err)
			if (p.own !== undefined) {
				void p.own.close().catch(() => {})
			}
		}
		this.finishTeardown()
	}

	/** finishTeardown：唤醒等待者、停 tick、反注册、关闭订阅。 */
	private finishTeardown(): void {
		if (this.tickTimer !== null) {
			clearInterval(this.tickTimer)
			this.tickTimer = null
		}
		if (this.rerr === null && this.finRecv) {
			this.rerr = EOF
		}
		if (this.werr === null && this.finRecv && this.closed) {
			// 对端正常关闭后本侧 write 报 closed 语义
			this.werr = netaccErr('closed', 'netacc: 聚合流已关闭')
		}
		// 进行中的 PATH_REQUEST 协调按流关闭收尾
		for (const pr of this.pendingRelay.values()) {
			pr.resolve(netaccErr('closed', 'netacc: 聚合流已关闭'))
		}
		this.pendingRelay.clear()
		this.broadcast(this.rWaiters)
		this.broadcast(this.wWaiters)
		if (this.onClosed !== undefined) {
			this.onClosed(this)
		}
		for (const s of this.subs) {
			s.end()
		}
		this.subs.clear()
	}

	// ---------- 观测 ----------

	/** stats 返回聚合流的指标快照（纯快照，无共享状态）。 */
	stats(): StreamStats {
		const now = performance.now()
		const st: StreamStats = {
			id: this.id.slice(),
			peer: this.peer?.toString() ?? '',
			paths: [],
			txRateBps: 0,
			rxRateBps: 0,
			sentBytes: this.sentOff,
			ackedBytes: this.cumAcked,
			inflight: 0,
			pendingBytes: this.pendingBytes,
			sendBufCap: this.cfg.sendBufCap,
			lostSegs: this.lostSegs,
			resentSegs: this.resentSegs,
			resentBytes: this.resentBytes,
			recvCum: this.rbuf.cum(),
			recvBufCap: this.rbuf.cap,
			recvBuffered: this.rbuf.sizeBytes(),
			recvReady: this.rbuf.avail(),
			recvDropped: this.rbuf.dropped,
		}
		for (const p of this.livePaths) {
			st.inflight += p.inflight
			st.txRateBps += p.effRate(now)
			st.rxRateBps += p.rxRate
			const pi = p.info(now)
			st.paths.push({
				...pi,
				delivered: p.delivered,
				rxBytes: p.rxBytes,
				rxRateBps: p.rxRate,
				resentSegs: p.resentSegs,
				resentBytes: p.resentBytes,
			} satisfies PathStats)
		}
		return st
	}

	/**
	 * subscribe 订阅本聚合流的路径事件（规格书 §8）：返回异步可迭代
	 * 事件流与退订函数；退订或流终结时迭代结束。
	 *
	 * maxQueue 是事件队列缓冲长度（默认 16）。慢订阅者不阻塞数据面：
	 * 队列满即丢该订阅者的本事件，下一条成功投递的事件经
	 * event.dropped 携带期间被丢弃的数量。
	 */
	subscribe(maxQueue = 16): { events: AsyncIterable<StreamEvent>; unsubscribe: () => void } {
		const q = new SubQueue(Math.max(1, maxQueue))
		if (this.closed) {
			q.end()
			return { events: q, unsubscribe: () => {} }
		}
		this.subs.add(q)
		let done = false
		const unsubscribe = () => {
			if (done) {
				return
			}
			done = true
			this.subs.delete(q)
			q.end()
		}
		return { events: q, unsubscribe }
	}

	private emitEvent(type: StreamEventType, info: PathInfo, cause?: Error): void {
		if (this.subs.size === 0) {
			return
		}
		const ev: StreamEvent = { type, pathId: info.id, info, cause, at: Date.now(), dropped: 0 }
		for (const s of this.subs) {
			s.push(ev)
		}
	}

	// ---------- 内部同步原语 ----------

	/** broadcast 唤醒一组等待者（等价于 chan 替换式广播）。 */
	private broadcast(waiters: Set<() => void>): void {
		for (const w of waiters) {
			w()
		}
		waiters.clear()
	}

	/** wait 在状态广播上挂起；被唤醒后由调用方重新检查条件。 */
	private wait(waiters: Set<() => void>): Promise<void> {
		return new Promise<void>((resolve) => {
			waiters.add(resolve)
		})
	}
}

function firstPath(paths: Path[]): Path {
	return paths[0]!
}

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/** abortPromise 在 signal abort 时 reject（配合 Promise.race 做超时/取消）。 */
function abortPromise(signal?: AbortSignal): Promise<never> {
	if (signal === undefined) {
		return new Promise<never>(() => {}) // 永不 resolve
	}
	if (signal.aborted) {
		return Promise.reject(signal.reason instanceof Error ? signal.reason : netaccErr('timeout', 'netacc: 操作被取消'))
	}
	return new Promise<never>((_, reject) => {
		signal.addEventListener('abort', () => {
			reject(signal.reason instanceof Error ? signal.reason : netaccErr('timeout', 'netacc: 操作被取消'))
		}, { once: true })
	})
}

/** firstHole 返回 [off,end) 内第一个未被 ranges 覆盖的子区间。 */
function firstHole(off: number, end: number, rs: ByteRange[]): [number, number] {
	let cur = off
	for (const r of rs) {
		if (r.end <= cur) {
			continue
		}
		if (r.start > cur) {
			return [cur, Math.min(r.start, end)]
		}
		cur = r.end
		if (cur >= end) {
			return [end, end]
		}
	}
	if (cur < end) {
		return [cur, end]
	}
	return [end, end]
}

/** coveredBy 报告 [off,end) 是否被某条已合并区间完整覆盖。 */
function coveredBy(rs: ByteRange[], off: number, end: number): boolean {
	for (const r of rs) {
		if (r.start <= off && r.end >= end) {
			return true
		}
	}
	return false
}

/** normalizeRanges 把对端发来的 SACK 区间排序合并为规范形式。 */
function normalizeRanges(rs: ByteRange[]): ByteRange[] {
	if (rs.length < 2) {
		return rs
	}
	rs.sort((a, b) => a.start - b.start)
	const out: ByteRange[] = []
	for (const r of rs) {
		const last = out[out.length - 1]
		if (last !== undefined && r.start <= last.end) {
			if (r.end > last.end) {
				last.end = r.end
			}
			continue
		}
		out.push(r)
	}
	return out
}

function validateAck(
	f: DecodedFrame,
	sentOff: number,
	cumAcked: number,
	peerAckSeq: number,
	gotPeerAck: boolean,
): Error | null {
	if (f.cum < cumAcked || f.cum > sentOff) {
		return new Error(`ACK cum ${f.cum} outside [${cumAcked},${sentOff}]`)
	}
	const prevSeq = gotPeerAck ? peerAckSeq : 0
	if (f.seq < prevSeq || f.seq - prevSeq > MAX_ACK_SEQ_ADVANCE) {
		return new Error(`ACK seq ${f.seq} exceeds accepted advance from ${prevSeq}`)
	}
	for (const r of f.ranges) {
		if (r.start <= f.cum || r.end > sentOff) {
			return new Error(`ACK range [${r.start},${r.end}) outside (${f.cum},${sentOff}]`)
		}
	}
	return null
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) {
		return false
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false
		}
	}
	return true
}

/** decodeRelayInfo 从 PathRequest 提取中继路径描述符（坏地址跳过）。 */
function decodeRelayInfo(req: { relayPeer: Uint8Array; relayAddrs: Uint8Array[] }): { peerId: PeerId; addrs: Multiaddr[] } {
	let peerId: PeerId | undefined
	// Go 端 peer.IDFromBytes 同时兼容 CID 与裸 multihash 两种字节形态。
	try {
		peerId = peerIdFromCID(CID.decode(req.relayPeer))
	} catch {
		try {
			peerId = peerIdFromMultihash(MultihashDigest.decode(req.relayPeer))
		} catch {
			/* fallthrough */
		}
	}
	if (peerId === undefined) {
		throw new Error('netacc: PathRequest 的 relay_peer 非法')
	}
	const addrs: Multiaddr[] = []
	for (const ab of req.relayAddrs) {
		try {
			let a = multiaddr(ab)
			// 剥离 /p2p/<peer> 后缀（AddrInfoToP2pAddrs 形态兼容）
			const comps = a.getComponents()
			const last = comps[comps.length - 1]
			if (last !== undefined && last.name === 'p2p' && last.value !== undefined) {
				a = a.decapsulate(`/p2p/${last.value}`)
			}
			addrs.push(a)
		} catch {
			continue
		}
	}
	return { peerId, addrs }
}

/** 带满丢策略的订阅队列。 */
class SubQueue implements AsyncIterable<StreamEvent> {
	private q: StreamEvent[] = []
	private waiters: Array<(r: IteratorResult<StreamEvent>) => void> = []
	private ended = false
	private droppedCount = 0

	constructor(private readonly cap: number) {}

	push(ev: StreamEvent): void {
		const withDropped = { ...ev, dropped: this.droppedCount }
		if (this.q.length >= this.cap) {
			this.droppedCount++
			return
		}
		this.droppedCount = 0
		const w = this.waiters.shift()
		if (w !== undefined) {
			w({ value: withDropped, done: false })
			return
		}
		this.q.push(withDropped)
	}

	end(): void {
		this.ended = true
		for (const w of this.waiters) {
			w({ value: undefined, done: true })
		}
		this.waiters = []
	}

	[Symbol.asyncIterator](): AsyncIterator<StreamEvent> {
		const self = this
		return {
			next(): Promise<IteratorResult<StreamEvent>> {
				const v = self.q.shift()
				if (v !== undefined) {
					return Promise.resolve({ value: v, done: false })
				}
				if (self.ended) {
					return Promise.resolve({ value: undefined, done: true })
				}
				return new Promise(resolve => self.waiters.push(resolve))
			},
		}
	}
}
