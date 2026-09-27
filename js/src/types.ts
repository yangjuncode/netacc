/**
 * 公开类型与错误定义。
 *
 * 错误以带 name 的 Error 子类暴露（无 Node 依赖、浏览器安全）；
 * 用 `e instanceof NetaccError`/`e.code` 或 `errors.ts` 导出的
 * 谓词判定。
 */

export class NetaccError extends Error {
	/** 稳定的机器可读错误码（与 Go 端 errors.Is 判定值一一对应）。 */
	constructor(
		public readonly code: NetaccErrorCode,
		message: string,
		options?: { cause?: unknown },
	) {
		super(message, options)
		this.name = 'NetaccError'
	}
}

export type NetaccErrorCode =
	| 'closed' // NetaccClient/聚合流已关闭
	| 'handshake' // 聚合流握手失败（对端未跑本协议/拒绝/回显不符）
	| 'min_paths' // WithMinPaths 门槛未满足
	| 'last_path' // 不能摘除最后一条数据路径
	| 'unknown_path' // path_id 不在存活集
	| 'no_paths' // 存活路径归零后的流终态
	| 'reset' // 流被对端 RST
	| 'timeout' // 操作超时（握手/读写 deadline）
	| 'internal'

export const ErrClosed = { code: 'closed' as const }
export const ErrHandshake = { code: 'handshake' as const }
export const ErrMinPaths = { code: 'min_paths' as const }
export const ErrLastPath = { code: 'last_path' as const }
export const ErrUnknownPath = { code: 'unknown_path' as const }
export const ErrNoPaths = { code: 'no_paths' as const }
export const ErrReset = { code: 'reset' as const }

export function netaccErr(code: NetaccErrorCode, message: string, cause?: unknown): NetaccError {
	return new NetaccError(code, message, cause !== undefined ? { cause } : undefined)
}

/** 对端正常关闭（EOF）：read() 用 null 表达，这里供逐路径终态内部传递。 */
export class StreamEOFError extends Error {
	constructor(message = 'netacc: EOF') {
		super(message)
		this.name = 'StreamEOFError'
	}
}

/** PathTransport 标识一条数据路径所用的底层传输（与 Go 端 PathTransport 一致）。 */
export type PathTransport =
	| 'unknown'
	| 'tcp'
	| 'websocket'
	| 'quic'
	| 'webtransport'
	| 'webrtc-direct'
	| 'relay'

/** PathInfo 是一条数据路径的观测快照（规格书 §8）。 */
export interface PathInfo {
	/** path_id（创建方命名空间，两端视图一致）。 */
	id: number
	/** 本侧是否为该路径的发起方（拨号+attach 一侧）。 */
	dialed: boolean
	/** 底层连接标识（尽力而为，区分同 peer 多连接的路径归属）。 */
	connId: string
	/** 底层传输类型（由本端/对端 multiaddr 反推）。 */
	transport: PathTransport
	/** 本端地址（multiaddr 字符串，取不到为空字符串）。 */
	local: string
	/** 对端地址。 */
	remote: string
	/** 挂接时刻（epoch ms）。 */
	attachedAt: number
	/** 平滑 RTT（ms，含底层排队延迟；无样本为 0）。 */
	srttMs: number
	/** 最小 RTT（ms，传播延迟基线）。 */
	minRttMs: number
	/** 速率估计（字节/秒，含对端 TELEMETRY 校准）。 */
	estRateBps: number
	/** 当前在途未确认字节数。 */
	inflight: number
	/** 软失效可疑标记：在途段超 k·srtt 未被 ACK/SACK 覆盖即置位。 */
	suspect: boolean
}

/** PathStats 在 PathInfo 之外附收发与重传累计计数。 */
export interface PathStats extends PathInfo {
	/** 本路径累计被对端累积确认的字节数。 */
	delivered: number
	/** 本路径累计到达字节数（本端收端视角，TELEMETRY 数据源）。 */
	rxBytes: number
	/** 本路径收端到达速率估计（字节/秒）。 */
	rxRateBps: number
	/** 在本路径上重发出的段（帧）数。 */
	resentSegs: number
	/** 在本路径上重发出的字节数。 */
	resentBytes: number
}

/** StreamStats 是聚合流的整体指标快照（纯快照，可安全长期持有）。 */
export interface StreamStats {
	/** agg_stream_id（握手协商出的 128bit 聚合流标识）。 */
	id: Uint8Array
	/** 对端 peer id 字符串（能拿到时）。 */
	peer: string
	/** 逐路径快照（存活路径全集）。 */
	paths: PathStats[]
	/** Σ 各路径有效速率估计（调度器视角的上行容量，字节/秒）。 */
	txRateBps: number
	/** Σ 各路径收端实测到达速率（字节/秒）。 */
	rxRateBps: number
	/** 已装帧下发的逻辑字节数（流内偏移总量，重发不重复计）。 */
	sentBytes: number
	/** 对端已累积确认的字节数。 */
	ackedBytes: number
	/** 当前在途未确认字节总量。 */
	inflight: number
	/** 待发队列字节数（已 write、尚未装帧）。 */
	pendingBytes: number
	/** 发送缓冲上限（待发+在途总量的硬顶）。 */
	sendBufCap: number
	/** 因路径死亡被改判「待换路重发」的段数。 */
	lostSegs: number
	/** 实际重发出的段数。 */
	resentSegs: number
	/** 实际重发出的字节数。 */
	resentBytes: number
	/** 已累计保序到达的字节数（累积 ACK 点）。 */
	recvCum: number
	/** 重排缓冲当前容量（随 Σ到达速率·max_srtt 联动）。 */
	recvBufCap: number
	/** 重排缓冲占用字节数（乱序暂存 + 已保序未读）。 */
	recvBuffered: number
	/** 其中已保序、可立即读走的字节数。 */
	recvReady: number
	/** 因超容量被丢弃的字节数（对端超发的防御性统计）。 */
	recvDropped: number
}

/** 聚合流事件类型（与 Go 端 EventType 对应）。 */
export type StreamEventType = 'path_added' | 'path_removed' | 'path_degraded'

/** 聚合流事件。 */
export interface StreamEvent {
	type: StreamEventType
	/** 涉事路径的 path_id。 */
	pathId: number
	/** 事发瞬间的路径快照（removed 为摘除前最后一眼）。 */
	info: PathInfo
	/** removed 事件的摘除原因（主动摘除为 undefined）。 */
	cause?: Error
	/** 事发时刻（epoch ms）。 */
	at: number
	/** 本订阅者在本事件之前被丢弃的事件数（队列满丢策略的漏报计数）。 */
	dropped: number
}
