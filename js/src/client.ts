/**
 * NetaccClient：聚合库的公开入口（对应 Go 端 Aggregator）。
 *
 * 构造时在一个已配置好的 js-libp2p node 上注册两条协议处理器：
 *   - /netacc/agg/1.0.0  聚合流握手（握手流升格为首条数据路径）
 *   - /netacc/path/1.0.0 路径绑定子流（PATH_ATTACH 凭 agg_stream_id 挂接）
 *
 * 传输能力（WebSocket/WebTransport/WebRTC/circuit-relay）完全由调用方
 * node 的 transport 配置决定，本包只消费 dial/dialProtocol/handle 与
 * multiaddr。
 */

import type { Connection, Libp2p, PeerId, Stream } from '@libp2p/interface'
import { peerIdFromString } from '@libp2p/peer-id'
import { multiaddr, type Multiaddr } from '@multiformats/multiaddr'
import { Libp2pByteStream } from './libp2p-stream.js'
import { StreamReader, type ByteStream } from './bytestream.js'
import { FrameDecoder, FrameType, encodeCtrlFrame } from './frame.js'
import { decodePathAttach, encodePathAttach } from './proto.js'
import {
	AGG_STREAM_ID_LEN,
	handshakeInitiator,
	handshakeResponderAck,
	handshakeResponderRead,
} from './handshake.js'
import {
	AggregatedStream,
	type AggregatedStreamOptions,
	type PathDialer,
	type RelayReserver,
} from './stream.js'
import { Path } from './path.js'
import { netaccErr } from './types.js'
import { transportOf } from './multiaddr.js'
import { tunnelFetch as tunnelFetchCore, type TunnelFetchInit } from './tunnel-http.js'
import { tunnelWs as tunnelWsCore, type TunnelWebSocket, type TunnelWSInit } from './tunnel-ws.js'

/** 聚合流握手协议号（兼首条数据路径）。 */
export const PROTOCOL_AGG = '/netacc/agg/1.0.0'
/** 路径绑定协议号（规格书 §4.2/§4.3）。 */
export const PROTOCOL_PATH = '/netacc/path/1.0.0'

const DEFAULT_ACCEPT_BACKLOG = 16
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 60_000

/** StreamTarget：openStream/tunnelFetchTo/tunnelWsTo 支持的对端寻址形态。 */
export type StreamTarget = PeerId | Multiaddr | Multiaddr[] | string

export interface NetaccClientOptions {
	/** 待 accept 的入向握手流排队长度（默认 16）。 */
	acceptBacklog?: number
	/** 握手阶段超时（默认 60s；调用方 signal 触发时以 signal 为准）。 */
	handshakeTimeoutMs?: number
	/** 收端重排缓冲（连接级接收窗口）下界（字节，默认 1MiB）。 */
	reorderMin?: number
	/** 收端重排缓冲硬顶（默认 32MiB）。 */
	reorderMax?: number
	/** 发送缓冲上限（待发+在途字节硬顶，默认 2×窗口下界）。 */
	sendBufCap?: number
	/**
	 * 建流成功门槛：至少 n 条数据路径 attached（默认 1）。
	 * n>1 时 openStream 在握手后用 peerStore 中的对端地址直拨补挂。
	 */
	minPaths?: number
	/** 数据面周期节拍（TELEMETRY/窗口重算/软失效扫描，默认 200ms）。 */
	telemetryIntervalMs?: number
	/**
	 * PATH_REQUEST 响应钩子：对端请求经中继建路径时调用，
	 * 应完成向指定中继的 reservation（js-libp2p 暂无公开的手动
	 * reservation API，需应用侧注入实现——例如调用自有 relay
	 * 协调通道或 @libp2p/circuit-relay-v2 内部接口）。
	 * 未提供时收到 PATH_REQUEST 会直接回 PATH_READY 错误应答。
	 */
	reserveRelay?: RelayReserver
	/**
	 * 是否允许 netacc 子流跑在「受限连接」（circuit-relay 限额连接）上。
	 * 聚合协议明确需要中继路径能力，默认 true。
	 */
	runOnLimitedConnection?: boolean
	/**
	 * tunnelFetch/tunnelWs 一参数形式的默认 server 对端。
	 * 未设置时这两个方法抛错，请改用 tunnelFetchTo/tunnelWsTo 显式指定。
	 */
	tunnelTarget?: StreamTarget
}

/** OpenStreamOptions 是单次 openStream 的生效配置（逐调用覆盖构造默认）。 */
export interface OpenStreamOptions {
	/** 取消/超时信号（AbortSignal.timeout(ms) 等）。 */
	signal?: AbortSignal
	/** 握手超时（覆盖构造默认）。 */
	handshakeTimeoutMs?: number
	/** 该条流的重排缓冲下界/硬顶。 */
	reorderMin?: number
	reorderMax?: number
	/** 该条流的发送缓冲上限。 */
	sendBufCap?: number
	/** 该条流的最小路径数门槛。 */
	minPaths?: number
	telemetryIntervalMs?: number
}

interface InboundHandshake {
	stream: Stream
	conn: Connection
}

/** 把 u64 path_id → 字符串 map key。 */
function idKey(id: Uint8Array): string {
	let s = ''
	for (const b of id) {
		s += String.fromCharCode(b)
	}
	return s // 16 字节二进制串作 key
}

/**
 * 把 openStream 目标转成 libp2p DialTarget：
 * PeerId | Multiaddr | Multiaddr[] | 字符串（'/...' → multiaddr，否则 peerID）。
 */
function toDialTarget(target: StreamTarget): PeerId | Multiaddr | Multiaddr[] {
	if (typeof target === 'string') {
		if (target.startsWith('/')) {
			return multiaddr(target)
		}
		return peerIdFromString(target)
	}
	if (Array.isArray(target)) {
		return target.map(a => (typeof a === 'string' ? multiaddr(a) : a))
	}
	return target
}

export class NetaccClient {
	private readonly node: Libp2p
	private readonly opts: Required<Pick<NetaccClientOptions, 'acceptBacklog' | 'handshakeTimeoutMs' | 'minPaths'>> &
		NetaccClientOptions
	private closed = false
	private readonly acceptQ: InboundHandshake[] = []
	private readonly acceptWaiters: Array<(v: InboundHandshake | null) => void> = []
	/** PATH_ATTACH 路由表：agg_stream_id → 聚合流。 */
	private readonly streams = new Map<string, AggregatedStream>()
	private readonly registration: Promise<void>
	private readonly streamHandlers: Array<(s: AggregatedStream) => void> = []
	private acceptLoop: Promise<void> | null = null

	/**
	 * 在 node 上创建 NetaccClient 并注册两个流处理器。
	 * 同一 node 上重复构造会互相覆盖处理器（js-libp2p handle 语义），应避免。
	 */
	constructor(node: Libp2p, options?: NetaccClientOptions) {
		this.node = node
		this.opts = {
			acceptBacklog: DEFAULT_ACCEPT_BACKLOG,
			handshakeTimeoutMs: DEFAULT_HANDSHAKE_TIMEOUT_MS,
			minPaths: 1,
			...options,
		}
		const runLimited = options?.runOnLimitedConnection ?? true
		// 注册失败（如重复 handle）会推迟到首个公开方法调用时抛出
		this.registration = Promise.all([
			node.handle(PROTOCOL_AGG, (stream, conn) => this.onInboundAgg(stream, conn), {
				runOnLimitedConnection: runLimited,
			}),
			node.handle(PROTOCOL_PATH, (stream, conn) => void this.onInboundPath(stream, conn), {
				runOnLimitedConnection: runLimited,
			}),
		]).then(() => {})
		this.registration.catch(() => {}) // 防 unhandled rejection
	}

	// ---------- 出向 ----------

	/**
	 * openStream 向对端发起一条聚合流：在（已有或新建）连接上开
	 * /netacc/agg/1.0.0 握手流，协商 128bit agg_stream_id；握手流
	 * 升格为第一条数据路径。
	 *
	 * target 可以是 PeerId、multiaddr（含/不含 /p2p/ 后缀）、multiaddr
	 * 数组或字符串。minPaths>1 时用 peerStore 地址继续直拨补挂。
	 */
	async openStream(
		target: StreamTarget,
		options?: OpenStreamOptions,
	): Promise<AggregatedStream> {
		await this.registration
		if (this.closed) {
			throw netaccErr('closed', 'netacc: NetaccClient 已关闭')
		}
		const hsTimeout = options?.handshakeTimeoutMs ?? this.opts.handshakeTimeoutMs
		const signal = combineSignal(options?.signal, hsTimeout)

		const conn = await this.node.dial(toDialTarget(target), { signal })
		const peer = conn.remotePeer
		let bs: Libp2pByteStream | undefined
		try {
			const stream = await conn.newStream(PROTOCOL_AGG, {
				signal,
				runOnLimitedConnection: this.opts.runOnLimitedConnection ?? true,
			})
			bs = new Libp2pByteStream(stream, conn)
		} catch (e) {
			throw wrapHandshakeErr(e)
		}
		// 握手超时兜底：signal abort 时 reset 底层流打断阻塞 I/O
		const onAbort = () => bs!.abort(netaccErr('timeout', 'netacc: 握手超时'))
		signal?.addEventListener('abort', onAbort, { once: true })
		try {
			const reader = new StreamReader(bs)
			const id = await handshakeInitiator(bs, reader)
			const st = new AggregatedStream(this.streamOpts({
				id,
				firstPath: bs,
				initiator: true,
				peer,
			}, options))
			// 先注册再 start：对端收到 HelloAck 后即可反向 PATH_ATTACH 过来
			this.register(st)
			st.start()
			const minPaths = options?.minPaths ?? this.opts.minPaths
			if (minPaths > 1) {
				try {
					await this.attachMinPaths(st, peer, minPaths, options?.signal)
				} catch (e) {
					await st.close().catch(() => {})
					throw e
				}
			}
			return st
		} catch (e) {
			bs.abort(toErr(e))
			throw wrapHandshakeErr(e)
		} finally {
			signal?.removeEventListener('abort', onAbort)
		}
	}

	/**
	 * attachMinPaths 兑现 minPaths(n)：用 peerStore 中的对端地址逐条
	 * 直拨补挂路径，直到存活路径数 ≥ n。一轮全地址零进展即放弃。
	 */
	private async attachMinPaths(st: AggregatedStream, peer: PeerId, n: number, signal?: AbortSignal): Promise<void> {
		let addrs: Multiaddr[] = []
		try {
			const info = await this.node.peerStore.get(peer, { signal })
			addrs = (info.addresses ?? []).map(a => a.multiaddr)
		} catch {
			// peerStore 无记录 → 无地址可补
		}
		if (addrs.length === 0) {
			throw netaccErr('min_paths', `netacc: peerStore 无对端地址，minPaths(${n}) 无法补挂路径`)
		}
		// 按 §3.3 传输偏好先排序：TCP/WS 优先于 UDP 系
		addrs.sort((a, b) => transportPriority(transportOf(a)) - transportPriority(transportOf(b)))
		while (st.paths().length < n) {
			let progress = false
			for (const addr of addrs) {
				if (st.paths().length >= n) {
					break
				}
				try {
					await st.addPath(addr, { signal })
					progress = true
				} catch {
					// 单地址失败不影响其它地址重试
				}
			}
			if (!progress) {
				throw netaccErr('min_paths', `netacc: 补挂路径后仍只有 ${st.paths().length} 条，不满足 minPaths(${n})`)
			}
		}
	}

	// ---------- tunnel（HTTP/WS over 聚合流） ----------

	/**
	 * tunnelFetch：经构造默认的 tunnelTarget 发一次 HTTP 请求
	 * （等价 tunnelFetchTo(this.opts.tunnelTarget, input, init)）。
	 * input 为 '/path'（server 本地 handler）或 http(s):// 绝对 URL（代理）。
	 */
	tunnelFetch(input: string | URL, init?: TunnelFetchInit): Promise<Response> {
		return this.tunnelFetchTo(this.requireTunnelTarget(), input, init)
	}

	/** tunnelFetchTo：向指定对端发一次 HTTP-over-tunnel 请求。 */
	tunnelFetchTo(target: StreamTarget, input: string | URL, init?: TunnelFetchInit): Promise<Response> {
		// 薄 wrapper：会话逻辑在 tunnel-http.ts，这里只注入 openStream
		return tunnelFetchCore(opts => this.openStream(target, opts), input, init)
	}

	/**
	 * tunnelWs：经构造默认的 tunnelTarget 建一条 WebSocket-over-tunnel
	 * 会话（等价 tunnelWsTo(this.opts.tunnelTarget, url, init)）。
	 * url 为 '/path'（server 本地 WS handler）或 ws(s):// 绝对 URL（代理）。
	 */
	tunnelWs(url: string | URL, init?: TunnelWSInit): TunnelWebSocket {
		return this.tunnelWsTo(this.requireTunnelTarget(), url, init)
	}

	/** tunnelWsTo：向指定对端建 WebSocket-over-tunnel，立即返回 CONNECTING 态对象。 */
	tunnelWsTo(target: StreamTarget, url: string | URL, init?: TunnelWSInit): TunnelWebSocket {
		return tunnelWsCore(opts => this.openStream(target, opts), url, init)
	}

	private requireTunnelTarget(): StreamTarget {
		const t = this.opts.tunnelTarget
		if (t === undefined) {
			throw netaccErr('internal', 'netacc: 未配置 tunnelTarget，请改用 tunnelFetchTo/tunnelWsTo 显式指定对端')
		}
		return t
	}

	// ---------- 入向 ----------

	/**
	 * accept 接收一条对端发起的聚合流，完成握手应答后返回。
	 * 无入流时挂起；可用 signal 取消。单条入流握手失败不影响后续入流。
	 */
	async accept(options?: { signal?: AbortSignal }): Promise<AggregatedStream> {
		await this.registration
		for (;;) {
			if (this.closed) {
				throw netaccErr('closed', 'netacc: NetaccClient 已关闭')
			}
			const item = await this.popInbound(options?.signal)
			if (item === null) {
				throw netaccErr('closed', 'netacc: NetaccClient 已关闭')
			}
			const bs = new Libp2pByteStream(item.stream, item.conn)
			const hsTimeout = this.opts.handshakeTimeoutMs
			const signal = combineSignal(options?.signal, hsTimeout)
			const onAbort = () => bs.abort(netaccErr('timeout', 'netacc: 握手超时'))
			signal?.addEventListener('abort', onAbort, { once: true })
			try {
				const reader = new StreamReader(bs)
				const id = await handshakeResponderRead(reader)
				const st = new AggregatedStream(this.streamOpts({
					id,
					firstPath: bs,
					initiator: false,
					peer: item.conn.remotePeer,
				}, undefined))
				// 注册先于回执：HelloAck 到达对端后，对端的 PATH_ATTACH
				// 随时可能到——路由表必须先就位。
				this.register(st)
				await handshakeResponderAck(bs, id)
				st.start()
				return st
			} catch (e) {
				bs.abort(new Error('netacc: 握手失败'))
				// 调用方 signal abort → 整个 accept 取消，不再消费后续入流
				if (options?.signal?.aborted === true) {
					throw options.signal.reason instanceof Error
						? options.signal.reason
						: netaccErr('timeout', 'netacc: accept 被取消')
				}
				continue // 坏握手不影响后续入流
			} finally {
				signal?.removeEventListener('abort', onAbort)
			}
		}
	}

	/**
	 * onStream 注册回调持续接收入向聚合流（accept 的事件式等价物）：
	 * 注册后启动后台 accept 循环，每条新流回调一次；返回退订函数。
	 * 回调内异常被忽略（如需逐个控制接收节奏请用 accept()）。
	 */
	onStream(handler: (s: AggregatedStream) => void): () => void {
		this.streamHandlers.push(handler)
		this.ensureAcceptLoop()
		return () => {
			const i = this.streamHandlers.indexOf(handler)
			if (i >= 0) {
				this.streamHandlers.splice(i, 1)
			}
		}
	}

	/** 后台 accept 循环：有订阅者时持续接收并回调。 */
	private ensureAcceptLoop(): void {
		if (this.acceptLoop !== null) {
			return
		}
		this.acceptLoop = (async () => {
			while (!this.closed && this.streamHandlers.length > 0) {
				try {
					const s = await this.accept()
					for (const h of this.streamHandlers.slice()) {
						try {
							h(s)
						} catch {
							// 应用回调异常不影响接收循环
						}
					}
				} catch {
					// 单条入流失败（含关闭期间）不影响循环
					if (this.closed) {
						return
					}
				}
			}
		})().finally(() => {
			this.acceptLoop = null
		})
	}

	/** 入向聚合流的异步迭代视图（内部驱动 accept 循环）。 */
	async *[Symbol.asyncIterator](): AsyncIterator<AggregatedStream> {
		for (;;) {
			if (this.closed) {
				return
			}
			try {
				yield await this.accept()
			} catch (e) {
				if (this.closed) {
					return
				}
				throw e
			}
		}
	}

	/** 关闭：注销流处理器；已建立的聚合流不受影响（路由表保留至各流自收尾）。 */
	async close(): Promise<void> {
		if (this.closed) {
			return
		}
		this.closed = true
		try {
			await this.node.unhandle([PROTOCOL_AGG, PROTOCOL_PATH])
		} catch {
			// 注销失败忽略
		}
		for (const w of this.acceptWaiters) {
			w(null)
		}
		this.acceptWaiters.length = 0
		this.acceptQ.length = 0
		this.streamHandlers.length = 0
	}

	// ---------- 内部 ----------

	/** onInboundAgg 是入向握手流处理器：只做入队，握手在 accept 里完成。 */
	private onInboundAgg(stream: Stream, conn: Connection): void {
		if (this.closed) {
			stream.abort(new Error('netacc: client 已关闭'))
			return
		}
		const item = { stream, conn }
		const w = this.acceptWaiters.shift()
		if (w !== undefined) {
			w(item)
			return
		}
		if (this.acceptQ.length >= this.opts.acceptBacklog) {
			stream.abort(new Error('netacc: accept 队列已满'))
			return
		}
		this.acceptQ.push(item)
	}

	private async popInbound(signal?: AbortSignal): Promise<InboundHandshake | null> {
		const q = this.acceptQ.shift()
		if (q !== undefined) {
			return q
		}
		if (this.closed) {
			return null
		}
		return Promise.race([
			new Promise<InboundHandshake | null>(resolve => this.acceptWaiters.push(resolve)),
			abortableNever(signal),
		])
	}

	/**
	 * onInboundPath 是入向路径绑定处理器（/netacc/path/1.0.0）：
	 * 读首帧 PATH_ATTACH → 按 agg_stream_id 查路由表 → 校验 path_id
	 * 落在对端命名空间且未用过 → 回显同一 PATH_ATTACH 帧表示接受 →
	 * 子流升格为该聚合流的新数据路径。任一步失败直接 reset。
	 */
	private async onInboundPath(stream: Stream, conn: Connection): Promise<void> {
		const fail = () => stream.abort(new Error('netacc: 路径绑定失败'))
		if (this.closed) {
			fail()
			return
		}
		const bs = new Libp2pByteStream(stream, conn)
		const reader = new StreamReader(bs)
		const fr = new FrameDecoder(reader)
		const hsTimeout = this.opts.handshakeTimeoutMs
		const timer = setTimeout(() => bs.abort(netaccErr('timeout', 'netacc: 路径绑定超时')), hsTimeout)
		try {
			const first = await fr.next()
			if (first === null || first.type !== FrameType.PathAttach) {
				fail()
				return
			}
			const { aggStreamId, pathId } = decodePathAttach(first.frame.body)
			if (aggStreamId.length !== AGG_STREAM_ID_LEN) {
				fail()
				return
			}
			const st = this.streams.get(idKey(aggStreamId))
			if (st === undefined) {
				fail() // 不认识的 agg_stream_id
				return
			}
			// 预定 path_id：分侧命名空间 + 不重复（死路径重加须换新 id）
			if (!st.reserveRemotePathID(pathId)) {
				fail()
				return
			}
			// 先回显再挂接：挂接后发送泵可能立刻往该子流条带 DATA，
			// 若回显与之交错会污染帧边界（回显在 attachPath 之前写出）。
			await bs.write(encodeCtrlFrame(FrameType.PathAttach, first.frame.body))
			st.attachPath(new Path({ id: pathId, conn: bs, dialed: false }), fr)
		} catch {
			fail()
		} finally {
			clearTimeout(timer)
		}
	}

	/** register/unregister 维护 PATH_ATTACH 路由表。 */
	private register(st: AggregatedStream): void {
		this.streams.set(idKey(st.id), st)
	}

	private unregister(st: AggregatedStream): void {
		const k = idKey(st.id)
		if (this.streams.get(k) === st) {
			this.streams.delete(k)
		}
	}

	/** 聚合流的拨号器：直拨 addr 建一条新物理连接并开绑定子流。 */
	private makeDialer(peer: PeerId | undefined): PathDialer | undefined {
		if (peer === undefined) {
			return undefined
		}
		return async (addr, opts) => {
			// force: true——swarm/dialer 去重会复用既有连接，聚合需要
			// 确定性的新物理路径（规格 §3.1 与 Go 端 dialDirect 对应）。
			const conn = await this.node.dial(addr, { force: true, signal: opts?.signal })
			// 防御拨错：无加密对端认证的传输不会校验 peer
			if (!conn.remotePeer.equals(peer)) {
				await conn.close().catch(() => {})
				throw netaccErr('handshake', `netacc: 直拨 ${addr.toString()} 的对端 ${conn.remotePeer.toString()} 不是目标 peer ${peer.toString()}`)
			}
			const ms = await conn.newStream(PROTOCOL_PATH, {
				signal: opts?.signal,
				runOnLimitedConnection: this.opts.runOnLimitedConnection ?? true,
			})
			return { stream: new Libp2pByteStream(ms, conn), conn }
		}
	}

	/** streamOpts 组装聚合流配置（构造默认 + 逐调用覆盖）。 */
	private streamOpts(
		base: Pick<AggregatedStreamOptions, 'id' | 'firstPath' | 'initiator' | 'peer'>,
		open?: OpenStreamOptions,
	): AggregatedStreamOptions {
		return {
			...base,
			minBuf: open?.reorderMin ?? this.opts.reorderMin,
			maxBuf: open?.reorderMax ?? this.opts.reorderMax,
			sendBufCap: open?.sendBufCap ?? this.opts.sendBufCap,
			telemetryIntervalMs: open?.telemetryIntervalMs ?? this.opts.telemetryIntervalMs,
			dialer: this.makeDialer(base.peer),
			reserveRelay: this.opts.reserveRelay,
			onClosed: s => this.unregister(s),
		}
	}
}

function toErr(e: unknown): Error {
	return e instanceof Error ? e : new Error(String(e))
}

/** wrapHandshakeErr 把握手阶段失败语义化为 handshake 错误码。 */
function wrapHandshakeErr(e: unknown): Error {
	if (e instanceof Error && 'code' in e && (e as { code: string }).code !== '') {
		return e
	}
	return netaccErr('handshake', `netacc: 聚合流握手失败: ${toErr(e).message}`, e)
}

/** combineSignal 合并调用方 signal 与超时。 */
function combineSignal(user: AbortSignal | undefined, timeoutMs: number | undefined): AbortSignal | undefined {
	const sigs: AbortSignal[] = []
	if (timeoutMs !== undefined && timeoutMs > 0) {
		sigs.push(AbortSignal.timeout(timeoutMs))
	}
	if (user !== undefined) {
		sigs.push(user)
	}
	if (sigs.length === 0) {
		return undefined
	}
	return sigs.length === 1 ? sigs[0] : AbortSignal.any(sigs)
}

/** abortableNever 返回一个在 signal abort 时 reject 的 promise（永不 resolve）。 */
function abortableNever(signal?: AbortSignal): Promise<never> {
	if (signal === undefined) {
		return new Promise<never>(() => {})
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

function transportPriority(t: string): number {
	switch (t) {
		case 'tcp':
		case 'websocket':
			return 0
		case 'quic':
			return 1
		case 'webtransport':
			return 2
		case 'webrtc-direct':
			return 3
		default:
			return 4
	}
}
