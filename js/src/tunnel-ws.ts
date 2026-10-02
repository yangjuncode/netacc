/**
 * tunnelWs：WebSocket 语义的 WS-over-tunnel 客户端。
 *
 * 一条 WS 会话对应一条聚合流：
 *   openStream → magic → OPEN(ws) → 读 RESULT（status=101 进入 OPEN；
 *   status/error 为握手拒绝）→ 双向 WS_MESSAGE / WS_PING / WS_PONG /
 *   WS_CLOSE。
 *
 * TunnelWebSocket 是 EventTarget，对外对齐浏览器 WebSocket 的面：
 *   readyState（CONNECTING/OPEN/CLOSING/CLOSED）、send/close、
 *   onopen/onmessage/onerror/onclose、binaryType、url、protocol、
 *   bufferedAmount；另有 opened Promise 便于 async/await 写法。
 *
 * 差异点：
 *   - send 在 CONNECTING 期间排队而不是抛 InvalidStateError；
 *   - url 只有 '/path'（server 本地 WS handler）与 ws(s):// 两种形态；
 *   - 握手拒绝/建流失败触发 error+close 事件，opened reject。
 */

import { netaccErr } from './types.js'
import {
	MAX_TUNNEL_WS_MESSAGE,
	TunnelFrameType,
	TunnelSession,
	WS_OPCODE_BINARY,
	WS_OPCODE_TEXT,
	abortCodeOf,
	checkTarget,
	decodeAbort,
	decodeResult,
	decodeWsClose,
	encodeJson,
	normalizeHeaders,
	checkToken,
	type TunnelOpenMsg,
	type TunnelOpenOptions,
	type TunnelResultMsg,
	type TunnelStreamOpener,
} from './tunnel-wire.js'

/** tunnelWs / tunnelWsTo 的逐连接选项。 */
export interface TunnelWSInit {
	/** 期望协商的子协议（写进 OPEN.protocols；等价 WebSocket 构造参数）。 */
	protocols?: string | string[]
	/** 握手期附加的请求头（Host 头会被提升为 OPEN.host）。 */
	headers?: HeadersInit
	/** 取消信号：CONNECTING 阶段触发则中止握手。 */
	signal?: AbortSignal | null
	/** 透传给 openStream 的逐调用选项。 */
	openOptions?: TunnelOpenOptions
}

/** TunnelWebSocket.send 支持的数据形态。 */
export type TunnelWSData = string | ArrayBuffer | ArrayBufferView | Blob

/** close 事件的载荷形态（CloseEvent 的字段子集，DOM 无全局 CloseEvent 时以 Event 承载）。 */
export interface TunnelCloseEvent extends Event {
	readonly code: number
	readonly reason: string
	readonly wasClean: boolean
}

/** 关闭帧等待对端 WS_CLOSE 应答的超时。 */
const CLOSE_WAIT_MS = 2000
/** close() reason 的 UTF-8 上限（对齐浏览器 WebSocket 约束）。 */
const MAX_CLOSE_REASON_BYTES = 123

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function toErr(e: unknown): Error {
	return e instanceof Error ? e : new Error(String(e))
}

function abortReason(signal?: AbortSignal): Error {
	const r = signal?.reason
	if (r instanceof Error) {
		return r
	}
	const e = new Error(typeof r === 'string' ? r : 'netacc: 操作已中止')
	e.name = 'AbortError'
	return e
}

/** WS_MESSAGE 载荷：1 字节 opcode + 消息体。 */
function wsMessagePayload(opcode: number, data: Uint8Array): Uint8Array {
	const out = new Uint8Array(1 + data.length)
	out[0] = opcode
	out.set(data, 1)
	return out
}

/** 构造带 code/reason/wasClean 字段的 close 事件。 */
function closeEvent(code: number, reason: string, wasClean: boolean): TunnelCloseEvent {
	const ev = new Event('close') as Event & { code: number; reason: string; wasClean: boolean }
	ev.code = code
	ev.reason = reason
	ev.wasClean = wasClean
	return ev
}

interface WsConnectArgs {
	/** OPEN.target（'/path' 或 'ws(s)://...'）。 */
	target: string
	/** 展示用 url 属性原文。 */
	displayUrl: string
	host: string
	protocols: string[]
	headers: [string, string][]
	signal?: AbortSignal
	openOptions?: TunnelOpenOptions
}

/** 解析 tunnelWs 的 url：'/...'（非 '//'）→ 本地 handler；ws/wss 绝对 URL → 代理。 */
export function parseWsTarget(url: string | URL): { target: string; host: string; displayUrl: string } {
	if (typeof url === 'string' && url.startsWith('/') && !url.startsWith('//')) {
		// fragment 不进入隧道请求；与 parseHttpTarget/绝对 URL 行为一致
		const cut = url.indexOf('#')
		return { target: checkTarget(cut >= 0 ? url.slice(0, cut) : url), host: '', displayUrl: url }
	}
	let u: URL
	try {
		u = url instanceof URL ? new URL(url.href) : new URL(url)
	} catch (e) {
		throw netaccErr('protocol', `netacc: tunnelWs 的 url 须为 '/path' 或 ws(s):// URL: ${String(url)}`, e)
	}
	if ((u.protocol !== 'ws:' && u.protocol !== 'wss:') || u.host === '') {
		throw netaccErr('protocol', `netacc: tunnelWs 不支持 ${u.protocol} URL（仅 ws/wss 或 '/path'，且必须有 host）`)
	}
	u.hash = ''
	return { target: checkTarget(u.href), host: u.host, displayUrl: u.href }
}

/** OPEN(ws) 帧 JSON 组装。 */
export function buildWsOpen(args: WsConnectArgs): Uint8Array {
	const open: TunnelOpenMsg = { v: 1, kind: 'ws', target: args.target }
	if (args.host !== '') {
		open.host = args.host
	}
	if (args.headers.length > 0) {
		open.headers = args.headers
	}
	if (args.protocols.length > 0) {
		open.protocols = args.protocols
	}
	return encodeJson(open)
}

interface QueuedMsg {
	opcode: number
	data: Uint8Array | Promise<Uint8Array>
	size: number // 已计入 bufferedAmount 的字节数（含 opcode 字节）
}

/**
 * TunnelWebSocket：WebSocket-like EventTarget。
 * 由 tunnelWs()/NetaccClient.tunnelWs 创建（构造器为内部接口），
 * 创建后立即在后台完成建流与握手。
 */
export class TunnelWebSocket extends EventTarget {
	static readonly CONNECTING = 0
	static readonly OPEN = 1
	static readonly CLOSING = 2
	static readonly CLOSED = 3
	readonly CONNECTING = TunnelWebSocket.CONNECTING
	readonly OPEN = TunnelWebSocket.OPEN
	readonly CLOSING = TunnelWebSocket.CLOSING
	readonly CLOSED = TunnelWebSocket.CLOSED

	readonly url: string
	/** 握手成功（RESULT 101 到达）resolve；拒绝/失败 reject。 */
	readonly opened: Promise<void>
	private resolveOpened!: () => void
	private rejectOpened!: (e: Error) => void

	private _readyState = TunnelWebSocket.CONNECTING
	private _protocol = ''
	private _bufferedAmount = 0
	binaryType: 'blob' | 'arraybuffer' = 'blob'

	private onopenFn: ((this: TunnelWebSocket, ev: Event) => unknown) | null = null
	private onmessageFn: ((this: TunnelWebSocket, ev: MessageEvent) => unknown) | null = null
	private onerrorFn: ((this: TunnelWebSocket, ev: Event) => unknown) | null = null
	private oncloseFn: ((this: TunnelWebSocket, ev: TunnelCloseEvent) => unknown) | null = null

	private session: TunnelSession | null = null
	private readonly queue: QueuedMsg[] = []
	private readonly openAbort = new AbortController()
	private sendChain: Promise<void> = Promise.resolve()
	private closeSent: { code: number; reason: string } | null = null
	private closeTimer: ReturnType<typeof setTimeout> | null = null
	private ended = false // 已派发过 close 事件（唯一终态）

	/** @internal 由 tunnelWs() 构造；不建议直接实例化。 */
	constructor(
		private readonly open: TunnelStreamOpener,
		args: WsConnectArgs,
	) {
		super()
		this.url = args.displayUrl
		this.opened = new Promise<void>((resolve, reject) => {
			this.resolveOpened = resolve
			this.rejectOpened = reject
		})
		this.opened.catch(() => {}) // 未 await opened 时防 unhandled rejection
		args.signal?.addEventListener('abort', () => {
			this.openAbort.abort()
			if (this._readyState === TunnelWebSocket.CONNECTING) {
				this.failConnect(abortReason(args.signal))
			}
		}, { once: true })
		queueMicrotask(() => void this.run(args))
	}

	get readyState(): number {
		return this._readyState
	}

	get protocol(): string {
		return this._protocol
	}

	get bufferedAmount(): number {
		return this._bufferedAmount
	}

	get onopen(): ((this: TunnelWebSocket, ev: Event) => unknown) | null {
		return this.onopenFn
	}
	set onopen(fn: ((this: TunnelWebSocket, ev: Event) => unknown) | null) {
		this.onopenFn = fn
	}
	get onmessage(): ((this: TunnelWebSocket, ev: MessageEvent) => unknown) | null {
		return this.onmessageFn
	}
	set onmessage(fn: ((this: TunnelWebSocket, ev: MessageEvent) => unknown) | null) {
		this.onmessageFn = fn
	}
	get onerror(): ((this: TunnelWebSocket, ev: Event) => unknown) | null {
		return this.onerrorFn
	}
	set onerror(fn: ((this: TunnelWebSocket, ev: Event) => unknown) | null) {
		this.onerrorFn = fn
	}
	get onclose(): ((this: TunnelWebSocket, ev: TunnelCloseEvent) => unknown) | null {
		return this.oncloseFn
	}
	set onclose(fn: ((this: TunnelWebSocket, ev: TunnelCloseEvent) => unknown) | null) {
		this.oncloseFn = fn
	}

	// ---------- 后台握手 ----------

	/** run 建流 → magic → OPEN(ws) → 等 RESULT 101 → 进入读循环。 */
	private async run(args: WsConnectArgs): Promise<void> {
		let st
		try {
			const openOptions = { ...args.openOptions }
			openOptions.signal = openOptions.signal === undefined
				? this.openAbort.signal
				: AbortSignal.any([openOptions.signal, this.openAbort.signal])
			st = await this.open(openOptions)
		} catch (e) {
			this.failConnect(args.signal?.aborted === true ? abortReason(args.signal) : toErr(e))
			return
		}
		if (this._readyState === TunnelWebSocket.CLOSING) {
			// CONNECTING 期间被 close()：放弃握手
			await st.close().catch(() => {})
			this.failConnect(netaccErr('closed', 'netacc: WebSocket 握手前被关闭'))
			return
		}
		const session = new TunnelSession(st)
		this.session = session
		try {
			await session.writeMagic()
			await session.send(TunnelFrameType.Open, buildWsOpen(args))
			for (;;) {
				if (this._readyState !== TunnelWebSocket.CONNECTING) {
					throw netaccErr('closed', 'netacc: WebSocket 握手完成前被关闭')
				}
				const f = await session.next()
				if (f === null) {
					throw netaccErr('protocol', 'netacc: 对端在 RESULT 之前关闭了隧道流')
				}
				if (f.type === TunnelFrameType.Abort) {
					const a = decodeAbort(f.payload)
					throw netaccErr('peer_abort', `netacc: 对端中止 WS 握手: ${a.code} ${a.message}`.trim())
				}
				if (f.type !== TunnelFrameType.Result) {
					throw netaccErr('protocol', `netacc: WS RESULT 之前出现帧类型 ${f.type}`)
				}
				const msg = decodeResult(f.payload)
				this.checkWsResult(msg)
				if (msg.protocol !== undefined && msg.protocol !== '' && !args.protocols.includes(msg.protocol)) {
					throw netaccErr('handshake', `netacc: 对端选择了未请求的 WS 子协议 ${msg.protocol}`)
				}
				this._protocol = msg.protocol ?? ''
				break
			}
		} catch (e) {
			this.failConnect(toErr(e))
			return
		}
		this._readyState = TunnelWebSocket.OPEN
		this.flushQueue() // 先排空 CONNECTING 队列，防止 onopen 内同步 send 插队
		this.emit('open', new Event('open'))
		this.resolveOpened()
		void this.readLoop()
	}

	/** RESULT(ws) 校验：status=101 表示接受；否则带 error 字段拒绝。 */
	private checkWsResult(msg: TunnelResultMsg): void {
		if (msg.kind !== 'ws') {
			throw netaccErr('protocol', `netacc: RESULT.kind=${msg.kind} 与 ws 会话不符`)
		}
		if (msg.status !== 101) {
			const detail = msg.error !== undefined && msg.error !== '' ? `: ${msg.error}` : ''
			throw netaccErr('handshake', `netacc: WS 握手被拒绝 status=${msg.status}${detail}`)
		}
	}

	/** failConnect：握手期失败的统一收口——error+close 事件，opened reject。 */
	private failConnect(err: Error): void {
		if (this.ended) {
			return
		}
		this._readyState = TunnelWebSocket.CLOSED
		if (this.closeSent === null) {
			// 用户在 CONNECTING 主动 close() 不算 error 事件
			this.emit('error', new Event('error'))
		}
		this.rejectOpened(err)
		this.emitClose(this.closeSent?.code ?? 1006, this.closeSent?.reason ?? '', false)
		void this.session?.fail(err, abortCodeOf(err))
	}

	// ---------- 读循环 ----------

	private async readLoop(): Promise<void> {
		const session = this.session
		if (session === null) {
			return
		}
		try {
			for (;;) {
				const f = await session.next()
				if (f === null) {
					this.onTransportEnd()
					return
				}
				switch (f.type) {
					case TunnelFrameType.WsMessage:
						this.onWsMessage(f.payload)
						continue
					case TunnelFrameType.WsPing:
						// 自动回 PONG（不回数据面失败的异常交由写链吞掉后由读侧终态收口）
						this.enqueueWsControl(TunnelFrameType.WsPong, f.payload)
						continue
					case TunnelFrameType.WsPong:
						continue
					case TunnelFrameType.WsClose: {
						const c = decodeWsClose(f.payload)
						this.onPeerClose(c.code, c.reason)
						return
					}
					case TunnelFrameType.Abort: {
						const a = decodeAbort(f.payload)
						throw netaccErr('peer_abort', `netacc: 对端中止 WS 会话: ${a.code} ${a.message}`.trim())
					}
					default:
						throw netaccErr('protocol', `netacc: WS 会话中出现帧类型 ${f.type}`)
				}
			}
		} catch (e) {
			if (this.ended) {
				return
			}
			this.failSession(toErr(e))
		}
	}

	/** WS_MESSAGE → message 事件：text 解 UTF-8，binary 按 binaryType 产出。 */
	private onWsMessage(payload: Uint8Array): void {
		if (payload.length === 0) {
			this.failSession(netaccErr('protocol', 'netacc: WS_MESSAGE 载荷缺少 opcode 字节'))
			return
		}
		const opcode = payload[0]
		const body = payload.subarray(1)
		let data: string | Blob | ArrayBuffer
		if (opcode === WS_OPCODE_TEXT) {
			data = textDecoder.decode(body)
		} else if (opcode === WS_OPCODE_BINARY) {
			data = this.binaryType === 'blob'
				? new Blob([body.slice()])
				: body.slice().buffer as ArrayBuffer
		} else {
			this.failSession(netaccErr('protocol', `netacc: WS_MESSAGE 未知 opcode ${opcode}`))
			return
		}
		const ev = new MessageEvent('message', { data })
		this.dispatchEvent(ev)
		this.onmessageFn?.call(this, ev)
	}

	/** 对端 WS_CLOSE：未回过则回显同 code/reason 的 WS_CLOSE，进入 CLOSED。 */
	private onPeerClose(code: number, reason: string): void {
		if (this.closeSent === null) {
			this.enqueueWsControl(TunnelFrameType.WsClose, encodeJson({ code, reason }))
		}
		this.toClosed(code, reason, true)
	}

	/** 底层流 EOF/终止（未经 WS_CLOSE 握手）：按异常关闭 1006 处理。 */
	private onTransportEnd(): void {
		this.toClosed(1006, '', false)
	}

	/** failSession：OPEN 阶段的会话错误——尽力发 ABORT，error 事件 + close(1006)。 */
	private failSession(err: Error): void {
		if (this.ended) {
			return
		}
		// 先 fail 再 toClosed：terminated 标记会让 gracefulClose 提前返回，
		// 顺序颠倒会导致 ABORT 帧根本不进写链
		void this.session?.fail(err, abortCodeOf(err))
		this.emit('error', new Event('error'))
		this.toClosed(1006, err.message, false)
	}

	/** toClosed：唯一终态收口——状态、close 事件、gracefulClose。 */
	private toClosed(code: number, reason: string, wasClean: boolean): void {
		if (this.ended) {
			return
		}
		this.ended = true
		this._readyState = TunnelWebSocket.CLOSED
		if (this.closeTimer !== null) {
			clearTimeout(this.closeTimer)
			this.closeTimer = null
		}
		this.emitClose(code, reason, wasClean)
		const s = this.session
		if (s !== null) {
			// 先等 sendChain 把回显的 WS_CLOSE 等控制帧排进会话写链，
			// 否则 gracefulClose 先置 terminated，回显帧会被丢弃
			void this.sendChain.then(() => s.gracefulClose())
		}
	}

	// ---------- 写 ----------

	/**
	 * send 发送一条消息。CONNECTING 时排队（OPEN 后按序发出）；
	 * CLOSING/CLOSED 时丢弃（对齐浏览器行为）。Blob 异步读入，
	 * 顺序由写链保证。
	 */
	send(data: TunnelWSData): void {
		if (this._readyState === TunnelWebSocket.CLOSING || this._readyState === TunnelWebSocket.CLOSED) {
			return
		}
		let opcode: number
		let bytes: Uint8Array | Promise<Uint8Array>
		if (typeof data === 'string') {
			opcode = WS_OPCODE_TEXT
			bytes = textEncoder.encode(data)
		} else if (data instanceof Blob) {
			opcode = WS_OPCODE_BINARY
			bytes = data.arrayBuffer().then(b => new Uint8Array(b))
		} else if (data instanceof ArrayBuffer) {
			opcode = WS_OPCODE_BINARY
			bytes = new Uint8Array(data)
		} else if (ArrayBuffer.isView(data)) {
			opcode = WS_OPCODE_BINARY
			bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice()
		} else {
			throw netaccErr('protocol', 'netacc: send 仅支持 string/ArrayBuffer/ArrayBufferView/Blob')
		}
		const size = 1 + (bytes instanceof Uint8Array ? bytes.length : (data as Blob).size)
		if (size > MAX_TUNNEL_WS_MESSAGE) {
			throw netaccErr('protocol', `netacc: WS 消息 ${size} 字节超过上限 ${MAX_TUNNEL_WS_MESSAGE}`)
		}
		const msg: QueuedMsg = { opcode, data: bytes, size }
		this._bufferedAmount += size
		if (this._readyState === TunnelWebSocket.CONNECTING) {
			this.queue.push(msg)
			return
		}
		this.enqueueMsg(msg)
	}

	/** enqueueMsg 经写链串行化发出一条 WS_MESSAGE。 */
	private enqueueMsg(m: QueuedMsg): void {
		const p = this.sendChain.then(async () => {
			const d = await m.data
			if (this.session === null || this._readyState !== TunnelWebSocket.OPEN) {
				return // 发出前会话已终结：丢弃
			}
			await this.session.send(TunnelFrameType.WsMessage, wsMessagePayload(m.opcode, d))
		})
		this.sendChain = p.catch(() => {}) // 链不被单条失败打断
		p.then(
			() => {
				this._bufferedAmount -= m.size
			},
			e => {
				this._bufferedAmount -= m.size
				this.failSession(toErr(e))
			},
		)
	}

	/** enqueueWsControl 写 PONG/WS_CLOSE 等控制帧（不占 bufferedAmount）。 */
	private enqueueWsControl(type: TunnelFrameType, payload?: Uint8Array): void {
		const p = this.sendChain.then(() => this.session?.send(type, payload) ?? Promise.resolve())
		this.sendChain = p.then(
			() => {},
			() => {},
		)
		p.catch(() => {}) // 尽力而为
	}

	/** OPEN 后把 CONNECTING 期间排队的消息按序灌进写链。 */
	private flushQueue(): void {
		for (const m of this.queue.splice(0)) {
			this.enqueueMsg(m)
		}
	}

	// ---------- 关闭 ----------

	/**
	 * close 发起关闭握手：OPEN 态发 WS_CLOSE 后对等对端回帧收尾；
	 * CONNECTING 态记为待关闭（握手流程检查点放弃）；重复调用幂等。
	 */
	close(code?: number, reason?: string): void {
		const c = code ?? 1000
		const r = reason ?? ''
		if (!Number.isInteger(c) || (c !== 1000 && (c < 3000 || c > 4999))) {
			throw new RangeError(`netacc: close code ${c} 非法（允许 1000 或 3000-4999）`)
		}
		if (textEncoder.encode(r).length > MAX_CLOSE_REASON_BYTES) {
			throw new SyntaxError(`netacc: close reason 超过 ${MAX_CLOSE_REASON_BYTES} 字节`)
		}
		if (this._readyState === TunnelWebSocket.CLOSED || this._readyState === TunnelWebSocket.CLOSING) {
			return
		}
		if (this._readyState === TunnelWebSocket.CONNECTING) {
			this._readyState = TunnelWebSocket.CLOSING
			this.closeSent = { code: c, reason: r }
			this.openAbort.abort()
			// 已建会话则直接断流，让等 RESULT 的握手循环收口
			void this.session?.stream.close().catch(() => {})
			return // run() 的握手检查点据此放弃并收口
		}
		this._readyState = TunnelWebSocket.CLOSING
		this.closeSent = { code: c, reason: r }
		this.enqueueWsControl(TunnelFrameType.WsClose, encodeJson({ code: c, reason: r }))
		this.closeTimer = setTimeout(() => {
			this.closeTimer = null
			this.toClosed(c, r, false) // 对端迟迟不回 WS_CLOSE：超时强制收尾
		}, CLOSE_WAIT_MS)
		if (typeof this.closeTimer === 'object' && this.closeTimer !== null && 'unref' in this.closeTimer) {
			;(this.closeTimer as { unref(): void }).unref()
		}
	}

	// ---------- 事件派发 ----------

	private emit(type: string, ev: Event): void {
		this.dispatchEvent(ev)
		switch (type) {
			case 'open':
				this.onopenFn?.call(this, ev)
				break
			case 'error':
				this.onerrorFn?.call(this, ev)
				break
		}
	}

	private emitClose(code: number, reason: string, wasClean: boolean): void {
		this.ended = true // close 是终态事件，任何路径只允许派发一次
		const ev = closeEvent(code, reason, wasClean)
		this.dispatchEvent(ev)
		this.oncloseFn?.call(this, ev)
	}
}

/**
 * tunnelWs 核心：在 open 开出的聚合流上跑一次 WS 握手。
 * 立即返回 TunnelWebSocket（CONNECTING），握手在后台完成。
 */
export function tunnelWs(open: TunnelStreamOpener, url: string | URL, init?: TunnelWSInit): TunnelWebSocket {
	const { target, host, displayUrl } = parseWsTarget(url)
	const protocols = normalizeProtocols(init?.protocols)
	const headers: [string, string][] = []
	let h = host
	for (const [k, v] of normalizeHeaders(init?.headers)) {
		if (k.toLowerCase() === 'host') {
			h = v
			continue
		}
		headers.push([k, v])
	}
	const signal = init?.signal ?? undefined
	// init.signal 与 openOptions.signal 任一触发都中止建流
	const openOptions: TunnelOpenOptions = { ...init?.openOptions }
	if (signal !== undefined || openOptions.signal !== undefined) {
		openOptions.signal = openOptions.signal === undefined
			? signal
			: signal === undefined
				? openOptions.signal
				: AbortSignal.any([openOptions.signal, signal])
	}
	const args: WsConnectArgs = { target, displayUrl, host: h, protocols, headers, signal, openOptions }
	return new TunnelWebSocket(open, args)
}

/** 校验/规整 protocols（token 形态，去重）。 */
function normalizeProtocols(p: string | string[] | undefined): string[] {
	if (p === undefined) {
		return []
	}
	const list = typeof p === 'string' ? [p] : p
	const out: string[] = []
	for (const s of list) {
		checkToken(s, 'WebSocket 子协议')
		if (!out.includes(s)) {
			out.push(s)
		}
	}
	return out
}
