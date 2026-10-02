/**
 * tunnelFetch：fetch 语义的 HTTP-over-tunnel 客户端。
 *
 * 一条请求对应一条聚合流：
 *   openStream → magic → OPEN(http) → 请求体 DATA* END →
 *   读 RESULT 构造 Response → 响应体经 ReadableStream 逐 DATA 帧产出，
 *   END/EOF 结束；任一侧异常由 ABORT 帧表达。
 *
 * 与浏览器 fetch 的差异：
 *   - 不做 forbidden-headers 过滤：Host/Content-Length 等任意头都可写
 *     （本端不是浏览器网络栈，头按原样进 OPEN JSON）；
 *   - input 只有两种形态：'/path'（server 本地 handler）与绝对
 *     http(s):// URL（server 代理），无同源/相对地址概念；
 *   - HTTP 4xx/5xx 照常返回 Response（不 reject）；协议违例、建流失败、
 *     对端 ABORT、signal 中止才 reject。
 */

import { netaccErr } from './types.js'
import {
	MAX_TUNNEL_DATA,
	MAX_TUNNEL_JSON,
	TunnelFrameType,
	TunnelSession,
	abortCodeOf,
	checkHeader,
	checkMethod,
	checkTarget,
	decodeAbort,
	decodeResult,
	encodeJson,
	normalizeHeaders,
	type TunnelOpenMsg,
	type TunnelOpenOptions,
	type TunnelResultMsg,
	type TunnelStreamOpener,
} from './tunnel-wire.js'

/** tunnelFetch 支持的请求体形态。 */
export type TunnelFetchBody =
	| string
	| Uint8Array
	| ArrayBuffer
	| ArrayBufferView
	| Blob
	| URLSearchParams
	| ReadableStream<Uint8Array>

/** tunnelFetch / tunnelFetchTo 的逐请求选项。 */
export interface TunnelFetchInit {
	/** HTTP method（默认 GET；校验为合法 token）。 */
	method?: string
	/**
	 * 请求头（Headers / [name,value][] / 对象）。任意头都允许；
	 * Host 头会被提升为 OPEN.host 字段而不重复进 headers 列表。
	 */
	headers?: HeadersInit
	/** 请求体；ReadableStream 逐块发出，不整包缓冲。GET/HEAD 不允许带体。 */
	body?: TunnelFetchBody | null
	/** 取消信号：中止时尽力发 ABORT 帧并关闭聚合流。 */
	signal?: AbortSignal | null
	/** 透传给 openStream 的逐调用选项。 */
	openOptions?: TunnelOpenOptions
}

/** AbortSignal 的 reason 归一成 Error（AbortController 默认为 AbortError）。 */
function abortReason(signal?: AbortSignal): Error {
	const r = signal?.reason
	if (r instanceof Error) {
		return r
	}
	const e = new Error(typeof r === 'string' ? r : 'netacc: 操作已中止')
	e.name = 'AbortError'
	return e
}

function toErr(e: unknown): Error {
	return e instanceof Error ? e : new Error(String(e))
}

interface TargetParts {
	/** 写进 OPEN.target 的字符串（'/path' 或 'http(s)://...'）。 */
	target: string
	/** 绝对 URL 推导出的 host（相对路径为 ''）。 */
	host: string
}

/** 解析 input：'/...'（非 '//'）→ 本地 handler；绝对 http(s) URL → server 代理。 */
export function parseHttpTarget(input: string | URL): TargetParts {
	if (typeof input === 'string' && input.startsWith('/') && !input.startsWith('//')) {
		// 对齐 Go 端 RequestURI 语义：fragment 不进 HTTP 请求
		const cut = input.indexOf('#')
		return { target: checkTarget(cut >= 0 ? input.slice(0, cut) : input), host: '' }
	}
	let u: URL
	try {
		u = input instanceof URL ? new URL(input.href) : new URL(input)
	} catch (e) {
		throw netaccErr('protocol', `netacc: tunnelFetch 的 input 须为 '/path' 或 http(s):// URL: ${String(input)}`, e)
	}
	if ((u.protocol !== 'http:' && u.protocol !== 'https:') || u.host === '') {
		throw netaccErr('protocol', `netacc: tunnelFetch 不支持 ${u.protocol} URL（仅 http/https 或 '/path'，且必须有 host）`)
	}
	u.hash = '' // fragment 不进 HTTP 请求
	return { target: checkTarget(u.href), host: u.host }
}

/** 把各种 body 形态归一成 AsyncIterable<Uint8Array> 分块流。 */
function bodyChunks(body: TunnelFetchBody): AsyncIterable<Uint8Array> {
	if (typeof body === 'string') {
		const b = new TextEncoder().encode(body)
		return (async function* () {
			yield b
		})()
	}
	if (body instanceof URLSearchParams) {
		const b = new TextEncoder().encode(body.toString())
		return (async function* () {
			yield b
		})()
	}
	if (body instanceof Uint8Array) {
		return (async function* () {
			yield body
		})()
	}
	if (body instanceof ArrayBuffer) {
		const b = new Uint8Array(body)
		return (async function* () {
			yield b
		})()
	}
	if (ArrayBuffer.isView(body)) {
		const b = new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice()
		return (async function* () {
			yield b
		})()
	}
	if (body instanceof Blob) {
		return streamChunks(body.stream())
	}
	if (body instanceof ReadableStream) {
		return streamChunks(body)
	}
	throw netaccErr('protocol', 'netacc: 不支持的 tunnelFetch body 类型')
}

/** streamChunks 把 ReadableStream 读侧包装成异步迭代。 */
async function* streamChunks(rs: ReadableStream<Uint8Array>): AsyncIterable<Uint8Array> {
	const rd = rs.getReader()
	try {
		for (;;) {
			const { done, value } = await rd.read()
			if (done) {
				return
			}
			if (value !== undefined && value.length > 0) {
				yield value
			}
		}
	} finally {
		rd.releaseLock()
	}
}

/** 固定长 body 的字节数（用于自动补 Content-Length）；流式返回 -1。 */
function fixedBodyLen(body: TunnelFetchBody): number {
	if (typeof body === 'string') {
		return new TextEncoder().encode(body).length
	}
	if (body instanceof URLSearchParams) {
		return new TextEncoder().encode(body.toString()).length
	}
	if (body instanceof Uint8Array) {
		return body.length
	}
	if (body instanceof ArrayBuffer) {
		return body.byteLength
	}
	if (ArrayBuffer.isView(body)) {
		return body.byteLength
	}
	if (body instanceof Blob) {
		return body.size
	}
	return -1
}

/** body 的默认 Content-Type（对齐 fetch 惯例）；不能确定的返回 ''。 */
function defaultBodyType(body: TunnelFetchBody): string {
	if (typeof body === 'string') {
		return 'text/plain;charset=UTF-8'
	}
	if (body instanceof URLSearchParams) {
		return 'application/x-www-form-urlencoded;charset=UTF-8'
	}
	if (body instanceof Blob && body.type !== '') {
		return body.type
	}
	return ''
}

interface RequestPlan {
	openJson: Uint8Array
	method: string
	body: AsyncIterable<Uint8Array>
}

/** 校验并组装 OPEN JSON 与请求体计划。 */
function planRequest(input: string | URL, init: TunnelFetchInit | undefined): RequestPlan {
	const { target, host: urlHost } = parseHttpTarget(input)
	const method = checkMethod((init?.method ?? 'GET').toUpperCase())
	const body = init?.body ?? null
	if (body !== null && (method === 'GET' || method === 'HEAD')) {
		throw netaccErr('protocol', `netacc: ${method} 请求不能携带 body`)
	}
	// Host 头提升为 OPEN.host；其余头原样进 headers 列表
	let host = urlHost
	const headers: [string, string][] = []
	for (const [k, v] of normalizeHeaders(init?.headers)) {
		if (k.toLowerCase() === 'host') {
			host = v
			continue
		}
		headers.push([k, v])
	}
	if (body !== null) {
		const names = new Set(headers.map(h => h[0].toLowerCase()))
		// 对齐 fetch：定长 body 自动补 Content-Length，常见形态补 Content-Type
		const len = fixedBodyLen(body)
		if (len >= 0 && !names.has('content-length') && !names.has('transfer-encoding')) {
			headers.push(checkHeader('content-length', String(len)))
		}
		const ct = defaultBodyType(body)
		if (ct !== '' && !names.has('content-type')) {
			headers.push(checkHeader('content-type', ct))
		}
	}
	const open: TunnelOpenMsg = { v: 1, kind: 'http', target, method }
	if (host !== '') {
		open.host = host
	}
	if (headers.length > 0) {
		open.headers = headers
	}
	const openJson = encodeJson(open)
	if (openJson.length > MAX_TUNNEL_JSON) {
		throw netaccErr('protocol', `netacc: OPEN 消息 ${openJson.length} 字节超过上限 ${MAX_TUNNEL_JSON}`)
	}
	return { openJson, method, body: body !== null ? bodyChunks(body) : emptyChunks() }
}

async function* emptyChunks(): AsyncIterable<Uint8Array> {
	// 空迭代：无请求体时直接 END
}

/**
 * HttpExchange 管理一条 tunnelFetch 会话的终态：
 * signal 中止 / 响应体 cancel / 协议违例 都汇聚到 terminate；
 * 正常结束（响应 END 读尽）走 gracefulClose。
 */
class HttpExchange {
	private termErr: Error | null = null
	private term!: (e: Error) => void
	private readonly termP: Promise<never>
	private bodyCtrl: ReadableStreamDefaultController<Uint8Array> | null = null
	private pullWait: (() => void) | null = null
	private done = false

	constructor(
		private readonly session: TunnelSession,
		private readonly signal: AbortSignal | undefined,
		private readonly method: string,
	) {
		this.termP = new Promise<never>((_, reject) => {
			this.term = reject
		})
		this.termP.catch(() => {}) // 防 unhandled rejection
		signal?.addEventListener('abort', () => this.terminate(abortReason(signal), 'aborted'), { once: true })
	}

	/** 请求体泵：DATA 分块 + END，后台执行；失败终止会话。 */
	pumpBody(body: AsyncIterable<Uint8Array>): void {
		const p = (async () => {
			for await (const chunk of body) {
				for (let off = 0; off < chunk.length; off += MAX_TUNNEL_DATA) {
					await this.session.send(TunnelFrameType.Data, chunk.subarray(off, off + MAX_TUNNEL_DATA))
				}
			}
			await this.session.send(TunnelFrameType.End)
		})()
		p.catch(e => {
			if (!this.done) {
				this.terminate(netaccErr('internal', `netacc: 发送请求体失败: ${toErr(e).message}`, e), 'internal_error')
			}
		})
	}

	/** 等 RESULT 帧；ABORT/EOF/其它帧类型/中止都在这里收口成 reject。 */
	async waitResult(): Promise<Response> {
		for (;;) {
			const f = await Promise.race([this.session.next(), this.termP])
			if (f === null) {
				throw netaccErr('protocol', 'netacc: 对端在 RESULT 之前关闭了隧道流')
			}
			switch (f.type) {
				case TunnelFrameType.Result: {
					const msg = decodeResult(f.payload)
					return this.makeResponse(msg)
				}
				case TunnelFrameType.Abort: {
					const a = decodeAbort(f.payload)
					throw netaccErr('peer_abort', `netacc: 对端中止请求: ${a.code} ${a.message}`.trim())
				}
				default:
					throw netaccErr('protocol', `netacc: RESULT 之前出现帧类型 ${f.type}`)
			}
		}
	}

	/** RESULT → Response；响应体经 ReadableStream 逐 DATA 帧产出。 */
	private makeResponse(msg: TunnelResultMsg): Response {
		if (msg.kind !== 'http') {
			throw netaccErr('protocol', `netacc: RESULT.kind=${msg.kind} 与 http 会话不符`)
		}
		if (!Number.isInteger(msg.status) || msg.status < 200 || msg.status > 599) {
			throw netaccErr('protocol', `netacc: RESULT.status 非法: ${msg.status}`)
		}
		let headers: Headers
		try {
			headers = new Headers(msg.headers)
		} catch (e) {
			throw netaccErr('protocol', 'netacc: RESULT.headers 含非法头', e)
		}
		// 204/304/101 与 HEAD 响应按规范无响应体：弃用 body 流，后台排空残余帧
		const nullBody = this.method === 'HEAD' || msg.status === 101 || msg.status === 204 || msg.status === 304
		const stream = nullBody ? null : this.makeBodyStream()
		if (nullBody) {
			this.bodyCtrl = null
			void this.drainLoop()
		}
		try {
			return new Response(stream, { status: msg.status, statusText: msg.statusText ?? '', headers })
		} catch {
			// statusText 含非法字符时去掉重试（容错；status/headers 已校验）
			return new Response(stream, { status: msg.status, statusText: '', headers })
		}
	}

	private makeBodyStream(): ReadableStream<Uint8Array> {
		return new ReadableStream<Uint8Array>({
			start: c => {
				this.bodyCtrl = c
				void this.bodyLoop()
			},
			pull: () => {
				// desiredSize 耗尽时 bodyLoop 挂在这里等下一次拉取
				const w = this.pullWait
				this.pullWait = null
				w?.()
			},
			cancel: () => {
				this.terminate(netaccErr('closed', 'netacc: 响应体被取消'), 'client_close')
			},
		})
	}

	/** 等下一次 pull（背压挂起点）。 */
	private waitPull(): Promise<void> {
		return new Promise<void>(resolve => {
			this.pullWait = resolve
		})
	}

	/** 响应体读循环：DATA→enqueue，END→收尾关闭，ABORT→错误，EOF→正常结束。 */
	private async bodyLoop(): Promise<void> {
		const ctrl = this.bodyCtrl
		if (ctrl === null) {
			return
		}
		try {
			for (;;) {
				if (this.done) {
					return
				}
				if (ctrl.desiredSize !== null && ctrl.desiredSize <= 0) {
					await this.waitPull()
					continue
				}
				const f = await this.session.next()
				if (f === null) {
					ctrl.close()
					this.finishOk()
					return
				}
				switch (f.type) {
					case TunnelFrameType.Data:
						if (f.payload.length > 0) {
							ctrl.enqueue(f.payload)
						}
						continue
					case TunnelFrameType.End:
						ctrl.close()
						this.finishOk()
						return
					case TunnelFrameType.Abort: {
						const a = decodeAbort(f.payload)
						throw netaccErr('peer_abort', `netacc: 对端中止响应: ${a.code} ${a.message}`.trim())
					}
					default:
						throw netaccErr('protocol', `netacc: HTTP 响应体内出现帧类型 ${f.type}`)
				}
			}
		} catch (e) {
			// 对端 ABORT 衍生的终态不再回发 ABORT
			const sendAbort = !(e instanceof Error && 'code' in e && (e as { code: string }).code === 'peer_abort')
			if (this.done) {
				return
			}
			const err = this.termErr ?? toErr(e)
			try {
				ctrl.error(err)
			} catch {
				// controller 已终态（cancel/close）时 error 无副作用
			}
			this.terminate(err, abortCodeOf(err), sendAbort)
		}
	}

	/** drainLoop 排空无响应体请求（204/304）的残余帧后收尾。 */
	private async drainLoop(): Promise<void> {
		try {
			for (;;) {
				if (this.done) {
					return
				}
				const f = await this.session.next()
				if (f === null || f.type === TunnelFrameType.End) {
					this.finishOk()
					return
				}
				if (f.type === TunnelFrameType.Abort) {
					const a = decodeAbort(f.payload)
					throw netaccErr('peer_abort', `netacc: 对端中止响应: ${a.code} ${a.message}`.trim())
				}
				throw netaccErr('protocol', `netacc: 无响应体状态收到非法帧 ${f.type}`)
			}
		} catch (e) {
			const sendAbort = !(e instanceof Error && 'code' in e && (e as { code: string }).code === 'peer_abort')
			const err = this.termErr ?? toErr(e)
			this.terminate(err, abortCodeOf(err), sendAbort)
		}
	}

	/** finishOk：响应正常读完，等已写帧被确认后关闭底层流。 */
	private finishOk(): void {
		if (this.done) {
			return
		}
		this.done = true
		this.term(netaccErr('closed', 'netacc: 会话已结束')) // 解锁还在等的竞态读
		this.pullWait?.() // 唤醒背压挂起点的读循环，让它看到 done 退出
		void this.session.gracefulClose()
	}

	/**
	 * terminate：异常终态——body 流报错、尽力发 ABORT 帧、关闭底层流。
	 * sendAbort=false 用于「错误来自对端 ABORT」场景：对端已知悉，
	 * 不再回发，直接收尾关闭。
	 */
	private terminate(err: Error, code: string, sendAbort = true): void {
		if (this.done) {
			return
		}
		this.done = true
		this.termErr = err
		if (this.bodyCtrl !== null) {
			try {
				this.bodyCtrl.error(err)
			} catch {
				// 已终态
			}
		}
		this.term(err)
		this.pullWait?.()
		if (sendAbort) {
			void this.session.fail(err, code)
		} else {
			void this.session.gracefulClose(500)
		}
	}

	/** fail 供外层 catch 收口：终止会话并按原错误抛出。 */
	async fail(e: Error): Promise<void> {
		this.terminate(e, abortCodeOf(e))
		await this.session.fail(e)
	}
}

/**
 * tunnelFetch 核心：在 open 开出的聚合流上执行一次 HTTP 请求。
 * open 通常是 `(opts) => client.openStream(target, opts)` 的绑定形态。
 */
export async function tunnelFetch(
	open: TunnelStreamOpener,
	input: string | URL,
	init?: TunnelFetchInit,
): Promise<Response> {
	const signal = init?.signal ?? undefined
	// aborted 是 getter、随时可翻转：经函数取值避免 TS 静态窄化
	const aborted = () => signal?.aborted === true
	if (aborted()) {
		throw abortReason(signal)
	}
	const plan = planRequest(input, init)
	// init.signal 与 openOptions.signal 任一触发都中止建流
	const openOpts: TunnelOpenOptions = { ...init?.openOptions }
	if (signal !== undefined || openOpts.signal !== undefined) {
		openOpts.signal = openOpts.signal === undefined
			? signal
			: signal === undefined
				? openOpts.signal
				: AbortSignal.any([openOpts.signal, signal])
	}
	let st
	try {
		st = await open(openOpts)
	} catch (e) {
		throw aborted() ? abortReason(signal) : toErr(e)
	}
	const session = new TunnelSession(st)
	const ex = new HttpExchange(session, signal, plan.method)
	if (aborted()) {
		await ex.fail(abortReason(signal))
		throw abortReason(signal)
	}
	try {
		await session.writeMagic()
		await session.send(TunnelFrameType.Open, plan.openJson)
		ex.pumpBody(plan.body)
		return await ex.waitResult()
	} catch (e) {
		const err = aborted() ? abortReason(signal) : toErr(e)
		await ex.fail(err)
		throw err
	}
}
