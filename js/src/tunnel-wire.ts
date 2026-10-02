/**
 * tunnel 线协议（与 Go 端 tunnel 实现严格一致）：HTTP/WebSocket 请求
 * 经一条聚合流承载到对端 tunnel server，由对端执行本地 handler 或
 * 代理到绝对 URL 隧道目标。
 *
 * 线格式：
 *   - 流起始由发起方写 5 字节 magic：ASCII "NTUN" + 0x01（版本）；
 *   - 之后逐帧：uvarint(type) + uvarint(payload_len) + payload。
 *
 * 帧类型：
 *   1 OPEN       C->S JSON：{"v":1,"kind":"http"|"ws","target":...}
 *   2 RESULT     S->C JSON：{"kind":"http","status":200,...} /
 *                {"kind":"ws","status":101,"protocol":...}
 *   3 DATA       双向二进制（HTTP body）
 *   4 END        双向空载荷（该方向 body 结束；聚合流无半关闭语义）
 *   5 ABORT      双向 JSON {"code","message"}：会话异常终止
 *   6 WS_MESSAGE 双向：1 字节 opcode（1=text/2=binary）+ 消息体
 *   7 WS_CLOSE   双向 JSON {"code","reason"}
 *   8 WS_PING    双向二进制
 *   9 WS_PONG    双向二进制
 *
 * 交互：
 *   - HTTP：OPEN → client DATA* END → server RESULT DATA* END；
 *   - WS：OPEN → server RESULT(101) → 双向 WS_MESSAGE/WS_PING/WS_PONG，
 *     WS_CLOSE 握手收尾。
 */

import { StreamReader } from './bytestream.js'
import { NetaccError, netaccErr } from './types.js'
import { appendUvarint, toSafeNumber } from './varint.js'

/** 协议 magic：每条聚合流起始的 5 字节 "NTUN"+0x01。 */
export const TUNNEL_MAGIC = new Uint8Array([0x4e, 0x54, 0x55, 0x4e, 0x01])

/** 隧道帧类型。 */
export enum TunnelFrameType {
	Open = 1,
	Result = 2,
	Data = 3,
	End = 4,
	Abort = 5,
	WsMessage = 6,
	WsClose = 7,
	WsPing = 8,
	WsPong = 9,
}

/** 解码端防御上限（与 Go 端一致）。 */
export const MAX_TUNNEL_JSON = 64 << 10 // JSON 控制帧（OPEN/RESULT/ABORT/WS_CLOSE）64KiB
export const MAX_TUNNEL_DATA = 64 << 10 // DATA 帧载荷上限
export const MAX_TUNNEL_WS_MESSAGE = 4 << 20 // WS_MESSAGE 载荷上限 4MiB
export const MAX_TUNNEL_URL = 8 << 10 // OPEN.target（路径/URL）上限

/** WS_MESSAGE 载荷首字节的 opcode。 */
export const WS_OPCODE_TEXT = 1
export const WS_OPCODE_BINARY = 2

/**
 * TunnelStream 是隧道会话对底层流的最小抽象：可靠保序字节流。
 * AggregatedStream 与测试用的 MemoryByteStream 都满足；
 * stats() 仅 AggregatedStream 有，用于收尾时等待 ACK（见 gracefulClose）。
 */
export interface TunnelStream {
	read(maxBytes?: number): Promise<Uint8Array | null>
	write(data: Uint8Array): Promise<void>
	close(): Promise<void>
	/** RST 语义终止（可选；缺省时退化为 close）。 */
	abort?(err?: Error): void
	/**
	 * 可选的发送进度快照：gracefulClose 用它等最后帧被对端确认。
	 * 对应 Go 侧 FlushProbe：sent=已下发字节、acked=已确认字节、
	 * pending=仍在发送缓冲未装帧的字节。
	 */
	stats?(): { sentBytes: number; ackedBytes: number; pendingBytes: number }
}

/**
 * TunnelOpenOptions 与 client.ts 的 OpenStreamOptions 同构（逐调用覆盖
 * 聚合流参数）。单独定义是为了让 tunnel-* 模块不 import client.ts，
 * 避免循环依赖；结构一致即可互相赋值。
 */
export interface TunnelOpenOptions {
	signal?: AbortSignal
	handshakeTimeoutMs?: number
	reorderMin?: number
	reorderMax?: number
	sendBufCap?: number
	minPaths?: number
	telemetryIntervalMs?: number
}

/** TunnelStreamOpener：开一条隧道会话流（client.openStream 的函数形态）。 */
export type TunnelStreamOpener = (options?: TunnelOpenOptions) => Promise<TunnelStream>

/** 一帧解码结果。 */
export interface TunnelFrame {
	type: TunnelFrameType
	payload: Uint8Array
}

// ---------- 帧编解码 ----------

/** 编码一帧：uvarint(type) + uvarint(len) + payload。 */
export function encodeTunnelFrame(type: TunnelFrameType, payload?: Uint8Array): Uint8Array {
	const p = payload ?? new Uint8Array(0)
	let out = appendUvarint(new Uint8Array(0), type)
	out = appendUvarint(out, p.length)
	const buf = new Uint8Array(out.length + p.length)
	buf.set(out, 0)
	buf.set(p, out.length)
	return buf
}

/** 各帧类型允许的最大载荷；返回 -1 表示未知帧类型。 */
export function maxTunnelPayload(type: number): number {
	switch (type) {
		case TunnelFrameType.Open:
		case TunnelFrameType.Result:
		case TunnelFrameType.Abort:
		case TunnelFrameType.WsClose:
			return MAX_TUNNEL_JSON
		case TunnelFrameType.Data:
			return MAX_TUNNEL_DATA
		case TunnelFrameType.WsMessage:
		case TunnelFrameType.WsPing:
		case TunnelFrameType.WsPong:
			return MAX_TUNNEL_WS_MESSAGE // 与 Go 侧一致：PING/PONG 共享 4MiB 上限
		case TunnelFrameType.End:
			return 0 // END 必须空载荷
		default:
			return -1
	}
}

/**
 * TunnelFrameReader 从会话流逐帧解码；EOF 恰在帧边界返回 null，
 * 中途断流/超限/未知类型抛 NetaccError。单一读者独占。
 */
export class TunnelFrameReader {
	constructor(private readonly r: StreamReader) {}

	async next(): Promise<TunnelFrame | null> {
		const t = await this.r.readUvarint()
		if (t === null) {
			return null
		}
		const type = Number(t)
		const max = maxTunnelPayload(type)
		if (max < 0) {
			throw netaccErr('protocol', `netacc: 未知隧道帧类型 ${type}`)
		}
		const lv = await this.r.readUvarint()
		if (lv === null) {
			throw netaccErr('protocol', 'netacc: 读隧道帧长度失败: 非预期 EOF')
		}
		const n = toSafeNumber(lv, '隧道帧长度')
		if (n > max) {
			throw netaccErr('protocol', `netacc: 隧道帧 ${type} 载荷 ${n} 超过上限 ${max}`)
		}
		const payload = n > 0 ? await this.r.readExact(n) : new Uint8Array(0)
		return { type: type as TunnelFrameType, payload }
	}
}

// ---------- JSON 消息 ----------

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

/** OPEN 帧 JSON（C->S）。target 为 '/path' 或绝对 http(s)/ws(s) URL。 */
export interface TunnelOpenMsg {
	v: number
	kind: 'http' | 'ws'
	target: string
	method?: string
	host?: string
	headers?: [string, string][]
	protocols?: string[]
}

/** RESULT 帧 JSON（S->C）。 */
export interface TunnelResultMsg {
	kind: 'http' | 'ws'
	status: number
	statusText?: string
	headers?: [string, string][]
	/** WS 协商出的子协议（kind=ws）。 */
	protocol?: string
	/** 拒绝原因（status 表示拒绝时）。 */
	error?: string
}

/** ABORT 帧 JSON：code 编码为字符串（解码宽容接受 number）。 */
export interface TunnelAbortMsg {
	code: string
	message: string
}

/** WS_CLOSE 帧 JSON。 */
export interface TunnelWsCloseMsg {
	code: number
	reason: string
}

export function encodeJson(v: unknown): Uint8Array {
	return textEncoder.encode(JSON.stringify(v))
}

export function decodeJson(payload: Uint8Array, what: string): unknown {
	let v: unknown
	try {
		v = JSON.parse(textDecoder.decode(payload))
	} catch (e) {
		throw netaccErr('protocol', `netacc: ${what} JSON 解析失败`, e)
	}
	if (v === null || typeof v !== 'object') {
		throw netaccErr('protocol', `netacc: ${what} 不是 JSON 对象`)
	}
	return v
}

/** str 取 JSON 字段的字符串值；缺失/类型不符返回 def（缺省 ''）。 */
export function jsonStr(o: unknown, key: string, def = ''): string {
	const v = (o as Record<string, unknown>)[key]
	if (v === undefined || v === null) {
		return def
	}
	if (typeof v !== 'string') {
		throw netaccErr('protocol', `netacc: JSON 字段 ${key} 应为字符串`)
	}
	return v
}

/** jsonNum 取 JSON 字段的数值；非法抛协议错误。 */
export function jsonNum(o: unknown, key: string, def = 0): number {
	const v = (o as Record<string, unknown>)[key]
	if (v === undefined || v === null) {
		return def
	}
	if (typeof v !== 'number' || !Number.isFinite(v)) {
		throw netaccErr('protocol', `netacc: JSON 字段 ${key} 应为数字`)
	}
	return v
}

/** headers 字段解码为 [name, value] 二元组列表。 */
export function jsonHeaders(o: unknown): [string, string][] {
	const v = (o as Record<string, unknown>).headers
	if (v === undefined || v === null) {
		return []
	}
	if (!Array.isArray(v)) {
		throw netaccErr('protocol', 'netacc: JSON 字段 headers 应为数组')
	}
	const out: [string, string][] = []
	for (const h of v) {
		if (!Array.isArray(h) || h.length !== 2 || typeof h[0] !== 'string' || typeof h[1] !== 'string') {
			throw netaccErr('protocol', 'netacc: headers 元素应为 [name, value] 二元组')
		}
		out.push([h[0], h[1]])
	}
	return out
}

/** 编码 RESULT 帧解码；校验 kind 与必需字段。 */
export function decodeResult(payload: Uint8Array): TunnelResultMsg {
	const o = decodeJson(payload, 'RESULT')
	const kind = jsonStr(o, 'kind')
	if (kind !== 'http' && kind !== 'ws') {
		throw netaccErr('protocol', `netacc: RESULT.kind 非法: ${kind}`)
	}
	return {
		kind,
		status: jsonNum(o, 'status'),
		statusText: jsonStr(o, 'statusText'),
		headers: jsonHeaders(o),
		protocol: jsonStr(o, 'protocol'),
		error: jsonStr(o, 'error'),
	}
}

/** 解码 ABORT 帧；code 宽容接受 number 并转字符串。 */
export function decodeAbort(payload: Uint8Array): TunnelAbortMsg {
	const o = decodeJson(payload, 'ABORT') as Record<string, unknown>
	const c = o.code
	const code = typeof c === 'string' ? c : typeof c === 'number' ? String(c) : ''
	return { code, message: jsonStr(o, 'message') }
}

/** receivedCloseCode：线上可接收的 WS close code（对齐 RFC6455/浏览器）。 */
function receivedCloseCode(code: number): boolean {
	return (code >= 1000 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006) ||
		(code >= 3000 && code <= 4999)
}

/** 解码并校验 WS_CLOSE 帧（reason ≤123 UTF-8 字节）。 */
export function decodeWsClose(payload: Uint8Array): TunnelWsCloseMsg {
	const o = decodeJson(payload, 'WS_CLOSE')
	const code = jsonNum(o, 'code')
	const reason = jsonStr(o, 'reason')
	if (!Number.isInteger(code) || !receivedCloseCode(code)) {
		throw netaccErr('protocol', `netacc: WS_CLOSE code ${code} 非法`)
	}
	if (textEncoder.encode(reason).length > 123) {
		throw netaccErr('protocol', 'netacc: WS_CLOSE reason 超过 123 字节')
	}
	return { code, reason }
}

// ---------- 输入校验 ----------

/** HTTP token 字符（RFC 9110 tchar），用于 method/header name。 */
const TOKEN_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/
/** header value 禁含 NUL/CR/LF；TAB 与其余可见字符（含 obs-text）放行。 */
const BAD_VALUE_RE = /[\0\r\n]/
/** target（path/URL）与控制帧内字符串禁含控制字符。 */
const BAD_CTRL_RE = /[\0-\x1f\x7f]/

/** 校验 RFC 9110 token 形态（method/header 名/子协议名共用字符集）。 */
export function checkToken(s: string, what = 'token'): string {
	if (!TOKEN_RE.test(s)) {
		throw netaccErr('protocol', `netacc: ${what}非法: ${JSON.stringify(s)}`)
	}
	return s
}

/** 校验 HTTP method（非空 token）。 */
export function checkMethod(method: string): string {
	return checkToken(method, 'HTTP method')
}

/** 校验单个 header 名/值，返回规整后的二元组（value 去首尾空白）。 */
export function checkHeader(name: string, value: string): [string, string] {
	checkToken(name, 'header 名')
	if (BAD_VALUE_RE.test(value)) {
		throw netaccErr('protocol', `netacc: header ${name} 值含控制字符`)
	}
	return [name, value.trim()]
}

/** 校验 OPEN.target 字符串：非空、无控制字符、UTF-8 后 ≤8KiB。 */
export function checkTarget(target: string): string {
	if (target === '') {
		throw netaccErr('protocol', 'netacc: tunnel target 不能为空')
	}
	if (BAD_CTRL_RE.test(target)) {
		throw netaccErr('protocol', 'netacc: tunnel target 含控制字符')
	}
	if (textEncoder.encode(target).length > MAX_TUNNEL_URL) {
		throw netaccErr('protocol', `netacc: tunnel target 超过 ${MAX_TUNNEL_URL} 字节上限`)
	}
	return target
}

/**
 * 规整 HeadersInit 为 [name, value] 列表：保留调用方给定的顺序、大小写
 * 与重复名（隧道不做 fetch 的 forbidden-headers 过滤），仅做 token/控制
 * 字符校验。值为 undefined/null 的键跳过。
 */
export function normalizeHeaders(init?: HeadersInit | null): [string, string][] {
	if (init == null) {
		return []
	}
	const out: [string, string][] = []
	if (init instanceof Headers) {
		init.forEach((v, k) => out.push(checkHeader(k, v)))
		return out
	}
	if (Array.isArray(init)) {
		for (const h of init) {
			if (!Array.isArray(h) || h.length !== 2) {
				throw netaccErr('protocol', 'netacc: headers 数组元素应为 [name, value]')
			}
			out.push(checkHeader(String(h[0]), String(h[1])))
		}
		return out
	}
	for (const [k, v] of Object.entries(init)) {
		if (v == null) {
			continue
		}
		out.push(checkHeader(k, String(v)))
	}
	return out
}

// ---------- 会话读写 ----------

const GRACEFUL_CLOSE_TIMEOUT_MS = 3000
const GRACEFUL_POLL_MS = 20

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * abortCodeOf 把本地错误映射成 ABORT.code 字符串——与 Go 侧词汇表一致
 * （protocol_error/internal_error/client_close/...）；NetaccError 的
 * 'protocol' 码译成 Go 的 'protocol_error'，其余原样使用。
 */
export function abortCodeOf(err: unknown): string {
	if (err instanceof NetaccError) {
		return err.code === 'protocol' ? 'protocol_error' : err.code
	}
	return 'internal_error'
}

/**
 * gracefulClose：写完最后一个业务帧后关闭底层流。
 *
 * AggregatedStream.write 只保证数据进发送缓冲——立刻 close 会让末尾
 * 字节来不及装帧发出被截断。因此对带 stats() 的流先轮询
 * pendingBytes===0 && ackedBytes>=sentBytes（最多 timeoutMs，
 * 与 Go 侧 FlushProbe 条件一致），确认对端收齐后再 close；
 * 无 stats 的普通流（测试管道等）直接 close。
 */
export async function gracefulClose(st: TunnelStream, timeoutMs = GRACEFUL_CLOSE_TIMEOUT_MS): Promise<void> {
	const stats = st.stats?.bind(st)
	if (stats !== undefined && timeoutMs > 0) {
		const deadline = Date.now() + timeoutMs
		for (;;) {
			const s = stats()
			if (s.pendingBytes === 0 && s.ackedBytes >= s.sentBytes) {
				break // 全部已写字节都被对端确认
			}
			if (Date.now() >= deadline) {
				break // 超时兜底：不等了，直接关
			}
			await delay(GRACEFUL_POLL_MS)
		}
	}
	try {
		await st.close()
	} catch {
		// 已终止的流 close 失败忽略
	}
}

/**
 * TunnelSession 把一条聚合流包装成隧道会话：写方向经 promise 链
 * 串行化（任意帧都是一个 write 调用，帧间不交错）；读方向经
 * StreamReader + TunnelFrameReader 逐帧解码（单读者）。
 */
export class TunnelSession {
	private readonly r: TunnelFrameReader
	private writeChain: Promise<void> = Promise.resolve()
	private terminated = false

	constructor(readonly stream: TunnelStream) {
		this.r = new TunnelFrameReader(new StreamReader(stream))
	}

	/** 写协议头 magic（会话建立后第一个写操作）。 */
	writeMagic(): Promise<void> {
		return this.enqueue(TUNNEL_MAGIC)
	}

	/**
	 * send 排队写一帧；调用方拿到的 promise 表示该帧已进发送缓冲。
	 * 载荷超类型上限时同步抛错（与 Go WriteFrame 一致：不写出任何字节）。
	 */
	send(type: TunnelFrameType, payload?: Uint8Array): Promise<void> {
		const p = payload ?? new Uint8Array(0)
		const max = maxTunnelPayload(type)
		if (max < 0 || p.length > max) {
			return Promise.reject(netaccErr('protocol',
				`netacc: 隧道帧 ${type} 载荷 ${p.length} 超过上限 ${max}`))
		}
		return this.enqueue(encodeTunnelFrame(type, p))
	}

	private enqueue(bytes: Uint8Array): Promise<void> {
		if (this.terminated) {
			return Promise.reject(netaccErr('closed', 'netacc: 隧道会话已关闭'))
		}
		const w = this.writeChain.then(() => this.stream.write(bytes))
		// 链上吞错：一帧写失败不阻断后续 enqueue 的排队语义（它们各自也会失败）
		this.writeChain = w.then(
			() => {},
			() => {},
		)
		return w
	}

	/** next 读下一帧；干净 EOF 返回 null。单读者。 */
	next(): Promise<TunnelFrame | null> {
		return this.r.next()
	}

	/** 正常收尾：等已写帧被对端确认后 close。 */
	async gracefulClose(timeoutMs = GRACEFUL_CLOSE_TIMEOUT_MS): Promise<void> {
		if (this.terminated) {
			return
		}
		this.terminated = true
		try {
			await this.writeChain // 先等排队帧进入发送缓冲
		} catch {
			// 写链失败的帧随流关闭一起丢弃
		}
		await gracefulClose(this.stream, timeoutMs)
	}

	/**
	 * fail 异常终止会话：尽力写一帧 ABORT 让对端感知原因，然后关闭底层流。
	 * 写 ABORT 也走 graceful 收尾（短超时），尽力送达但不阻塞。
	 * code 取 Go 侧同词汇表（protocol_error/internal_error/client_close/...）。
	 */
	async fail(err: Error | NetaccError, code?: string): Promise<void> {
		if (this.terminated) {
			return
		}
		this.terminated = true
		const abort = encodeTunnelFrame(TunnelFrameType.Abort,
			encodeJson({ code: code ?? abortCodeOf(err), message: err.message }))
		await this.writeChain
			.then(() => this.stream.write(abort))
			.catch(() => {})
		if (this.stream.abort !== undefined) {
			this.stream.abort(err)
			return
		}
		// 无 abort 语义时走完整 gracefulClose（与 Go 侧 closeWait 一致），
		// 尽力把 ABORT 帧送达；后台执行不阻塞调用方
		await gracefulClose(this.stream)
	}
}
