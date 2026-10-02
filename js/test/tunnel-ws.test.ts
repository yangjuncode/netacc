/**
 * tunnelWs 测试：MemoryByteStream 配对 + 手写假 WS server，
 * 验证 OPEN(ws)/RESULT(101) 握手、CONNECTING 排队、text/binary 消息、
 * ping→pong、双向 WS_CLOSE 握手、握手拒绝、建流失败。
 */
import { describe, expect, it } from 'vitest'
import { StreamReader } from '../src/bytestream.js'
import { MemoryByteStream } from '../src/memory-stream.js'
import { parseWsTarget, tunnelWs, TunnelWebSocket, type TunnelCloseEvent } from '../src/tunnel-ws.js'
import {
	TUNNEL_MAGIC,
	TunnelFrameReader,
	TunnelFrameType,
	WS_OPCODE_BINARY,
	WS_OPCODE_TEXT,
	decodeJson,
	decodeWsClose,
	encodeJson,
	encodeTunnelFrame,
	type TunnelFrame,
	type TunnelOpenMsg,
	type TunnelOpenOptions,
	type TunnelStream,
} from '../src/tunnel-wire.js'

const enc = new TextEncoder()
const dec = new TextDecoder()

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

/** FakeWsServer：server 侧隧道会话。 */
class FakeWsServer {
	readonly reader: TunnelFrameReader
	private readonly raw: StreamReader
	constructor(readonly st: TunnelStream) {
		this.raw = new StreamReader(st)
		this.reader = new TunnelFrameReader(this.raw)
	}

	async expectMagicAndOpen(): Promise<TunnelOpenMsg> {
		expect(await this.raw.readExact(5)).toEqual(TUNNEL_MAGIC)
		const f = await this.reader.next()
		expect(f).not.toBeNull()
		expect(f!.type).toBe(TunnelFrameType.Open)
		return decodeJson(f!.payload, 'OPEN') as TunnelOpenMsg
	}

	async next(): Promise<TunnelFrame | null> {
		return this.reader.next()
	}

	async send(type: TunnelFrameType, payload?: Uint8Array): Promise<void> {
		await this.st.write(encodeTunnelFrame(type, payload))
	}

	async accept101(protocol = ''): Promise<void> {
		await this.send(TunnelFrameType.Result, encodeJson({ kind: 'ws', status: 101, protocol }))
	}

	async reject(status: number, error: string): Promise<void> {
		await this.send(TunnelFrameType.Result, encodeJson({ kind: 'ws', status, error }))
	}

	/** wsMessage 读一条 WS_MESSAGE，返回 {opcode, body}。 */
	async readMessage(): Promise<{ opcode: number; body: Uint8Array }> {
		const f = await this.next()
		expect(f).not.toBeNull()
		expect(f!.type).toBe(TunnelFrameType.WsMessage)
		expect(f!.payload.length).toBeGreaterThan(0)
		return { opcode: f!.payload[0]!, body: f!.payload.subarray(1) }
	}
}

function pair(): { open: () => Promise<TunnelStream>; server: FakeWsServer } {
	const [client, server] = MemoryByteStream.pair()
	return { open: () => Promise.resolve(client), server: new FakeWsServer(server) }
}

/** 等 ws 进入指定 readyState（轮询，1s 超时）。 */
async function waitState(ws: TunnelWebSocket, state: number): Promise<void> {
	for (let i = 0; i < 100 && ws.readyState !== state; i++) {
		await sleep(10)
	}
	expect(ws.readyState).toBe(state)
}

describe('tunnelWs 握手', () => {
	it('OPEN(ws) → RESULT 101：opened 解决、onopen、protocol 生效', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			const msg = await server.expectMagicAndOpen()
			expect(msg).toEqual({
				v: 1,
				kind: 'ws',
				target: '/ws',
				host: 'app.internal',
				headers: [['X-Auth', 't1']],
				protocols: ['chat', 'v2'],
			})
			await server.accept101('chat')
		})()
		const ws = tunnelWs(open, '/ws', {
			protocols: ['chat', 'v2'],
			headers: { Host: 'app.internal', 'X-Auth': 't1' },
		})
		expect(ws.readyState).toBe(TunnelWebSocket.CONNECTING)
		const openedEv = new Promise<Event>(r => {
			ws.onopen = r
		})
		await ws.opened
		expect(ws.readyState).toBe(TunnelWebSocket.OPEN)
		expect(ws.protocol).toBe('chat')
		expect(ws.url).toBe('/ws')
		await openedEv
		await serverJob
		ws.close()
	})

	it('ws:// 绝对 URL：target/host 进 OPEN', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			const msg = await server.expectMagicAndOpen()
			expect(msg.target).toBe('wss://ws.example.com/chat')
			expect(msg.host).toBe('ws.example.com')
			await server.accept101()
			// 收到 WS_CLOSE（client close）后回显收尾
			const f = await server.next()
			expect(f!.type).toBe(TunnelFrameType.WsClose)
			const c = decodeWsClose(f!.payload)
			await server.send(TunnelFrameType.WsClose, encodeJson(c))
		})()
		const ws = tunnelWs(open, 'wss://ws.example.com/chat')
		await ws.opened
		ws.close(1000)
		await waitState(ws, TunnelWebSocket.CLOSED)
		await serverJob
	})

	it('握手拒绝（status!=101 + error）：error+close 事件，opened reject', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagicAndOpen()
			await server.reject(403, '需要鉴权')
		})()
		const ws = tunnelWs(open, '/ws')
		const got = { error: 0, close: null as TunnelCloseEvent | null }
		ws.onerror = () => {
			got.error++
		}
		ws.onclose = ev => {
			got.close = ev
		}
		await expect(ws.opened).rejects.toThrow(/403|鉴权/)
		await waitState(ws, TunnelWebSocket.CLOSED)
		expect(got.error).toBe(1)
		expect(got.close).not.toBeNull()
		expect(got.close!.wasClean).toBe(false)
		await serverJob
	})

	it('对端选择未请求的 protocol：握手失败', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagicAndOpen()
			await server.accept101('ghost')
		})()
		const ws = tunnelWs(open, '/ws', { protocols: ['chat'] })
		await expect(ws.opened).rejects.toThrow(/未请求.*ghost/)
		await waitState(ws, TunnelWebSocket.CLOSED)
		await serverJob
	})

	it('建流失败：error+close，opened reject', async () => {
		const ws = tunnelWs(() => Promise.reject(new Error('无可用路径')), '/ws')
		await expect(ws.opened).rejects.toThrow('无可用路径')
		await waitState(ws, TunnelWebSocket.CLOSED)
	})

	it('CONNECTING 期间 close()：握手放弃且取消 pending openStream', async () => {
		let openSignal: AbortSignal | undefined
		const open = (opts?: TunnelOpenOptions) => {
			openSignal = opts?.signal
			return new Promise<never>((_, reject) => {
				if (openSignal?.aborted === true) {
					reject(new Error('open aborted'))
					return
				}
				openSignal?.addEventListener('abort', () => reject(new Error('open aborted')), { once: true })
			})
		}
		const ws = tunnelWs(open, '/ws')
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		ws.close(3001, '先关了')
		const ev = await closed
		expect(ev.code).toBe(3001)
		expect(openSignal?.aborted).toBe(true)
		await expect(ws.opened).rejects.toThrow('open aborted')
	})

	it('非法 url / protocols 抛错', () => {
		const { open } = pair()
		expect(() => tunnelWs(open, 'http://x/')).toThrow(/仅 ws\/wss/)
		expect(() => tunnelWs(open, '/ws', { protocols: ['bad proto'] })).toThrow(/子协议/)
		expect(parseWsTarget('/ws?x=1#frag').target).toBe('/ws?x=1')
		expect(parseWsTarget('wss://a.b/ws#frag').target).toBe('wss://a.b/ws')
	})
})

describe('tunnelWs 消息面', () => {
	/** 建一条已握手的 ws + server（server 端 readLoop 由用例自管）。 */
	async function established(): Promise<{ ws: TunnelWebSocket; server: FakeWsServer }> {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagicAndOpen()
			await server.accept101()
		})()
		const ws = tunnelWs(open, '/ws')
		await ws.opened
		await serverJob
		return { ws, server }
	}

	it('CONNECTING 期间 send 排队，OPEN 后按序发出', async () => {
		const { open, server } = pair()
		let accept!: () => void
		const gate = new Promise<void>(r => {
			accept = r
		})
		const serverJob = (async () => {
			await server.expectMagicAndOpen()
			await gate // 拖住 101，让 client 先排队
			await server.accept101()
			const m1 = await server.readMessage()
			expect(dec.decode(m1.body)).toBe('q1')
			const m2 = await server.readMessage()
			expect(dec.decode(m2.body)).toBe('q2')
		})()
		const ws = tunnelWs(open, '/ws')
		ws.send('q1') // CONNECTING 时排队
		ws.send('q2')
		expect(ws.bufferedAmount).toBeGreaterThan(0)
		accept()
		await ws.opened
		await serverJob
		for (let i = 0; i < 100 && ws.bufferedAmount > 0; i++) {
			await sleep(10)
		}
		expect(ws.bufferedAmount).toBe(0)
		ws.close()
	})

	it('onopen 里同步 send 不会插到 CONNECTING 队列前', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagicAndOpen()
			await server.accept101()
			const m1 = await server.readMessage()
			expect(dec.decode(m1.body)).toBe('queued')
			const m2 = await server.readMessage()
			expect(dec.decode(m2.body)).toBe('in-open')
		})()
		const ws = tunnelWs(open, '/ws')
		ws.send('queued')
		ws.onopen = () => ws.send('in-open')
		await ws.opened
		await serverJob
		ws.close()
	})

	it('text/binary 消息双向收发（binaryType=arraybuffer）', async () => {
		const { ws, server } = await established()
		ws.binaryType = 'arraybuffer'
		const got = new Promise<{ s?: string; b?: ArrayBuffer }>(resolve => {
			const out: { s?: string; b?: ArrayBuffer } = {}
			let n = 0
			ws.onmessage = ev => {
				if (typeof ev.data === 'string') {
					out.s = ev.data
				} else {
					out.b = ev.data as ArrayBuffer
				}
				if (++n === 2) {
					resolve(out)
				}
			}
		})
		ws.send('你好')
		ws.send(new Uint8Array([1, 2, 3]))
		// server 收两条并回显
		const m1 = await server.readMessage()
		expect(m1.opcode).toBe(WS_OPCODE_TEXT)
		expect(dec.decode(m1.body)).toBe('你好')
		const m2 = await server.readMessage()
		expect(m2.opcode).toBe(WS_OPCODE_BINARY)
		expect(m2.body).toEqual(new Uint8Array([1, 2, 3]))
		await server.send(TunnelFrameType.WsMessage, new Uint8Array([WS_OPCODE_TEXT, ...enc.encode('回声')]))
		await server.send(TunnelFrameType.WsMessage, new Uint8Array([WS_OPCODE_BINARY, 9, 8]))
		const r = await got
		expect(r.s).toBe('回声')
		expect(new Uint8Array(r.b!)).toEqual(new Uint8Array([9, 8]))
		ws.close()
	})

	it('WS_PING 自动回 WS_PONG；WS_PONG 静默吸收', async () => {
		const { ws, server } = await established()
		await server.send(TunnelFrameType.WsPing, enc.encode('ping-data'))
		await server.send(TunnelFrameType.WsPong, enc.encode('self-pong'))
		const f = await server.next()
		expect(f!.type).toBe(TunnelFrameType.WsPong)
		expect(dec.decode(f!.payload)).toBe('ping-data')
		ws.close()
	})

	it('对端 WS_CLOSE → 回显 + close 事件（code/reason/wasClean）', async () => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		await server.send(TunnelFrameType.WsClose, encodeJson({ code: 1001, reason: '走了' }))
		const ev = await closed
		expect(ev.code).toBe(1001)
		expect(ev.reason).toBe('走了')
		expect(ev.wasClean).toBe(true)
		expect(ws.readyState).toBe(TunnelWebSocket.CLOSED)
		// client 应回显 WS_CLOSE
		const f = await server.next()
		expect(f!.type).toBe(TunnelFrameType.WsClose)
		expect(decodeWsClose(f!.payload)).toEqual({ code: 1001, reason: '走了' })
		expect(await server.next()).toBeNull() // gracefulClose 后 EOF
	})

	it('对端 WS_CLOSE code 非法 → 协议错误 + close(1006)', async () => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		let errors = 0
		ws.onerror = () => {
			errors++
		}
		await server.send(TunnelFrameType.WsClose, encodeJson({ code: 999, reason: '' }))
		const ev = await closed
		expect(errors).toBe(1)
		expect(ev.code).toBe(1006)
		expect(ev.wasClean).toBe(false)
		const abort = await server.next()
		expect(abort?.type).toBe(TunnelFrameType.Abort)
	})

	it.each([1005, 1006])('拒绝保留 WS_CLOSE code %i', async code => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		await server.send(TunnelFrameType.WsClose, encodeJson({ code, reason: '' }))
		const ev = await closed
		expect(ev.code).toBe(1006)
		expect(ev.wasClean).toBe(false)
		expect((await server.next())?.type).toBe(TunnelFrameType.Abort)
	})

	it('本端 close() → WS_CLOSE 握手 → CLOSED', async () => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		ws.close(4000, 'done')
		expect(ws.readyState).toBe(TunnelWebSocket.CLOSING)
		const f = await server.next()
		expect(f!.type).toBe(TunnelFrameType.WsClose)
		expect(decodeWsClose(f!.payload)).toEqual({ code: 4000, reason: 'done' })
		await server.send(TunnelFrameType.WsClose, encodeJson({ code: 4000, reason: 'done' }))
		const ev = await closed
		expect(ev.code).toBe(4000)
		expect(ev.wasClean).toBe(true)
		expect(await server.next()).toBeNull()
	})

	it('对端 ABORT → error + close(1006)', async () => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		let errors = 0
		ws.onerror = () => {
			errors++
		}
		await server.send(TunnelFrameType.Abort, encodeJson({ code: 'boom', message: '炸了' }))
		const ev = await closed
		expect(errors).toBe(1)
		expect(ev.code).toBe(1006)
		expect(ev.wasClean).toBe(false)
	})

	it('对端 EOF（无 WS_CLOSE）→ close(1006, wasClean=false)', async () => {
		const { ws, server } = await established()
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		await server.st.close()
		const ev = await closed
		expect(ev.code).toBe(1006)
		expect(ev.wasClean).toBe(false)
	})

	it('send 超 4MiB 上限同步拒绝；CLOSED 后 send 丢弃', async () => {
		const { ws, server } = await established()
		expect(() => ws.send(new Uint8Array(5 << 20))).toThrow(/上限/)
		ws.close(1000)
		// server 回显 WS_CLOSE 让 client 走完关闭握手
		const f = await server.next()
		expect(f!.type).toBe(TunnelFrameType.WsClose)
		await server.send(TunnelFrameType.WsClose, f!.payload)
		// CLOSED 后 send 静默丢弃（不抛）
		await waitState(ws, TunnelWebSocket.CLOSED)
		ws.send('ignored')
	})
})
