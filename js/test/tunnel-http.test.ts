/**
 * tunnelFetch 测试：MemoryByteStream 配对 + 手写假 server，
 * 验证 magic/OPEN 字节、请求体 DATA/END 切分、Response 构造、
 * 流式 body、4xx 语义、ABORT/signal/body cancel 收尾。
 */
import { describe, expect, it } from 'vitest'
import { StreamReader } from '../src/bytestream.js'
import { MemoryByteStream } from '../src/memory-stream.js'
import { tunnelFetch, parseHttpTarget } from '../src/tunnel-http.js'
import {
	TUNNEL_MAGIC,
	TunnelFrameReader,
	TunnelFrameType,
	decodeJson,
	encodeJson,
	encodeTunnelFrame,
	type TunnelFrame,
	type TunnelOpenMsg,
	type TunnelStream,
} from '../src/tunnel-wire.js'
import type { NetaccError } from '../src/types.js'

const enc = new TextEncoder()
const dec = new TextDecoder()

/** FakeServer：server 侧隧道会话（读 magic + 逐帧读 + 写帧）。 */
class FakeServer {
	readonly reader: TunnelFrameReader
	private readonly raw: StreamReader
	constructor(private readonly st: TunnelStream) {
		this.raw = new StreamReader(st)
		this.reader = new TunnelFrameReader(this.raw)
	}

	async expectMagic(): Promise<void> {
		expect(await this.raw.readExact(5)).toEqual(TUNNEL_MAGIC)
	}

	async next(): Promise<TunnelFrame | null> {
		return this.reader.next()
	}

	async send(type: TunnelFrameType, payload?: Uint8Array): Promise<void> {
		await this.st.write(encodeTunnelFrame(type, payload))
	}

	async sendResult(status: number, body?: Uint8Array | string, statusText = 'OK'): Promise<void> {
		await this.send(TunnelFrameType.Result, encodeJson({ kind: 'http', status, statusText }))
		if (body !== undefined) {
			const b = typeof body === 'string' ? enc.encode(body) : body
			if (b.length > 0) {
				await this.send(TunnelFrameType.Data, b)
			}
		}
		await this.send(TunnelFrameType.End)
	}

	async readOpen(): Promise<TunnelOpenMsg> {
		const f = await this.next()
		expect(f).not.toBeNull()
		expect(f!.type).toBe(TunnelFrameType.Open)
		return decodeJson(f!.payload, 'OPEN') as TunnelOpenMsg
	}

	/** readRequestBody 读尽请求体 DATA 帧直到 END，返回拼接字节。 */
	async readRequestBody(): Promise<Uint8Array> {
		const parts: Uint8Array[] = []
		for (;;) {
			const f = await this.next()
			if (f === null) {
				throw new Error('EOF before END')
			}
			if (f.type === TunnelFrameType.Data) {
				parts.push(f.payload)
				continue
			}
			if (f.type === TunnelFrameType.End) {
				break
			}
			throw new Error(`请求体中出现帧类型 ${f.type}`)
		}
		const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
		let off = 0
		for (const p of parts) {
			out.set(p, off)
			off += p.length
		}
		return out
	}
}

function pair(): { open: () => Promise<TunnelStream>; server: FakeServer; serverSt: TunnelStream } {
	const [client, server] = MemoryByteStream.pair()
	return {
		open: () => Promise.resolve(client),
		server: new FakeServer(server),
		serverSt: server,
	}
}

describe('tunnelFetch 请求路径', () => {
	it('GET /api/a?x=1：OPEN 金样字段 + RESULT 200 + 响应体', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			const msg = await server.readOpen()
			expect(msg).toEqual({ v: 1, kind: 'http', target: '/api/a?x=1', method: 'GET' })
			// 空请求体也应收 END
			expect(await server.readRequestBody()).toEqual(new Uint8Array(0))
			await server.sendResult(200, 'hello')
		})()
		const res = await tunnelFetch(open, '/api/a?x=1')
		expect(res.status).toBe(200)
		expect(await res.text()).toBe('hello')
		await serverJob
	})

	it('204/HEAD 响应 body 为 null，残余帧后台排空', async () => {
		{
			const { open, server } = pair()
			const serverJob = (async () => {
				await server.expectMagic()
				await server.readOpen()
				await server.readRequestBody()
				await server.sendResult(204, '', 'No Content')
			})()
			const res = await tunnelFetch(open, '/empty')
			expect(res.status).toBe(204)
			expect(res.body).toBeNull()
			expect(await res.text()).toBe('')
			await serverJob
		}
		{
			const { open, server } = pair()
			const serverJob = (async () => {
				await server.expectMagic()
				await server.readOpen()
				await server.readRequestBody()
				await server.sendResult(200, 'ignored')
			})()
			const res = await tunnelFetch(open, '/head', { method: 'HEAD' })
			expect(res.status).toBe(200)
			expect(res.body).toBeNull()
			await serverJob
		}
	})

	it('无响应体状态收到 DATA 时按协议违例中止', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await server.readRequestBody()
			await server.send(TunnelFrameType.Result, encodeJson({ kind: 'http', status: 204 }))
			await server.send(TunnelFrameType.Data, enc.encode('illegal'))
			const abort = await server.next()
			expect(abort?.type).toBe(TunnelFrameType.Abort)
		})()
		const res = await tunnelFetch(open, '/empty')
		expect(res.status).toBe(204)
		expect(res.body).toBeNull()
		await serverJob
	})

	it('绝对 http(s) URL：target 为完整 URL、host 取 URL.host、hash 剥离', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			const msg = await server.readOpen()
			expect(msg.target).toBe('https://example.com:8443/x/y?z=1')
			expect(msg.host).toBe('example.com:8443')
			await server.readRequestBody()
			await server.sendResult(200, 'ok')
		})()
		const res = await tunnelFetch(open, 'https://example.com:8443/x/y?z=1#frag')
		expect(res.status).toBe(200)
		await res.arrayBuffer()
		await serverJob
	})

	it('POST string body：DATA 帧内容 + 自动 Content-Length/Content-Type', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			const msg = await server.readOpen()
			expect(msg.method).toBe('POST')
			expect(msg.headers).toContainEqual(['content-length', '5'])
			expect(msg.headers).toContainEqual(['content-type', 'text/plain;charset=UTF-8'])
			expect(dec.decode(await server.readRequestBody())).toBe('hello')
			await server.sendResult(201, 'created')
		})()
		const res = await tunnelFetch(open, '/submit', { method: 'post', body: 'hello' })
		expect(res.status).toBe(201)
		await serverJob
	})

	it('ReadableStream 请求体逐块发出（不整包缓冲）', async () => {
		const { open, server } = pair()
		const chunks = [enc.encode('part1-'), enc.encode('part2-'), enc.encode('part3')]
		const serverJob = (async () => {
			await server.expectMagic()
			const msg = await server.readOpen()
			// 流式 body 不应带 content-length
			expect(msg.headers ?? []).not.toContainEqual(['content-length', expect.anything()])
			const got = await server.readRequestBody()
			expect(dec.decode(got)).toBe('part1-part2-part3')
			await server.sendResult(200, 'done')
		})()
		const rs = new ReadableStream<Uint8Array>({
			start(c) {
				for (const ch of chunks) {
					c.enqueue(ch)
				}
				c.close()
			},
		})
		const res = await tunnelFetch(open, '/up', { method: 'POST', body: rs })
		expect(res.status).toBe(200)
		await serverJob
	})

	it('Host 头提升为 OPEN.host 且不重复进 headers', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			const msg = await server.readOpen()
			expect(msg.host).toBe('backend.internal')
			expect(msg.headers ?? []).toContainEqual(['X-A', '1'])
			expect((msg.headers ?? []).some(h => h[0].toLowerCase() === 'host')).toBe(false)
			await server.readRequestBody()
			await server.sendResult(200, 'ok')
		})()
		const res = await tunnelFetch(open, '/p', { headers: { Host: 'backend.internal', 'X-A': '1' } })
		expect(res.status).toBe(200)
		await serverJob
	})

	it('HTTP 404 返回 Response 不 reject；响应头透传', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await server.readRequestBody()
			await server.send(TunnelFrameType.Result, encodeJson({
				kind: 'http',
				status: 404,
				statusText: 'Not Found',
				headers: [['x-why', 'gone']],
			}))
			await server.send(TunnelFrameType.Data, enc.encode('nf'))
			await server.send(TunnelFrameType.End)
		})()
		const res = await tunnelFetch(open, '/missing')
		expect(res.status).toBe(404)
		expect(res.headers.get('x-why')).toBe('gone')
		expect(await res.text()).toBe('nf')
		await serverJob
	})

	it('GET/HEAD 带 body 拒绝；非法 input/method 拒绝', async () => {
		const { open } = pair()
		await expect(tunnelFetch(open, '/x', { body: 'b' })).rejects.toThrow(/GET\/HEAD|不能携带/)
		await expect(tunnelFetch(open, 'ftp://x/')).rejects.toThrow(/仅 http/)
		await expect(tunnelFetch(open, 'notaurl', {})).rejects.toThrow(/须为/)
		await expect(tunnelFetch(open, '/x', { method: 'GE T' })).rejects.toThrow(/method/)
	})
})

describe('tunnelFetch 异常与收尾', () => {
	it('RESULT 前收 ABORT → reject peer_abort', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await server.send(TunnelFrameType.Abort, encodeJson({ code: 'denied', message: '不允许' }))
		})()
		await expect(tunnelFetch(open, '/x')).rejects.toMatchObject({ code: 'peer_abort' } satisfies Partial<NetaccError>)
		await serverJob
	})

	it('RESULT 前出现 DATA 帧 → 协议错误 reject', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await server.send(TunnelFrameType.Data, enc.encode('early'))
		})()
		await expect(tunnelFetch(open, '/x')).rejects.toMatchObject({ code: 'protocol' })
		await serverJob
	})

	it('对端 EOF（无 RESULT）→ reject', async () => {
		const { open, server, serverSt } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await serverSt.close()
		})()
		await expect(tunnelFetch(open, '/x')).rejects.toThrow(/关闭/)
		await serverJob
	})

	it('openStream 建流失败 → 原错误抛出', async () => {
		const open = () => Promise.reject(new Error('拨不通'))
		await expect(tunnelFetch(open, '/x')).rejects.toThrow('拨不通')
	})

	it('signal 已中止 → 立刻 reject 且不开流', async () => {
		let opened = false
		const open = () => {
			opened = true
			return pair().open()
		}
		const ac = new AbortController()
		ac.abort()
		await expect(tunnelFetch(open, '/x', { signal: ac.signal })).rejects.toThrow()
		expect(opened).toBe(false)
	})

	it('等 RESULT 期间 signal 中止 → reject + 对端收到 ABORT/EOF', async () => {
		const { open, server } = pair()
		const ac = new AbortController()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			// client 中止后：尽力 ABORT 帧（abort 语义下可能被流传输丢弃），
			// 最终对端读到 EOF 或读错误都算合法收尾
			try {
				const f = await server.next()
				if (f !== null) {
					expect(f.type).toBe(TunnelFrameType.Abort)
					expect(await server.next()).toBeNull()
				}
			} catch {
				// 流传输中止：同样合法
			}
		})()
		const p = tunnelFetch(open, '/slow', { signal: ac.signal })
		await new Promise(r => setTimeout(r, 20))
		ac.abort()
		await expect(p).rejects.toThrow()
		await serverJob
	})

	it('响应体 cancel 终止聚合流（对端 EOF/ABORT）', async () => {
		const { open, server } = pair()
		const serverJob = (async () => {
			await server.expectMagic()
			await server.readOpen()
			await server.readRequestBody()
			await server.send(TunnelFrameType.Result, encodeJson({ kind: 'http', status: 200, statusText: 'OK' }))
			// 持续供应 DATA，直到对端关闭
			try {
				for (let i = 0; i < 1000; i++) {
					await server.send(TunnelFrameType.Data, enc.encode(`chunk${i}`))
					await new Promise(r => setTimeout(r, 5))
				}
			} catch {
				// 对端关闭后写失败：符合预期
			}
		})()
		const res = await tunnelFetch(open, '/big')
		expect(res.status).toBe(200)
		await res.body!.cancel()
		// cancel 后底层流应被终止：对端读侧最终 EOF/出错
		await serverJob
	})
})

describe('parseHttpTarget', () => {
	it("'/path' → 本地 handler", () => {
		expect(parseHttpTarget('/api')).toEqual({ target: '/api', host: '' })
	})
	it('http/https URL → 代理目标 + host', () => {
		const t = parseHttpTarget('http://a.b:8080/p?q')
		expect(t.target).toBe('http://a.b:8080/p?q')
		expect(t.host).toBe('a.b:8080')
	})
})
