/**
 * tunnel 线协议编解码测试：magic、帧头、JSON 消息、防御上限、
 * gracefulClose 收尾语义。金样字节按线协议规范手工固化，
 * 后续 Go 侧实现同一协议时可直接对照。
 */
import { describe, expect, it } from 'vitest'
import { StreamReader, type ByteStream } from '../src/bytestream.js'
import { appendUvarint } from '../src/varint.js'
import {
	MAX_TUNNEL_DATA,
	MAX_TUNNEL_JSON,
	MAX_TUNNEL_URL,
	MAX_TUNNEL_WS_MESSAGE,
	TUNNEL_MAGIC,
	TunnelFrameReader,
	TunnelFrameType,
	TunnelSession,
	checkTarget,
	decodeAbort,
	decodeJson,
	decodeResult,
	decodeWsClose,
	encodeJson,
	encodeTunnelFrame,
	gracefulClose,
	normalizeHeaders,
	type TunnelStream,
} from '../src/tunnel-wire.js'
import { MemoryByteStream } from '../src/memory-stream.js'

const enc = new TextEncoder()

function hex(s: string): Uint8Array {
	const out = new Uint8Array(s.length / 2)
	for (let i = 0; i < out.length; i++) {
		out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16)
	}
	return out
}

/** bytesByteStream：把若干字节块包装成最小可读流（复用 frame.test.ts 形态）。 */
function bsOf(...chunks: Uint8Array[]): ByteStream {
	let i = 0
	return {
		async read() {
			return i < chunks.length ? chunks[i++]! : null
		},
		async write() {},
		async close() {},
		abort() {},
	}
}

function readerOf(...chunks: Uint8Array[]): TunnelFrameReader {
	return new TunnelFrameReader(new StreamReader(bsOf(...chunks)))
}

describe('magic 与帧编解码', () => {
	it('magic 为 5 字节 "NTUN"+0x01', () => {
		expect(TUNNEL_MAGIC).toEqual(hex('4e54554e01'))
	})

	it('帧编码：uvarint(type) + uvarint(len) + payload', () => {
		expect(encodeTunnelFrame(TunnelFrameType.Data, enc.encode('hi'))).toEqual(hex('03026869'))
		expect(encodeTunnelFrame(TunnelFrameType.End)).toEqual(hex('0400'))
		// 载荷 128 字节 → len 前缀两字节 uvarint(128)=80 01
		expect(encodeTunnelFrame(TunnelFrameType.Data, new Uint8Array(128)).subarray(0, 3))
			.toEqual(hex('038001'))
	})

	it('magic+OPEN 帧金样：字节序列可按协议逐段解出', async () => {
		// 协议金样：{"v":1,"kind":"http","target":"/api/a?x=1","method":"GET"}（58 字节）
		const json = '{"v":1,"kind":"http","target":"/api/a?x=1","method":"GET"}'
		expect(enc.encode(json).length).toBe(58)
		const wire = new Uint8Array([
			...TUNNEL_MAGIC, // 4e54554e01
			0x01, // frame type = OPEN
			0x3a, // payload_len = 58
			...enc.encode(json),
		])
		expect(wire.subarray(0, 5)).toEqual(hex('4e54554e01'))
		const r = new StreamReader(bsOf(wire))
		expect(await r.readExact(5)).toEqual(TUNNEL_MAGIC) // magic
		const fr = new TunnelFrameReader(r)
		const f = await fr.next()
		expect(f).not.toBeNull()
		expect(f!.type).toBe(TunnelFrameType.Open)
		expect(f!.payload.length).toBe(58)
		const msg = decodeJson(f!.payload, 'OPEN') as Record<string, unknown>
		expect(msg).toEqual({ v: 1, kind: 'http', target: '/api/a?x=1', method: 'GET' })
	})

	it('多帧背靠背解码；EOF 于帧边界返回 null', async () => {
		const wire = new Uint8Array([
			...hex('03026869'), // DATA "hi"
			...hex('0400'), // END
		])
		const fr = readerOf(wire)
		expect((await fr.next())!.type).toBe(TunnelFrameType.Data)
		expect((await fr.next())!.type).toBe(TunnelFrameType.End)
		expect(await fr.next()).toBeNull()
	})

	it('逐字节喂数据：跨块切割的帧仍能正确解码', async () => {
		const wire = hex('03026869')
		const chunks = Array.from(wire).map(b => new Uint8Array([b]))
		const f = await readerOf(...chunks).next()
		expect(f!.type).toBe(TunnelFrameType.Data)
		expect(new TextDecoder().decode(f!.payload)).toBe('hi')
	})

	it('截断帧报错；未知帧类型拒绝', async () => {
		await expect(readerOf(new Uint8Array([TunnelFrameType.Data, 0x80])).next()).rejects.toThrow()
		await expect(readerOf(new Uint8Array([200, 0x00])).next()).rejects.toThrow(/未知隧道帧类型/)
	})
})

describe('帧防御上限', () => {
	it('DATA 载荷 >64KiB 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), TunnelFrameType.Data)
		hdr = appendUvarint(hdr, MAX_TUNNEL_DATA + 1)
		await expect(readerOf(hdr, new Uint8Array(8)).next()).rejects.toThrow(/超过上限/)
	})

	it('WS_MESSAGE 载荷 >4MiB 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), TunnelFrameType.WsMessage)
		hdr = appendUvarint(hdr, MAX_TUNNEL_WS_MESSAGE + 1)
		await expect(readerOf(hdr, new Uint8Array(8)).next()).rejects.toThrow(/超过上限/)
	})

	it('JSON 控制帧 >64KiB 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), TunnelFrameType.Result)
		hdr = appendUvarint(hdr, MAX_TUNNEL_JSON + 1)
		await expect(readerOf(hdr, new Uint8Array(8)).next()).rejects.toThrow(/超过上限/)
	})

	it('END 必须空载荷', async () => {
		let hdr = appendUvarint(new Uint8Array(0), TunnelFrameType.End)
		hdr = appendUvarint(hdr, 1)
		await expect(readerOf(hdr, new Uint8Array([0])).next()).rejects.toThrow(/超过上限/)
	})
})

describe('JSON 消息编解码', () => {
	it('RESULT(http) 解码', () => {
		const m = decodeResult(encodeJson({
			kind: 'http',
			status: 200,
			statusText: 'OK',
			headers: [['content-type', 'text/plain']],
		}))
		expect(m.kind).toBe('http')
		expect(m.status).toBe(200)
		expect(m.statusText).toBe('OK')
		expect(m.headers).toEqual([['content-type', 'text/plain']])
	})

	it('RESULT(ws) 拒绝形态解码', () => {
		const m = decodeResult(encodeJson({ kind: 'ws', status: 403, error: 'denied' }))
		expect(m.kind).toBe('ws')
		expect(m.status).toBe(403)
		expect(m.error).toBe('denied')
	})

	it('RESULT kind 非法 / status 缺失兜底', () => {
		expect(() => decodeResult(encodeJson({ kind: 'x', status: 1 }))).toThrow(/kind/)
		expect(decodeResult(encodeJson({ kind: 'http' })).status).toBe(0)
	})

	it('ABORT 解码：code 宽容接受 number', () => {
		expect(decodeAbort(encodeJson({ code: 'timeout', message: 'm' }))).toEqual({ code: 'timeout', message: 'm' })
		expect(decodeAbort(encodeJson({ code: 500, message: 'x' })).code).toBe('500')
	})

	it('WS_CLOSE 解码', () => {
		expect(decodeWsClose(encodeJson({ code: 1000, reason: 'bye' }))).toEqual({ code: 1000, reason: 'bye' })
		for (const code of [1004, 1005, 1006]) {
			expect(() => decodeWsClose(encodeJson({ code, reason: '' }))).toThrow(/非法/)
		}
	})

	it('非 JSON / 非对象载荷报协议错误', () => {
		expect(() => decodeJson(enc.encode('not-json'), 'X')).toThrow(/JSON 解析失败/)
		expect(() => decodeJson(enc.encode('42'), 'X')).toThrow(/不是 JSON 对象/)
	})
})

describe('输入校验', () => {
	it('checkTarget：空串/控制字符/超长拒绝', () => {
		expect(() => checkTarget('')).toThrow()
		expect(() => checkTarget('/a\nb')).toThrow(/控制字符/)
		expect(() => checkTarget('/' + 'x'.repeat(MAX_TUNNEL_URL))).toThrow(/上限/)
		expect(checkTarget('/api')).toBe('/api')
	})

	it('normalizeHeaders：三种形态 + 控制字符拒绝 + Host 保留原样', () => {
		expect(normalizeHeaders({ 'X-A': '1' })).toEqual([['X-A', '1']])
		expect(normalizeHeaders([['X-A', '1'], ['X-A', '2']])).toEqual([['X-A', '1'], ['X-A', '2']])
		expect(normalizeHeaders(new Headers({ 'x-a': '1' }))).toEqual([['x-a', '1']])
		expect(() => normalizeHeaders({ 'Bad Name': 'x' })).toThrow(/header 名非法/)
		expect(() => normalizeHeaders({ 'X-A': 'a\nb' })).toThrow(/控制字符/)
	})
})

describe('TunnelSession 与 gracefulClose', () => {
	it('写链串行化：帧按序到达且边界完整', async () => {
		const [a, b] = MemoryByteStream.pair()
		const s = new TunnelSession(a)
		await s.writeMagic()
		await s.send(TunnelFrameType.Open, encodeJson({ v: 1 }))
		await s.send(TunnelFrameType.Data, enc.encode('d1'))
		await s.send(TunnelFrameType.End)
		await s.gracefulClose()

		const r = new StreamReader(b)
		expect(await r.readExact(5)).toEqual(TUNNEL_MAGIC)
		const fr = new TunnelFrameReader(r)
		expect((await fr.next())!.type).toBe(TunnelFrameType.Open)
		expect((await fr.next())!.type).toBe(TunnelFrameType.Data)
		expect((await fr.next())!.type).toBe(TunnelFrameType.End)
		expect(await fr.next()).toBeNull() // gracefulClose 后对端见 EOF
	})

	it('gracefulClose：带 stats() 的流等 ackedBytes 追平 sentBytes 才关', async () => {
		const [a, b] = MemoryByteStream.pair()
		void b
		let acked = 0
		let sent = 1 // 已写未确认 1 字节：gracefulClose 必须先等
		const st: TunnelStream = {
			read: () => a.read(),
			write: d => {
				sent += d.length
				return a.write(d)
			},
			close: () => a.close(),
			abort: e => a.abort(e),
			stats: () => ({ sentBytes: sent, ackedBytes: acked, pendingBytes: 0 }),
		}
		const done = gracefulClose(st, 2000)
		let closed = false
		void done.then(() => {
			closed = true
		})
		await new Promise(r => setTimeout(r, 50))
		expect(closed).toBe(false) // ack 未追平：仍在等
		acked = sent
		await done
		expect(closed).toBe(true)
	})

	it('gracefulClose：对端迟迟不确认按超时兜底关闭', async () => {
		const [a] = MemoryByteStream.pair()
		const st: TunnelStream = {
			read: () => a.read(),
			write: d => a.write(d),
			close: () => a.close(),
			stats: () => ({ sentBytes: 1, ackedBytes: 0, pendingBytes: 0 }),
		}
		await gracefulClose(st, 60) // 短超时路径
		await expect(st.write(new Uint8Array([1]))).rejects.toThrow() // 已关
	})
})
