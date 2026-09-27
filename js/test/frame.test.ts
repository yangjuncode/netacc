/**
 * 帧编解码测试：金向量全部来自 Go 端 frame.go + proto.Marshal 的真实输出
 * （一次性 go test 转储，见 genvec）。覆盖 frame_test.go 的全部用例：
 * DATA/ACK/控制帧往返、解码上限（64KiB 载荷 / 32 ranges / 64KiB 控制体）、
 * 截断输入、非法 range、未知帧类型、多帧流式解码。
 */
import { describe, expect, it } from 'vitest'
import {
	FrameDecoder,
	FrameType,
	MAX_ACK_RANGES,
	MAX_CTRL_BODY,
	MAX_FRAME_PAYLOAD,
	encodeAckFrame,
	encodeCtrlFrame,
	encodeDataFrame,
} from '../src/frame.js'
import { StreamReader, type ByteStream } from '../src/bytestream.js'
import { appendUvarint } from '../src/varint.js'

const ID = new Uint8Array(16).map((_, i) => i + 1)

function hex(s: string): Uint8Array {
	const out = new Uint8Array(s.length / 2)
	for (let i = 0; i < out.length; i++) {
		out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16)
	}
	return out
}

/** bytesByteStream：把一块字节包装成最小 ByteStream（供帧解码喂数据）。 */
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

function decoderOf(...chunks: Uint8Array[]): FrameDecoder {
	return new FrameDecoder(new StreamReader(bsOf(...chunks)))
}

describe('帧线格式（Go 金向量）', () => {
	it('DATA: off=5 ts=11 "hello"', async () => {
		const wire = hex('01050b0568656c6c6f')
		expect(encodeDataFrame(5, 11, new TextEncoder().encode('hello'))).toEqual(wire)
		const r = await decoderOf(wire).next()
		expect(r).not.toBeNull()
		expect(r!.type).toBe(FrameType.Data)
		expect(r!.frame.off).toBe(5)
		expect(r!.frame.sendTs).toBe(11n)
		expect(new TextDecoder().decode(r!.frame.payload)).toBe('hello')
	})

	it('DATA: 大 offset/ts 跨 varint 多字节边界', async () => {
		// off=1<<40+3, ts=1<<33+7, 载荷 0xaa
		const wire = hex('01838080808020878080802001aa')
		const enc = encodeDataFrame(2 ** 40 + 3, (1n << 33n) + 7n, new Uint8Array([0xaa]))
		expect(enc).toEqual(wire)
		const r = await decoderOf(wire).next()
		expect(r!.frame.off).toBe(2 ** 40 + 3)
		expect(r!.frame.sendTs).toBe((1n << 33n) + 7n)
		expect(r!.frame.payload).toEqual(new Uint8Array([0xaa]))
	})

	it('ACK: cum+ts_echo+ts_path+window+seq+ranges', async () => {
		const ranges = [
			{ start: 100, end: 200 },
			{ start: 300, end: 350 },
			{ start: 2 ** 40, end: 2 ** 40 + 999 },
		]
		const wire = hex('028020959aef3a2a808004070364c801ac02de02808080808020e78780808020')
		expect(encodeAckFrame(4096, 123456789, 42, 65536, 7, ranges)).toEqual(wire)
		const r = await decoderOf(wire).next()
		const f = r!.frame
		expect(r!.type).toBe(FrameType.Ack)
		expect(f.cum).toBe(4096)
		expect(f.tsEcho).toBe(123456789n)
		expect(f.tsPath).toBe(42)
		expect(f.window).toBe(65536)
		expect(f.seq).toBe(7)
		expect(f.ranges).toEqual(ranges)
	})

	it('ACK: 无 ranges', async () => {
		const wire = hex('02050b0180080000')
		expect(encodeAckFrame(5, 11, 1, 1024, 0, [])).toEqual(wire)
		const r = await decoderOf(wire).next()
		expect(r!.frame.cum).toBe(5)
		expect(r!.frame.ranges).toEqual([])
	})

	it('控制帧: PATH_ATTACH/PATH_DROP/PING/TELEMETRY/FIN/RST', async () => {
		expect(encodeCtrlFrame(FrameType.PathAttach, hex('0a100102030405060708090a0b0c0d0e0f101005')))
			.toEqual(hex('03140a100102030405060708090a0b0c0d0e0f101005'))
		expect(encodeCtrlFrame(FrameType.PathDrop, hex('0807'))).toEqual(hex('04020807'))
		expect(encodeCtrlFrame(FrameType.Ping, hex('08b9601001'))).toEqual(hex('070508b9601001'))
		expect(encodeCtrlFrame(FrameType.Telemetry, hex('0a05080110e8070a05080210d00f')))
			.toEqual(hex('080e0a05080110e8070a05080210d00f'))
		expect(encodeCtrlFrame(FrameType.Fin)).toEqual(hex('0900'))
		expect(encodeCtrlFrame(FrameType.Rst)).toEqual(hex('0a00'))
		expect(encodeCtrlFrame(FrameType.PathReady, hex('080912046e6f7065'))).toEqual(hex('0608080912046e6f7065'))

		// 解码校验
		const r = await decoderOf(hex('04020807')).next()
		expect(r!.type).toBe(FrameType.PathDrop)
		expect(r!.frame.body).toEqual(hex('0807'))
	})

	it('多帧流式解码：DATA+ACK+FIN 背靠背，EOF 返回 null', async () => {
		const wire = new Uint8Array([
			...hex('01050b0568656c6c6f'), // DATA
			...hex('02050b0180080000'), // ACK_NR
			...hex('0900'), // FIN
		])
		const fr = decoderOf(wire)
		const r1 = await fr.next()
		expect(r1!.type).toBe(FrameType.Data)
		const r2 = await fr.next()
		expect(r2!.type).toBe(FrameType.Ack)
		const r3 = await fr.next()
		expect(r3!.type).toBe(FrameType.Fin)
		expect(await fr.next()).toBeNull() // 干净 EOF
	})

	it('分块喂数据：跨块切割的帧仍能正确解码', async () => {
		// 把一条 DATA 帧拆成 1 字节一粒的块
		const wire = hex('01050b0568656c6c6f')
		const chunks = Array.from(wire).map(b => new Uint8Array([b]))
		const r = await decoderOf(...chunks).next()
		expect(r!.type).toBe(FrameType.Data)
		expect(new TextDecoder().decode(r!.frame.payload)).toBe('hello')
	})
})

describe('帧解码防御上限', () => {
	it('DATA 载荷 > 64KiB 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), FrameType.Data)
		hdr = appendUvarint(hdr, 0) // offset
		hdr = appendUvarint(hdr, 0) // ts
		hdr = appendUvarint(hdr, MAX_FRAME_PAYLOAD + 1) // len 超限
		await expect(decoderOf(hdr, new Uint8Array(8)).next()).rejects.toThrow()
	})

	it('ACK ranges > 32 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), FrameType.Ack)
		for (let i = 0; i < 5; i++) {
			hdr = appendUvarint(hdr, 0) // cum/ts_echo/ts_path/window/seq
		}
		hdr = appendUvarint(hdr, MAX_ACK_RANGES + 1)
		await expect(decoderOf(hdr).next()).rejects.toThrow()
	})

	it('ACK range end <= start 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), FrameType.Ack)
		for (let i = 0; i < 5; i++) {
			hdr = appendUvarint(hdr, 0)
		}
		hdr = appendUvarint(hdr, 1) // n_ranges = 1
		hdr = appendUvarint(hdr, 100) // start
		hdr = appendUvarint(hdr, 100) // end == start 非法
		await expect(decoderOf(hdr).next()).rejects.toThrow(/非法/)
	})

	it('控制帧体 > 64KiB 拒绝', async () => {
		let hdr = appendUvarint(new Uint8Array(0), FrameType.PathAttach)
		hdr = appendUvarint(hdr, MAX_CTRL_BODY + 1)
		await expect(decoderOf(hdr).next()).rejects.toThrow()
	})

	it('截断帧头 / 截断载荷报错', async () => {
		await expect(decoderOf(new Uint8Array([FrameType.Data, 0x80])).next()).rejects.toThrow()
		// DATA 声明 5 字节载荷但只到 2 字节
		const wire = hex('01050b0568656c6c6f')
		await expect(decoderOf(wire.subarray(0, wire.length - 2)).next()).rejects.toThrow()
	})

	it('未知帧类型拒绝', async () => {
		await expect(decoderOf(new Uint8Array([200, 0x01])).next()).rejects.toThrow(/未知帧类型/)
	})

	it('ACK ranges 编码截断到 32', () => {
		const many = Array.from({ length: 40 }, (_, i) => ({ start: i * 10, end: i * 10 + 5 }))
		const wire = encodeAckFrame(0, 0, 0, 0, 0, many)
		// 解码只应解出 32 条
		return decoderOf(wire).next().then(r => {
			expect(r!.frame.ranges.length).toBe(MAX_ACK_RANGES)
		})
	})
})
