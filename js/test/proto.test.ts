/**
 * protobuf 线格式测试：金向量由 Go 端 google.golang.org/protobuf
 * proto.Marshal 实际编码生成（见仓库 genvec_test.go 的一次性输出），
 * JS 编解码必须逐字节一致。
 */
import { describe, expect, it } from 'vitest'
import {
	decodeHello,
	decodeHelloAck,
	decodePathAttach,
	decodePathDrop,
	decodePathReady,
	decodePathRequest,
	decodePing,
	decodeTelemetry,
	encodeHello,
	encodeHelloAck,
	encodePathAttach,
	encodePathDrop,
	encodePathReady,
	encodePathRequest,
	encodePing,
	encodeTelemetry,
} from '../src/proto.js'

const ID = new Uint8Array(16).map((_, i) => i + 1) // 01 02 … 10

function hex(s: string): Uint8Array {
	const out = new Uint8Array(s.length / 2)
	for (let i = 0; i < out.length; i++) {
		out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16)
	}
	return out
}

describe('protobuf 金向量（Go proto.Marshal 生成）', () => {
	it('Hello/HelloAck：{bytes agg_stream_id=1}', () => {
		const want = hex('0a100102030405060708090a0b0c0d0e0f10')
		expect(encodeHello(ID)).toEqual(want)
		const m = decodeHello(want)
		expect(m.aggStreamId).toEqual(ID)
		expect(m.auth).toEqual(new Uint8Array(0))
	})

	it('Hello.auth：{bytes auth=2}；HelloAck.error：{string error=2}', () => {
		// Hello{agg_stream_id=ID, auth="s3"}
		const hello = hex('0a100102030405060708090a0b0c0d0e0f1012027333')
		expect(encodeHello(ID, new Uint8Array([0x73, 0x33]))).toEqual(hello)
		const m = decodeHello(hello)
		expect(m.auth).toEqual(new Uint8Array([0x73, 0x33]))

		// HelloAck{agg_stream_id=ID, error="unauthorized"}
		const reason = 'unauthorized'
		const enc = new TextEncoder().encode(reason)
		const want = new Uint8Array([...hex('0a100102030405060708090a0b0c0d0e0f10'), 0x12, enc.length, ...enc])
		expect(encodeHelloAck(ID, reason)).toEqual(want)
		const ack = decodeHelloAck(want)
		expect(ack.aggStreamId).toEqual(ID)
		expect(ack.error).toBe(reason)
		// 旧端只回显 id（无 error）→ error 为空串
		expect(decodeHelloAck(encodeHelloAck(ID)).error).toBe('')
	})

	it('PathAttach：{bytes=1, uint64 path_id=2}', () => {
		const want = hex('0a100102030405060708090a0b0c0d0e0f101005')
		expect(encodePathAttach(ID, 5)).toEqual(want)
		const m = decodePathAttach(want)
		expect(m.aggStreamId).toEqual(ID)
		expect(m.pathId).toBe(5)
	})

	it('PathDrop：{uint64 path_id=1}', () => {
		const want = hex('0807')
		expect(encodePathDrop(7)).toEqual(want)
		expect(decodePathDrop(want).pathId).toBe(7)
	})

	it('Ping：{send_ts=1, reply=2}', () => {
		const want = hex('08b9601001')
		expect(encodePing(12345, true)).toEqual(want)
		const m = decodePing(want)
		expect(m.sendTs).toBe(12345n)
		expect(m.reply).toBe(true)
	})

	it('Telemetry：repeated PathRate', () => {
		const want = hex('0a05080110e8070a05080210d00f')
		expect(encodeTelemetry([
			{ pathId: 1, rateBps: 1000 },
			{ pathId: 2, rateBps: 2000 },
		])).toEqual(want)
		const m = decodeTelemetry(want)
		expect(m.rates).toEqual([
			{ pathId: 1, rateBps: 1000 },
			{ pathId: 2, rateBps: 2000 },
		])
	})

	it('PathRequest：request_id + relay_peer + repeated relay_addrs + ac_addr', () => {
		const want = hex('08091202aabb1a0201021a010322020909')
		expect(encodePathRequest({
			requestId: 9,
			relayPeer: hex('aabb'),
			relayAddrs: [new Uint8Array([1, 2]), new Uint8Array([3])],
			acAddr: new Uint8Array([9, 9]),
		})).toEqual(want)
		const m = decodePathRequest(want)
		expect(m.requestId).toBe(9)
		expect(m.relayPeer).toEqual(hex('aabb'))
		expect(m.relayAddrs).toEqual([new Uint8Array([1, 2]), new Uint8Array([3])])
		expect(m.acAddr).toEqual(new Uint8Array([9, 9]))
	})

	it('PathReady：request_id + error', () => {
		const want = hex('080912046e6f7065')
		expect(encodePathReady(9, 'nope')).toEqual(want)
		const m = decodePathReady(want)
		expect(m.requestId).toBe(9)
		expect(m.error).toBe('nope')
	})

	it('proto3 canonical：零值字段省略', () => {
		expect(encodePathDrop(0)).toEqual(new Uint8Array(0))
		expect(encodePing(0, false)).toEqual(new Uint8Array(0))
		expect(encodePathReady(0, '')).toEqual(new Uint8Array(0))
	})

	it('解码跳过未知字段（前向兼容）', () => {
		// 在合法 PathDrop{path_id=7} 后追加一个未知 varint 字段 field=99
		const extra = new Uint8Array([0x08, 0x07, 0xf8, 0x06, 0x2a]) // tag=99<<3|0, val=42
		const m = decodePathDrop(extra)
		expect(m.pathId).toBe(7)
	})

	it('截断报错误而非静默', () => {
		expect(() => decodeHello(new Uint8Array([0x0a, 0x10, 0x01]))).toThrow()
	})
})
