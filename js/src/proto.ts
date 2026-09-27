/**
 * agg.proto 的手写 protobuf wire 编解码。
 *
 * 选择手写而非依赖 protobuf-es/protobufjs 的理由：消息集固定且字段少
 * （bytes/uint64/bool/string/repeated bytes/repeated message），手写 codec
 * 与 google.golang.org/protobuf 的 proto.Marshal 输出逐字节一致
 * （proto3 canonical：字段按序、零值省略），且不引入任何运行时依赖。
 *
 * 解码器跳过未知字段（wire type 0/1/2/5），与 proto.Unmarshal 的前向
 * 兼容语义一致。
 */

import { appendUvarint, decodeUvarint, toSafeNumber } from './varint.js'

// protobuf wire type
const WT_VARINT = 0
const WT_LEN = 2

export class ProtoWriter {
	buf: Uint8Array = new Uint8Array(0)

	/** varint 字段（uint64/bool/enum）；v 为零值时按 proto3 省略。 */
	varint(field: number, v: bigint | number | boolean): this {
		const n = typeof v === 'boolean' ? (v ? 1n : 0n) : BigInt(v)
		if (n === 0n) {
			return this
		}
		this.buf = appendUvarint(this.buf, BigInt(field << 3 | WT_VARINT))
		this.buf = appendUvarint(this.buf, n)
		return this
	}

	/** 长度定界字段（bytes/string/message）；空值省略。 */
	bytes(field: number, v: Uint8Array | undefined): this {
		if (v == null || v.length === 0) {
			return this
		}
		this.buf = appendUvarint(this.buf, BigInt(field << 3 | WT_LEN))
		this.buf = appendUvarint(this.buf, v.length)
		const out = new Uint8Array(this.buf.length + v.length)
		out.set(this.buf, 0)
		out.set(v, this.buf.length)
		this.buf = out
		return this
	}

	str(field: number, v: string | undefined): this {
		if (v == null || v === '') {
			return this
		}
		return this.bytes(field, new TextEncoder().encode(v))
	}
}

/** ProtoReader 是顺序游标式读取器；next() 返回下一个字段的 (field, wire)。 */
export class ProtoReader {
	private pos = 0
	constructor(public readonly buf: Uint8Array) {}

	get done(): boolean {
		return this.pos >= this.buf.length
	}

	private readTag(): { field: number; wire: number } | null {
		const r = decodeUvarint(this.buf, this.pos)
		if (r == null || r.value === 0n) {
			throw new Error('protobuf: 非法 tag')
		}
		this.pos += r.size
		return { field: toSafeNumber(r.value >> 3n, 'tag'), wire: Number(r.value & 7n) }
	}

	/** 读取下一个字段头；流结束返回 null。 */
	next(): { field: number; wire: number } | null {
		if (this.done) {
			return null
		}
		return this.readTag()
	}

	/** 读 varint 值（调用方须确认当前字段 wire=0）。 */
	varint(): bigint {
		const r = decodeUvarint(this.buf, this.pos)
		if (r == null) {
			throw new Error('protobuf: varint 截断')
		}
		this.pos += r.size
		return r.value
	}

	/** 读长度定界值（wire=2）。 */
	bytes(): Uint8Array {
		const len = decodeUvarint(this.buf, this.pos)
		if (len == null) {
			throw new Error('protobuf: 长度前缀截断')
		}
		this.pos += len.size
		const n = toSafeNumber(len.value, 'protobuf 字段长度')
		if (this.pos + n > this.buf.length) {
			throw new Error('protobuf: 字段体截断')
		}
		const out = this.buf.subarray(this.pos, this.pos + n)
		this.pos += n
		return out
	}

	/** 跳过当前字段体（按已读出的 wire type）。 */
	skip(wire: number): void {
		switch (wire) {
			case WT_VARINT:
				this.varint()
				return
			case WT_LEN:
				this.bytes()
				return
			case 1: { // i64
				if (this.pos + 8 > this.buf.length) throw new Error('protobuf: i64 截断')
				this.pos += 8
				return
			}
			case 5: { // i32
				if (this.pos + 4 > this.buf.length) throw new Error('protobuf: i32 截断')
				this.pos += 4
				return
			}
			default:
				throw new Error(`protobuf: 不支持的 wire type ${wire}`)
		}
	}
}

function num(v: bigint): number {
	return toSafeNumber(v, 'uint64 字段')
}

// ---------- 握手消息 ----------

/** Hello / HelloAck：{ bytes agg_stream_id = 1 } */
export function encodeHello(aggStreamId: Uint8Array): Uint8Array {
	return new ProtoWriter().bytes(1, aggStreamId).buf
}

export function decodeHello(body: Uint8Array): { aggStreamId: Uint8Array } {
	const r = new ProtoReader(body)
	let aggStreamId = new Uint8Array(0)
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_LEN) {
			aggStreamId = r.bytes().slice()
		} else {
			r.skip(f.wire)
		}
	}
	return { aggStreamId }
}

// ---------- PATH_ATTACH / PATH_DROP ----------

/** PathAttach { bytes agg_stream_id = 1; uint64 path_id = 2 } */
export function encodePathAttach(aggStreamId: Uint8Array, pathId: number | bigint): Uint8Array {
	return new ProtoWriter().bytes(1, aggStreamId).varint(2, pathId).buf
}

export function decodePathAttach(body: Uint8Array): { aggStreamId: Uint8Array; pathId: number } {
	const r = new ProtoReader(body)
	let aggStreamId = new Uint8Array(0)
	let pathId = 0
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_LEN) {
			aggStreamId = r.bytes().slice()
		} else if (f.field === 2 && f.wire === WT_VARINT) {
			pathId = num(r.varint())
		} else {
			r.skip(f.wire)
		}
	}
	return { aggStreamId, pathId }
}

/** PathDrop { uint64 path_id = 1 } */
export function encodePathDrop(pathId: number | bigint): Uint8Array {
	return new ProtoWriter().varint(1, pathId).buf
}

export function decodePathDrop(body: Uint8Array): { pathId: number } {
	const r = new ProtoReader(body)
	let pathId = 0
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_VARINT) {
			pathId = num(r.varint())
		} else {
			r.skip(f.wire)
		}
	}
	return { pathId }
}

// ---------- PATH_REQUEST / PATH_READY ----------

export interface PathRequestMsg {
	requestId: number
	relayPeer: Uint8Array
	relayAddrs: Uint8Array[]
	acAddr: Uint8Array
}

/** PathRequest { request_id=1; relay_peer=2; relay_addrs=3(repeated); ac_addr=4 } */
export function encodePathRequest(m: PathRequestMsg): Uint8Array {
	const w = new ProtoWriter().varint(1, m.requestId).bytes(2, m.relayPeer)
	for (const a of m.relayAddrs) {
		w.bytes(3, a)
	}
	return w.bytes(4, m.acAddr).buf
}

export function decodePathRequest(body: Uint8Array): PathRequestMsg {
	const r = new ProtoReader(body)
	const m: PathRequestMsg = { requestId: 0, relayPeer: new Uint8Array(0), relayAddrs: [], acAddr: new Uint8Array(0) }
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_VARINT) {
			m.requestId = num(r.varint())
		} else if (f.field === 2 && f.wire === WT_LEN) {
			m.relayPeer = r.bytes().slice()
		} else if (f.field === 3 && f.wire === WT_LEN) {
			m.relayAddrs.push(r.bytes().slice())
		} else if (f.field === 4 && f.wire === WT_LEN) {
			m.acAddr = r.bytes().slice()
		} else {
			r.skip(f.wire)
		}
	}
	return m
}

/** PathReady { request_id=1; error=2 } */
export function encodePathReady(requestId: number | bigint, error = ''): Uint8Array {
	return new ProtoWriter().varint(1, requestId).str(2, error).buf
}

export function decodePathReady(body: Uint8Array): { requestId: number; error: string } {
	const r = new ProtoReader(body)
	let requestId = 0
	let error = ''
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_VARINT) {
			requestId = num(r.varint())
		} else if (f.field === 2 && f.wire === WT_LEN) {
			error = new TextDecoder().decode(r.bytes())
		} else {
			r.skip(f.wire)
		}
	}
	return { requestId, error }
}

// ---------- PING / TELEMETRY ----------

/** Ping { send_ts=1; reply=2 } */
export function encodePing(sendTs: bigint | number, reply: boolean): Uint8Array {
	return new ProtoWriter().varint(1, sendTs).varint(2, reply).buf
}

export function decodePing(body: Uint8Array): { sendTs: bigint; reply: boolean } {
	const r = new ProtoReader(body)
	let sendTs = 0n
	let reply = false
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_VARINT) {
			sendTs = r.varint()
		} else if (f.field === 2 && f.wire === WT_VARINT) {
			reply = r.varint() !== 0n
		} else {
			r.skip(f.wire)
		}
	}
	return { sendTs, reply }
}

export interface PathRateMsg {
	pathId: number
	rateBps: number
}

function encodePathRate(m: PathRateMsg): Uint8Array {
	return new ProtoWriter().varint(1, m.pathId).varint(2, m.rateBps).buf
}

function decodePathRate(body: Uint8Array): PathRateMsg {
	const r = new ProtoReader(body)
	const m: PathRateMsg = { pathId: 0, rateBps: 0 }
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_VARINT) {
			m.pathId = num(r.varint())
		} else if (f.field === 2 && f.wire === WT_VARINT) {
			m.rateBps = num(r.varint())
		} else {
			r.skip(f.wire)
		}
	}
	return m
}

/** Telemetry { repeated PathRate rates = 1 } */
export function encodeTelemetry(rates: PathRateMsg[]): Uint8Array {
	const w = new ProtoWriter()
	for (const rate of rates) {
		w.bytes(1, encodePathRate(rate))
	}
	return w.buf
}

export function decodeTelemetry(body: Uint8Array): { rates: PathRateMsg[] } {
	const r = new ProtoReader(body)
	const rates: PathRateMsg[] = []
	for (let f = r.next(); f != null; f = r.next()) {
		if (f.field === 1 && f.wire === WT_LEN) {
			rates.push(decodePathRate(r.bytes()))
		} else {
			r.skip(f.wire)
		}
	}
	return { rates }
}
