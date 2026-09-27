/**
 * 数据面帧编解码，与 Go 端 frame.go 线格式严格一致：
 *
 *	DATA(1):        type | offset | send_ts_us | payload_len | payload
 *	ACK(2):         type | cum | ts_echo | ts_path | window | seq | n_ranges | (start,end)*n
 *	控制帧(3–10):   type | body_len | protobuf_body
 *
 * 全部头字段为 uvarint；序号空间为字节级绝对偏移（非帧号）。
 */

import { appendUvarint } from './varint.js'
import type { StreamReader } from './bytestream.js'

export enum FrameType {
	Data = 1,
	Ack = 2,
	PathAttach = 3,
	PathDrop = 4,
	PathRequest = 5,
	PathReady = 6,
	Ping = 7,
	Telemetry = 8,
	Fin = 9,
	Rst = 10,
}

/** 解码端防御上限（与 Go 端一致）。 */
export const MAX_FRAME_PAYLOAD = 64 << 10 // DATA 载荷上限 64KiB
export const MAX_ACK_RANGES = 32 // ACK SACK 区间数上限
export const MAX_CTRL_BODY = 64 << 10 // 控制帧 protobuf 体上限

/** byteRange 是半开字节区间 [start, end)，用于 ACK 的 SACK ranges。 */
export interface ByteRange {
	start: number
	end: number
}

/** DecodedFrame 是解码后的一帧，按 type 取用相应字段。 */
export interface DecodedFrame {
	// DATA
	off: number // 载荷流内字节偏移
	sendTs: bigint // 发送时间戳（µs，发送端本地基准）
	payload: Uint8Array

	// ACK
	cum: number // 累积确认偏移
	tsEcho: bigint // 触发本 ACK 的 DATA 帧时间戳回显
	tsPath: number // 回显 DATA 的到达路径 path_id+1（0=无有效回显）
	window: number // 接收窗口通告（重排缓冲剩余量）
	seq: number // 发送端单调递增 ACK 序号（丢弃跨路径超车的旧 ACK）
	ranges: ByteRange[] // 乱序已收区间（SACK）

	// 控制帧原始 protobuf 体（由上层按帧类型解析）
	body: Uint8Array
}

/** 编码一帧 DATA。 */
export function encodeDataFrame(off: number | bigint, sendTs: number | bigint, payload: Uint8Array): Uint8Array {
	let out = appendUvarint(new Uint8Array(0), FrameType.Data)
	out = appendUvarint(out, off)
	out = appendUvarint(out, sendTs)
	out = appendUvarint(out, payload.length)
	const buf = new Uint8Array(out.length + payload.length)
	buf.set(out, 0)
	buf.set(payload, out.length)
	return buf
}

/** 编码一帧 ACK；ranges 会被截断到 MAX_ACK_RANGES。 */
export function encodeAckFrame(
	cum: number | bigint,
	tsEcho: number | bigint,
	tsPath: number | bigint,
	window: number | bigint,
	seq: number | bigint,
	ranges: ByteRange[],
): Uint8Array {
	const rs = ranges.length > MAX_ACK_RANGES ? ranges.slice(0, MAX_ACK_RANGES) : ranges
	let out = appendUvarint(new Uint8Array(0), FrameType.Ack)
	out = appendUvarint(out, cum)
	out = appendUvarint(out, tsEcho)
	out = appendUvarint(out, tsPath)
	out = appendUvarint(out, window)
	out = appendUvarint(out, seq)
	out = appendUvarint(out, rs.length)
	for (const r of rs) {
		out = appendUvarint(out, r.start)
		out = appendUvarint(out, r.end)
	}
	return out
}

/** 编码一帧控制消息（FIN/RST 的 body 为空）。 */
export function encodeCtrlFrame(ft: FrameType, body?: Uint8Array): Uint8Array {
	const b = body ?? new Uint8Array(0)
	let out = appendUvarint(new Uint8Array(0), ft)
	out = appendUvarint(out, b.length)
	const buf = new Uint8Array(out.length + b.length)
	buf.set(out, 0)
	buf.set(b, out.length)
	return buf
}

function blankFrame(): DecodedFrame {
	return {
		off: 0,
		sendTs: 0n,
		payload: new Uint8Array(0),
		cum: 0,
		tsEcho: 0n,
		tsPath: 0,
		window: 0,
		seq: 0,
		ranges: [],
		body: new Uint8Array(0),
	}
}

function uvarintOrThrow(v: bigint | null, what: string): bigint {
	if (v == null) {
		throw new Error(`读${what}失败: 非预期 EOF`)
	}
	return v
}

function num(v: bigint | null, what: string): number {
	const b = uvarintOrThrow(v, what)
	if (b > BigInt(Number.MAX_SAFE_INTEGER)) {
		throw new Error(`${what} 超出安全整数范围: ${b}`)
	}
	return Number(b)
}

/**
 * FrameDecoder 从路径字节流逐帧解码；流在帧边界结束返回 null（EOF），
 * 格式违例/中途断流抛 Error。
 *
 * 同一个 StreamReader 可在握手阶段读出控制帧后继续喂数据面帧——
 * 缓冲属于 StreamReader，协议层切换不丢预读字节。
 */
export class FrameDecoder {
	constructor(private readonly r: StreamReader) {}

	/** 读下一帧；干净 EOF 返回 null。 */
	async next(): Promise<{ type: FrameType; frame: DecodedFrame } | null> {
		const f = blankFrame()
		const t = await this.r.readUvarint()
		if (t == null) {
			return null // 帧边界上的 EOF
		}
		const ft = Number(t) as FrameType
		switch (ft) {
			case FrameType.Data: {
				f.off = num(await this.r.readUvarint(), 'DATA.offset')
				f.sendTs = uvarintOrThrow(await this.r.readUvarint(), 'DATA.send_ts')
				const n = num(await this.r.readUvarint(), 'DATA.payload_len')
				if (n > MAX_FRAME_PAYLOAD) {
					throw new Error(`DATA 载荷 ${n} 超过上限 ${MAX_FRAME_PAYLOAD}`)
				}
				f.payload = await this.r.readExact(n)
				return { type: ft, frame: f }
			}
			case FrameType.Ack: {
				f.cum = num(await this.r.readUvarint(), 'ACK.cum')
				f.tsEcho = uvarintOrThrow(await this.r.readUvarint(), 'ACK.ts_echo')
				f.tsPath = num(await this.r.readUvarint(), 'ACK.ts_path')
				f.window = num(await this.r.readUvarint(), 'ACK.window')
				f.seq = num(await this.r.readUvarint(), 'ACK.seq')
				const nr = num(await this.r.readUvarint(), 'ACK.n_ranges')
				if (nr > MAX_ACK_RANGES) {
					throw new Error(`ACK ranges 数 ${nr} 超过上限 ${MAX_ACK_RANGES}`)
				}
				if (nr > 0) {
					f.ranges = new Array(nr)
					for (let i = 0; i < nr; i++) {
						const start = num(await this.r.readUvarint(), 'ACK.range.start')
						const end = num(await this.r.readUvarint(), 'ACK.range.end')
						if (end <= start) {
							throw new Error(`ACK range 非法: [${start},${end})`)
						}
						f.ranges[i] = { start, end }
					}
				}
				return { type: ft, frame: f }
			}
			case FrameType.PathAttach:
			case FrameType.PathDrop:
			case FrameType.PathRequest:
			case FrameType.PathReady:
			case FrameType.Ping:
			case FrameType.Telemetry:
			case FrameType.Fin:
			case FrameType.Rst: {
				const n = num(await this.r.readUvarint(), '控制帧.body_len')
				if (n > MAX_CTRL_BODY) {
					throw new Error(`控制帧体 ${n} 超过上限 ${MAX_CTRL_BODY}`)
				}
				if (n > 0) {
					f.body = await this.r.readExact(n)
				}
				return { type: ft, frame: f }
			}
			default:
				throw new Error(`未知帧类型 ${t}`)
		}
	}
}
