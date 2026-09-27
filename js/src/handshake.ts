/**
 * 握手消息读写（与 Go 端 handshake.go / go-msgio varint 消息格式一致）：
 * 一条握手消息 = uvarint 长度前缀 + protobuf 体（Hello/HelloAck）。
 *
 * 读写都经过 StreamReader：握手阶段预读的字节留在缓冲里，握手完成后
 * 同一 reader 继续喂数据面帧解码器，协议层切换不丢字节。
 */

import type { ByteStream, StreamReader } from './bytestream.js'
import { appendUvarint } from './varint.js'
import { encodeHello, decodeHello } from './proto.js'
import { netaccErr } from './types.js'

/** agg_stream_id 的字节长度（128bit，规格书 §4.1）。 */
export const AGG_STREAM_ID_LEN = 16

/** 单条握手消息的最大长度（防御上限，与 Go 端 maxHandshakeMsg 一致）。 */
export const MAX_HANDSHAKE_MSG = 4 << 10

/** 写一条 varint 长度前缀消息。 */
export async function writeMsg(bs: ByteStream, body: Uint8Array): Promise<void> {
	const out = appendUvarint(new Uint8Array(0), body.length)
	const buf = new Uint8Array(out.length + body.length)
	buf.set(out, 0)
	buf.set(body, out.length)
	await bs.write(buf)
}

/** 读一条 varint 长度前缀消息；EOF 恰在消息边界返回 null。 */
export async function readMsg(r: StreamReader, max = MAX_HANDSHAKE_MSG): Promise<Uint8Array | null> {
	const len = await r.readUvarint()
	if (len === null) {
		return null
	}
	if (len > BigInt(max)) {
		throw netaccErr('handshake', `netacc: 握手消息长度 ${len} 超过上限 ${max}`)
	}
	return r.readExact(Number(len))
}

/**
 * handshakeInitiator 在握手流上执行发起方握手：
 * 生成 128bit 随机 agg_stream_id，发 Hello，收 HelloAck 并校验回显一致。
 * 握手完成后该流即升格为聚合流的第一条数据路径（规格书 §4.1）。
 */
export async function handshakeInitiator(bs: ByteStream, r: StreamReader): Promise<Uint8Array> {
	const id = new Uint8Array(AGG_STREAM_ID_LEN)
	globalThis.crypto.getRandomValues(id)
	await writeMsg(bs, encodeHello(id))
	const raw = await readMsg(r)
	if (raw === null) {
		throw netaccErr('handshake', 'netacc: 等待 HelloAck 失败: EOF')
	}
	const ack = decodeHello(raw) // HelloAck 与 Hello 线格式同构（bytes agg_stream_id=1）
	if (!bytesEqual(ack.aggStreamId, id)) {
		throw netaccErr('handshake', 'netacc: HelloAck 回显的 agg_stream_id 与本地生成不一致')
	}
	return id
}

/**
 * handshakeResponderRead 是接收方握手前半：收 Hello
 * （校验 agg_stream_id 恰为 16 字节），返回协商出的聚合流标识。
 */
export async function handshakeResponderRead(r: StreamReader): Promise<Uint8Array> {
	const raw = await readMsg(r)
	if (raw === null) {
		throw netaccErr('handshake', 'netacc: 等待 Hello 失败: EOF')
	}
	const hello = decodeHello(raw)
	if (hello.aggStreamId.length !== AGG_STREAM_ID_LEN) {
		throw netaccErr('handshake', `netacc: agg_stream_id 应为 ${AGG_STREAM_ID_LEN} 字节，实收 ${hello.aggStreamId.length}`)
	}
	return hello.aggStreamId.slice()
}

/**
 * handshakeResponderAck 是接收方握手后半：回写 HelloAck 表示接受。
 * 应答发出后，握手流升格为该聚合流的第一条数据路径。
 */
export async function handshakeResponderAck(bs: ByteStream, id: Uint8Array): Promise<void> {
	await writeMsg(bs, encodeHello(id)) // HelloAck{agg_stream_id=1} 编码相同
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) {
		return false
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false
		}
	}
	return true
}
