/**
 * 聚合流数据面测试（stream_test.go 的移植）：以 MemoryByteStream 对作为
 * 底层路径（等价 net.Pipe），覆盖收发/FIN/RST/窗口背压/乱序注入/ACK 字段/
 * 路径增删/失联重发。
 */
import { describe, expect, it } from 'vitest'
import { AggregatedStream, AGG_STREAM_ID_LEN } from '../src/stream.js'
import { MemoryByteStream } from '../src/memory-stream.js'
import { FrameDecoder, FrameType, encodeCtrlFrame, encodeDataFrame, type DecodedFrame } from '../src/frame.js'
import { StreamReader, type ByteStream } from '../src/bytestream.js'
import { encodePathAttach, decodePathAttach } from '../src/proto.js'
import { Path } from '../src/path.js'

const ID = new Uint8Array(AGG_STREAM_ID_LEN).fill(1)

function testCfg(bufCap: number) {
	return { minBuf: bufCap, maxBuf: bufCap, sendBufCap: 2 * bufCap, telemetryIntervalMs: 20 }
}

/** pipeStream 建一对挂在内存管道两端的聚合流。 */
function pipeStream(cfg = testCfg(16 << 10)): { a: AggregatedStream; b: AggregatedStream } {
	const [c1, c2] = MemoryByteStream.pair()
	const a = new AggregatedStream({ id: ID, firstPath: c1, initiator: true, ...cfg })
	const b = new AggregatedStream({ id: ID, firstPath: c2, initiator: false, ...cfg })
	a.start()
	b.start()
	return { a, b }
}

/** rawPipe：一端挂聚合流，另一端返回裸 ByteStream 供测试直接读写帧。 */
function rawPipe(cfg = testCfg(16 << 10)): { s: AggregatedStream; wire: ByteStream; frames: FrameDecoder } {
	const [c1, c2] = MemoryByteStream.pair()
	const s = new AggregatedStream({ id: ID, firstPath: c1, initiator: true, ...cfg })
	s.start()
	return { s, wire: c2, frames: new FrameDecoder(new StreamReader(c2)) }
}

async function recvFrame(fr: FrameDecoder): Promise<{ type: FrameType; frame: DecodedFrame }> {
	const r = await fr.next()
	if (r === null) throw new Error('帧流意外结束')
	return r
}

/** readN 从聚合流读足 n 字节（直到 EOF 才允许短读）。 */
async function readN(s: AggregatedStream, n: number): Promise<Uint8Array> {
	const parts: Uint8Array[] = []
	let got = 0
	while (got < n) {
		const c = await s.read()
		if (c === null) break
		parts.push(c)
		got += c.length
	}
	const out = new Uint8Array(got)
	let off = 0
	for (const p of parts) {
		out.set(p, off)
		off += p.length
	}
	return out
}

/** sleep 辅助。 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

describe('AggregatedStream 数据面（内存管道）', () => {
	it('echo：写出量大于接收窗口，走窗口背压路径仍数据一致', async () => {
		const cap = 16 << 10
		const { a, b } = pipeStream(testCfg(cap))
		const want = new Uint8Array(104 * 1024).map((_, i) => i & 0xff) // ~104KiB > 窗口 16KiB
		// b 侧 echo（对端 close 后迭代以「已关闭」收尾，吞掉）
		const echoDone = (async () => {
			try {
				for await (const c of b) {
					await b.write(c)
				}
			} catch {
				/* 对端关闭终止迭代 */
			}
		})()
		void a.write(want)
		const got = await readN(a, want.length)
		expect(got).toEqual(want)
		await a.close()
		await b.close()
		await echoDone
	})

	it('乱序注入：Read 输出保序字节流，cum 推进到 40', async () => {
		const { s, wire, frames } = rawPipe()
		const ann = await recvFrame(frames) // 初始窗口通告
		expect(ann.type).toBe(FrameType.Ack)
		expect(ann.frame.window).toBe(16 << 10)

		// 逆序注入四段 [30,40) [20,30) [10,20) [0,10)
		const payload = new TextEncoder().encode('0123456789abcdefghij0123456789ABCDEFGHIJ')
		for (let off = 30; off >= 0; off -= 10) {
			await wire.write(encodeDataFrame(off, 1, payload.subarray(off, off + 10)))
		}
		const got = await readN(s, payload.length)
		expect(got).toEqual(payload)

		// 排干 ACK：应见到一条 cum=40
		for (let i = 0; i < 20; i++) {
			const f = await recvFrame(frames)
			if (f.type === FrameType.Ack && f.frame.cum === payload.length) {
				return
			}
		}
		throw new Error('未见到 cum=40 的 ACK')
	})

	it('ACK 字段：窗口通告 + ts_echo 回显 + SACK ranges', async () => {
		const { s, wire, frames } = rawPipe()
		const cap = 16 << 10
		// 初始通告：window=满容量 ts_echo=0
		const ann = await recvFrame(frames)
		expect(ann.frame.window).toBe(cap)
		expect(ann.frame.cum).toBe(0)
		expect(ann.frame.tsEcho).toBe(0n)

		// 对端发一帧带 ts=7777 的 DATA
		const data = new TextEncoder().encode('hello-ack')
		await wire.write(encodeDataFrame(0, 7777, data))
		let ack = (await recvFrame(frames)).frame
		expect(ack.tsEcho).toBe(7777n)
		expect(ack.cum).toBe(data.length)
		expect(ack.window).toBe(cap - data.length)
		expect(ack.ranges).toEqual([])

		// 乱序注入 [100,110)：cum 不动，ranges 报告洞后段
		await wire.write(encodeDataFrame(100, 8888, new Uint8Array(10)))
		ack = (await recvFrame(frames)).frame
		expect(ack.cum).toBe(data.length)
		expect(ack.ranges).toEqual([{ start: 100, end: 110 }])
		expect(ack.tsEcho).toBe(8888n)
		await s.close()
	})

	it('窗口背压：对端不读时 write 挂起，读走后推进', async () => {
		const cap = 8 << 10
		const { a, b } = pipeStream({ minBuf: cap, maxBuf: cap, sendBufCap: 2 * cap, telemetryIntervalMs: 20 })
		const total = 6 * cap
		let done = false
		const w = a.write(new Uint8Array(total).fill(0x5a)).then(() => { done = true })
		await sleep(100)
		expect(done).toBe(false) // 窗口满 + 发送缓冲满 → write 必须停住
		const got = await readN(b, total)
		expect(got.length).toBe(total)
		await w
		expect(done).toBe(true)
		await a.close()
		await b.close()
	})

	it('对端 close → 读尽残余数据后得到 null（EOF）', async () => {
		const { a, b } = pipeStream()
		const want = new TextEncoder().encode('bye')
		void (async () => {
			await a.write(want)
			await sleep(20) // 让数据先落地再关
			await a.close()
		})()
		const got = await readN(b, want.length)
		expect(got).toEqual(want)
		expect(await b.read()).toBeNull()
	})

	it('RST 帧：流被对端重置后读写报错', async () => {
		const { s, wire, frames } = rawPipe()
		await recvFrame(frames) // 吃掉初始窗口通告
		await wire.write(encodeCtrlFrame(FrameType.Rst))
		await expect(s.read()).rejects.toThrow(/重置/)
		await expect(s.write(new Uint8Array(1))).rejects.toThrow()
	})

	it('path 增删：attachPath/RemovePath 与 path_id 命名空间', async () => {
		const { a, b } = pipeStream()
		expect(a.paths().length).toBe(1)
		expect(a.paths()[0]!.id).toBe(0)

		// 模拟发起方 addPath 的完整流程：新管道 + PATH_ATTACH 绑定握手
		const [x1, x2] = MemoryByteStream.pair()
		const pathId = 2 // 发起方偶数命名空间
		await x1.write(encodeCtrlFrame(FrameType.PathAttach, encodePathAttach(ID, pathId)))
		// 对端侧：读首帧 → 校验 → 回显 → 挂接
		const frB = new FrameDecoder(new StreamReader(x2))
		const att = await frB.next()
		expect(att!.type).toBe(FrameType.PathAttach)
		const m = decodePathAttach(att!.frame.body)
		expect(m.pathId).toBe(pathId)
		expect(b.reserveRemotePathID(pathId)).toBe(true)
		await x2.write(encodeCtrlFrame(FrameType.PathAttach, att!.frame.body))
		b.attachPath(new Path({ id: pathId, conn: x2, dialed: false }), frB)
		// 发起方侧：读回显 → 挂接
		const frA = new FrameDecoder(new StreamReader(x1))
		const echo = await frA.next()
		expect(echo!.type).toBe(FrameType.PathAttach)
		a.attachPath(new Path({ id: pathId, conn: x1, dialed: true }), frA)

		expect(a.paths().length).toBe(2)
		expect(b.paths().length).toBe(2)

		// 双路径下数据照常互通
		const want = new Uint8Array(4096).map((_, i) => i & 0xff)
		void a.write(want)
		expect(await readN(b, want.length)).toEqual(want)

		// 摘除：最后一条拒绝
		b.removePath(pathId)
		expect(b.paths().length).toBe(1)
		expect(() => b.removePath(0)).toThrow(/最后一条/)
		await a.close()
		await b.close()
	})

	it('路径失联：在途未确认数据自动重发到存活路径', async () => {
		const cap = 8 << 10
		const { a, b } = pipeStream(testCfg(cap))
		// 给 a 再加一条路径（会断的那条），先让数据落在它上面：
		// 这里反过来——先写一大块占满首路径窗口，再加第二条路径把余量发出去
		const [x1, x2] = MemoryByteStream.pair()
		const pathId = 2
		await x1.write(encodeCtrlFrame(FrameType.PathAttach, encodePathAttach(ID, pathId)))
		const frB = new FrameDecoder(new StreamReader(x2))
		const att = await frB.next()
		b.reserveRemotePathID(pathId)
		await x2.write(encodeCtrlFrame(FrameType.PathAttach, att!.frame.body))
		b.attachPath(new Path({ id: pathId, conn: x2, dialed: false }), frB)
		const frA = new FrameDecoder(new StreamReader(x1))
		await frA.next()
		a.attachPath(new Path({ id: pathId, conn: x1, dialed: true }), frA)

		// 写 > 单窗口的数据 → 两条路径都会参与发送
		const want = new Uint8Array(24 * 1024).map((_, i) => i & 0xff)
		void a.write(want)
		await sleep(50)
		// 杀掉发起方侧 path 2 的底层连接：未确认段应回池重发
		x1.abort(new Error('模拟断链'))
		const got = await readN(b, want.length)
		expect(got).toEqual(want)
		await a.close()
		await b.close()
	})

	it('异步迭代与 stats/paths 快照', async () => {
		const { a, b } = pipeStream()
		const want = new Uint8Array(3000).map((_, i) => i & 0x7f)
		void a.write(want)
		const parts: Uint8Array[] = []
		for await (const c of b) {
			parts.push(c)
			if (parts.reduce((n, p) => n + p.length, 0) >= want.length) break
		}
		const got = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
		let off = 0
		for (const p of parts) {
			got.set(p, off)
			off += p.length
		}
		expect(got).toEqual(want)
		// 等对端的 ACK 到达并完成 cum 推进（异步，需让出事件循环）
		for (let i = 0; i < 100 && a.stats().ackedBytes === 0; i++) {
			await sleep(10)
		}
		const st = a.stats()
		expect(st.paths.length).toBe(1)
		expect(st.sentBytes).toBeGreaterThan(0)
		expect(st.ackedBytes).toBeGreaterThan(0)
		await a.close()
		await b.close()
	})
})
