/**
 * 端到端集成测试：两个真实 js-libp2p 节点（TCP loopback + Noise + Yamux），
 * 验证 NetaccClient 的 openStream/accept/handle/addPath 全流程。
 */
import { describe, expect, it } from 'vitest'
import { createLibp2p, type Libp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { NetaccClient } from '../src/client.js'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function mkNode(): Promise<Libp2p> {
	return createLibp2p({
		addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
		transports: [tcp()],
		connectionEncrypters: [noise()],
		streamMuxers: [yamux()],
	})
}

async function readN(s: { read(): Promise<Uint8Array | null> }, n: number): Promise<Uint8Array> {
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

describe('NetaccClient（真实 js-libp2p TCP 节点）', () => {
	it('openStream + accept：握手 → 互通 → echo', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		try {
			const ca = new NetaccClient(na)
			const cb = new NetaccClient(nb)

			// 服务端：accept 一条流并 echo
			const serverDone = (async () => {
				const s = await cb.accept()
				try {
					for await (const c of s) {
						await s.write(c)
					}
				} catch {
					/* 对端关闭终止 */
				}
				return s
			})()

			const st = await ca.openStream(nb.getMultiaddrs()[0]!)
			expect(st.id.length).toBe(16)
			expect(st.paths().length).toBe(1)
			expect(st.paths()[0]!.transport).toBe('tcp')

			const want = new Uint8Array(70 * 1024).map((_, i) => i & 0xff)
			void st.write(want)
			const got = await readN(st, want.length)
			expect(got).toEqual(want)

			await st.close() // 先关：服务端 for-await 才能收 EOF 收尾
			const sst = await serverDone
			expect(sst.stats().recvCum).toBeGreaterThan(0)
			await ca.close()
			await cb.close()
		} finally {
			await na.stop()
			await nb.stop()
		}
	}, 30000)

	it('addPath：对同一节点拨出第二条 TCP 路径并摘除', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		try {
			const ca = new NetaccClient(na)
			const cb = new NetaccClient(nb)
			void cb // 需要其 /netacc/path 处理器在线
			const serverDone = (async () => {
				const s = await cb.accept()
				for await (const c of s) {
					await s.write(c)
				}
			})()
			const st = await ca.openStream(nb.getMultiaddrs()[0]!)
			const pathId = await st.addPath(nb.getMultiaddrs()[0]!)
			expect(pathId).toBe(2) // 发起方偶数命名空间
			for (let i = 0; i < 50 && st.paths().length < 2; i++) {
				await sleep(20)
			}
			expect(st.paths().length).toBe(2)
			// 数据依旧互通
			const want = new Uint8Array(32 * 1024).map((_, i) => i & 0xff)
			void st.write(want)
			expect(await readN(st, want.length)).toEqual(want)
			st.removePath(pathId)
			expect(st.paths().length).toBe(1)
			expect(() => st.removePath(0)).toThrow(/最后一条/)
			await st.close()
			await serverDone.catch(() => {})
			await ca.close()
			await cb.close()
		} finally {
			await na.stop()
			await nb.stop()
		}
	}, 30000)

	it('鉴权：authToken 匹配放行，错/缺 token 被拒', async () => {
		const token = new TextEncoder().encode('s3cr3t-token-0123456')
		const na = await mkNode()
		const nb = await mkNode()
		const nc = await mkNode()
		const nd = await mkNode()
		try {
			const serverAddr = nb.getMultiaddrs()[0]!
			const cb = new NetaccClient(nb, { authToken: token })
			// accept 循环持续消费入向握手（被拒的不会堵队列）；
			// 接受的不立刻关——立即 close 的 RST 会抢在 HelloAck
			// 到达前发回，让对端握手读不到应答
			const accepted: { close(): Promise<unknown> }[] = []
			const serverDone = (async () => {
				for (;;) {
					accepted.push(await cb.accept())
				}
			})()

			const ca = new NetaccClient(na, { authToken: token })
			const st = await ca.openStream(serverAddr)
			expect(st.id.length).toBe(16)
			await st.close()
			for (const s of accepted) {
				await s.close().catch(() => {})
			}

			const cc = new NetaccClient(nc, { authToken: new TextEncoder().encode('nope') })
			await expect(cc.openStream(serverAddr)).rejects.toThrow(/unauthorized/)

			const cd = new NetaccClient(nd)
			await expect(cd.openStream(serverAddr)).rejects.toThrow(/unauthorized/)

			await ca.close()
			await cb.close()
			await cc.close()
			await cd.close()
			await serverDone.catch(() => {})
		} finally {
			await na.stop()
			await nb.stop()
			await nc.stop()
			await nd.stop()
		}
	}, 30000)

	it('鉴权：authHandler 自定义校验，拒绝原因透传对端', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		const nc = await mkNode()
		try {
			const cb = new NetaccClient(nb, {
				authHandler: (_peer, auth) =>
					new TextDecoder().decode(auth) === 'let-me-in' ? true : 'banned',
			})
			const accepted: { close(): Promise<unknown> }[] = []
			const serverDone = (async () => {
				for (;;) {
					accepted.push(await cb.accept())
				}
			})()
			const serverAddr = nb.getMultiaddrs()[0]!

			const ca = new NetaccClient(na, { authToken: new TextEncoder().encode('let-me-in') })
			const st = await ca.openStream(serverAddr)
			await st.close()
			for (const s of accepted) {
				await s.close().catch(() => {})
			}

			const cc = new NetaccClient(nc, { authToken: new TextEncoder().encode('knock') })
			await expect(cc.openStream(serverAddr)).rejects.toThrow(/banned/)

			await ca.close()
			await cb.close()
			await cc.close()
			await serverDone.catch(() => {})
		} finally {
			await na.stop()
			await nb.stop()
			await nc.stop()
		}
	}, 30000)

	it('鉴权：异步 handler 超时后 onStream 继续接收，迟到结果不建流', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		const ca = new NetaccClient(na, { handshakeTimeoutMs: 2000 })
		let finishAuth!: (v: boolean) => void
		const pendingAuth = new Promise<boolean>(resolve => { finishAuth = resolve })
		const cb = new NetaccClient(nb, {
			handshakeTimeoutMs: 300,
			authHandler: (_peer, auth) => new TextDecoder().decode(auth) === 'hang' ? pendingAuth : true,
		})
		const accepted: Awaited<ReturnType<NetaccClient['accept']>>[] = []
		const incoming = new Promise<Awaited<ReturnType<NetaccClient['accept']>>>(resolve => {
			cb.onStream(st => {
				accepted.push(st)
				resolve(st)
			})
		})
		try {
			const addr = nb.getMultiaddrs()[0]!
			await expect(ca.openStream(addr, { authToken: new TextEncoder().encode('hang') })).rejects.toThrow()
			const st = await ca.openStream(addr)
			const serverStream = await incoming
			await st.write(new TextEncoder().encode('ok'))
			expect(new TextDecoder().decode(await readN(serverStream, 2))).toBe('ok')
			finishAuth(true)
			await sleep(20)
			expect(accepted).toHaveLength(1)
			await st.close()
		} finally {
			finishAuth(false)
			for (const st of accepted) await st.close().catch(() => {})
			await ca.close()
			await cb.close()
			await na.stop()
			await nb.stop()
		}
	}, 15000)

	it('鉴权：取消 accept 打断 handler 等待，迟到 rejection 被消费', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		const ca = new NetaccClient(na, { handshakeTimeoutMs: 2000 })
		let enterAuth!: () => void
		const entered = new Promise<void>(resolve => { enterAuth = resolve })
		let rejectAuth!: (err: Error) => void
		const pendingAuth = new Promise<boolean>((_resolve, reject) => { rejectAuth = reject })
		const cb = new NetaccClient(nb, {
			handshakeTimeoutMs: 0,
			authHandler: () => { enterAuth(); return pendingAuth },
		})
		const controller = new AbortController()
		try {
			const outcome = cb.accept({ signal: controller.signal }).then(
				() => '意外接受',
				(err: Error) => err.message,
			)
			const opening = ca.openStream(nb.getMultiaddrs()[0]!).catch(() => {})
			await entered
			controller.abort(new Error('取消鉴权'))
			expect(await Promise.race([outcome, sleep(1000).then(() => '取消未生效')])).toBe('取消鉴权')
			rejectAuth(new Error('迟到的鉴权失败'))
			await opening
		} finally {
			controller.abort()
			rejectAuth(new Error('清理鉴权等待'))
			await ca.close()
			await cb.close()
			await na.stop()
			await nb.stop()
		}
	}, 15000)

	it('对端 RST/断开 → read 得到终态错误', async () => {
		const na = await mkNode()
		const nb = await mkNode()
		try {
			const ca = new NetaccClient(na)
			const cb = new NetaccClient(nb)
			// openStream 需对端 accept 完成握手——收掉但不做任何读写
			const serverDone = (async () => {
				const s = await cb.accept()
				await sleep(100)
				await nb.stop() // 杀掉底层连接 → 本侧路径死亡 → 流终态
				return s
			})()
			const st = await ca.openStream(nb.getMultiaddrs()[0]!)
			await serverDone // 等握手完成且 nb 已停
			await expect(readN(st, 1)).rejects.toThrow()
			await st.close()
			await ca.close()
			await cb.close()
		} finally {
			await na.stop()
		}
	}, 30000)
})
